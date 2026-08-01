'use strict';

// Drives the dashboard from the control plane's HTTP API and SSE stream.
// Column counts for Queued, Claimed, OCR and Stored come straight from the
// stats snapshot every tick, so they never drift. The Captured column has
// no matching status bucket (only the cdc lane emits a capture event, and
// it is a transient point between source and claim), so it is tracked
// live from the pipeline stream instead and reset whenever a file moves on.

(function () {
  const MAX_LIVE_CHIPS = 120;
  const CHIP_REMOVAL_DELAY_MS = 2000;
  const FLAG_CLEAR_DELAY_MS = 2500;
  const THROUGHPUT_WINDOW_MS = 10000;
  const TRACE_POLL_INTERVAL_MS = 1000;
  const TRACE_POLL_TIMEOUT_MS = 30000;

  const stageToColumn = {
    CDC_CAPTURED: 'captured',
    CLAIMED: 'claimed',
    OCR_DONE: 'ocr',
    STORED: 'stored',
  };

  const chipRegistry = new Map(); // sourceId (string) -> { el, column }
  let capturedTally = 0;
  let suppressedThisWindow = 0;
  let serverDroppedThisWindow = 0;

  const storedTimestamps = { cdc: [], backfill: [] };

  let activeTrace = null;
  let vendorRequestInFlight = false;

  function setPillState(pillEl, valueEl, state, text) {
    pillEl.classList.remove('state-calm', 'state-amber', 'state-red');
    if (state) {
      pillEl.classList.add('state-' + state);
    }
    valueEl.textContent = text;
  }

  function updateBreakerIndicator(state) {
    const pill = document.getElementById('breaker-indicator');
    const value = document.getElementById('breaker-value');
    const isOpen = state === 'OPEN';
    setPillState(pill, value, isOpen ? 'red' : 'calm', state || '-');
  }

  function updateVendorIndicator(mode) {
    const pill = document.getElementById('vendor-indicator');
    const value = document.getElementById('vendor-value');
    let state = 'calm';
    if (mode === 'slow' || mode === 'rate_limited') {
      state = 'amber';
    } else if (mode === 'erroring' || mode === 'down') {
      state = 'red';
    } else if (mode !== 'healthy') {
      state = null;
    }
    setPillState(pill, value, state, mode || '-');
    if (!vendorRequestInFlight) {
      const select = document.getElementById('vendor-mode-select');
      if (mode && select.value !== mode) {
        select.value = mode;
      }
    }
  }

  function updateConnIndicator(state, text) {
    const pill = document.getElementById('conn-indicator');
    const value = document.getElementById('conn-value');
    setPillState(pill, value, state, text);
  }

  function formatDuration(ms) {
    if (!Number.isFinite(ms) || ms < 0) {
      return '0ms';
    }
    if (ms < 1000) {
      return Math.round(ms) + 'ms';
    }
    const seconds = ms / 1000;
    if (seconds < 60) {
      return seconds.toFixed(1) + 's';
    }
    const minutes = Math.floor(seconds / 60);
    const remSeconds = Math.round(seconds - minutes * 60);
    return minutes + 'm ' + remSeconds + 's';
  }

  function formatSeconds(totalSeconds) {
    const seconds = Math.max(0, Math.round(totalSeconds || 0));
    if (seconds < 60) {
      return seconds + 's';
    }
    const minutes = Math.floor(seconds / 60);
    const remSeconds = seconds % 60;
    if (minutes < 60) {
      return minutes + 'm ' + remSeconds + 's';
    }
    const hours = Math.floor(minutes / 60);
    const remMinutes = minutes % 60;
    return hours + 'h ' + remMinutes + 'm';
  }

  // --- stats-driven state ---------------------------------------------------

  function updateGauge(lagSeconds, alertSeconds, breachSeconds) {
    const scaleMax = Math.max(breachSeconds * 1.15, lagSeconds * 1.05, 1);
    const fillPct = Math.min(100, (lagSeconds / scaleMax) * 100);
    const alertPct = Math.min(100, (alertSeconds / scaleMax) * 100);
    const breachPct = Math.min(100, (breachSeconds / scaleMax) * 100);

    let state = 'calm';
    if (lagSeconds >= breachSeconds) {
      state = 'red';
    } else if (lagSeconds >= alertSeconds) {
      state = 'amber';
    }

    const fillEl = document.getElementById('gauge-fill');
    fillEl.style.width = fillPct + '%';
    fillEl.style.backgroundColor = 'var(--' + state + ')';

    document.getElementById('gauge-marker-alert').style.left = alertPct + '%';
    document.getElementById('gauge-marker-breach').style.left = breachPct + '%';
    document.getElementById('gauge-alert-label').textContent = 'alert (' + formatSeconds(alertSeconds) + ')';
    document.getElementById('gauge-breach-label').textContent = 'breach (' + formatSeconds(breachSeconds) + ')';
    document.getElementById('sla-lag-value').textContent = formatSeconds(lagSeconds) + ' outstanding';
  }

  function applyStats(stats) {
    updateBreakerIndicator(stats.breakerState);
    updateVendorIndicator(stats.vendorMode);

    const byStatus = stats.byStatus || {};
    const totals = stats.totals || {};

    setColumnCount('source', totals.source || 0);
    setColumnCount('captured', capturedTally);
    setColumnCount('queued', byStatus.PENDING || 0);
    setColumnCount('claimed', byStatus.IN_FLIGHT || 0);
    setColumnCount('ocr', byStatus.OCR_DONE || 0);
    setColumnCount('stored', byStatus.DONE || 0);

    updateGauge(stats.slaLagSeconds || 0, stats.slaAlertSeconds, stats.slaTargetSeconds);

    const suppressedLabel = document.getElementById('pipeline-suppressed');
    let text = suppressedThisWindow + ' events not drawn (rolling)';
    if (serverDroppedThisWindow > 0) {
      text += ', ' + serverDroppedThisWindow + ' dropped by the server this tick';
    }
    suppressedLabel.textContent = text;
    suppressedThisWindow = 0;
    serverDroppedThisWindow = 0;
  }

  function setColumnCount(column, value) {
    const el = document.getElementById('count-' + column);
    if (el) {
      el.textContent = String(value);
    }
  }

  function updateCapturedCount() {
    setColumnCount('captured', capturedTally);
  }

  // --- chip rendering --------------------------------------------------------

  function moveChip(sourceId, lane, column) {
    let entry = chipRegistry.get(sourceId);
    if (!entry) {
      if (chipRegistry.size >= MAX_LIVE_CHIPS) {
        suppressedThisWindow += 1;
        return;
      }
      const el = document.createElement('div');
      el.className = 'chip';
      entry = { el, column: null };
      chipRegistry.set(sourceId, entry);
    }

    if (entry.column === 'captured' && column !== 'captured') {
      capturedTally = Math.max(0, capturedTally - 1);
      updateCapturedCount();
    }
    if (column === 'captured' && entry.column !== 'captured') {
      capturedTally += 1;
      updateCapturedCount();
    }

    entry.el.classList.toggle('lane-backfill', lane === 'backfill');
    entry.column = column;

    const tray = document.getElementById('tray-' + column);
    if (tray && entry.el.parentNode !== tray) {
      tray.appendChild(entry.el);
    }
    entry.el.classList.remove('chip-enter');
    void entry.el.offsetWidth;
    entry.el.classList.add('chip-enter');
  }

  function flagChip(sourceId, flag) {
    const entry = chipRegistry.get(sourceId);
    if (!entry) {
      return;
    }
    entry.el.classList.add('flag-' + flag);
    setTimeout(function () {
      entry.el.classList.remove('flag-' + flag);
    }, FLAG_CLEAR_DELAY_MS);
  }

  function clearFlags(sourceId) {
    const entry = chipRegistry.get(sourceId);
    if (entry) {
      entry.el.classList.remove('flag-retry', 'flag-dlq');
    }
  }

  function scheduleChipRemoval(sourceId) {
    setTimeout(function () {
      const entry = chipRegistry.get(sourceId);
      if (entry && entry.column === 'stored') {
        entry.el.remove();
        chipRegistry.delete(sourceId);
      }
    }, CHIP_REMOVAL_DELAY_MS);
  }

  // --- throughput --------------------------------------------------------

  function recordThroughput(lane) {
    if (lane === 'cdc' || lane === 'backfill') {
      storedTimestamps[lane].push(Date.now());
    }
  }

  function computeThroughput() {
    const now = Date.now();
    ['cdc', 'backfill'].forEach(function (lane) {
      const list = storedTimestamps[lane];
      while (list.length > 0 && now - list[0] > THROUGHPUT_WINDOW_MS) {
        list.shift();
      }
      const rate = list.length / (THROUGHPUT_WINDOW_MS / 1000);
      document.getElementById('throughput-' + lane).textContent = rate.toFixed(1) + ' files/s';
    });
  }

  // --- pipeline events -----------------------------------------------------

  function handlePipelineEvent(evt) {
    const stage = evt.stage;
    const lane = evt.lane;
    const sourceId = evt.source_id === null || evt.source_id === undefined ? null : String(evt.source_id);

    if (stage === 'BREAKER_OPEN' || stage === 'BREAKER_CLOSED') {
      updateBreakerIndicator(stage === 'BREAKER_OPEN' ? 'OPEN' : 'CLOSED');
      return;
    }

    if (sourceId === null) {
      return;
    }

    const column = stageToColumn[stage];
    if (column) {
      moveChip(sourceId, lane, column);
      clearFlags(sourceId);
      if (stage === 'STORED') {
        recordThroughput(lane);
        scheduleChipRemoval(sourceId);
      }
    } else if (stage === 'RETRY') {
      flagChip(sourceId, 'retry');
    } else if (stage === 'DLQ') {
      flagChip(sourceId, 'dlq');
    }

    if (activeTrace && activeTrace.sourceId === sourceId) {
      appendTraceEvent({ id: evt.id, stage: stage, lane: lane, at: evt.created_at });
    }
  }

  // --- trace panel ---------------------------------------------------------

  function renderTrace() {
    const list = document.getElementById('trace-list');
    list.innerHTML = '';
    let prevAtMs = null;
    activeTrace.stages.forEach(function (stage) {
      const li = document.createElement('li');

      const stageSpan = document.createElement('span');
      stageSpan.className = 'trace-stage';
      stageSpan.textContent = stage.stage;

      const laneSpan = document.createElement('span');
      laneSpan.className = 'trace-lane';
      laneSpan.textContent = stage.lane || '';

      const deltaSpan = document.createElement('span');
      deltaSpan.className = 'trace-delta';
      const atMs = new Date(stage.at).getTime();
      deltaSpan.textContent = prevAtMs === null ? 'start' : '+' + formatDuration(atMs - prevAtMs);
      prevAtMs = atMs;

      li.appendChild(stageSpan);
      li.appendChild(laneSpan);
      li.appendChild(deltaSpan);
      list.appendChild(li);
    });

    const objectDiv = document.getElementById('trace-object');
    objectDiv.innerHTML = '';
    const storedStage = activeTrace.stages.find(function (s) {
      return s.stage === 'STORED';
    });
    if (storedStage) {
      const key = 'files/' + activeTrace.sourceId;
      const label = document.createElement('div');
      label.textContent = 'Object key: ' + key;
      const link = document.createElement('a');
      link.href = 'http://localhost:9001/browser/documents';
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
      link.textContent = 'Open in MinIO console';
      objectDiv.appendChild(label);
      objectDiv.appendChild(link);
    }
  }

  function appendTraceEvent(evt) {
    if (!activeTrace) {
      return;
    }
    const idKey = String(evt.id);
    if (activeTrace.seenIds.has(idKey)) {
      return;
    }
    activeTrace.seenIds.add(idKey);
    activeTrace.stages.push({ stage: evt.stage, lane: evt.lane, at: evt.at });
    activeTrace.stages.sort(function (a, b) {
      return new Date(a.at) - new Date(b.at);
    });
    renderTrace();
    if (evt.stage === 'STORED' && activeTrace.pollTimer) {
      clearInterval(activeTrace.pollTimer);
      activeTrace.pollTimer = null;
    }
  }

  async function fetchTraceSnapshot(sourceId) {
    try {
      const res = await fetch('/api/trace/' + sourceId);
      if (!res.ok) {
        return;
      }
      const body = await res.json();
      (body.events || []).forEach(function (evt) {
        appendTraceEvent({ id: evt.id, stage: evt.stage, lane: evt.lane, at: evt.at });
      });
    } catch (err) {
      // Transient network error, next poll retries.
    }
  }

  function startTrace(sourceId) {
    if (activeTrace && activeTrace.pollTimer) {
      clearInterval(activeTrace.pollTimer);
    }
    const idString = String(sourceId);
    activeTrace = { sourceId: idString, stages: [], seenIds: new Set(), pollTimer: null };
    document.getElementById('trace-list').innerHTML = '';
    document.getElementById('trace-object').innerHTML = '';
    document.getElementById('trace-subheading').textContent = 'source id ' + idString + ' (tracing)';

    fetchTraceSnapshot(idString);
    activeTrace.pollTimer = setInterval(function () {
      fetchTraceSnapshot(idString);
    }, TRACE_POLL_INTERVAL_MS);

    setTimeout(function () {
      if (activeTrace && activeTrace.sourceId === idString && activeTrace.pollTimer) {
        clearInterval(activeTrace.pollTimer);
        activeTrace.pollTimer = null;
      }
    }, TRACE_POLL_TIMEOUT_MS);
  }

  // --- reconciliation --------------------------------------------------------

  const RECONCILE_LIST_KEYS = [
    'checksumMismatches',
    'ocrMismatches',
    'missingObjects',
    'unreadableObjects',
    'missingDocuments',
    'orphanDocuments',
    'missingLedgerRows',
    'orphanLedgerRows',
  ];

  function formatListItems(list) {
    if (!Array.isArray(list) || list.length === 0) {
      return 'none';
    }
    const shown = list.slice(0, 50).map(function (item) {
      if (item && typeof item === 'object') {
        const id = item.id !== undefined ? item.id : JSON.stringify(item);
        const extra = item.error ? ' (' + item.error + ')' : '';
        return id + extra;
      }
      return String(item);
    });
    const suffix = list.length > 50 ? ', +' + (list.length - 50) + ' more' : '';
    return shown.join(', ') + suffix;
  }

  function renderReconcileResult(result) {
    const container = document.getElementById('reconcile-result');
    container.innerHTML = '';

    const banner = document.createElement('div');
    banner.className = 'reconcile-banner ' + (result.clean ? 'clean' : 'dirty');
    banner.textContent = result.clean ? 'clean' : 'not clean';
    container.appendChild(banner);

    const summary = document.createElement('div');
    summary.className = 'reconcile-summary';
    const summaryFields = [
      ['source rows', result.sourceCount],
      ['ledger rows', result.ledgerCount],
      ['document rows', result.documentCount],
      ['rows examined', result.rowsExamined],
      ['permanent failures', Array.isArray(result.permanentFailures) ? result.permanentFailures.length : 0],
    ];
    summaryFields.forEach(function (pair) {
      const span = document.createElement('span');
      const valueSpan = document.createElement('span');
      valueSpan.className = 'stat-value';
      valueSpan.textContent = String(pair[1]);
      span.appendChild(document.createTextNode(pair[0] + ': '));
      span.appendChild(valueSpan);
      summary.appendChild(span);
    });
    container.appendChild(summary);

    const grid = document.createElement('div');
    grid.className = 'reconcile-lists';
    RECONCILE_LIST_KEYS.forEach(function (key) {
      const list = result[key] || [];
      const card = document.createElement('div');
      card.className = 'reconcile-list-card' + (list.length > 0 ? ' has-items' : '');

      const heading = document.createElement('h3');
      const label = document.createElement('span');
      label.textContent = key;
      const count = document.createElement('span');
      count.className = 'list-count';
      count.textContent = String(list.length);
      heading.appendChild(label);
      heading.appendChild(count);
      card.appendChild(heading);

      const items = document.createElement('div');
      items.className = 'reconcile-list-items';
      items.textContent = formatListItems(list);
      card.appendChild(items);

      grid.appendChild(card);
    });
    container.appendChild(grid);
  }

  // --- controls --------------------------------------------------------------

  function wireControls() {
    const addFileBtn = document.getElementById('add-file-btn');
    const addFileHint = document.getElementById('add-file-hint');
    addFileBtn.addEventListener('click', async function () {
      addFileBtn.disabled = true;
      addFileHint.textContent = 'adding...';
      try {
        const res = await fetch('/api/files', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            filename: 'dashboard-' + Date.now() + '.txt',
            text: 'Added from the dashboard at ' + new Date().toISOString(),
          }),
        });
        const body = await res.json();
        if (!res.ok) {
          addFileHint.textContent = 'failed: ' + (body.message || res.status);
        } else {
          addFileHint.textContent = 'added source id ' + body.sourceId + ', tracing';
          startTrace(body.sourceId);
        }
      } catch (err) {
        addFileHint.textContent = 'request failed';
      } finally {
        addFileBtn.disabled = false;
      }
    });

    const vendorSelect = document.getElementById('vendor-mode-select');
    vendorSelect.addEventListener('change', async function () {
      vendorRequestInFlight = true;
      try {
        const res = await fetch('/api/vendor/mode', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ mode: vendorSelect.value }),
        });
        const body = await res.json();
        if (res.ok && body.mode) {
          updateVendorIndicator(body.mode);
        }
      } catch (err) {
        // The next stats tick will reassert the actual vendor mode.
      } finally {
        vendorRequestInFlight = false;
      }
    });

    const reconcileBtn = document.getElementById('reconcile-btn');
    const reconcileHint = document.getElementById('reconcile-hint');
    reconcileBtn.addEventListener('click', async function () {
      reconcileBtn.disabled = true;
      reconcileHint.textContent = 'running...';
      try {
        const res = await fetch('/api/reconcile', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({}),
        });
        const body = await res.json();
        if (!res.ok) {
          reconcileHint.textContent = 'failed: ' + (body.message || res.status);
        } else {
          reconcileHint.textContent = 'done';
          renderReconcileResult(body);
        }
      } catch (err) {
        reconcileHint.textContent = 'request failed';
      } finally {
        reconcileBtn.disabled = false;
      }
    });
  }

  // --- bootstrap ---------------------------------------------------------

  function connectStream() {
    const source = new EventSource('/api/stream');
    source.addEventListener('open', function () {
      updateConnIndicator('calm', 'connected');
    });
    source.addEventListener('error', function () {
      updateConnIndicator('red', 'reconnecting');
    });
    source.addEventListener('stats', function (e) {
      applyStats(JSON.parse(e.data));
    });
    source.addEventListener('pipeline', function (e) {
      const payload = JSON.parse(e.data);
      if (payload.droppedCount) {
        serverDroppedThisWindow += payload.droppedCount;
      }
      (payload.events || []).forEach(handlePipelineEvent);
    });
  }

  async function bootstrap() {
    wireControls();
    setInterval(computeThroughput, 500);
    try {
      const res = await fetch('/api/stats');
      if (res.ok) {
        applyStats(await res.json());
      }
    } catch (err) {
      // The stream connection below will populate state once it opens.
    }
    connectStream();
  }

  document.addEventListener('DOMContentLoaded', bootstrap);
})();
