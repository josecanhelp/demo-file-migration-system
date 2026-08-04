'use strict';

// Drives the dashboard from the control plane's HTTP API and SSE stream.
//
// Column counts for Waiting in line, In progress, Reading the text, Stored
// and the set-aside tray come straight from the stats snapshot every tick,
// so they never drift, and the paced chip animation below never touches
// them. The Picked up column has no matching status bucket (both lanes
// emit a capture event when they notice a file, but it is a transient
// point between source and claim rather than a status any row holds), so
// it is tracked locally from the pipeline stream instead, starting over at
// 0 on every page load; the interface says so next to the column.
//
// A file completes in well under a second in reality, so a chip is paced
// to sit in each column for at least PACED_DWELL_MS before advancing, and
// its transitions are queued so it steps through every column in order
// instead of teleporting. That pacing is purely visual: it never changes
// when a count on screen updates, and the real, unpaced duration for each
// finished file is shown in the Recently stored gallery.
//
// A row that reaches DLQ has its status set to FAILED_PERMANENT at that
// same moment, so a DLQ event is a safe trigger for moving that row's chip
// out of the in-flight trays and into the set-aside tray. That tray's own
// count still comes from stats (byStatus.FAILED_PERMANENT), never from
// counting chips or DLQ events, so it cannot say something different from
// what the row's current status actually is. FAILED_PERMANENT is not final
// on the live lane: a later real stage event for the same source id moves
// its chip straight back into an in-flight column. The tray itself stays
// hidden while that same count is zero and reappears the moment it is not,
// driven by the same stats value rather than by chip movement.

(function () {
  const MAX_LIVE_CHIPS = 120;
  const CHIP_REMOVAL_DELAY_MS = 2000;
  const FAILED_CHIP_REMOVAL_DELAY_MS = 6000;
  const FLAG_CLEAR_DELAY_MS = 2500;
  const THROUGHPUT_WINDOW_MS = 10000;
  const RECENT_POLL_INTERVAL_MS = 2000;
  const RECENT_LIMIT = 8;

  // Minimum time a chip visibly sits in one pipeline column before moving
  // to the next, so a run that finishes in milliseconds still reads as a
  // sequence of distinct steps instead of a teleport.
  const PACED_DWELL_MS = 350;

  const stageToColumn = {
    CDC_CAPTURED: 'captured',
    QUEUED: 'queued',
    CLAIMED: 'claimed',
    OCR_DONE: 'ocr',
    STORED: 'stored',
  };

  // The order a chip visibly travels through. Source is not included: it
  // is a row count only, chips never render there.
  const COLUMN_SEQUENCE = ['captured', 'queued', 'claimed', 'ocr', 'stored'];

  function columnIndex(column) {
    return COLUMN_SEQUENCE.indexOf(column);
  }

  const chipRegistry = new Map(); // sourceId (string) -> { el, column }
  const chipQueues = new Map(); // sourceId (string) -> { steps: [], running, currentIndex }
  let capturedTally = 0;
  let suppressedThisWindow = 0;
  let serverDroppedThisWindow = 0;

  const storedTimestamps = { cdc: [], backfill: [] };

  let bulkRequestInFlight = false;
  let vendorRequestInFlight = false;
  let restartRequestInFlight = false;
  let minioConsoleUrl = 'http://localhost:9001';
  let renderedRecentIds = new Set();

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

  function formatDuration(seconds) {
    if (!Number.isFinite(seconds) || seconds < 0) {
      return 'unknown';
    }
    if (seconds < 1) {
      return Math.round(seconds * 1000) + 'ms';
    }
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

  function formatBytes(bytes) {
    if (!Number.isFinite(bytes)) {
      return 'unknown size';
    }
    if (bytes < 1024) {
      return bytes + ' B';
    }
    const kb = bytes / 1024;
    if (kb < 1024) {
      return kb.toFixed(1) + ' KB';
    }
    const mb = kb / 1024;
    return mb.toFixed(1) + ' MB';
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
    const byLane = stats.byLane || {};

    setColumnCount('source', totals.source || 0);
    setColumnCount('captured', capturedTally);
    setColumnCount('queued', byStatus.PENDING || 0);
    setColumnCount('claimed', byStatus.IN_FLIGHT || 0);
    setColumnCount('ocr', byStatus.OCR_DONE || 0);
    setColumnCount('stored', byStatus.DONE || 0);
    const failedCount = byStatus.FAILED_PERMANENT || 0;
    setColumnCount('failed', failedCount);
    updateFailedTrayVisibility(failedCount);

    updateQueueDepth('cdc', byLane.cdc || 0);
    updateQueueDepth('backfill', byLane.backfill || 0);

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

  // The Set aside row is hidden while nothing is in it, since with only
  // Vendor: working and Vendor: offline exposed on the dashboard a vendor
  // failure is always classified transient and never lands a file here.
  // It reappears the moment the count is genuinely non-zero, for example a
  // file the vendor rejects outright as unreadable, and hides again once
  // that clears.
  function updateFailedTrayVisibility(count) {
    const row = document.querySelector('.failed-row');
    if (row) {
      row.hidden = count <= 0;
    }
  }

  // Rows not yet DONE for one lane: a lane that stops making progress
  // holds steady or climbs here, rather than the ever-growing total a
  // plain per-lane row count would show.
  function updateQueueDepth(lane, value) {
    const el = document.getElementById('queue-depth-' + lane);
    if (el) {
      el.textContent = String(value);
    }
  }

  function updateCapturedCount() {
    setColumnCount('captured', capturedTally);
  }

  // --- chip rendering --------------------------------------------------------
  //
  // moveChip is the low-level renderer: it places a chip's element in a
  // tray right now. It is only ever called either directly for an
  // exceptional path (the set-aside tray) or from runChipQueue below,
  // which is what makes a chip's movement paced rather than instant.

  function moveChip(sourceId, lane, column) {
    let entry = chipRegistry.get(sourceId);
    if (!entry) {
      if (chipRegistry.size >= MAX_LIVE_CHIPS) {
        suppressedThisWindow += 1;
        return null;
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
    return entry;
  }

  // --- paced chip queue --------------------------------------------------

  // Advances a chip one column at a time toward whatever target the server
  // last reported for it, waiting PACED_DWELL_MS between each step. Steps
  // already queued or already reached are never replayed.
  function enqueueChipSteps(sourceId, lane, targetColumn) {
    const targetIndex = columnIndex(targetColumn);
    if (targetIndex === -1) {
      return;
    }
    let q = chipQueues.get(sourceId);
    if (!q) {
      q = { steps: [], running: false, currentIndex: -1 };
      chipQueues.set(sourceId, q);
    }
    const lastQueuedIndex = q.steps.length > 0
      ? columnIndex(q.steps[q.steps.length - 1].column)
      : q.currentIndex;
    for (let i = lastQueuedIndex + 1; i <= targetIndex; i += 1) {
      q.steps.push({ column: COLUMN_SEQUENCE[i], lane: lane });
    }
    runChipQueue(sourceId);
  }

  function runChipQueue(sourceId) {
    const q = chipQueues.get(sourceId);
    if (!q || q.running) {
      return;
    }
    const step = q.steps.shift();
    if (!step) {
      return;
    }
    q.running = true;
    q.currentIndex = columnIndex(step.column);
    moveChip(sourceId, step.lane, step.column);

    if (step.column === 'stored') {
      recordThroughput(step.lane);
      scheduleChipRemoval(sourceId);
      pollRecent();
    }

    setTimeout(function () {
      q.running = false;
      runChipQueue(sourceId);
    }, PACED_DWELL_MS);
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
        chipQueues.delete(sourceId);
      }
    }, CHIP_REMOVAL_DELAY_MS);
  }

  // Moves a chip out of whichever in-flight tray it was sitting in and
  // into the set-aside tray immediately: this is an exceptional path, not
  // part of the paced left-to-right flow, so it bypasses the chip queue.
  // Any queued forward steps are dropped, since the row is no longer
  // progressing toward them; a later real stage event enqueues fresh steps
  // from wherever the row's progress actually left off.
  function moveToFailedTray(sourceId, lane) {
    const entry = chipRegistry.get(sourceId);
    if (!entry) {
      return;
    }
    const q = chipQueues.get(sourceId);
    if (q) {
      q.steps = [];
    }
    moveChip(sourceId, lane, 'failed');
    entry.el.classList.remove('flag-retry');
    entry.el.classList.add('flag-dlq');
    scheduleFailedChipRemoval(sourceId);
  }

  function scheduleFailedChipRemoval(sourceId) {
    setTimeout(function () {
      const entry = chipRegistry.get(sourceId);
      if (entry && entry.column === 'failed') {
        entry.el.remove();
        chipRegistry.delete(sourceId);
        chipQueues.delete(sourceId);
      }
    }, FAILED_CHIP_REMOVAL_DELAY_MS);
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
      clearFlags(sourceId);
      enqueueChipSteps(sourceId, lane, column);
    } else if (stage === 'RETRY') {
      flagChip(sourceId, 'retry');
    } else if (stage === 'DLQ') {
      moveToFailedTray(sourceId, lane);
    }
  }

  // --- recently stored gallery -----------------------------------------------

  function renderRecent(items) {
    const grid = document.getElementById('recent-grid');
    const nextIds = new Set(items.map(function (item) { return item.sourceId; }));

    if (items.length === 0) {
      grid.innerHTML = '<p class="recent-empty">Nothing stored yet.</p>';
      renderedRecentIds = nextIds;
      return;
    }

    grid.innerHTML = '';
    items.forEach(function (item) {
      const card = document.createElement('div');
      card.className = 'recent-card';
      if (!renderedRecentIds.has(item.sourceId)) {
        card.classList.add('card-enter');
      }

      const filename = document.createElement('div');
      filename.className = 'recent-filename';
      filename.textContent = item.filename;
      card.appendChild(filename);

      const objectKey = document.createElement('div');
      objectKey.className = 'recent-object-key';
      objectKey.textContent = 'Stored at: ' + item.objectKey;
      card.appendChild(objectKey);

      const size = document.createElement('div');
      size.className = 'recent-field';
      size.textContent = formatBytes(item.byteSize) + ' | confidence ' +
        (Number.isFinite(item.ocrConfidence) ? Math.round(item.ocrConfidence * 100) + '%' : 'unknown');
      card.appendChild(size);

      if (item.ocrText) {
        const ocrText = document.createElement('div');
        ocrText.className = 'recent-ocr-text';
        ocrText.textContent = item.ocrText;
        card.appendChild(ocrText);
      }

      const duration = document.createElement('div');
      duration.className = 'recent-duration';
      duration.textContent = 'Actual time from source to stored: ' + formatDuration(item.durationSeconds);
      card.appendChild(duration);

      grid.appendChild(card);
    });

    renderedRecentIds = nextIds;
  }

  async function pollRecent() {
    try {
      const res = await fetch('/api/recent?limit=' + RECENT_LIMIT);
      if (!res.ok) {
        return;
      }
      const body = await res.json();
      renderRecent(body.items || []);
    } catch (err) {
      // The next poll retries; this one silently keeps the last render.
    }
  }

  // --- controls --------------------------------------------------------------

  function wireControls() {
    const addFileHint = document.getElementById('add-file-hint');
    const addButtons = Array.prototype.slice.call(document.querySelectorAll('[data-add-count]'));
    const vendorSelect = document.getElementById('vendor-mode-select');
    const restartBtn = document.getElementById('restart-btn');

    function setButtonsDisabled(disabled) {
      addButtons.forEach(function (btn) {
        btn.disabled = disabled;
      });
    }

    // Used while a restart is running: every other control is disabled for
    // its duration, since a restart wipes the exact tables an add or a
    // vendor mode change would otherwise touch.
    function setAllControlsDisabled(disabled) {
      setButtonsDisabled(disabled);
      vendorSelect.disabled = disabled;
      restartBtn.disabled = disabled;
    }

    addButtons.forEach(function (btn) {
      btn.addEventListener('click', async function () {
        if (bulkRequestInFlight) {
          return;
        }
        const count = parseInt(btn.getAttribute('data-add-count'), 10);
        bulkRequestInFlight = true;
        setButtonsDisabled(true);
        addFileHint.textContent = 'adding...';
        try {
          const payload = count === 1
            ? {
              filename: 'dashboard-' + Date.now() + '.txt',
              text: 'Added from the dashboard at ' + new Date().toISOString(),
            }
            : { count: count };
          const res = await fetch('/api/files', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(payload),
          });
          const body = await res.json();
          if (!res.ok) {
            addFileHint.textContent = 'failed: ' + (body.message || res.status);
          } else if (body.sourceId !== undefined) {
            addFileHint.textContent = 'added source id ' + body.sourceId;
          } else {
            addFileHint.textContent = 'added ' + body.count + ' files starting at source id ' + body.firstSourceId;
          }
        } catch (err) {
          addFileHint.textContent = 'request failed';
        } finally {
          bulkRequestInFlight = false;
          setButtonsDisabled(false);
        }
      });
    });

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

    // Restart demo. Destructive, so a single click never fires it: the
    // first click only arms it, swapping the label to a short-lived
    // confirmation state, and only a second click within that window
    // actually calls the endpoint. Any other outcome (the timeout elapsing,
    // or the request itself finishing) puts the button back to its
    // resting label.
    const RESTART_CONFIRM_TIMEOUT_MS = 4000;
    const restartHint = document.getElementById('restart-hint');
    const restartBtnDefaultLabel = restartBtn.textContent;
    let restartArmed = false;
    let restartConfirmTimer = null;

    function disarmRestart() {
      restartArmed = false;
      restartBtn.textContent = restartBtnDefaultLabel;
      restartBtn.classList.remove('confirming');
      if (restartConfirmTimer) {
        clearTimeout(restartConfirmTimer);
        restartConfirmTimer = null;
      }
    }

    restartBtn.addEventListener('click', async function () {
      if (restartRequestInFlight) {
        return;
      }

      if (!restartArmed) {
        restartArmed = true;
        restartBtn.textContent = 'Click again to confirm';
        restartBtn.classList.add('confirming');
        restartHint.textContent = '';
        restartConfirmTimer = setTimeout(disarmRestart, RESTART_CONFIRM_TIMEOUT_MS);
        return;
      }

      disarmRestart();
      restartRequestInFlight = true;
      setAllControlsDisabled(true);
      restartHint.textContent = 'restarting...';
      try {
        const res = await fetch('/api/restart', { method: 'POST' });
        const body = await res.json();
        if (!res.ok) {
          restartHint.textContent = 'failed: ' + (body.message || res.status);
        } else {
          restartHint.textContent = 'reloaded ' + body.filesReloaded + ' files';
        }
      } catch (err) {
        restartHint.textContent = 'request failed';
      } finally {
        restartRequestInFlight = false;
        setAllControlsDisabled(false);
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
    setInterval(pollRecent, RECENT_POLL_INTERVAL_MS);
    try {
      const configRes = await fetch('/api/config');
      if (configRes.ok) {
        const config = await configRes.json();
        if (config.minioConsoleUrl) {
          minioConsoleUrl = config.minioConsoleUrl;
          const link = document.getElementById('minio-link');
          link.href = minioConsoleUrl + '/browser/documents';
        }
      }
    } catch (err) {
      // Falls back to the default set above.
    }
    try {
      const res = await fetch('/api/stats');
      if (res.ok) {
        applyStats(await res.json());
      }
    } catch (err) {
      // The stream connection below will populate state once it opens.
    }
    pollRecent();
    connectStream();
  }

  document.addEventListener('DOMContentLoaded', bootstrap);
})();
