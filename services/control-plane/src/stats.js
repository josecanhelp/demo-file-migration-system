'use strict';

// Assembles the /api/stats snapshot and the payload emitted every tick on
// the `stats` SSE event. Every query here reads current state straight
// from migration_state and the source tables; nothing is derived from
// migration_event history, since FAILED_PERMANENT rows on the cdc lane can
// be revived by a later update and status is the only field that reflects
// that.

const { fetchBreakerState } = require('./breaker');
const { toFiniteNumber } = require('./numbers');

const SLA_QUERY = `
  SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MIN(source_created_at))), 0) AS sla_lag_seconds
    FROM migration_state
   WHERE lane = 'cdc' AND status <> 'DONE'
`;

async function fetchByStatus(pgPool) {
  const result = await pgPool.query('SELECT status, COUNT(*) AS count FROM migration_state GROUP BY status');
  const byStatus = {};
  for (const row of result.rows) {
    byStatus[row.status] = toFiniteNumber(row.count, 0);
  }
  return byStatus;
}

const LANE_QUEUE_DEPTH_QUERY = `
  SELECT lane, COUNT(*) AS count
    FROM migration_state
   WHERE status <> 'DONE'
   GROUP BY lane
`;

// Rows not yet DONE, grouped by lane: the actual queue depth. A lane that
// stops making progress shows a number that holds steady or climbs here,
// rather than a total that only ever goes up and cannot tell a starved
// lane apart from a healthy one.
async function fetchByLane(pgPool) {
  const result = await pgPool.query(LANE_QUEUE_DEPTH_QUERY);
  const byLane = {};
  for (const row of result.rows) {
    byLane[row.lane] = toFiniteNumber(row.count, 0);
  }
  return byLane;
}

async function fetchSlaLagSeconds(pgPool) {
  const result = await pgPool.query(SLA_QUERY);
  return toFiniteNumber(result.rows[0] && result.rows[0].sla_lag_seconds, 0);
}

async function fetchTotals(pgPool, mysqlPool) {
  const [ledgerResult, documentResult, [mysqlRows]] = await Promise.all([
    pgPool.query('SELECT COUNT(*) AS count FROM migration_state'),
    pgPool.query('SELECT COUNT(*) AS count FROM document'),
    mysqlPool.query('SELECT COUNT(*) AS count FROM files'),
  ]);
  return {
    source: toFiniteNumber(mysqlRows[0] && mysqlRows[0].count, 0),
    ledger: toFiniteNumber(ledgerResult.rows[0].count, 0),
    document: toFiniteNumber(documentResult.rows[0].count, 0),
  };
}

async function fetchVendorMode(vendorBaseUrl) {
  try {
    const response = await fetch(`${vendorBaseUrl}/admin/mode`);
    if (!response.ok) {
      return 'unknown';
    }
    const body = await response.json();
    return body.mode || 'unknown';
  } catch (err) {
    return 'unknown';
  }
}

async function getStats({ pgPool, mysqlPool, vendorBaseUrl, slaAlertSeconds, slaTargetSeconds }) {
  const [byStatus, byLane, slaLagSeconds, breakerState, totals, vendorMode] = await Promise.all([
    fetchByStatus(pgPool),
    fetchByLane(pgPool),
    fetchSlaLagSeconds(pgPool),
    fetchBreakerState(pgPool),
    fetchTotals(pgPool, mysqlPool),
    fetchVendorMode(vendorBaseUrl),
  ]);

  return {
    byStatus,
    byLane,
    slaLagSeconds,
    slaAlertSeconds,
    slaTargetSeconds,
    breakerState,
    vendorMode,
    totals,
  };
}

module.exports = { getStats, fetchSlaLagSeconds, fetchByLane, SLA_QUERY, LANE_QUEUE_DEPTH_QUERY };
