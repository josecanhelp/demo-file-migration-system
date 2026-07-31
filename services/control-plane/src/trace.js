'use strict';

// Returns the full, ordered event history for one source id. Deliberately
// not deduplicated by stage: a row on the cdc lane can be condemned to
// FAILED_PERMANENT and later revived, so the same stage (including DLQ)
// can appear more than once across a row's life, and the trace should
// show that rather than collapsing it.
async function getTrace(pgPool, sourceId) {
  const result = await pgPool.query(
    'SELECT id, stage, lane, detail, created_at FROM migration_event WHERE source_id = $1 ORDER BY id ASC',
    [sourceId]
  );
  return result.rows.map((row) => ({
    id: Number(row.id),
    stage: row.stage,
    lane: row.lane,
    detail: row.detail,
    at: row.created_at,
  }));
}

module.exports = { getTrace };
