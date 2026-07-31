'use strict';

// Circuit breaker state is not tracked anywhere as a column: it only exists
// as a trail of BREAKER_OPEN and BREAKER_CLOSED rows in migration_event.
// The current state is whichever of those two stages happened last.

const OPEN_STAGE = 'BREAKER_OPEN';
const CLOSED_STAGE = 'BREAKER_CLOSED';

// events: array of {id, stage}, any order. Picks the highest id among the
// two breaker stages and reports the state that stage represents. With no
// matching events at all, the breaker has never tripped, so it is closed.
function deriveBreakerState(events) {
  let latest = null;
  for (const event of events) {
    if (event.stage !== OPEN_STAGE && event.stage !== CLOSED_STAGE) {
      continue;
    }
    if (!latest || event.id > latest.id) {
      latest = event;
    }
  }
  if (!latest) {
    return 'CLOSED';
  }
  return latest.stage === OPEN_STAGE ? 'OPEN' : 'CLOSED';
}

// Queries the single most recent breaker stage directly rather than
// pulling every breaker event, since only the last one matters.
async function fetchBreakerState(pgPool) {
  const result = await pgPool.query(
    `SELECT id, stage FROM migration_event
      WHERE stage IN ($1, $2)
      ORDER BY id DESC LIMIT 1`,
    [OPEN_STAGE, CLOSED_STAGE]
  );
  return deriveBreakerState(result.rows);
}

module.exports = { deriveBreakerState, fetchBreakerState };
