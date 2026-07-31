'use strict';

// Tails migration_event by polling rather than listening for a push, since
// there is no message broker between Postgres and this service. Holds the
// high-water mark in memory only: a restart re-tails from the beginning,
// which is acceptable here because nothing downstream treats replayed
// pipeline events as new work, only as display updates.

// fetchEvents(sinceId, limit) must return rows with id > sinceId, ascending
// by id, and at most `limit` of them. maxPerTick caps how many of those
// rows are forwarded to SSE clients in one tick, so a large backfill
// cannot flood the browser; the high-water mark only advances past the
// rows actually forwarded, so anything left over is picked up on the next
// tick instead of being lost.
class EventTailer {
  constructor({ fetchEvents, limit, maxPerTick, startId = 0 }) {
    this.fetchEvents = fetchEvents;
    this.limit = limit;
    this.maxPerTick = maxPerTick;
    this.highWaterMark = startId;
  }

  async tick() {
    const rows = await this.fetchEvents(this.highWaterMark, this.limit);
    if (!rows || rows.length === 0) {
      return { emitted: [], truncated: false, droppedCount: 0, highWaterMark: this.highWaterMark };
    }

    const truncated = rows.length > this.maxPerTick;
    const emitted = truncated ? rows.slice(0, this.maxPerTick) : rows;
    const droppedCount = truncated ? rows.length - emitted.length : 0;

    this.highWaterMark = emitted[emitted.length - 1].id;

    return { emitted, truncated, droppedCount, highWaterMark: this.highWaterMark };
  }
}

function makePostgresEventFetcher(pgPool) {
  return async function fetchEvents(sinceId, limit) {
    const result = await pgPool.query(
      'SELECT id, source_id, stage, lane, detail, created_at FROM migration_event WHERE id > $1 ORDER BY id LIMIT $2',
      [sinceId, limit]
    );
    return result.rows;
  };
}

module.exports = { EventTailer, makePostgresEventFetcher };
