'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { EventTailer } = require('../src/events');

function rowRange(startId, endId) {
  const rows = [];
  for (let id = startId; id <= endId; id += 1) {
    rows.push({ id, stage: 'QUEUED' });
  }
  return rows;
}

test('high-water mark advances past every emitted row and never goes backwards', async () => {
  const allRows = rowRange(1, 10);
  const tailer = new EventTailer({
    fetchEvents: async (sinceId, limit) => allRows.filter((r) => r.id > sinceId).slice(0, limit),
    limit: 500,
    maxPerTick: 200,
  });

  const first = await tailer.tick();
  assert.equal(first.emitted.length, 10);
  assert.equal(tailer.highWaterMark, 10);

  const second = await tailer.tick();
  assert.equal(second.emitted.length, 0);
  assert.equal(tailer.highWaterMark, 10, 'high-water mark must not move backwards on an empty poll');
});

test('never re-emits an id it already sent', async () => {
  const allRows = rowRange(1, 5);
  const tailer = new EventTailer({
    fetchEvents: async (sinceId, limit) => allRows.filter((r) => r.id > sinceId).slice(0, limit),
    limit: 500,
    maxPerTick: 200,
  });

  const seen = new Set();
  for (let i = 0; i < 3; i += 1) {
    const { emitted } = await tailer.tick();
    for (const row of emitted) {
      assert.ok(!seen.has(row.id), `id ${row.id} was already emitted`);
      seen.add(row.id);
    }
  }
  assert.deepEqual([...seen].sort((a, b) => a - b), [1, 2, 3, 4, 5]);
});

test('fan-out cap truncates a large batch and reports the truncation', async () => {
  const allRows = rowRange(1, 500);
  const tailer = new EventTailer({
    fetchEvents: async (sinceId, limit) => allRows.filter((r) => r.id > sinceId).slice(0, limit),
    limit: 500,
    maxPerTick: 200,
  });

  const result = await tailer.tick();
  assert.equal(result.emitted.length, 200);
  assert.equal(result.truncated, true);
  assert.equal(result.droppedCount, 300);
  assert.equal(tailer.highWaterMark, 200, 'high-water mark should stop at the last emitted row, not the whole fetch');

  const next = await tailer.tick();
  assert.equal(next.emitted.length, 200);
  assert.equal(next.emitted[0].id, 201, 'the remainder from the cap must arrive on the next tick, not be lost');
});

test('a batch at or under the cap is not marked truncated', async () => {
  const allRows = rowRange(1, 200);
  const tailer = new EventTailer({
    fetchEvents: async (sinceId, limit) => allRows.filter((r) => r.id > sinceId).slice(0, limit),
    limit: 500,
    maxPerTick: 200,
  });

  const result = await tailer.tick();
  assert.equal(result.emitted.length, 200);
  assert.equal(result.truncated, false);
  assert.equal(result.droppedCount, 0);
});

test('an empty poll reports no emitted events and is not truncated', async () => {
  const tailer = new EventTailer({
    fetchEvents: async () => [],
    limit: 500,
    maxPerTick: 200,
  });

  const result = await tailer.tick();
  assert.deepEqual(result.emitted, []);
  assert.equal(result.truncated, false);
  assert.equal(result.droppedCount, 0);
});
