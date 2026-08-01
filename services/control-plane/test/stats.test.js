'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { toFiniteNumber } = require('../src/numbers');
const { fetchSlaLagSeconds, fetchByLane, fetchByLaneTotal } = require('../src/stats');

// The SLA query already wraps its own aggregate in COALESCE(..., 0), but a
// row left with source_created_at NULL by the test suite is exactly the
// case that makes the underlying EXTRACT(EPOCH FROM (now() - NULL))
// resolve to SQL NULL when it is the only row in scope, and the pg driver
// can hand that back as a JS null rather than a number. toFiniteNumber is
// the guard between that raw driver value and the API response.

test('a null sla_lag_seconds (all matching rows NULL) tolerates to the fallback instead of NaN', () => {
  assert.equal(toFiniteNumber(null, 0), 0);
});

test('a numeric string from the driver parses to a real number', () => {
  assert.equal(toFiniteNumber('123.45', 0), 123.45);
});

test('a genuine number passes through unchanged', () => {
  assert.equal(toFiniteNumber(42, 0), 42);
});

test('undefined tolerates to the fallback instead of NaN', () => {
  assert.equal(toFiniteNumber(undefined, 0), 0);
});

test('a non-numeric string tolerates to the fallback instead of NaN', () => {
  assert.equal(toFiniteNumber('not-a-number', 0), 0);
});

test('fetchSlaLagSeconds tolerates a NULL aggregate from the driver', async () => {
  const fakePgPool = {
    query: async () => ({ rows: [{ sla_lag_seconds: null }] }),
  };
  assert.equal(await fetchSlaLagSeconds(fakePgPool), 0);
});

test('fetchSlaLagSeconds returns the real lag when the driver hands back a value', async () => {
  const fakePgPool = {
    query: async () => ({ rows: [{ sla_lag_seconds: '742.5' }] }),
  };
  assert.equal(await fetchSlaLagSeconds(fakePgPool), 742.5);
});

test('fetchByLane filters out DONE work, so it reports queue depth rather than a total that only grows', async () => {
  let capturedSql = null;
  const fakePgPool = {
    query: async (sql) => {
      capturedSql = sql;
      return { rows: [{ lane: 'cdc', count: '3' }, { lane: 'backfill', count: '0' }] };
    },
  };
  const byLane = await fetchByLane(fakePgPool);
  assert.match(capturedSql, /status\s*<>\s*'DONE'/);
  assert.deepEqual(byLane, { cdc: 3, backfill: 0 });
});

test('fetchByLaneTotal counts every row per lane, DONE included', async () => {
  let capturedSql = null;
  const fakePgPool = {
    query: async (sql) => {
      capturedSql = sql;
      return { rows: [{ lane: 'cdc', count: '480' }, { lane: 'backfill', count: '500' }] };
    },
  };
  const byLaneTotal = await fetchByLaneTotal(fakePgPool);
  assert.doesNotMatch(capturedSql, /WHERE/);
  assert.deepEqual(byLaneTotal, { cdc: 480, backfill: 500 });
});
