'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { deriveBreakerState } = require('../src/breaker');

test('no breaker events at all reports CLOSED', () => {
  assert.equal(deriveBreakerState([]), 'CLOSED');
});

test('a single BREAKER_OPEN reports OPEN', () => {
  assert.equal(deriveBreakerState([{ id: 1, stage: 'BREAKER_OPEN' }]), 'OPEN');
});

test('picks the highest id, not array order, when open is followed by close', () => {
  const events = [
    { id: 5, stage: 'BREAKER_CLOSED' },
    { id: 3, stage: 'BREAKER_OPEN' },
  ];
  assert.equal(deriveBreakerState(events), 'CLOSED');
});

test('picks the highest id when close is followed by a later open', () => {
  const events = [
    { id: 3, stage: 'BREAKER_CLOSED' },
    { id: 5, stage: 'BREAKER_OPEN' },
  ];
  assert.equal(deriveBreakerState(events), 'OPEN');
});

test('ignores unrelated stages mixed in with breaker events', () => {
  const events = [
    { id: 1, stage: 'BREAKER_OPEN' },
    { id: 2, stage: 'CLAIMED' },
    { id: 3, stage: 'STORED' },
  ];
  assert.equal(deriveBreakerState(events), 'OPEN');
});

test('a condemned-then-revived row can still flip the breaker closed after it reopened', () => {
  const events = [
    { id: 1, stage: 'BREAKER_OPEN' },
    { id: 2, stage: 'DLQ' },
    { id: 3, stage: 'BREAKER_CLOSED' },
    { id: 4, stage: 'DLQ' },
  ];
  assert.equal(deriveBreakerState(events), 'CLOSED');
});
