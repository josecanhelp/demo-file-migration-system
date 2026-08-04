'use strict';

// Guards against the one real risk of duplicating the seeder's document
// generator into this service (see src/document.js for why it is
// duplicated rather than imported): the two copies drifting apart so a
// restarted file's content no longer matches what the seeder itself would
// have produced for the same id. This test requires the seeder's own
// source file directly, so any future edit to either copy without the
// matching edit to the other fails right here instead of silently shipping
// a dashboard that reloads different bytes than a fresh `docker compose up`
// would have seeded.

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');

const { buildDocument: controlPlaneBuildDocument } = require('../src/document');
const { buildDocument: seederBuildDocument } = require(
  path.join(__dirname, '..', '..', 'seeder', 'src', 'document.js')
);

function assertSameDocument(id, targetBytes) {
  const a = controlPlaneBuildDocument(id, targetBytes);
  const b = seederBuildDocument(id, targetBytes);
  assert.equal(a.filename, b.filename, `filename mismatch for id ${id}`);
  assert.equal(a.contentType, b.contentType, `contentType mismatch for id ${id}`);
  assert.equal(a.text, b.text, `text mismatch for id ${id}`);
  assert.equal(a.bytes.toString('hex'), b.bytes.toString('hex'), `byte content mismatch for id ${id}`);
}

test('control-plane document generator is byte-identical to the seeder for a range of ids', () => {
  for (let id = 0; id <= 500; id += 1) {
    assertSameDocument(id, 2048);
  }
});

test('control-plane document generator matches the seeder across different target sizes', () => {
  const ids = [1, 2, 5, 37, 999, 123456];
  const sizes = [0, 1, 32, 500, 2048, 10000];
  for (const id of ids) {
    for (const targetBytes of sizes) {
      assertSameDocument(id, targetBytes);
    }
  }
});
