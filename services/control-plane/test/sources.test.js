'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  buildInsert,
  insertManyFiles,
  validateBulkCount,
  BulkCountError,
  MAX_BULK_COUNT,
  BULK_INSERT_CHUNK_SIZE,
} = require('../src/sources');

test('text is encoded as the file content and its own byte length', () => {
  const row = buildInsert({ filename: 'invoice.txt', text: 'INVOICE 12345678 VENDOR INITECH LLC' });
  assert.equal(row.filename, 'invoice.txt');
  assert.equal(row.contentType, 'text/plain');
  assert.equal(row.content.toString('utf8'), 'INVOICE 12345678 VENDOR INITECH LLC');
  assert.equal(row.byteSize, Buffer.byteLength('INVOICE 12345678 VENDOR INITECH LLC'));
});

test('a missing filename gets a generated default', () => {
  const row = buildInsert({ text: 'hello' });
  assert.match(row.filename, /^upload-\d+\.txt$/);
});

test('a missing text produces an empty, zero-length file rather than throwing', () => {
  const row = buildInsert({ filename: 'empty.txt' });
  assert.equal(row.content.length, 0);
  assert.equal(row.byteSize, 0);
});

// --- bulk insert path -------------------------------------------------------

function makeFakeMysqlPool(startingInsertId) {
  const calls = [];
  let nextInsertId = startingInsertId;
  return {
    calls,
    query: async (sql, values) => {
      calls.push({ sql, values });
      const rowCount = values.length / 4;
      const insertId = nextInsertId;
      nextInsertId += rowCount;
      return [{ insertId, affectedRows: rowCount }];
    },
  };
}

test('validateBulkCount accepts a positive whole number up to the cap', () => {
  assert.equal(validateBulkCount(1), 1);
  assert.equal(validateBulkCount(1000), MAX_BULK_COUNT);
});

test('validateBulkCount rejects anything over the cap with a clear message', () => {
  assert.throws(() => validateBulkCount(1001), BulkCountError);
  assert.throws(() => validateBulkCount(1001), /1000/);
});

test('validateBulkCount rejects zero, negative, and non-integer counts', () => {
  assert.throws(() => validateBulkCount(0), BulkCountError);
  assert.throws(() => validateBulkCount(-5), BulkCountError);
  assert.throws(() => validateBulkCount(2.5), BulkCountError);
  assert.throws(() => validateBulkCount('lots'), BulkCountError);
});

test('insertManyFiles issues a single multi-row INSERT when the count fits in one chunk', async () => {
  const pool = makeFakeMysqlPool(501);
  const result = await insertManyFiles(pool, 10);
  assert.equal(pool.calls.length, 1);
  assert.equal(pool.calls[0].values.length, 10 * 4);
  assert.match(pool.calls[0].sql, /^INSERT INTO files/);
  assert.equal(result.firstSourceId, 501);
  assert.equal(result.count, 10);
});

test('insertManyFiles chunks a large count into multiple statements and reports the first id of the whole batch', async () => {
  const pool = makeFakeMysqlPool(1000);
  const count = BULK_INSERT_CHUNK_SIZE * 2 + 50;
  const result = await insertManyFiles(pool, count);
  assert.equal(pool.calls.length, 3);
  assert.equal(pool.calls[0].values.length, BULK_INSERT_CHUNK_SIZE * 4);
  assert.equal(pool.calls[1].values.length, BULK_INSERT_CHUNK_SIZE * 4);
  assert.equal(pool.calls[2].values.length, 50 * 4);
  assert.equal(result.firstSourceId, 1000, 'firstSourceId must come from the first chunk, not a later one');
  assert.equal(result.count, count);
});

test('insertManyFiles at exactly the cap succeeds', async () => {
  const pool = makeFakeMysqlPool(1);
  const result = await insertManyFiles(pool, MAX_BULK_COUNT);
  assert.equal(result.count, MAX_BULK_COUNT);
});

test('insertManyFiles rejects a count over the cap without touching the database', async () => {
  const pool = makeFakeMysqlPool(1);
  await assert.rejects(() => insertManyFiles(pool, MAX_BULK_COUNT + 1), BulkCountError);
  assert.equal(pool.calls.length, 0);
});
