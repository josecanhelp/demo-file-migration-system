'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { buildInsert } = require('../src/sources');

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
