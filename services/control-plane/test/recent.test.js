'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { getRecent, clampLimit, DEFAULT_LIMIT, MAX_LIMIT, OCR_TEXT_PREVIEW_CHARS } = require('../src/recent');

test('clampLimit falls back to the default when missing or not a usable number', () => {
  assert.equal(clampLimit(undefined), DEFAULT_LIMIT);
  assert.equal(clampLimit('not-a-number'), DEFAULT_LIMIT);
  assert.equal(clampLimit(0), DEFAULT_LIMIT);
  assert.equal(clampLimit(-3), DEFAULT_LIMIT);
});

test('clampLimit caps an oversized request at MAX_LIMIT', () => {
  assert.equal(clampLimit(9999), MAX_LIMIT);
});

test('clampLimit passes a valid value straight through', () => {
  assert.equal(clampLimit(3), 3);
  assert.equal(clampLimit('5'), 5);
});

test('getRecent queries with the clamped limit and the server-side text preview length', async () => {
  let capturedParams = null;
  const fakePgPool = {
    query: async (sql, params) => {
      capturedParams = params;
      return { rows: [] };
    },
  };
  await getRecent(fakePgPool, 5);
  assert.deepEqual(capturedParams, [5, OCR_TEXT_PREVIEW_CHARS]);
});

test('getRecent shapes a row into camelCase fields with a real, non-animated duration', async () => {
  const fakePgPool = {
    query: async () => ({
      rows: [{
        source_id: '42',
        filename: 'invoice.txt',
        object_key: 'files/42',
        byte_size: 2048,
        ocr_text: 'INVOICE 123',
        ocr_confidence: '0.987',
        ocr_page_count: 1,
        migrated_at: new Date('2026-08-01T00:00:10Z'),
        duration_seconds: '10',
      }],
    }),
  };
  const items = await getRecent(fakePgPool, 5);
  assert.equal(items.length, 1);
  const item = items[0];
  assert.equal(item.sourceId, '42');
  assert.equal(item.filename, 'invoice.txt');
  assert.equal(item.objectKey, 'files/42');
  assert.equal(item.byteSize, 2048);
  assert.equal(item.ocrText, 'INVOICE 123');
  assert.equal(item.ocrConfidence, 0.987);
  assert.equal(item.ocrPageCount, 1);
  assert.equal(item.durationSeconds, 10);
});

test('getRecent tolerates a missing event history and reports a null duration rather than NaN', async () => {
  const fakePgPool = {
    query: async () => ({
      rows: [{
        source_id: '7',
        filename: 'a.txt',
        object_key: 'files/7',
        byte_size: 10,
        ocr_text: null,
        ocr_confidence: null,
        ocr_page_count: null,
        migrated_at: new Date(),
        duration_seconds: null,
      }],
    }),
  };
  const items = await getRecent(fakePgPool, 1);
  assert.equal(items[0].durationSeconds, null);
  assert.equal(items[0].ocrConfidence, null);
  assert.equal(items[0].ocrPageCount, null);
});
