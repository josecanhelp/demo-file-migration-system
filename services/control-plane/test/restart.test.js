'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  restart,
  clearBucketPrefix,
  clearPostgresTables,
  truncateSourceTable,
  reseedSourceTable,
  resetVendorMode,
} = require('../src/restart');

// --- fakes -------------------------------------------------------------

function makeFakeMysqlPool() {
  const calls = [];
  return {
    calls,
    query: async (sql, values) => {
      calls.push({ sql, values });
      return [{ insertId: 1, affectedRows: 0 }];
    },
  };
}

function makeFakePgPool() {
  const calls = [];
  return {
    calls,
    query: async (sql, values) => {
      calls.push({ sql, values });
      return { rows: [] };
    },
  };
}

// Pages two batches of objects under the prefix, so pagination itself gets
// exercised, then reports the bucket empty.
function makeFakeS3Client(objectKeys) {
  const sentCommands = [];
  const remaining = objectKeys.slice();
  const PAGE_SIZE = 3;
  return {
    sentCommands,
    send: async (command) => {
      sentCommands.push(command);
      const name = command.constructor.name;
      if (name === 'ListObjectsV2Command') {
        const page = remaining.splice(0, PAGE_SIZE);
        return {
          Contents: page.map((Key) => ({ Key })),
          IsTruncated: remaining.length > 0,
          NextContinuationToken: remaining.length > 0 ? 'more' : undefined,
        };
      }
      if (name === 'DeleteObjectsCommand') {
        return { Deleted: command.input.Delete.Objects };
      }
      throw new Error(`unexpected command ${name}`);
    },
  };
}

// --- clearBucketPrefix ---------------------------------------------------

test('clearBucketPrefix pages through every object and deletes all of them', async () => {
  const keys = Array.from({ length: 7 }, (_, i) => `files/${i}`);
  const s3Client = makeFakeS3Client(keys);
  const deleted = await clearBucketPrefix(s3Client, 'documents', 'files/');
  assert.equal(deleted, 7);
  const listCommands = s3Client.sentCommands.filter((c) => c.constructor.name === 'ListObjectsV2Command');
  assert.ok(listCommands.length >= 3, 'should page more than once for 7 objects at a page size of 3');
  for (const command of listCommands) {
    assert.equal(command.input.Bucket, 'documents');
    assert.equal(command.input.Prefix, 'files/');
  }
});

test('clearBucketPrefix does nothing and reports zero when the prefix is already empty', async () => {
  const s3Client = makeFakeS3Client([]);
  const deleted = await clearBucketPrefix(s3Client, 'documents', 'files/');
  assert.equal(deleted, 0);
  const deleteCommands = s3Client.sentCommands.filter((c) => c.constructor.name === 'DeleteObjectsCommand');
  assert.equal(deleteCommands.length, 0);
});

// --- clearPostgresTables -------------------------------------------------

test('clearPostgresTables truncates all four tables in one statement without resetting identity', async () => {
  const pgPool = makeFakePgPool();
  await clearPostgresTables(pgPool);
  assert.equal(pgPool.calls.length, 1);
  const sql = pgPool.calls[0].sql;
  assert.match(sql, /TRUNCATE TABLE/i);
  assert.match(sql, /\bdocument\b/);
  assert.match(sql, /\bmigration_state\b/);
  assert.match(sql, /\bmigration_event\b/);
  assert.match(sql, /\bbackfill_checkpoint\b/);
  assert.doesNotMatch(sql, /RESTART IDENTITY/i, 'must not reset migration_event ids or the live SSE tailer stalls');
});

// --- truncateSourceTable ---------------------------------------------------

test('truncateSourceTable truncates rather than deletes, so AUTO_INCREMENT resets', async () => {
  const mysqlPool = makeFakeMysqlPool();
  await truncateSourceTable(mysqlPool);
  assert.equal(mysqlPool.calls.length, 1);
  assert.match(mysqlPool.calls[0].sql, /^TRUNCATE TABLE files$/);
});

// --- reseedSourceTable -----------------------------------------------------

test('reseedSourceTable inserts ids starting at 1 in batches of the given size', async () => {
  const mysqlPool = makeFakeMysqlPool();
  const inserted = await reseedSourceTable(mysqlPool, 10, 2048, 4);
  assert.equal(inserted, 10);
  // 4 + 4 + 2
  assert.equal(mysqlPool.calls.length, 3);
  assert.equal(mysqlPool.calls[0].values.length, 4 * 5);
  assert.equal(mysqlPool.calls[1].values.length, 4 * 5);
  assert.equal(mysqlPool.calls[2].values.length, 2 * 5);
  // First value of the first row of the first batch is id 1.
  assert.equal(mysqlPool.calls[0].values[0], 1);
  // First value of the first row of the last batch is id 9.
  assert.equal(mysqlPool.calls[2].values[0], 9);
});

test('reseedSourceTable produces the same content buildDocument would for each id', async () => {
  const { buildDocument } = require('../src/document');
  const mysqlPool = makeFakeMysqlPool();
  await reseedSourceTable(mysqlPool, 3, 2048, 500);
  const [, filename, contentType, bytes] = mysqlPool.calls[0].values;
  const expected = buildDocument(1, 2048);
  assert.equal(filename, expected.filename);
  assert.equal(contentType, expected.contentType);
  assert.equal(bytes.toString('hex'), expected.bytes.toString('hex'));
});

// --- resetVendorMode ---------------------------------------------------

test('resetVendorMode posts healthy mode and reports success', async () => {
  const originalFetch = global.fetch;
  let capturedUrl = null;
  let capturedInit = null;
  global.fetch = async (url, init) => {
    capturedUrl = url;
    capturedInit = init;
    return { ok: true };
  };
  try {
    const ok = await resetVendorMode('http://vendor-mock:8088');
    assert.equal(ok, true);
    assert.equal(capturedUrl, 'http://vendor-mock:8088/admin/mode');
    assert.equal(JSON.parse(capturedInit.body).mode, 'healthy');
  } finally {
    global.fetch = originalFetch;
  }
});

test('resetVendorMode reports failure instead of throwing when the vendor is unreachable', async () => {
  const originalFetch = global.fetch;
  global.fetch = async () => {
    throw new Error('connection refused');
  };
  try {
    const ok = await resetVendorMode('http://vendor-mock:8088');
    assert.equal(ok, false);
  } finally {
    global.fetch = originalFetch;
  }
});

// --- restart orchestration -----------------------------------------------

test('restart clears state and the object store before reseeding, and reports a summary', async () => {
  const order = [];
  const mysqlPool = makeFakeMysqlPool();
  const originalQuery = mysqlPool.query;
  mysqlPool.query = async (sql, values) => {
    if (/^TRUNCATE TABLE files$/.test(sql)) {
      order.push('truncate-mysql');
    } else if (/^INSERT INTO files/.test(sql)) {
      order.push('insert-mysql');
    }
    return originalQuery(sql, values);
  };

  const pgPool = makeFakePgPool();
  const originalPgQuery = pgPool.query;
  pgPool.query = async (sql, values) => {
    order.push('clear-postgres');
    return originalPgQuery(sql, values);
  };

  const s3Client = makeFakeS3Client(['files/1', 'files/2']);
  const originalSend = s3Client.send;
  s3Client.send = async (command) => {
    if (command.constructor.name === 'DeleteObjectsCommand') {
      order.push('clear-objects');
    }
    return originalSend(command);
  };

  const originalFetch = global.fetch;
  global.fetch = async () => ({ ok: true });

  try {
    const summary = await restart({
      pgPool,
      mysqlPool,
      s3Client,
      bucket: 'documents',
      vendorBaseUrl: 'http://vendor-mock:8088',
      fileCount: 5,
      fileSizeBytes: 2048,
      batchSize: 500,
    });

    assert.equal(summary.filesReloaded, 5);
    assert.equal(summary.objectsDeleted, 2);
    assert.equal(summary.vendorReset, true);
    assert.deepEqual(summary.tablesCleared, ['document', 'migration_state', 'migration_event', 'backfill_checkpoint']);

    const truncateIndex = order.indexOf('truncate-mysql');
    const clearPgIndex = order.indexOf('clear-postgres');
    const clearObjectsIndex = order.indexOf('clear-objects');
    const insertIndex = order.indexOf('insert-mysql');

    assert.ok(truncateIndex < clearPgIndex, 'source table must be truncated before postgres is cleared');
    assert.ok(clearPgIndex < insertIndex, 'postgres must be cleared before reinserting');
    assert.ok(clearObjectsIndex < insertIndex, 'objects must be cleared before reinserting');
  } finally {
    global.fetch = originalFetch;
  }
});

test('restart still reloads files even when the vendor mock cannot be reached', async () => {
  const mysqlPool = makeFakeMysqlPool();
  const pgPool = makeFakePgPool();
  const s3Client = makeFakeS3Client([]);
  const originalFetch = global.fetch;
  global.fetch = async () => {
    throw new Error('connection refused');
  };
  try {
    const summary = await restart({
      pgPool,
      mysqlPool,
      s3Client,
      bucket: 'documents',
      vendorBaseUrl: 'http://vendor-mock:8088',
      fileCount: 3,
      fileSizeBytes: 2048,
      batchSize: 500,
    });
    assert.equal(summary.vendorReset, false);
    assert.equal(summary.filesReloaded, 3);
  } finally {
    global.fetch = originalFetch;
  }
});
