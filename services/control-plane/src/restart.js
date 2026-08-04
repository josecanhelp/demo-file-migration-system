'use strict';

// Backs POST /api/restart: wipes the demo back to a clean slate and
// reloads the original seeded corpus, using the same deterministic
// document generator the seeder uses (see src/document.js) so a restarted
// file's content matches what a fresh `docker compose up` would have
// produced for the same id.
//
// Order matters. The source table is truncated first, which is what makes
// AUTO_INCREMENT restart at 1 for the reloaded rows; with the CDC lane
// live, that truncate itself flows to Debezium as a truncate op, and the
// reinserted rows that follow flow through the live lane rather than the
// backfill lane, which is expected. The Postgres ledger tables and the
// object store are cleared next, before a single row is reinserted, so the
// fresh inserts are never racing a delete of their own freshly written
// rows. The vendor mode is reset up front, independent of the rest, so a
// clean start is genuinely clean.
//
// This function does not wait for the reloaded files to actually migrate:
// the dashboard already shows that progress live, and blocking the
// response on it would make a demo reset feel like it hung.

const { S3Client, ListObjectsV2Command, DeleteObjectsCommand } = require('@aws-sdk/client-s3');
const { buildDocument } = require('./document');

const OBJECT_KEY_PREFIX = 'files/';

// The S3 DeleteObjects API accepts at most 1000 keys per request.
const S3_DELETE_CHUNK_SIZE = 1000;

function makeS3Client({ endpoint, region, accessKeyId, secretAccessKey }) {
  return new S3Client({
    endpoint,
    region,
    credentials: { accessKeyId, secretAccessKey },
    // MinIO's default setup does not support the virtual-hosted addressing
    // style (bucket.host:port) the SDK uses otherwise; see the migrator's
    // ObjectStoreConfig for the same requirement on the Java side.
    forcePathStyle: true,
  });
}

// Deletes every object under `prefix` in `bucket`, paging through
// ListObjectsV2 and batching deletes at the API's own per-request cap.
// Returns how many objects were actually deleted. Leaves the bucket itself
// untouched.
async function clearBucketPrefix(s3Client, bucket, prefix) {
  let continuationToken;
  let deleted = 0;

  do {
    const listResult = await s3Client.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      ContinuationToken: continuationToken,
    }));
    const keys = (listResult.Contents || []).map((obj) => ({ Key: obj.Key }));

    for (let i = 0; i < keys.length; i += S3_DELETE_CHUNK_SIZE) {
      const chunk = keys.slice(i, i + S3_DELETE_CHUNK_SIZE);
      await s3Client.send(new DeleteObjectsCommand({
        Bucket: bucket,
        Delete: { Objects: chunk },
      }));
      deleted += chunk.length;
    }

    continuationToken = listResult.IsTruncated ? listResult.NextContinuationToken : undefined;
  } while (continuationToken);

  return deleted;
}

// Plain TRUNCATE, deliberately without RESTART IDENTITY: migration_event's
// id sequence must keep climbing past whatever the live SSE tailer has
// already seen (see EventTailer's in-memory high-water mark in events.js).
// Resetting it back to 1 would make every event for the reloaded files look
// already-seen and the dashboard would go silent until the sequence
// climbed back past the old mark.
async function clearPostgresTables(pgPool) {
  await pgPool.query('TRUNCATE TABLE document, migration_state, migration_event, backfill_checkpoint');
}

// TRUNCATE, not DELETE: this is what resets AUTO_INCREMENT so the reloaded
// files start at id 1, the same as a genuinely fresh database.
async function truncateSourceTable(mysqlPool) {
  await mysqlPool.query('TRUNCATE TABLE files');
}

async function insertBatch(mysqlPool, ids, fileSizeBytes) {
  const placeholders = ids.map(() => '(?, ?, ?, ?, ?)').join(', ');
  const values = [];
  for (const id of ids) {
    const doc = buildDocument(id, fileSizeBytes);
    values.push(id, doc.filename, doc.contentType, doc.bytes, doc.bytes.length);
  }
  await mysqlPool.query(
    `INSERT INTO files (id, filename, content_type, content, byte_size) VALUES ${placeholders}`,
    values
  );
}

// Reinserts `count` rows with ids 1..count, in batches rather than one
// statement per row.
async function reseedSourceTable(mysqlPool, count, fileSizeBytes, batchSize) {
  let inserted = 0;
  while (inserted < count) {
    const chunkSize = Math.min(batchSize, count - inserted);
    const ids = [];
    for (let i = 0; i < chunkSize; i += 1) {
      ids.push(inserted + i + 1);
    }
    await insertBatch(mysqlPool, ids, fileSizeBytes);
    inserted += chunkSize;
  }
  return inserted;
}

// Best-effort: a restart should still finish and reload files even if the
// vendor mock happens to be unreachable at that moment. The caller reports
// whether this actually succeeded rather than assuming it did.
async function resetVendorMode(vendorBaseUrl) {
  try {
    const response = await fetch(`${vendorBaseUrl}/admin/mode`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ mode: 'healthy' }),
    });
    return response.ok;
  } catch (err) {
    return false;
  }
}

async function restart({
  pgPool,
  mysqlPool,
  s3Client,
  bucket,
  vendorBaseUrl,
  fileCount,
  fileSizeBytes,
  batchSize,
}) {
  const vendorReset = await resetVendorMode(vendorBaseUrl);

  await truncateSourceTable(mysqlPool);
  await clearPostgresTables(pgPool);
  const objectsDeleted = await clearBucketPrefix(s3Client, bucket, OBJECT_KEY_PREFIX);

  const filesReloaded = await reseedSourceTable(mysqlPool, fileCount, fileSizeBytes, batchSize);

  return {
    filesReloaded,
    objectsDeleted,
    vendorReset,
    tablesCleared: ['document', 'migration_state', 'migration_event', 'backfill_checkpoint'],
  };
}

module.exports = {
  restart,
  makeS3Client,
  clearBucketPrefix,
  clearPostgresTables,
  truncateSourceTable,
  reseedSourceTable,
  resetVendorMode,
  OBJECT_KEY_PREFIX,
};
