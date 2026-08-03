'use strict';

// Inserts new rows into sourcedb.files and nothing else. The row's own
// insert is what the Debezium connector picks up off the binlog, so this
// function must not touch Kafka, Postgres, or MinIO directly: the point of
// the endpoint is to prove a file reaches the target system by the same
// path a real application write would take, through CDC end to end. That
// holds for both the single-file path and the bulk path below: a bulk
// request is still nothing more than rows landing in this same table, just
// more of them in one round trip.

const MAX_BULK_COUNT = 1000;
const BULK_INSERT_CHUNK_SIZE = 200;

class BulkCountError extends Error {}

function buildInsert({ filename, text }) {
  const safeText = typeof text === 'string' ? text : '';
  const content = Buffer.from(safeText, 'utf8');
  const safeFilename = typeof filename === 'string' && filename.trim() !== ''
    ? filename.trim()
    : `upload-${Date.now()}.txt`;

  return {
    filename: safeFilename,
    contentType: 'text/plain',
    content,
    byteSize: content.length,
  };
}

// One row's shape for a bulk insert. Each gets its own filename (an index
// keeps them unique within the batch) and a small text body, the same
// spirit as the single-file default, just generated N times instead of
// typed once.
function buildBulkRow(nonce, index) {
  const text = 'Added from the dashboard bulk control at ' + new Date(nonce).toISOString();
  const content = Buffer.from(text, 'utf8');
  return {
    filename: `dashboard-bulk-${nonce}-${index}.txt`,
    contentType: 'text/plain',
    content,
    byteSize: content.length,
  };
}

function validateBulkCount(rawCount) {
  const count = Number(rawCount);
  if (!Number.isInteger(count) || count <= 0) {
    throw new BulkCountError('count must be a positive whole number');
  }
  if (count > MAX_BULK_COUNT) {
    throw new BulkCountError(`count cannot be more than ${MAX_BULK_COUNT} files in a single request`);
  }
  return count;
}

// Inserts `count` rows across as few multi-row INSERT statements as
// needed, chunked at BULK_INSERT_CHUNK_SIZE so a single statement never
// carries an unreasonable number of placeholders. MySQL hands back the
// auto-increment id of the first row of a multi-row INSERT, so the id from
// the first chunk is the first id of the whole batch.
async function insertManyFiles(mysqlPool, rawCount) {
  const count = validateBulkCount(rawCount);
  const nonce = Date.now();
  let firstSourceId = null;
  let rowIndex = 0;
  let remaining = count;

  while (remaining > 0) {
    const chunkSize = Math.min(BULK_INSERT_CHUNK_SIZE, remaining);
    const values = [];
    const placeholders = [];
    for (let i = 0; i < chunkSize; i += 1) {
      const row = buildBulkRow(nonce, rowIndex);
      placeholders.push('(?, ?, ?, ?)');
      values.push(row.filename, row.contentType, row.content, row.byteSize);
      rowIndex += 1;
    }
    const sql = `INSERT INTO files (filename, content_type, content, byte_size) VALUES ${placeholders.join(', ')}`;
    const [result] = await mysqlPool.query(sql, values);
    if (firstSourceId === null) {
      firstSourceId = result.insertId;
    }
    remaining -= chunkSize;
  }

  return { firstSourceId, count };
}

async function insertFile(mysqlPool, body = {}) {
  const { filename, text, count } = body;
  if (count !== undefined && count !== null) {
    return insertManyFiles(mysqlPool, count);
  }
  const row = buildInsert({ filename, text });
  const [result] = await mysqlPool.query(
    'INSERT INTO files (filename, content_type, content, byte_size) VALUES (?, ?, ?, ?)',
    [row.filename, row.contentType, row.content, row.byteSize]
  );
  return { sourceId: result.insertId };
}

module.exports = {
  insertFile,
  buildInsert,
  insertManyFiles,
  validateBulkCount,
  BulkCountError,
  MAX_BULK_COUNT,
  BULK_INSERT_CHUNK_SIZE,
};
