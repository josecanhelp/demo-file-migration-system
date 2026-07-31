'use strict';

// Populates sourcedb.files with synthetic invoice documents so the
// migration pipeline has data to move. Safe to run more than once:
// it tops up the table to the target count instead of duplicating rows.

const mysql = require('mysql2/promise');
const { buildDocument } = require('./document');

const SEED_FILE_COUNT = parseInt(process.env.SEED_FILE_COUNT || '500', 10);
const SEED_FILE_SIZE_BYTES = parseInt(process.env.SEED_FILE_SIZE_BYTES || '2048', 10);
const SEED_BATCH_SIZE = parseInt(process.env.SEED_BATCH_SIZE || '500', 10);
const SEED_PROGRESS_LOG_INTERVAL = parseInt(process.env.SEED_PROGRESS_LOG_INTERVAL || '5000', 10);

const MYSQL_HOST = process.env.MYSQL_HOST || 'mysql';
const MYSQL_PORT = parseInt(process.env.MYSQL_PORT || '3306', 10);
const MYSQL_USER = process.env.MYSQL_USER || 'root';
const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD || 'root';
const MYSQL_DATABASE = process.env.MYSQL_DATABASE || 'sourcedb';

// Inserts one multi-row statement for the given ids. Batched by the
// caller so a single INSERT never spans the whole seed run.
async function insertBatch(connection, ids) {
  const placeholders = ids.map(() => '(?, ?, ?, ?, ?)').join(', ');
  const values = [];
  for (const id of ids) {
    const doc = buildDocument(id, SEED_FILE_SIZE_BYTES);
    values.push(id, doc.filename, doc.contentType, doc.bytes, doc.bytes.length);
  }
  const sql = `INSERT INTO files (id, filename, content_type, content, byte_size) VALUES ${placeholders}`;
  await connection.query(sql, values);
}

async function main() {
  const connection = await mysql.createConnection({
    host: MYSQL_HOST,
    port: MYSQL_PORT,
    user: MYSQL_USER,
    password: MYSQL_PASSWORD,
    database: MYSQL_DATABASE,
  });

  try {
    const [[{ currentCount, maxId }]] = await connection.query(
      'SELECT COUNT(*) AS currentCount, COALESCE(MAX(id), 0) AS maxId FROM files'
    );

    if (currentCount >= SEED_FILE_COUNT) {
      console.log(
        `files table already has ${currentCount} rows, target is ${SEED_FILE_COUNT}, nothing to do`
      );
      return;
    }

    const rowsNeeded = SEED_FILE_COUNT - currentCount;
    const startId = maxId + 1;
    let inserted = 0;
    let loggedThrough = 0;

    while (inserted < rowsNeeded) {
      const batchSize = Math.min(SEED_BATCH_SIZE, rowsNeeded - inserted);
      const ids = [];
      for (let i = 0; i < batchSize; i += 1) {
        ids.push(startId + inserted + i);
      }
      await insertBatch(connection, ids);
      inserted += batchSize;

      if (inserted - loggedThrough >= SEED_PROGRESS_LOG_INTERVAL || inserted === rowsNeeded) {
        console.log(`inserted ${inserted}/${rowsNeeded} rows (table total ${currentCount + inserted})`);
        loggedThrough = inserted;
      }
    }

    console.log(`seeded ${SEED_FILE_COUNT} files`);
  } finally {
    await connection.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
