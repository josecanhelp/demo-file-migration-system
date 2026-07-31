'use strict';

// Inserts a new row into sourcedb.files and nothing else. The row's own
// insert is what the Debezium connector picks up off the binlog, so this
// function must not touch Kafka, Postgres, or MinIO directly: the point of
// the endpoint is to prove a file reaches the target system by the same
// path a real application write would take, through CDC end to end.

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

async function insertFile(mysqlPool, { filename, text } = {}) {
  const row = buildInsert({ filename, text });
  const [result] = await mysqlPool.query(
    'INSERT INTO files (filename, content_type, content, byte_size) VALUES (?, ?, ?, ?)',
    [row.filename, row.contentType, row.content, row.byteSize]
  );
  return { sourceId: result.insertId };
}

module.exports = { insertFile, buildInsert };
