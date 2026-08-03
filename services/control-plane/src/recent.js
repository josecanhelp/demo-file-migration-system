'use strict';

// Builds the payload for GET /api/recent: the most recently stored
// documents, newest first. Every field here describes one specific file so
// a non-technical viewer can see what actually happened to it, including
// how long it really took. The duration comes from that file's own rows in
// migration_event: the gap between its earliest recorded stage (whichever
// one that is, since the two lanes start at different stages) and its
// latest one, which for anything sitting in `document` is STORED. That is
// the file's real end-to-end time, not the dashboard's paced chip
// animation.

const { toFiniteNumber } = require('./numbers');

const DEFAULT_LIMIT = 8;
const MAX_LIMIT = 50;
const OCR_TEXT_PREVIEW_CHARS = 200;

const RECENT_QUERY = `
  SELECT d.source_id,
         d.filename,
         d.object_key,
         d.byte_size,
         LEFT(d.ocr_text, $2) AS ocr_text,
         d.ocr_confidence,
         d.ocr_page_count,
         d.migrated_at,
         EXTRACT(EPOCH FROM (evt.last_at - evt.first_at)) AS duration_seconds
    FROM document d
    LEFT JOIN (
      SELECT source_id, MIN(created_at) AS first_at, MAX(created_at) AS last_at
        FROM migration_event
       WHERE source_id IS NOT NULL
       GROUP BY source_id
    ) evt ON evt.source_id = d.source_id
   ORDER BY d.migrated_at DESC
   LIMIT $1
`;

function clampLimit(rawLimit) {
  const parsed = parseInt(rawLimit, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_LIMIT;
  }
  return Math.min(parsed, MAX_LIMIT);
}

function shapeRow(row) {
  return {
    sourceId: String(row.source_id),
    filename: row.filename,
    objectKey: row.object_key,
    byteSize: toFiniteNumber(row.byte_size, 0),
    ocrText: row.ocr_text,
    ocrConfidence: row.ocr_confidence === null || row.ocr_confidence === undefined
      ? null
      : Number(row.ocr_confidence),
    ocrPageCount: row.ocr_page_count === null || row.ocr_page_count === undefined
      ? null
      : Number(row.ocr_page_count),
    migratedAt: row.migrated_at,
    durationSeconds: row.duration_seconds === null || row.duration_seconds === undefined
      ? null
      : toFiniteNumber(row.duration_seconds, null),
  };
}

async function getRecent(pgPool, rawLimit) {
  const limit = clampLimit(rawLimit);
  const result = await pgPool.query(RECENT_QUERY, [limit, OCR_TEXT_PREVIEW_CHARS]);
  return result.rows.map(shapeRow);
}

module.exports = {
  getRecent,
  clampLimit,
  shapeRow,
  DEFAULT_LIMIT,
  MAX_LIMIT,
  OCR_TEXT_PREVIEW_CHARS,
  RECENT_QUERY,
};
