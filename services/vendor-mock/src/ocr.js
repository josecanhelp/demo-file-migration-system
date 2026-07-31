'use strict';

// Canonical OCR transform. Pure and deterministic: same input bytes always
// produce the same output, so callers can verify correctness by recomputing
// this function instead of trusting the vendor blindly.

const crypto = require('crypto');

function extractText(buffer) {
  return buffer.toString('utf8').toUpperCase().replace(/\s+/g, ' ').trim();
}

function ocrFor(id, buffer) {
  const text = extractText(buffer);
  const digest = crypto.createHash('sha256').update(buffer).digest();
  return {
    id,
    text,
    confidence: Number((0.90 + (digest[0] % 100) / 1000).toFixed(3)),
    pageCount: Math.max(1, Math.ceil(buffer.length / 1800)),
    jobId: `job_${crypto.createHash('sha1').update(String(id)).digest('hex').slice(0, 12)}`,
  };
}
module.exports = { extractText, ocrFor };
