'use strict';

// Fake vendor OCR API. Workers call POST /v1/ocr/batch to enrich blob content
// with extracted text; the admin endpoints let a test flip the vendor into a
// failure mode on demand, to prove the migration pipeline's circuit breaker
// and retry logic actually engage under vendor trouble.

const express = require('express');
const { ocrFor } = require('./ocr');
const mode = require('./mode');

const PORT = parseInt(process.env.PORT || '8088', 10);
const VENDOR_BATCH_SIZE = parseInt(process.env.VENDOR_BATCH_SIZE || '25', 10);
const VENDOR_LATENCY_MS = parseInt(process.env.VENDOR_LATENCY_MS || '150', 10);
const VENDOR_FAILURE_MODE = process.env.VENDOR_FAILURE_MODE || 'healthy';

if (!mode.setMode(VENDOR_FAILURE_MODE)) {
  console.error(`invalid VENDOR_FAILURE_MODE "${VENDOR_FAILURE_MODE}", falling back to healthy`);
  mode.setMode('healthy');
}

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok' });
});

app.get('/admin/mode', (req, res) => {
  res.status(200).json(mode.getStatus());
});

app.post('/admin/mode', (req, res) => {
  const requested = req.body && req.body.mode;
  if (!mode.setMode(requested)) {
    res.status(400).json({ code: 'INVALID_MODE' });
    return;
  }
  res.status(200).json(mode.getStatus());
});

app.post('/v1/ocr/batch', (req, res) => {
  mode.applyMode(req, res, VENDOR_LATENCY_MS, () => processBatch(req, res));
});

function processBatch(req, res) {
  const documents = (req.body && req.body.documents) || [];

  if (documents.length > VENDOR_BATCH_SIZE) {
    res.status(400).json({ code: 'BATCH_TOO_LARGE' });
    return;
  }

  const buffers = [];
  for (const doc of documents) {
    const buffer = Buffer.from(doc.contentBase64 || '', 'base64');
    if (buffer.length === 0) {
      res.status(400).json({ code: 'UNPROCESSABLE_DOCUMENT' });
      return;
    }
    buffers.push({ id: doc.id, buffer });
  }

  const results = buffers.map(({ id, buffer }) => ocrFor(id, buffer));
  res.status(200).json({ results });
}

app.listen(PORT, () => {
  console.log(`vendor-mock listening on ${PORT}, boot mode ${mode.getStatus().mode}`);
});
