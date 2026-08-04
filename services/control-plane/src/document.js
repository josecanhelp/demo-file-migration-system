'use strict';

// Mirrors services/seeder/src/document.js exactly. The control plane cannot
// import across service directories because each Docker image only copies
// its own directory at build time, so this file exists purely to give the
// restart endpoint the same deterministic content the seeder produces for a
// given id. test/document-parity.test.js compares this file's output
// against the seeder's, byte for byte, across a range of ids and sizes, so
// any drift between the two fails the test suite immediately rather than
// surfacing later as a dashboard that reloads different content than a
// fresh `docker compose up` would have seeded.

const VENDORS = ['ACME SUPPLY CO', 'NORTHWIND TRADING', 'GLOBEX INDUSTRIES', 'INITECH LLC'];

function buildDocument(id, targetBytes) {
  const vendor = VENDORS[id % VENDORS.length];
  const amount = ((id * 37) % 100000) / 100;
  const header = `INVOICE ${String(id).padStart(8, '0')}\nVENDOR ${vendor}\nAMOUNT DUE ${amount.toFixed(2)}\n`;
  const filler = `LINE ITEM REFERENCE ${id} `.repeat(
    Math.max(0, Math.ceil((targetBytes - header.length) / 24))
  );
  const text = (header + filler).slice(0, Math.max(header.length, targetBytes));
  return {
    filename: `invoice-${String(id).padStart(8, '0')}.txt`,
    contentType: 'text/plain',
    text,
    bytes: Buffer.from(text, 'utf8'),
  };
}
module.exports = { buildDocument };
