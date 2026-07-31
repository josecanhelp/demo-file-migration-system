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
