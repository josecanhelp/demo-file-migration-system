'use strict';

// Thin pass-through proxies so the dashboard has one API to call instead of
// reaching into vendor-mock and migrator-worker on their own ports. Each
// function forwards the request and hands back the upstream body and
// status unchanged.

async function proxyJson(url, { method = 'GET', body } = {}) {
  const init = { method, headers: {} };
  if (body !== undefined) {
    init.headers['content-type'] = 'application/json';
    init.body = JSON.stringify(body);
  }
  const response = await fetch(url, init);
  const text = await response.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch (err) {
    json = { raw: text };
  }
  return { status: response.status, body: json };
}

function getVendorMode(vendorBaseUrl) {
  return proxyJson(`${vendorBaseUrl}/admin/mode`);
}

function setVendorMode(vendorBaseUrl, mode) {
  return proxyJson(`${vendorBaseUrl}/admin/mode`, { method: 'POST', body: { mode } });
}

function reconcile(migratorBaseUrl, body) {
  return proxyJson(`${migratorBaseUrl}/internal/reconcile`, { method: 'POST', body: body || {} });
}

module.exports = { proxyJson, getVendorMode, setVendorMode, reconcile };
