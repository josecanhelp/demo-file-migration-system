'use strict';

// API and live event stream for the migration dashboard. Reads state from
// Postgres (migration_state, migration_event, document) and MySQL
// (sourcedb.files), proxies vendor-mock and migrator-worker for the two
// admin actions the dashboard exposes, and accepts new source files
// through the same binlog path a real application write would take.

const path = require('path');
const express = require('express');
const { Pool } = require('pg');
const mysql = require('mysql2/promise');

const { getStats } = require('./stats');
const { EventTailer, makePostgresEventFetcher } = require('./events');
const { insertFile } = require('./sources');
const { getTrace } = require('./trace');
const { getVendorMode, setVendorMode, reconcile } = require('./proxy');

const PORT = parseInt(process.env.PORT || '8080', 10);

const MYSQL_HOST = process.env.MYSQL_HOST || 'mysql';
const MYSQL_PORT = parseInt(process.env.MYSQL_PORT || '3306', 10);
const MYSQL_USER = process.env.MYSQL_USER || 'root';
const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD || 'root';
const MYSQL_DATABASE = process.env.MYSQL_DATABASE || 'sourcedb';

const POSTGRES_HOST = process.env.POSTGRES_HOST || 'postgres';
const POSTGRES_PORT = parseInt(process.env.POSTGRES_PORT || '5432', 10);
const POSTGRES_USER = process.env.POSTGRES_USER || 'postgres';
const POSTGRES_PASSWORD = process.env.POSTGRES_PASSWORD || 'postgres';
const POSTGRES_DATABASE = process.env.POSTGRES_DATABASE || 'targetdb';

const VENDOR_BASE_URL = process.env.VENDOR_BASE_URL || 'http://vendor-mock:8088';
const MIGRATOR_BASE_URL = process.env.MIGRATOR_BASE_URL || 'http://migrator-worker:8082';
const MINIO_CONSOLE_URL = process.env.MINIO_CONSOLE_URL || 'http://localhost:9001';

const EVENT_POLL_INTERVAL_MS = parseInt(process.env.EVENT_POLL_INTERVAL_MS || '500', 10);
const EVENT_POLL_LIMIT = parseInt(process.env.EVENT_POLL_LIMIT || '500', 10);
const SSE_MAX_EVENTS_PER_TICK = parseInt(process.env.SSE_MAX_EVENTS_PER_TICK || '200', 10);
const STATS_STREAM_INTERVAL_MS = parseInt(process.env.STATS_STREAM_INTERVAL_MS || '1000', 10);

const SLA_ALERT_SECONDS = parseInt(process.env.SLA_ALERT_SECONDS || '1800', 10);
const SLA_TARGET_SECONDS = parseInt(process.env.SLA_TARGET_SECONDS || '3600', 10);

const pgPool = new Pool({
  host: POSTGRES_HOST,
  port: POSTGRES_PORT,
  user: POSTGRES_USER,
  password: POSTGRES_PASSWORD,
  database: POSTGRES_DATABASE,
});

const mysqlPool = mysql.createPool({
  host: MYSQL_HOST,
  port: MYSQL_PORT,
  user: MYSQL_USER,
  password: MYSQL_PASSWORD,
  database: MYSQL_DATABASE,
});

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok' });
});

// Static, deployment-specific values the dashboard needs but has no other
// way to learn, since it is served as plain static files rather than
// templated per environment.
app.get('/api/config', (req, res) => {
  res.status(200).json({ minioConsoleUrl: MINIO_CONSOLE_URL });
});

app.get('/api/stats', async (req, res) => {
  try {
    const stats = await getStats({
      pgPool,
      mysqlPool,
      vendorBaseUrl: VENDOR_BASE_URL,
      slaAlertSeconds: SLA_ALERT_SECONDS,
      slaTargetSeconds: SLA_TARGET_SECONDS,
    });
    res.status(200).json(stats);
  } catch (err) {
    console.error('GET /api/stats failed', err);
    res.status(500).json({ code: 'STATS_FAILED', message: err.message });
  }
});

app.get('/api/trace/:sourceId', async (req, res) => {
  const sourceId = parseInt(req.params.sourceId, 10);
  if (!Number.isFinite(sourceId)) {
    res.status(400).json({ code: 'INVALID_SOURCE_ID' });
    return;
  }
  try {
    const events = await getTrace(pgPool, sourceId);
    res.status(200).json({ sourceId, events });
  } catch (err) {
    console.error('GET /api/trace failed', err);
    res.status(500).json({ code: 'TRACE_FAILED', message: err.message });
  }
});

app.post('/api/files', async (req, res) => {
  try {
    const result = await insertFile(mysqlPool, req.body || {});
    res.status(201).json(result);
  } catch (err) {
    console.error('POST /api/files failed', err);
    res.status(500).json({ code: 'INSERT_FAILED', message: err.message });
  }
});

app.get('/api/vendor/mode', async (req, res) => {
  try {
    const { status, body } = await getVendorMode(VENDOR_BASE_URL);
    res.status(status).json(body);
  } catch (err) {
    console.error('GET /api/vendor/mode failed', err);
    res.status(502).json({ code: 'VENDOR_UNREACHABLE', message: err.message });
  }
});

app.post('/api/vendor/mode', async (req, res) => {
  try {
    const { status, body } = await setVendorMode(VENDOR_BASE_URL, req.body && req.body.mode);
    res.status(status).json(body);
  } catch (err) {
    console.error('POST /api/vendor/mode failed', err);
    res.status(502).json({ code: 'VENDOR_UNREACHABLE', message: err.message });
  }
});

app.post('/api/reconcile', async (req, res) => {
  try {
    const { status, body } = await reconcile(MIGRATOR_BASE_URL, req.body);
    res.status(status).json(body);
  } catch (err) {
    console.error('POST /api/reconcile failed', err);
    res.status(502).json({ code: 'MIGRATOR_UNREACHABLE', message: err.message });
  }
});

// --- SSE stream -----------------------------------------------------------
//
// One shared tailer and one shared stats loop, not one per connected
// client. The event tailer keeps polling on its own interval whether or
// not a browser is attached, so the high-water mark keeps moving; the
// stats loop skips its query when nobody is listening. Either way, every
// query goes through the same pg pool methods used elsewhere, which check
// a client out and back in per call, so there is no per-client interval or
// pg client to leak when a browser disconnects, only an entry removed from
// the client set below.

const sseClients = new Set();

function sendSse(res, event, data) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

function broadcastSse(event, data) {
  for (const client of sseClients) {
    sendSse(client, event, data);
  }
}

app.get('/api/stream', (req, res) => {
  res.status(200);
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });
  res.flushHeaders();

  sseClients.add(res);

  req.on('close', () => {
    sseClients.delete(res);
  });
});

const eventTailer = new EventTailer({
  fetchEvents: makePostgresEventFetcher(pgPool),
  limit: EVENT_POLL_LIMIT,
  maxPerTick: SSE_MAX_EVENTS_PER_TICK,
});

async function eventTick() {
  try {
    const { emitted, truncated, droppedCount } = await eventTailer.tick();
    if (emitted.length === 0) {
      return;
    }
    broadcastSse('pipeline', { events: emitted, truncated, droppedCount });
  } catch (err) {
    console.error('event tailer tick failed', err);
  }
}

async function statsTick() {
  if (sseClients.size === 0) {
    return;
  }
  try {
    const stats = await getStats({
      pgPool,
      mysqlPool,
      vendorBaseUrl: VENDOR_BASE_URL,
      slaAlertSeconds: SLA_ALERT_SECONDS,
      slaTargetSeconds: SLA_TARGET_SECONDS,
    });
    broadcastSse('stats', stats);
  } catch (err) {
    console.error('stats tick failed', err);
  }
}

// --- static dashboard placeholder -----------------------------------------

app.use(express.static(path.join(__dirname, '..', 'public')));

const server = app.listen(PORT, () => {
  console.log(`control-plane listening on ${PORT}`);
});

const eventInterval = setInterval(eventTick, EVENT_POLL_INTERVAL_MS);
const statsInterval = setInterval(statsTick, STATS_STREAM_INTERVAL_MS);

function shutdown() {
  clearInterval(eventInterval);
  clearInterval(statsInterval);
  for (const client of sseClients) {
    client.end();
  }
  server.close(() => {
    Promise.all([pgPool.end(), mysqlPool.end()]).finally(() => process.exit(0));
  });
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

module.exports = { app };
