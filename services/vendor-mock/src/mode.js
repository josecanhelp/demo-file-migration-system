'use strict';

// Tracks the vendor's current chaos mode and applies its behavior to an
// in-flight OCR batch request. Mode lives in memory only: a container
// restart always comes back up in the boot mode from VENDOR_FAILURE_MODE.

const VALID_MODES = ['healthy', 'slow', 'rate_limited', 'erroring', 'down'];

let currentMode;
let since;

function setMode(mode) {
  if (!VALID_MODES.includes(mode)) {
    return false;
  }
  currentMode = mode;
  since = new Date().toISOString();
  return true;
}

function getStatus() {
  return { mode: currentMode, since };
}

// Applies the current mode to the request/response pair.
// - healthy/slow: waits the given delay, then calls onReady so the caller
//   can run the normal batch processing path.
// - rate_limited/erroring: sends the mode's response directly, unconditionally,
//   regardless of what the request body contains.
// - down: destroys the socket with no response, forcing the client to see a
//   connection reset or timeout instead of an HTTP status.
function applyMode(req, res, latencyMs, onReady) {
  switch (currentMode) {
    case 'slow':
      setTimeout(onReady, latencyMs * 20);
      break;
    case 'rate_limited':
      res.set('Retry-After', '2');
      res.status(429).json({ code: 'RATE_LIMITED' });
      break;
    case 'erroring':
      res.status(500).json({ code: 'VENDOR_ERROR' });
      break;
    case 'down':
      req.socket.destroy();
      break;
    case 'healthy':
    default:
      setTimeout(onReady, latencyMs);
      break;
  }
}

module.exports = { VALID_MODES, setMode, getStatus, applyMode };
