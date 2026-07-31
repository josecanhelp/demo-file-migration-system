'use strict';

// pg returns numeric/double columns as strings for some types and as null
// when an aggregate has nothing to aggregate (for example MIN() over a
// column that is entirely NULL, which happens on rows the test suite left
// with no source_created_at). Coerces either case to a finite number
// instead of letting NaN or null leak into an API response.
function toFiniteNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

module.exports = { toFiniteNumber };
