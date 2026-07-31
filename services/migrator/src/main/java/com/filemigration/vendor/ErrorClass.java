package com.filemigration.vendor;

/**
 * How a failed vendor call should be handled downstream: a permanent
 * failure goes to the dead letter queue, a rate limit waits for the
 * carried retryAfter duration, and a transient failure counts toward
 * circuit breaker tripping.
 */
public enum ErrorClass {
    PERMANENT,
    RATE_LIMITED,
    TRANSIENT
}
