package com.filemigration.governor;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

/**
 * Builds a Governor for tests that exercise MigrationService, BackfillConsumer,
 * or CdcConsumer without meaning to exercise Governor's own rate limiting,
 * breaking, or retrying: a rate limiter far too generous for a single test
 * to ever exhaust, a breaker whose default minimum call count only
 * GovernorTest and GovernorIT are meant to actually trip, and exactly one
 * attempt, so a vendor failure a test injects is reported to the caller on
 * the first try instead of being retried out from under the assertion.
 */
public final class TestGovernorFactory {

    private TestGovernorFactory() {
    }

    public static Governor passthrough() {
        LaneRateLimiter rateLimiter = new LaneRateLimiter(1_000_000, 0);
        CircuitBreaker circuitBreaker = CircuitBreaker.ofDefaults("test-vendor");
        return new Governor(rateLimiter, circuitBreaker, new FakeDlqPublisher(), 1, 0);
    }
}
