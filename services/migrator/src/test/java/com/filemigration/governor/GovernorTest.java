package com.filemigration.governor;

import com.filemigration.vendor.ErrorClass;
import com.filemigration.vendor.VendorException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises Governor's retry policy and its circuit breaker wiring in
 * isolation from MigrationService: a plain Supplier stands in for a vendor
 * call, so exactly how many times it actually ran, and what the breaker's
 * state ended up as, can be checked directly.
 */
class GovernorTest {

    private static final int SLIDING_WINDOW_SIZE = 4;

    private LaneRateLimiter rateLimiter;
    private FakeDlqPublisher dlqPublisher;

    @AfterEach
    void tearDown() {
        if (rateLimiter != null) {
            rateLimiter.close();
        }
    }

    @Test
    void retriesATransientFailureAndSucceedsOnceTheUnderlyingCallRecovers() {
        Governor governor = newGovernor(defaultBreaker(), 3, 0);
        AtomicInteger calls = new AtomicInteger();

        String result = governor.execute("backfill", () -> {
            if (calls.incrementAndGet() < 3) {
                throw new VendorException(ErrorClass.TRANSIENT, null, "vendor blip");
            }
            return "done";
        });

        assertEquals("done", result);
        assertEquals(3, calls.get(), "the third attempt must be the one that finally succeeds");
    }

    @Test
    void permanentFailureIsNeverRetried() {
        Governor governor = newGovernor(defaultBreaker(), 5, 0);
        AtomicInteger calls = new AtomicInteger();

        VendorException thrown = assertThrows(VendorException.class, () -> governor.execute("backfill", () -> {
            calls.incrementAndGet();
            throw new VendorException(ErrorClass.PERMANENT, null, "unprocessable");
        }));

        assertEquals(ErrorClass.PERMANENT, thrown.errorClass());
        assertEquals(1, calls.get(), "a permanent failure must bypass retry entirely");
    }

    @Test
    void exhaustingEveryAttemptRethrowsTheLastTransientFailure() {
        Governor governor = newGovernor(defaultBreaker(), 3, 0);
        AtomicInteger calls = new AtomicInteger();

        VendorException thrown = assertThrows(VendorException.class, () -> governor.execute("cdc", () -> {
            calls.incrementAndGet();
            throw new VendorException(ErrorClass.TRANSIENT, null, "vendor still down");
        }));

        assertEquals(ErrorClass.TRANSIENT, thrown.errorClass());
        assertEquals(3, calls.get(), "every configured attempt must be spent before giving up");
    }

    /**
     * baseBackoffMs is set to 3000, far longer than retryAfter's 100ms: if
     * this ever waited the exponential backoff curve instead of
     * VendorException.retryAfter(), the elapsed wait would be measured in
     * seconds, not milliseconds. Measuring the actual wall-clock wait,
     * rather than only the call count, is what a baseBackoffMs of 0 could
     * never distinguish: both paths would look instantaneous.
     */
    @Test
    void rateLimitedWaitsForRetryAfterRatherThanTheBackoffCurveThenSucceeds() {
        Governor governor = newGovernor(defaultBreaker(), 2, 3_000);
        AtomicInteger calls = new AtomicInteger();

        long startNanos = System.nanoTime();
        String result = governor.execute("backfill", () -> {
            if (calls.incrementAndGet() == 1) {
                throw new VendorException(ErrorClass.RATE_LIMITED, Duration.ofMillis(100), "slow down");
            }
            return "done";
        });
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startNanos).toMillis();

        assertEquals("done", result);
        assertEquals(2, calls.get());
        assertTrue(elapsedMillis >= 90,
                "the wait must be at least roughly retryAfter's 100ms, not skip it entirely: was " + elapsedMillis
                        + "ms");
        assertTrue(elapsedMillis < 2_000,
                "the wait must be nowhere near the 3000ms backoff curve, proving retryAfter was honored instead "
                        + "of the curve: was " + elapsedMillis + "ms");
    }

    @Test
    void repeatedTransientFailuresTripTheBreaker() {
        CircuitBreaker breaker = testBreaker();
        Governor governor = newGovernor(breaker, 1, 0);

        for (int i = 0; i < SLIDING_WINDOW_SIZE; i++) {
            assertThrows(VendorException.class, () -> governor.execute("backfill", () -> {
                throw new VendorException(ErrorClass.TRANSIENT, null, "vendor down");
            }));
        }

        assertEquals(CircuitBreaker.State.OPEN, breaker.getState(),
                "a full window of TRANSIENT failures must trip the breaker");
    }

    @Test
    void repeatedPermanentFailuresNeverTripTheBreaker() {
        CircuitBreaker breaker = testBreaker();
        Governor governor = newGovernor(breaker, 1, 0);

        for (int i = 0; i < SLIDING_WINDOW_SIZE; i++) {
            assertThrows(VendorException.class, () -> governor.execute("backfill", () -> {
                throw new VendorException(ErrorClass.PERMANENT, null, "unprocessable");
            }));
        }

        assertEquals(CircuitBreaker.State.CLOSED, breaker.getState(),
                "a vendor that keeps rejecting individual documents is not a vendor outage");
    }

    @Test
    void deadLetterDelegatesStraightToTheDlqPublisher() {
        Governor governor = newGovernor(defaultBreaker(), 1, 0);

        governor.deadLetter(42L, "cdc", "PERMANENT", 1, "unprocessable");

        assertEquals(1, dlqPublisher.published().size());
        FakeDlqPublisher.Published published = dlqPublisher.published().get(0);
        assertEquals(42L, published.sourceId());
        assertEquals("cdc", published.lane());
        assertEquals("PERMANENT", published.errorClass());
        assertEquals(1, published.attempts());
        assertEquals("unprocessable", published.lastError());
    }

    private Governor newGovernor(CircuitBreaker breaker, int maxAttempts, long baseBackoffMs) {
        rateLimiter = new LaneRateLimiter(1_000_000, 0);
        dlqPublisher = new FakeDlqPublisher();
        return new Governor(rateLimiter, breaker, dlqPublisher, maxAttempts, baseBackoffMs);
    }

    private static CircuitBreaker defaultBreaker() {
        return CircuitBreaker.ofDefaults("test-vendor-" + System.nanoTime());
    }

    /**
     * A breaker sized like GovernorConfig's real one, just small enough for
     * a handful of calls in a test to actually trip it.
     */
    private static CircuitBreaker testBreaker() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(SLIDING_WINDOW_SIZE)
                .minimumNumberOfCalls(SLIDING_WINDOW_SIZE)
                .recordException(t -> t instanceof VendorException ve && ve.errorClass() == ErrorClass.TRANSIENT)
                .build();
        return CircuitBreaker.of("test-vendor-" + System.nanoTime(), config);
    }
}
