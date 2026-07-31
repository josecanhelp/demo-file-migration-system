package com.filemigration.governor;

import com.filemigration.vendor.ErrorClass;
import com.filemigration.vendor.VendorException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Random;
import java.util.function.Supplier;

/**
 * Wraps every vendor call with the rate limit, circuit breaker, and retry
 * policy that decide whether, when, and how often it actually reaches the
 * vendor. A PERMANENT VendorException is never retried, since a batch the
 * vendor has already rejected will not succeed on a second try. A
 * RATE_LIMITED one waits out the vendor's own Retry-After instead of the
 * exponential curve, since the vendor already told us how long to wait. A
 * TRANSIENT one, or a call refused outright because the breaker is open,
 * backs off with jitter and tries again, up to the configured attempt
 * cap; once that cap is spent the last failure is handed back to the
 * caller to classify and record. Deciding whether and when to retry a
 * vendor call belongs entirely here, never inside VendorClient itself.
 */
@Component
public class Governor {

    private final LaneRateLimiter rateLimiter;
    private final CircuitBreaker vendorCircuitBreaker;
    private final DlqPublisher dlqPublisher;
    private final int maxAttempts;
    private final Duration baseBackoff;
    private final Random random = new Random();

    public Governor(LaneRateLimiter rateLimiter, CircuitBreaker vendorCircuitBreaker, DlqPublisher dlqPublisher,
            @Value("${migrator.max-retry-attempts}") int maxAttempts,
            @Value("${migrator.governor.retry-base-backoff-ms}") long baseBackoffMs) {
        this.rateLimiter = rateLimiter;
        this.vendorCircuitBreaker = vendorCircuitBreaker;
        this.dlqPublisher = dlqPublisher;
        this.maxAttempts = Math.max(maxAttempts, 1);
        this.baseBackoff = Duration.ofMillis(baseBackoffMs);
    }

    /**
     * Runs call under the rate limiter and circuit breaker for the given
     * lane, retrying a non-permanent failure up to the configured attempt
     * cap. Every VendorException this ultimately throws, whether from the
     * vendor itself or from the breaker refusing the call outright, is
     * classified PERMANENT, RATE_LIMITED, or TRANSIENT, so a caller only
     * ever has that one exception type to branch on.
     */
    public <T> T execute(String lane, Supplier<T> call) {
        VendorException lastFailure = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            rateLimiter.acquire(lane);
            try {
                return vendorCircuitBreaker.executeSupplier(call);
            } catch (CallNotPermittedException breakerOpen) {
                // The breaker itself has already tripped; nothing about
                // retrying immediately can help, and every attempt here
                // would just spend the wait duration for nothing. Handed
                // back as TRANSIENT so the caller retries this file later
                // exactly like any other vendor trouble, rather than
                // treating an outage as an unprocessable document.
                throw new VendorException(ErrorClass.TRANSIENT, null,
                        "Vendor circuit breaker is open; vendor calls are currently suspended");
            } catch (VendorException e) {
                if (e.errorClass() == ErrorClass.PERMANENT) {
                    throw e;
                }
                lastFailure = e;
                if (attempt == maxAttempts) {
                    throw e;
                }
                sleep(delayFor(e, attempt));
            }
        }
        // Unreachable: the loop above always either returns or throws
        // before attempt exceeds maxAttempts.
        throw lastFailure;
    }

    /**
     * Records a dead letter for an id this call has already decided is
     * permanently failed. Purely a pass-through to {@link DlqPublisher};
     * kept on Governor so every resilience decision, retrying a call and
     * giving up on one, is made in the same place.
     */
    public void deadLetter(long sourceId, String lane, String errorClass, int attempts, String lastError) {
        dlqPublisher.publish(sourceId, lane, errorClass, attempts, lastError);
    }

    private Duration delayFor(VendorException e, int attempt) {
        if (e.errorClass() == ErrorClass.RATE_LIMITED && e.retryAfter() != null) {
            return e.retryAfter();
        }
        long exponentialMs = baseBackoff.toMillis() * (1L << (attempt - 1));
        long jitterMs = exponentialMs == 0 ? 0 : (long) (random.nextDouble() * exponentialMs * 0.5);
        return Duration.ofMillis(exponentialMs + jitterMs);
    }

    private void sleep(Duration duration) {
        if (duration.isZero() || duration.isNegative()) {
            return;
        }
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting to retry a vendor call", e);
        }
    }
}
