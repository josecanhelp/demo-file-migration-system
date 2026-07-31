package com.filemigration.governor;

import com.filemigration.vendor.ErrorClass;
import com.filemigration.vendor.VendorException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Builds the single circuit breaker that guards every vendor call, shared
 * across both lanes since both call the same vendor: a lane-specific
 * breaker would let cdc keep hammering an already-broken vendor while
 * backfill's copy had already tripped, or the reverse. Only a TRANSIENT
 * VendorException counts toward the failure rate that can open it; a
 * PERMANENT one means the vendor answered and rejected the document, which
 * says nothing about whether the vendor itself is healthy, so it is
 * recorded as a success instead. The sliding window is small and the call
 * minimum low on purpose, since what this breaker is protecting against is
 * a vendor outage, which shows up as a run of TRANSIENT failures in short
 * order, not as a slow trend across hundreds of calls.
 *
 * permittedNumberOfCallsInHalfOpenState is lowered to 1 for the same
 * reason: resilience4j's own default of 10 never evaluates HALF_OPEN at
 * all until 10 calls actually happen while it is open, and recovery from
 * a real outage can easily arrive as only one or two in-flight messages
 * rather than ten, which would otherwise leave the breaker stuck in
 * HALF_OPEN indefinitely, resumed but never formally confirmed closed,
 * for lack of enough traffic to ever finish evaluating. A single
 * successful trial call is treated as enough evidence to close; any
 * subsequent failure is caught by the same minimumNumberOfCalls and
 * slidingWindowSize evaluation CLOSED already uses.
 */
@Configuration
public class GovernorConfig {

    private static final int SLIDING_WINDOW_SIZE = 10;
    private static final int MINIMUM_NUMBER_OF_CALLS = 5;
    private static final int PERMITTED_CALLS_IN_HALF_OPEN_STATE = 1;

    @Bean
    public CircuitBreaker vendorCircuitBreaker(
            @Value("${migrator.breaker.failure-rate-threshold}") float failureRateThreshold,
            @Value("${migrator.breaker.open-duration-seconds}") long openDurationSeconds) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(failureRateThreshold)
                .waitDurationInOpenState(Duration.ofSeconds(openDurationSeconds))
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(SLIDING_WINDOW_SIZE)
                .minimumNumberOfCalls(MINIMUM_NUMBER_OF_CALLS)
                .permittedNumberOfCallsInHalfOpenState(PERMITTED_CALLS_IN_HALF_OPEN_STATE)
                .recordException(GovernorConfig::isTransientVendorFailure)
                // BreakerListener pauses every listener container while
                // OPEN, so nothing would ever call the vendor again to let
                // this breaker discover a recovery: without automatic
                // transition, OPEN only ever re-evaluates lazily, on the
                // next call attempt, and pausing has made sure there is
                // never going to be one. Automatic transition is what
                // lets the breaker move itself to HALF_OPEN on a
                // background timer once waitDurationInOpenState elapses,
                // which is what BreakerListener treats as the signal to
                // resume consumption long enough for a handful of trial
                // calls to prove whether the vendor actually recovered.
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
        return CircuitBreaker.of("vendor", config);
    }

    private static boolean isTransientVendorFailure(Throwable throwable) {
        return throwable instanceof VendorException vendorException
                && vendorException.errorClass() == ErrorClass.TRANSIENT;
    }
}
