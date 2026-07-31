package com.filemigration.governor;

import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Splits one vendor rate budget between the backfill and cdc lanes so a
 * saturated backfill can never starve cdc of its reserved share. Each lane
 * has its own resilience4j RateLimiter sized to its share of
 * VENDOR_RATE_LIMIT_RPS: cdc gets the full rate, backfill gets whatever
 * percentage is left over after CDC_RESERVED_RATE_PCT is set aside. Neither
 * limiter alone is enough to protect the reserve, since cdc's own limiter
 * would still allow it to run at the full rate even while backfill is also
 * running at its own capped rate, together exceeding what the vendor can
 * actually take; a shared permit pool sized to the combined rate is what
 * keeps the two lanes from ever adding up to more than that.
 *
 * Every permit, lane-specific and shared, is checked and consumed without
 * blocking; the pool is refilled back to full once a second on a background
 * thread rather than trickling in continuously, which is enough to bound a
 * lane's rate without needing a precise token-bucket refill schedule.
 */
@Component
public class LaneRateLimiter implements AutoCloseable {

    static final String CDC_LANE = "cdc";
    static final String BACKFILL_LANE = "backfill";

    private final RateLimiter cdcLimiter;
    private final RateLimiter backfillLimiter;
    private final Semaphore sharedPermits;
    private final int sharedCapacity;
    private final ScheduledExecutorService refillExecutor;

    public LaneRateLimiter(
            @Value("${migrator.vendor.rate-limit-rps}") int ratePerSecond,
            @Value("${migrator.cdc.reserved-rate-pct}") int cdcReservedRatePct) {
        int backfillRatePerSecond = ratePerSecond * (100 - cdcReservedRatePct) / 100;
        this.cdcLimiter = RateLimiter.of(CDC_LANE, laneConfig(ratePerSecond));
        this.backfillLimiter = RateLimiter.of(BACKFILL_LANE, laneConfig(backfillRatePerSecond));
        this.sharedCapacity = ratePerSecond;
        this.sharedPermits = new Semaphore(ratePerSecond);
        this.refillExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "lane-rate-limiter-refill");
            thread.setDaemon(true);
            return thread;
        });
        this.refillExecutor.scheduleAtFixedRate(this::refill, 1, 1, TimeUnit.SECONDS);
    }

    private static RateLimiterConfig laneConfig(int limitForPeriod) {
        return RateLimiterConfig.custom()
                .limitForPeriod(Math.max(limitForPeriod, 0))
                .limitRefreshPeriod(Duration.ofSeconds(1))
                // Zero timeout makes acquirePermission() answer immediately
                // with true or false instead of blocking; acquire(String)
                // below is what supplies the blocking wait this class
                // advertises.
                .timeoutDuration(Duration.ZERO)
                .build();
    }

    private void refill() {
        sharedPermits.drainPermits();
        sharedPermits.release(sharedCapacity);
    }

    /**
     * Answers immediately: true if lane both stayed under its own share and
     * the combined ceiling still had room, false otherwise. Never blocks.
     */
    public boolean tryAcquire(String lane) {
        if (!limiterFor(lane).acquirePermission()) {
            return false;
        }
        return sharedPermits.tryAcquire();
    }

    /**
     * Blocks the calling thread until a token for the given lane is
     * available, polling rather than waiting on a single condition, since
     * the two gates a token depends on (the lane's own limiter and the
     * shared pool) refill independently.
     */
    public void acquire(String lane) {
        while (!tryAcquire(lane)) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for a rate limit token for lane " + lane,
                        e);
            }
        }
    }

    private RateLimiter limiterFor(String lane) {
        if (CDC_LANE.equals(lane)) {
            return cdcLimiter;
        }
        if (BACKFILL_LANE.equals(lane)) {
            return backfillLimiter;
        }
        throw new IllegalArgumentException("Unknown lane: " + lane);
    }

    @PreDestroy
    @Override
    public void close() {
        refillExecutor.shutdownNow();
    }
}
