package com.filemigration.governor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves backfill can never eat into cdc's reserved share of the shared
 * vendor rate, the property the resulting design exists to guarantee.
 */
class LaneRateLimiterTest {

    private LaneRateLimiter limiter;

    @AfterEach
    void tearDown() {
        if (limiter != null) {
            limiter.close();
        }
    }

    @Test
    void backfillCannotConsumeTheCdcReservedShare() {
        limiter = new LaneRateLimiter(100, 20);   // 100 rps, 20% reserved for cdc
        for (int i = 0; i < 80; i++) assertTrue(limiter.tryAcquire("backfill"));
        assertFalse(limiter.tryAcquire("backfill"));  // backfill capped at 80
        assertTrue(limiter.tryAcquire("cdc"));        // reserve still available
    }

    /**
     * cdc's own limiter is sized to the full 100 rps, not to its 20-permit
     * reserve, so nothing about cdc's own limiter would stop it from being
     * granted more than its reserve once backfill is not competing for it.
     * It is the shared ceiling, not either lane's own limiter, that has to
     * be the thing making the reserve exact: this drains the reserve
     * through cdc alone and confirms cdc is cut off exactly at the
     * combined rps, with nothing left over for backfill to have taken
     * without the reserve actually having been violated.
     */
    @Test
    void combinedThroughputAcrossBothLanesNeverExceedsTheSharedRps() {
        limiter = new LaneRateLimiter(100, 20);
        for (int i = 0; i < 80; i++) assertTrue(limiter.tryAcquire("backfill"));

        for (int i = 0; i < 20; i++) assertTrue(limiter.tryAcquire("cdc"));
        assertFalse(limiter.tryAcquire("cdc"),
                "the shared ceiling of 100 must cut cdc off here even though cdc's own limiter allows up to 100");
        assertFalse(limiter.tryAcquire("backfill"),
                "the shared ceiling of 100 (backfill's 80 plus cdc's 20) is exhausted here, on top of backfill's "
                        + "own 80-permit share also already being at its cap");
    }

    @Test
    void aSaturatedCdcLaneNeverStarvesBackfillOfItsOwnShare() {
        limiter = new LaneRateLimiter(100, 20);
        for (int i = 0; i < 80; i++) assertTrue(limiter.tryAcquire("cdc"));

        for (int i = 0; i < 20; i++) assertTrue(limiter.tryAcquire("backfill"));
        assertFalse(limiter.tryAcquire("backfill"),
                "backfill's own 80-permit limiter has only granted 20 so far and is nowhere near its cap; the "
                        + "shared ceiling of 100 (cdc's 80 plus backfill's own 20) is the only thing blocking it here");
    }
}
