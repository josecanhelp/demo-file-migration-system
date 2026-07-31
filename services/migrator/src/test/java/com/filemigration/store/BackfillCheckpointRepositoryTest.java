package com.filemigration.store;

import com.filemigration.model.BackfillRange;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pure arithmetic test for how a source table is sliced into backfill
 * ranges. No database involved: the actual planRanges() insert is only
 * safe to exercise against a real database with a reserved id band, and
 * this method always starts counting at 1, so it is checked here instead
 * rather than against a shared table where a wrong range could leak
 * outside any reserved band a test could clean up after itself.
 */
class BackfillCheckpointRepositoryTest {

    @Test
    void coversExactMultipleWithNoRemainderRange() {
        List<BackfillRange> ranges = BackfillCheckpointRepository.computeRanges(3000, 1000);

        assertEquals(List.of(
                new BackfillRange(1, 1000),
                new BackfillRange(1001, 2000),
                new BackfillRange(2001, 3000)), ranges);
    }

    @Test
    void finalRangeExtendsPastMaxIdWhenItDoesNotDivideEvenly() {
        List<BackfillRange> ranges = BackfillCheckpointRepository.computeRanges(2500, 1000);

        assertEquals(List.of(
                new BackfillRange(1, 1000),
                new BackfillRange(1001, 2000),
                new BackfillRange(2001, 3000)), ranges,
                "the last range covers up to rangeSize past maxId rather than stopping short");
    }

    @Test
    void maxIdSmallerThanRangeSizeProducesOneRange() {
        List<BackfillRange> ranges = BackfillCheckpointRepository.computeRanges(500, 1000);

        assertEquals(List.of(new BackfillRange(1, 1000)), ranges);
    }

    @Test
    void zeroOrNegativeMaxIdProducesNoRanges() {
        assertTrue(BackfillCheckpointRepository.computeRanges(0, 1000).isEmpty());
        assertTrue(BackfillCheckpointRepository.computeRanges(-5, 1000).isEmpty());
    }
}
