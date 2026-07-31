package com.filemigration.store;

import com.filemigration.model.BackfillRange;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The backfill_checkpoint table: the set of source id ranges the backfill
 * coordinator has divided the source table into, and how far each one has
 * gotten. A range held CLAIMED by a coordinator that dies before finishing
 * it is not lost: once the claim's lease expires, the range is treated as
 * PENDING again by the same reasoning migration_state uses for individual
 * files, so a fresh coordinator picks it back up.
 */
@Repository
public class BackfillCheckpointRepository {

    // Copied exactly: the SELECT ... FOR UPDATE SKIP LOCKED is what lets
    // two coordinators run this at the same moment without either one
    // blocking behind whichever row the other already has locked, since
    // the loser simply skips that row and looks for a different one
    // instead of waiting on it. Package-private, not private, so the
    // integration test that proves this can run the identical statement
    // directly against a deliberately held-open transaction rather than
    // keeping a second copy of this text that could drift from the real
    // one.
    static final String CLAIM_SQL =
            "UPDATE backfill_checkpoint SET status='CLAIMED', claimed_at=now()\n"
            + " WHERE (range_start, range_end) = (\n"
            + "   SELECT range_start, range_end FROM backfill_checkpoint\n"
            + "    WHERE status='PENDING' ORDER BY range_start\n"
            + "    FOR UPDATE SKIP LOCKED LIMIT 1)\n"
            + "RETURNING range_start, range_end";

    private final JdbcTemplate targetJdbc;
    private final long rangeLeaseSeconds;

    public BackfillCheckpointRepository(@Qualifier("targetJdbc") JdbcTemplate targetJdbc,
            @Value("${migrator.backfill.range-lease-seconds}") long rangeLeaseSeconds) {
        this.targetJdbc = targetJdbc;
        this.rangeLeaseSeconds = rangeLeaseSeconds;
    }

    /**
     * Inserts one PENDING row per range of rangeSize needed to cover ids 1
     * through maxId inclusive, leaving any range already present
     * untouched. A restart calling this again with the same maxId inserts
     * nothing new. Does nothing when maxId is not positive.
     */
    public int planRanges(long maxId, long rangeSize) {
        List<BackfillRange> ranges = computeRanges(maxId, rangeSize);
        if (ranges.isEmpty()) {
            return 0;
        }
        List<Object[]> batchArgs = new ArrayList<>(ranges.size());
        for (BackfillRange range : ranges) {
            batchArgs.add(new Object[] { range.rangeStart(), range.rangeEnd() });
        }
        String sql = "INSERT INTO backfill_checkpoint (range_start, range_end, status) "
                + "VALUES (?, ?, 'PENDING') ON CONFLICT (range_start, range_end) DO NOTHING";
        int[] results = targetJdbc.batchUpdate(sql, batchArgs);
        int inserted = 0;
        for (int result : results) {
            if (result > 0) {
                inserted += result;
            }
        }
        return inserted;
    }

    /**
     * Works out the sequence of inclusive ranges of rangeSize needed to
     * cover ids 1 through maxId, with no gap and no overlap. Kept separate
     * from the insert itself so the arithmetic can be checked without a
     * database. Returns an empty list when there is nothing to cover.
     */
    static List<BackfillRange> computeRanges(long maxId, long rangeSize) {
        if (maxId <= 0 || rangeSize <= 0) {
            return List.of();
        }
        List<BackfillRange> ranges = new ArrayList<>();
        long start = 1;
        while (start <= maxId) {
            long end = start + rangeSize - 1;
            ranges.add(new BackfillRange(start, end));
            start = end + 1;
        }
        return ranges;
    }

    /**
     * Puts every CLAIMED range whose lease has expired back to PENDING, so
     * a coordinator that died mid-range is not the only thing standing
     * between that range and ever being finished.
     */
    public int reapExpiredClaims() {
        return targetJdbc.update(
                "UPDATE backfill_checkpoint SET status = 'PENDING', claimed_at = NULL "
                        + "WHERE status = 'CLAIMED' AND claimed_at < now() - make_interval(secs => ?)",
                rangeLeaseSeconds);
    }

    /**
     * Claims the single lowest-numbered PENDING range, if one exists, and
     * marks it CLAIMED. Two callers racing to claim at the same instant
     * never receive the same range back.
     */
    public Optional<BackfillRange> claimNextRange() {
        List<BackfillRange> rows = targetJdbc.query(CLAIM_SQL,
                (rs, rowNum) -> new BackfillRange(rs.getLong("range_start"), rs.getLong("range_end")));
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    /**
     * Marks a claimed range fully seeded and published, so it is never
     * claimed again.
     */
    public void markDone(long rangeStart, long rangeEnd) {
        targetJdbc.update(
                "UPDATE backfill_checkpoint SET status = 'DONE' WHERE range_start = ? AND range_end = ?",
                rangeStart, rangeEnd);
    }
}
