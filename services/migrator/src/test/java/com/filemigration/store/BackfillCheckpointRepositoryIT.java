package com.filemigration.store;

import com.filemigration.model.BackfillRange;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises BackfillCheckpointRepository against a real Postgres instance,
 * proving two properties a fake database could not: that claiming never
 * blocks behind a range a concurrent, uncommitted transaction already
 * has locked, and that a range whose claim has outlived its lease becomes
 * claimable again. If that database is not reachable, connecting fails
 * loudly here rather than being swallowed into a skip.
 *
 * Every row this test writes uses a range_start at or above the reserved
 * value below, chosen far above anything the real range planner (which
 * always starts counting at 1) would ever produce for a source table of
 * any realistic size, and every one of those rows is removed after each
 * test.
 */
class BackfillCheckpointRepositoryIT {

    private static final long BASE_START = 90_000_000L;
    private static final long LEASE_SECONDS = 300L;

    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbcTemplate;
    private static BackfillCheckpointRepository checkpointRepo;

    @BeforeAll
    static void connect() {
        String url = System.getenv().getOrDefault("TARGET_JDBC_URL",
                "jdbc:postgresql://localhost:5432/targetdb");
        String username = System.getenv().getOrDefault("TARGET_JDBC_USERNAME", "postgres");
        String password = System.getenv().getOrDefault("TARGET_JDBC_PASSWORD", "postgres");

        dataSource = DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .driverClassName("org.postgresql.Driver")
                .url(url)
                .username(username)
                .password(password)
                .build();
        jdbcTemplate = new JdbcTemplate(dataSource);
        // Deliberately not wrapped in try/catch: if the target database is
        // not reachable, this throws and the whole class fails instead of
        // quietly reporting a pass with nothing exercised.
        jdbcTemplate.queryForObject("SELECT 1", Integer.class);
        checkpointRepo = new BackfillCheckpointRepository(jdbcTemplate, LEASE_SECONDS);
    }

    @AfterAll
    static void disconnect() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @AfterEach
    void cleanUpReservedRows() {
        jdbcTemplate.update("DELETE FROM backfill_checkpoint WHERE range_start >= ?", BASE_START);
    }

    @Test
    void claimNextRangeClaimsThePendingRowAndStampsClaimedAt() {
        insertRange(BASE_START, BASE_START + 999, "PENDING", null);

        Optional<BackfillRange> claimed = checkpointRepo.claimNextRange();

        assertEquals(Optional.of(new BackfillRange(BASE_START, BASE_START + 999)), claimed);
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, claimed_at FROM backfill_checkpoint WHERE range_start = ?", BASE_START);
        assertEquals("CLAIMED", row.get("status"));
        assertNotNull(row.get("claimed_at"), "claiming a range must stamp claimed_at");
    }

    @Test
    void claimNextRangeReturnsEmptyWhenNothingIsPending() {
        insertRange(BASE_START, BASE_START + 999, "DONE", Instant.now());

        assertTrue(checkpointRepo.claimNextRange().isEmpty());
    }

    /**
     * A plain two-thread race releasing from a latch was tried first and
     * kept passing even with FOR UPDATE SKIP LOCKED deleted from
     * CLAIM_SQL entirely: Postgres's own row-versioning already stops a
     * second autocommit UPDATE from double-assigning a row a first one
     * just committed, so two independent statements landing microseconds
     * apart essentially never both observe the row as available at once.
     * That is not what SKIP LOCKED is for. What it actually buys is not
     * blocking behind a row a concurrent transaction has locked but not
     * yet committed, so this test forces exactly that: it holds an
     * uncommitted claim open on the only pending range on a separate,
     * manually-controlled connection, then proves a second, ordinary
     * claimNextRange() call comes back empty (not a match, since the only
     * row is locked) essentially immediately, rather than sitting there
     * until the first transaction finishes. With SKIP LOCKED deleted,
     * rerunning this test reliably made the second call block for the
     * whole time the first transaction stayed open instead of returning
     * right away, which is what the timing assertion below catches. That
     * deliberate break was reverted immediately after confirming it.
     */
    @Test
    void claimDoesNotBlockOnARangeAnUncommittedTransactionAlreadyHasLocked() throws Exception {
        insertRange(BASE_START, BASE_START + 999, "PENDING", null);

        ExecutorService pool = Executors.newFixedThreadPool(1);
        CountDownLatch holderHasClaimed = new CountDownLatch(1);
        CountDownLatch releaseHolder = new CountDownLatch(1);
        try (Connection holderConnection = dataSource.getConnection()) {
            holderConnection.setAutoCommit(false);
            Future<?> holderTask = pool.submit(() -> {
                try (PreparedStatement ps = holderConnection.prepareStatement(BackfillCheckpointRepository.CLAIM_SQL)) {
                    ps.execute();
                    holderHasClaimed.countDown();
                    releaseHolder.await(10, TimeUnit.SECONDS);
                } finally {
                    holderConnection.rollback();
                }
                return null;
            });

            assertTrue(holderHasClaimed.await(10, TimeUnit.SECONDS),
                    "the holder must have claimed the only pending range before the timed call below runs");

            long startNanos = System.nanoTime();
            Optional<BackfillRange> result = checkpointRepo.claimNextRange();
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

            releaseHolder.countDown();
            holderTask.get(10, TimeUnit.SECONDS);

            assertTrue(result.isEmpty(),
                    "the only pending range is locked by another, uncommitted transaction, so nothing is claimable yet");
            assertTrue(elapsedMs < 2000,
                    "claimNextRange must skip a locked range rather than wait for it; took " + elapsedMs + "ms");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void reapExpiredClaimsResetsAnAgedClaimButLeavesAFreshOneAlone() {
        long agedStart = BASE_START;
        long freshStart = BASE_START + 1000;
        insertRange(agedStart, agedStart + 999, "CLAIMED", Instant.now().minusSeconds(LEASE_SECONDS + 60));
        insertRange(freshStart, freshStart + 999, "CLAIMED", Instant.now());

        int reaped = checkpointRepo.reapExpiredClaims();

        assertEquals(1, reaped);
        Map<String, Object> aged = jdbcTemplate.queryForMap(
                "SELECT status, claimed_at FROM backfill_checkpoint WHERE range_start = ?", agedStart);
        assertEquals("PENDING", aged.get("status"));
        assertNull(aged.get("claimed_at"), "reaping a range must clear its stale claimed_at");
        Map<String, Object> fresh = jdbcTemplate.queryForMap(
                "SELECT status FROM backfill_checkpoint WHERE range_start = ?", freshStart);
        assertEquals("CLAIMED", fresh.get("status"), "a claim still within its lease must not be reaped");
    }

    @Test
    void aReapedRangeBecomesClaimableAgainWhileAFreshOneDoesNot() {
        long agedStart = BASE_START;
        long freshStart = BASE_START + 1000;
        insertRange(agedStart, agedStart + 999, "CLAIMED", Instant.now().minusSeconds(LEASE_SECONDS + 60));
        insertRange(freshStart, freshStart + 999, "CLAIMED", Instant.now());

        checkpointRepo.reapExpiredClaims();
        Optional<BackfillRange> claimed = checkpointRepo.claimNextRange();

        assertEquals(Optional.of(new BackfillRange(agedStart, agedStart + 999)), claimed,
                "the aged range must be the one that became claimable");
        assertTrue(checkpointRepo.claimNextRange().isEmpty(),
                "the fresh range's lease has not expired, so nothing else should be claimable");
    }

    private void insertRange(long start, long end, String status, Instant claimedAt) {
        jdbcTemplate.update(
                "INSERT INTO backfill_checkpoint (range_start, range_end, status, claimed_at) VALUES (?, ?, ?, ?)",
                start, end, status, claimedAt == null ? null : Timestamp.from(claimedAt));
    }
}
