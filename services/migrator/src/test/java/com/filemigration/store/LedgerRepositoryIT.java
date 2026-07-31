package com.filemigration.store;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises LedgerRepository against a real Postgres instance (the same
 * target database docker compose starts for the running services), not a
 * mock. If that database is not reachable, connecting fails loudly here
 * rather than being swallowed into a skip: a store layer contract that
 * only "passes" when nothing was actually checked protects nothing.
 *
 * Every row this test writes uses a source id at or above the reserved
 * range below, and every one of those rows is removed after each test, so
 * running this repeatedly never leaves data behind or collides with
 * anything else in the database.
 */
class LedgerRepositoryIT {

    private static final long BASE_ID = 9_000_000L;
    private static final long LEASE_SECONDS = 300L;

    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbcTemplate;
    private static LedgerRepository ledger;

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
        ledger = new LedgerRepository(jdbcTemplate, LEASE_SECONDS);
    }

    @AfterAll
    static void disconnect() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @AfterEach
    void cleanUpReservedRows() {
        jdbcTemplate.update("DELETE FROM migration_state WHERE source_id >= ?", BASE_ID);
        jdbcTemplate.update("DELETE FROM document WHERE source_id >= ?", BASE_ID);
    }

    @Test
    void claimOnPendingRowReturnsIdAndMovesItToInFlight() {
        long id = BASE_ID + 1;
        insertState(id, "cdc", "PENDING", 0);

        List<Long> claimed = ledger.claim(List.of(id));

        assertEquals(List.of(id), claimed);
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, attempts FROM migration_state WHERE source_id = ?", id);
        assertEquals("IN_FLIGHT", row.get("status"));
        assertEquals(1, row.get("attempts"));
    }

    @Test
    void secondClaimOnAlreadyInFlightRowReturnsNothing() {
        long id = BASE_ID + 2;
        insertState(id, "cdc", "PENDING", 0);
        List<Long> firstClaim = ledger.claim(List.of(id));
        assertEquals(List.of(id), firstClaim);

        List<Long> secondClaim = ledger.claim(List.of(id));

        assertTrue(secondClaim.isEmpty(), "a row already IN_FLIGHT must not be claimable again");
    }

    @Test
    void failedRetryableRowStaysClaimable() {
        long id = BASE_ID + 3;
        insertState(id, "backfill", "FAILED_RETRYABLE", 2);

        List<Long> claimed = ledger.claim(List.of(id));

        assertEquals(List.of(id), claimed);
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, attempts FROM migration_state WHERE source_id = ?", id);
        assertEquals("IN_FLIGHT", row.get("status"));
        assertEquals(3, row.get("attempts"));
    }

    @Test
    void ocrDoneRowWithCachedPayloadStaysClaimableOnceItsLeaseExpires() {
        long id = BASE_ID + 4;
        insertState(id, "backfill", "OCR_DONE", 1);
        jdbcTemplate.update(
                "UPDATE migration_state SET ocr_payload = ?::jsonb, updated_at = now() - interval '1 hour' "
                        + "WHERE source_id = ?",
                "{\"id\":" + id + ",\"text\":\"hello\",\"confidence\":0.9,\"pageCount\":1,\"jobId\":\"job-1\"}", id);

        List<Long> claimed = ledger.claim(List.of(id));

        assertEquals(List.of(id), claimed, "a row with a cached OCR payload must be claimable once its lease expires");
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, attempts, ocr_payload FROM migration_state WHERE source_id = ?", id);
        assertEquals("IN_FLIGHT", row.get("status"));
        assertEquals(2, row.get("attempts"));
        assertTrue(row.get("ocr_payload").toString().contains("hello"),
                "claiming an OCR_DONE row must not disturb its cached payload");
    }

    @Test
    void freshInFlightRowIsNotClaimableWithinItsLease() {
        long id = BASE_ID + 7;
        insertState(id, "cdc", "IN_FLIGHT", 1);

        List<Long> claimed = ledger.claim(List.of(id));

        assertTrue(claimed.isEmpty(), "a row still within its claim lease must not be claimable");
    }

    @Test
    void inFlightRowPastItsLeaseBecomesClaimableAgain() {
        long id = BASE_ID + 8;
        insertState(id, "cdc", "IN_FLIGHT", 1);
        jdbcTemplate.update("UPDATE migration_state SET updated_at = now() - interval '1 hour' "
                + "WHERE source_id = ?", id);

        List<Long> claimed = ledger.claim(List.of(id));

        assertEquals(List.of(id), claimed, "a row whose claim lease has expired must be claimable again");
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, attempts FROM migration_state WHERE source_id = ?", id);
        assertEquals("IN_FLIGHT", row.get("status"));
        assertEquals(2, row.get("attempts"));
    }

    @Test
    void findCachedOcrPayloadsReturnsOnlyIdsWithANonNullPayload() {
        long withPayload = BASE_ID + 5;
        long withoutPayload = BASE_ID + 6;
        insertState(withPayload, "backfill", "OCR_DONE", 1);
        insertState(withoutPayload, "backfill", "PENDING", 0);
        jdbcTemplate.update(
                "UPDATE migration_state SET ocr_payload = ?::jsonb WHERE source_id = ?",
                "{\"id\":" + withPayload + ",\"text\":\"hello\",\"confidence\":0.9,\"pageCount\":1,\"jobId\":\"job-1\"}",
                withPayload);

        Map<Long, String> payloads = ledger.findCachedOcrPayloads(List.of(withPayload, withoutPayload));

        assertEquals(1, payloads.size());
        assertTrue(payloads.get(withPayload).contains("hello"));
    }

    @Test
    void claimReturnsOnlyTheClaimableIdsFromAMixedBatch() {
        long pending = BASE_ID + 10;
        long done = BASE_ID + 11;
        long inFlight = BASE_ID + 12;
        long failedRetryable = BASE_ID + 13;
        long failedPermanent = BASE_ID + 14;
        insertState(pending, "cdc", "PENDING", 0);
        insertState(done, "cdc", "DONE", 1);
        insertState(inFlight, "cdc", "IN_FLIGHT", 1);
        insertState(failedRetryable, "cdc", "FAILED_RETRYABLE", 1);
        insertState(failedPermanent, "cdc", "FAILED_PERMANENT", 4);

        List<Long> claimed = ledger.claim(
                List.of(pending, done, inFlight, failedRetryable, failedPermanent));

        assertEquals(List.of(pending, failedRetryable), sorted(claimed));
    }

    @Test
    void seedPendingDoesNotDuplicateOrOverwriteAnExistingRow() {
        long id = BASE_ID + 20;

        int firstInsert = ledger.seedPending(List.of(id), "cdc", Map.of(id, Instant.now()));
        assertEquals(1, firstInsert);

        // Move the row along so a naive re-seed would be an observable
        // regression if it reset state back to PENDING instead of leaving
        // it alone.
        ledger.claim(List.of(id));

        int secondInsert = ledger.seedPending(List.of(id), "backfill", Map.of(id, Instant.now()));

        assertEquals(0, secondInsert, "seeding an id that is already tracked must insert nothing");
        Long rowCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM migration_state WHERE source_id = ?", Long.class, id);
        assertEquals(1L, rowCount);
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, lane FROM migration_state WHERE source_id = ?", id);
        assertEquals("IN_FLIGHT", row.get("status"));
        assertEquals("cdc", row.get("lane"));
    }

    @Test
    void findUnresolvedExcludesOnlyDoneAndPermanentlyFailedRows() {
        long done = BASE_ID + 30;
        long permanentlyFailed = BASE_ID + 31;
        long inFlight = BASE_ID + 32;
        long retryable = BASE_ID + 33;
        long pending = BASE_ID + 34;
        insertState(done, "backfill", "DONE", 1);
        insertState(permanentlyFailed, "backfill", "FAILED_PERMANENT", 1);
        insertState(inFlight, "backfill", "IN_FLIGHT", 1);
        insertState(retryable, "backfill", "FAILED_RETRYABLE", 1);
        insertState(pending, "backfill", "PENDING", 0);

        List<Long> unresolved = ledger.findUnresolved(
                List.of(done, permanentlyFailed, inFlight, retryable, pending));

        assertEquals(List.of(inFlight, retryable, pending), sorted(unresolved));
    }

    @Test
    void failExceededAttemptsMovesAtCapRowsToFailedPermanentAndPreservesLastErrorAndLeavesOthersAlone() {
        long atCap = BASE_ID + 50;
        long underCap = BASE_ID + 51;
        // Lifetime attempts is deliberately different from, and larger
        // than, consecutive_failures on both rows, to prove the cap reads
        // the latter, never the former.
        insertState(atCap, "cdc", "FAILED_RETRYABLE", 10, 5);
        insertState(underCap, "cdc", "FAILED_RETRYABLE", 8, 4);
        jdbcTemplate.update("UPDATE migration_state SET last_error = ? WHERE source_id = ?", "boom", atCap);

        List<LedgerRepository.ExceededAttempt> exceeded = ledger.failExceededAttempts(List.of(atCap, underCap), 5);

        assertEquals(1, exceeded.size());
        assertEquals(atCap, exceeded.get(0).sourceId());
        assertEquals(10, exceeded.get(0).attempts(), "the returned attempts value is the lifetime diagnostic "
                + "count, not the consecutive-failures count that actually gated this");
        assertEquals("boom", exceeded.get(0).lastError());
        assertEquals("FAILED_PERMANENT", jdbcTemplate.queryForObject(
                "SELECT status FROM migration_state WHERE source_id = ?", String.class, atCap));
        assertEquals("FAILED_RETRYABLE", jdbcTemplate.queryForObject(
                "SELECT status FROM migration_state WHERE source_id = ?", String.class, underCap));
    }

    /**
     * The critical case: a row can have a lifetime attempts count past any
     * reasonable cap purely from ordinary successful reclaims (a file
     * claimed once to backfill it, then reclaimed on four more legitimate
     * updates, none of which ever failed) and must never be condemned,
     * because the cap reads consecutive_failures, which stayed at zero
     * the whole time.
     */
    @Test
    void failExceededAttemptsNeverCondemnsARowWithZeroConsecutiveFailuresNoMatterHowHighLifetimeAttemptsIs() {
        long id = BASE_ID + 58;
        insertState(id, "backfill", "PENDING", 10, 0);

        List<LedgerRepository.ExceededAttempt> exceeded = ledger.failExceededAttempts(List.of(id), 5);

        assertTrue(exceeded.isEmpty(), "zero consecutive failures must never be condemned regardless of "
                + "lifetime attempts");
        assertEquals("PENDING", jdbcTemplate.queryForObject(
                "SELECT status FROM migration_state WHERE source_id = ?", String.class, id));
    }

    @Test
    void failExceededAttemptsLeavesInFlightAndOcrDoneRowsAlone() {
        long inFlight = BASE_ID + 54;
        long ocrDone = BASE_ID + 56;
        insertState(inFlight, "cdc", "IN_FLIGHT", 6, 6);
        insertState(ocrDone, "cdc", "OCR_DONE", 6, 6);

        List<LedgerRepository.ExceededAttempt> exceeded = ledger.failExceededAttempts(
                List.of(inFlight, ocrDone), 5);

        assertTrue(exceeded.isEmpty(), "a row currently owned by a live or recently-live claim is never a "
                + "candidate for the retry cap, only PENDING or FAILED_RETRYABLE is");
    }

    /**
     * In the ordinary CdcConsumer flow, resetForUpdate always clears
     * consecutive_failures back to zero before a row can ever reach this
     * check as PENDING (see resetForUpdateClearsConsecutiveFailures
     * below), so a PENDING row only ever reaches this method with a
     * nonzero consecutive_failures count through a path other than the
     * normal update flow. This proves the SQL condition itself still
     * catches that case defensively, whatever puts a row in it.
     */
    @Test
    void failExceededAttemptsCatchesAPendingRowWithConsecutiveFailuresAtTheCap() {
        long pending = BASE_ID + 53;
        insertState(pending, "cdc", "PENDING", 5, 5);

        List<LedgerRepository.ExceededAttempt> exceeded = ledger.failExceededAttempts(List.of(pending), 5);

        assertEquals(1, exceeded.size());
        assertEquals(pending, exceeded.get(0).sourceId());
        assertEquals("FAILED_PERMANENT", jdbcTemplate.queryForObject(
                "SELECT status FROM migration_state WHERE source_id = ?", String.class, pending));
    }

    @Test
    void failExceededAttemptsLeavesAPendingRowUnderTheCapAlone() {
        long pending = BASE_ID + 57;
        insertState(pending, "cdc", "PENDING", 2, 2);

        List<LedgerRepository.ExceededAttempt> exceeded = ledger.failExceededAttempts(List.of(pending), 5);

        assertTrue(exceeded.isEmpty());
        assertEquals("PENDING", jdbcTemplate.queryForObject(
                "SELECT status FROM migration_state WHERE source_id = ?", String.class, pending));
    }

    @Test
    void claimNoLongerReclaimsAnIdAlreadyMovedToFailedPermanentByTheRetryCap() {
        long id = BASE_ID + 52;
        insertState(id, "cdc", "FAILED_RETRYABLE", 5, 5);
        ledger.failExceededAttempts(List.of(id), 5);

        List<Long> claimed = ledger.claim(List.of(id));

        assertTrue(claimed.isEmpty(), "an id already moved to FAILED_PERMANENT by the retry cap must not be "
                + "claimable, which is exactly what stops it from nacking forever");
    }

    @Test
    void attemptsOfReturnsTheCurrentAttemptsCountOrZeroForAnUnknownId() {
        long id = BASE_ID + 55;
        insertState(id, "cdc", "FAILED_RETRYABLE", 3, 3);

        assertEquals(3, ledger.attemptsOf(id));
        assertEquals(0, ledger.attemptsOf(id + 1_000_000));
    }

    @Test
    void resetForUpdateClearsConsecutiveFailuresSinceAnUpdateIsNewWork() {
        long id = BASE_ID + 59;
        insertState(id, "cdc", "FAILED_RETRYABLE", 6, 4);

        ledger.resetForUpdate(id, 99L);

        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, consecutive_failures FROM migration_state WHERE source_id = ?", id);
        assertEquals("PENDING", row.get("status"));
        assertEquals(0, row.get("consecutive_failures"));
    }

    @Test
    void markDoneClearsConsecutiveFailuresOnSuccess() {
        long id = BASE_ID + 60;
        insertState(id, "backfill", "IN_FLIGHT", 4, 3);

        ledger.markDone(id, "checksum");

        assertEquals(0, jdbcTemplate.queryForObject(
                "SELECT consecutive_failures FROM migration_state WHERE source_id = ?", Integer.class, id));
    }

    @Test
    void markFailedIncrementsConsecutiveFailuresOnlyWhenItCountsTowardTheRetryCap() {
        long structural = BASE_ID + 61;
        long vendorOutage = BASE_ID + 62;
        insertState(structural, "cdc", "IN_FLIGHT", 1, 0);
        insertState(vendorOutage, "cdc", "IN_FLIGHT", 1, 0);

        ledger.markFailed(structural, "source row gone", false, true);
        ledger.markFailed(vendorOutage, "vendor down", false, false);

        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT consecutive_failures FROM migration_state WHERE source_id = ?", Integer.class, structural),
                "a structural failure retrying can never fix must increment consecutive_failures");
        assertEquals(0, jdbcTemplate.queryForObject(
                "SELECT consecutive_failures FROM migration_state WHERE source_id = ?", Integer.class, vendorOutage),
                "a vendor TRANSIENT/RATE_LIMITED failure must never increment consecutive_failures; the "
                        + "breaker and pausing already protect that case");
    }

    @Test
    void renewingAnInFlightClaimKeepsItFromBeingStolenOnceItsOriginalLeaseWouldHaveExpired() {
        long id = BASE_ID + 40;
        insertState(id, "backfill", "IN_FLIGHT", 1);
        jdbcTemplate.update("UPDATE migration_state SET updated_at = now() - interval '1 hour' "
                + "WHERE source_id = ?", id);

        int renewed = ledger.renewClaims(List.of(id));
        assertEquals(1, renewed);

        List<Long> claimed = ledger.claim(List.of(id));

        assertTrue(claimed.isEmpty(),
                "a claim renewed after its original lease would have expired must not be stolen");
    }

    @Test
    void renewClaimsLeavesRowsThatAreNotInFlightUntouched() {
        long done = BASE_ID + 41;
        long pending = BASE_ID + 42;
        insertState(done, "backfill", "DONE", 1);
        insertState(pending, "backfill", "PENDING", 0);

        int renewed = ledger.renewClaims(List.of(done, pending));

        assertEquals(0, renewed, "renewClaims must only touch rows currently IN_FLIGHT");
    }

    /**
     * Simulates two attempts at overlapping ids: one (attempt 1) already
     * holds source_id "foreign" in a live, unexpired claim, and a second
     * attempt claims a batch that happens to include both "own" and
     * "foreign". claim() correctly leaves "foreign" out of what it
     * returns, and the point of this test is that renewing exactly that
     * returned list, and nothing else, never touches "foreign": a caller
     * that renews whatever it actually claimed, rather than whatever ids
     * it was originally asked about, can never extend a claim it does not
     * hold.
     */
    @Test
    void renewingOnlyWhatWasActuallyClaimedNeverTouchesAnotherAttemptsLiveClaim() {
        long ownId = BASE_ID + 43;
        long foreignId = BASE_ID + 44;
        insertState(ownId, "backfill", "PENDING", 0);
        insertState(foreignId, "backfill", "IN_FLIGHT", 1);
        Timestamp foreignUpdatedAtBefore = jdbcTemplate.queryForObject(
                "SELECT updated_at FROM migration_state WHERE source_id = ?", Timestamp.class, foreignId);

        List<Long> claimed = ledger.claim(List.of(ownId, foreignId));
        assertEquals(List.of(ownId), claimed, "the still-live foreign claim must not be handed to this attempt");

        ledger.renewClaims(claimed);

        Timestamp foreignUpdatedAtAfter = jdbcTemplate.queryForObject(
                "SELECT updated_at FROM migration_state WHERE source_id = ?", Timestamp.class, foreignId);
        assertEquals(foreignUpdatedAtBefore, foreignUpdatedAtAfter,
                "renewing only what this attempt claimed must never touch another attempt's still-live claim");
    }

    private void insertState(long sourceId, String lane, String status, int attempts) {
        jdbcTemplate.update(
                "INSERT INTO migration_state (source_id, lane, status, attempts) VALUES (?, ?, ?, ?)",
                sourceId, lane, status, attempts);
    }

    private void insertState(long sourceId, String lane, String status, int attempts, int consecutiveFailures) {
        jdbcTemplate.update(
                "INSERT INTO migration_state (source_id, lane, status, attempts, consecutive_failures) "
                        + "VALUES (?, ?, ?, ?, ?)",
                sourceId, lane, status, attempts, consecutiveFailures);
    }

    private static List<Long> sorted(List<Long> ids) {
        return ids.stream().sorted().toList();
    }
}
