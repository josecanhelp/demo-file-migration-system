package com.filemigration.store;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;

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

    private void insertState(long sourceId, String lane, String status, int attempts) {
        jdbcTemplate.update(
                "INSERT INTO migration_state (source_id, lane, status, attempts) VALUES (?, ?, ?, ?)",
                sourceId, lane, status, attempts);
    }

    private static List<Long> sorted(List<Long> ids) {
        return ids.stream().sorted().toList();
    }
}
