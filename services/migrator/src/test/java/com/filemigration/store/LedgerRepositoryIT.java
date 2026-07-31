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
        ledger = new LedgerRepository(jdbcTemplate);
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

    private void insertState(long sourceId, String lane, String status, int attempts) {
        jdbcTemplate.update(
                "INSERT INTO migration_state (source_id, lane, status, attempts) VALUES (?, ?, ?, ?)",
                sourceId, lane, status, attempts);
    }

    private static List<Long> sorted(List<Long> ids) {
        return ids.stream().sorted().toList();
    }
}
