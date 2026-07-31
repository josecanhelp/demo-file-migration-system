package com.filemigration.store;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises LedgerRepository against a real Postgres instance instead of
 * mocking the JDBC layer, since the claim query is the one statement every
 * worker's safety depends on and a mock cannot prove what a database with
 * concurrent-safe semantics actually does with it.
 *
 * The container is started once for the class and the ledger tables are
 * cleared between tests so each test starts from a known state. If no
 * Docker daemon is reachable from this test run, every test here is
 * skipped rather than failed or replaced with a mock, and the reason is
 * reported so it is visible rather than silently passed over.
 */
class LedgerRepositoryIntegrationTest {

    private static PostgreSQLContainer<?> postgres;
    private static JdbcTemplate jdbcTemplate;
    private static LedgerRepository ledger;

    @BeforeAll
    static void startDatabase() {
        Assumptions.assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                "No Docker daemon reachable from this test run; skipping the Postgres-backed "
                        + "LedgerRepository tests. Run with access to a Docker daemon "
                        + "(for example, mount /var/run/docker.sock into the test container) "
                        + "to exercise them.");

        postgres = new PostgreSQLContainer<>("postgres:16")
                .withInitScript("db/01-schema.sql");
        postgres.start();

        DataSource dataSource = DataSourceBuilder.create()
                .url(postgres.getJdbcUrl())
                .username(postgres.getUsername())
                .password(postgres.getPassword())
                .driverClassName(postgres.getDriverClassName())
                .build();
        jdbcTemplate = new JdbcTemplate(dataSource);
        ledger = new LedgerRepository(jdbcTemplate);
    }

    @AfterAll
    static void stopDatabase() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @BeforeEach
    void clearLedger() {
        jdbcTemplate.update("TRUNCATE TABLE migration_event, document, migration_state, "
                + "backfill_checkpoint");
    }

    @Test
    void claimOnPendingRowReturnsIdAndMovesItToInFlight() {
        insertState(1L, "cdc", "PENDING", 0);

        List<Long> claimed = ledger.claim(List.of(1L));

        assertEquals(List.of(1L), claimed);
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, attempts FROM migration_state WHERE source_id = 1");
        assertEquals("IN_FLIGHT", row.get("status"));
        assertEquals(1, row.get("attempts"));
    }

    @Test
    void secondClaimOnAlreadyInFlightRowReturnsNothing() {
        insertState(2L, "cdc", "PENDING", 0);
        List<Long> firstClaim = ledger.claim(List.of(2L));
        assertEquals(List.of(2L), firstClaim);

        List<Long> secondClaim = ledger.claim(List.of(2L));

        assertTrue(secondClaim.isEmpty(), "a row already IN_FLIGHT must not be claimable again");
    }

    @Test
    void failedRetryableRowStaysClaimable() {
        insertState(3L, "backfill", "FAILED_RETRYABLE", 2);

        List<Long> claimed = ledger.claim(List.of(3L));

        assertEquals(List.of(3L), claimed);
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, attempts FROM migration_state WHERE source_id = 3");
        assertEquals("IN_FLIGHT", row.get("status"));
        assertEquals(3, row.get("attempts"));
    }

    @Test
    void claimReturnsOnlyTheClaimableIdsFromAMixedBatch() {
        insertState(10L, "cdc", "PENDING", 0);
        insertState(11L, "cdc", "DONE", 1);
        insertState(12L, "cdc", "IN_FLIGHT", 1);
        insertState(13L, "cdc", "FAILED_RETRYABLE", 1);
        insertState(14L, "cdc", "FAILED_PERMANENT", 4);

        List<Long> claimed = ledger.claim(List.of(10L, 11L, 12L, 13L, 14L));

        assertEquals(List.of(10L, 13L), sorted(claimed));
    }

    @Test
    void seedPendingDoesNotDuplicateOrOverwriteAnExistingRow() {
        int firstInsert = ledger.seedPending(List.of(20L), "cdc", Map.of(20L, Instant.now()));
        assertEquals(1, firstInsert);

        // Move the row along so a naive re-seed would be an observable regression
        // if it reset state back to PENDING instead of leaving it alone.
        ledger.claim(List.of(20L));

        int secondInsert = ledger.seedPending(List.of(20L), "backfill", Map.of(20L, Instant.now()));

        assertEquals(0, secondInsert, "seeding an id that is already tracked must insert nothing");
        Long rowCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM migration_state WHERE source_id = 20", Long.class);
        assertEquals(1L, rowCount);
        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT status, lane FROM migration_state WHERE source_id = 20");
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
