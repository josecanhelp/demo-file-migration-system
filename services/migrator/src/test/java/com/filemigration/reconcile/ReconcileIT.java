package com.filemigration.reconcile;

import com.filemigration.store.ObjectStore;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.client.RestClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the reconciler through the control plane's HTTP proxy, the real
 * migrator-worker process it forwards to, and the real Postgres, MySQL, and
 * MinIO docker compose starts, proving it can both report a clean migration
 * and catch the corruption each test below introduces on purpose: a stale
 * checksum column, OCR text that no longer
 * matches the source, a document row's object missing from the store
 * entirely, a document row's object present but holding the wrong bytes, a
 * source row with no matching document row on its own (breaking the raw
 * count agreement outright), that same condition instead compensated by an
 * unrelated document row with no matching source row (so the raw counts
 * still agree with each other even though nothing about the migration is
 * clean), the identical pair of conditions against the ledger table
 * instead of the document table, and a row currently FAILED_PERMANENT. A
 * reconciler that cannot fail any one of these is not verifying the check
 * it claims to. Not covered here: a document object that exists but
 * cannot be read for a reason other than being absent (reported in
 * unreadableObjects); there is no reliable way to make MinIO fail that
 * way on demand in this environment.
 *
 * Every corruption test writes its own row under a reserved id range, well
 * clear of the ids the real seeded migration and every other suite in this
 * project use, so it never disturbs that migration's own counts other than
 * by the exact corruption each test introduces on purpose. Every row is
 * removed again in @AfterEach, whether or not the test passed.
 */
class ReconcileIT {

    private static final long BASE_ID = 9_700_000L;

    /**
     * The lowest id any IT suite in this project reserves for its own
     * fixtures (GovernorIT, BackfillIT, this class, and others each use
     * their own range at or above this floor). aFullyMigratedSourceTableReconcilesClean
     * proves the real seeded corpus, ids below this floor, is clean; it
     * does not, and must not, depend on no other suite having left a row
     * behind somewhere above it; that is a separate concern those suites'
     * own cleanup is responsible for.
     */
    private static final long RESERVED_RANGE_FLOOR = 9_000_000L;

    private static final String BUCKET = "documents";
    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int READ_TIMEOUT_MS = 60000;

    private static HikariDataSource targetDataSource;
    private static HikariDataSource sourceDataSource;
    private static JdbcTemplate targetJdbc;
    private static JdbcTemplate sourceJdbc;
    private static S3Client s3Client;
    private static ObjectStore objectStore;
    private static RestClient httpClient;
    private static String reconcileUrl;

    private boolean wroteToSourceTable;

    @BeforeAll
    static void connect() {
        String targetUrl = System.getenv().getOrDefault("TARGET_JDBC_URL",
                "jdbc:postgresql://localhost:5432/targetdb");
        String targetUser = System.getenv().getOrDefault("TARGET_JDBC_USERNAME", "postgres");
        String targetPassword = System.getenv().getOrDefault("TARGET_JDBC_PASSWORD", "postgres");
        targetDataSource = DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .driverClassName("org.postgresql.Driver")
                .url(targetUrl)
                .username(targetUser)
                .password(targetPassword)
                .build();
        targetJdbc = new JdbcTemplate(targetDataSource);
        // Deliberately not wrapped in try/catch: if either real database or
        // the real migrator-worker process is not reachable, this fails
        // loudly here instead of being swallowed into a skip.
        targetJdbc.queryForObject("SELECT 1", Integer.class);

        String sourceUrl = System.getenv().getOrDefault("SOURCE_JDBC_URL",
                "jdbc:mysql://localhost:3306/sourcedb?useSSL=false&allowPublicKeyRetrieval=true");
        String sourceUser = System.getenv().getOrDefault("SOURCE_JDBC_USERNAME", "root");
        String sourcePassword = System.getenv().getOrDefault("SOURCE_JDBC_PASSWORD", "root");
        sourceDataSource = DataSourceBuilder.create()
                .type(HikariDataSource.class)
                .driverClassName("com.mysql.cj.jdbc.Driver")
                .url(sourceUrl)
                .username(sourceUser)
                .password(sourcePassword)
                .build();
        sourceJdbc = new JdbcTemplate(sourceDataSource);
        sourceJdbc.queryForObject("SELECT 1", Integer.class);

        String minioEndpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");
        String minioAccessKey = System.getenv().getOrDefault("MINIO_ACCESS_KEY", "minioadmin");
        String minioSecretKey = System.getenv().getOrDefault("MINIO_SECRET_KEY", "minioadmin");
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(minioEndpoint))
                .region(Region.of("us-east-1"))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(minioAccessKey, minioSecretKey)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
        objectStore = new ObjectStore(s3Client, BUCKET);

        // Defaults to the control plane's own published port and its proxy
        // route, not to migrator-worker directly: migrator-worker is the one
        // service `docker compose up --scale migrator-worker=N` runs more
        // than one of, so it publishes an ephemeral host port that changes
        // on every start and cannot be hard-coded here. The control plane's
        // port never changes and is never scaled, and its /api/reconcile
        // route forwards the request and hands back the worker's response
        // body unchanged, so hitting it is equivalent to hitting the worker
        // directly. RECONCILE_HEALTH_URL and RECONCILE_URL each override
        // independently for anyone who wants to bypass the proxy and talk
        // to one specific worker instance instead, at
        // http://localhost:<port>/actuator/health and .../internal/reconcile.
        String healthUrl = System.getenv().getOrDefault("RECONCILE_HEALTH_URL", "http://localhost:8080/health");
        reconcileUrl = System.getenv().getOrDefault("RECONCILE_URL", "http://localhost:8080/api/reconcile");
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(CONNECT_TIMEOUT_MS);
        requestFactory.setReadTimeout(READ_TIMEOUT_MS);
        httpClient = RestClient.builder()
                .requestFactory(requestFactory)
                .build();
        // Deliberately not wrapped in try/catch, same reasoning as the two
        // JDBC checks above: if the control plane (or whatever this was
        // overridden to point at) is not reachable, fail loudly here rather
        // than deep inside the first test's own assertions.
        httpClient.get().uri(healthUrl).retrieve().toBodilessEntity();
    }

    @AfterAll
    static void disconnect() {
        if (targetDataSource != null) {
            targetDataSource.close();
        }
        if (sourceDataSource != null) {
            sourceDataSource.close();
        }
        if (s3Client != null) {
            s3Client.close();
        }
    }

    /**
     * Deletes every row this suite could have written under its reserved id
     * range, then keeps re-deleting on a timer for a window longer than the
     * live migrator-worker container's own retry chain can possibly last:
     * every insert or delete this class makes against the real MySQL files
     * table is a real row change, so the real Debezium connector captures
     * it, and that same container's CDC consumer, sharing this database,
     * can claim and reprocess the id after this method's first delete has
     * already run, re-creating a migration_state row for an id this suite
     * just removed with no source row left to back it. With a 10 second
     * nack backoff and 5 allowed attempts, a single failing message's own
     * retry chain spans roughly 50 seconds after this suite's own change,
     * but the same consumer thread can also be working through other
     * messages queued ahead of it on the same partition first, pushing the
     * actual wall-clock delay out further still; the sweep below runs for
     * 100 seconds for margin against that, rather than merely reducing how
     * often the race is lost. This is a timing-based mitigation, not a
     * guarantee: a message queued deeply enough behind others can still
     * arrive after this window closes, which is why cleanUpReservedRows
     * cannot be the only thing standing between this suite and a stray
     * row; whatever runs this suite is still responsible for checking the
     * ledger table is genuinely clean afterward.
     */
    @AfterEach
    void cleanUpReservedRows() {
        // The extended sweep below only matters for a test that actually
        // wrote to the real MySQL files table; a test that never did has
        // no Debezium event in flight for it to race against, so one
        // plain delete pass is enough.
        Instant deadline = wroteToSourceTable ? Instant.now().plus(Duration.ofSeconds(100)) : Instant.now();
        do {
            targetJdbc.update("DELETE FROM document WHERE source_id >= ?", BASE_ID);
            targetJdbc.update("DELETE FROM migration_state WHERE source_id >= ?", BASE_ID);
            sourceJdbc.update("DELETE FROM files WHERE id >= ?", BASE_ID);
            if (Instant.now().isBefore(deadline)) {
                sleep(Duration.ofSeconds(3));
            }
        } while (Instant.now().isBefore(deadline));
        for (long id = BASE_ID + 1; id <= BASE_ID + 8; id++) {
            objectStore.delete(objectStore.keyFor(id));
        }
    }

    /**
     * The real seeded migration this docker compose stack already ran end
     * to end. Polls rather than asserting on the first response, since the
     * backfill lane may still be finishing when this suite starts. Every
     * list-based assertion is scoped to ids below RESERVED_RANGE_FLOOR
     * rather than to the endpoint's raw, unscoped lists: this suite runs
     * alongside others (GovernorIT, BackfillIT, CdcIT, and more) that
     * write their own fixtures directly against the same live database
     * under their own reserved ranges, and a row one of them left behind
     * is that suite's problem to clean up, not evidence that the actual
     * seeded corpus this test is responsible for is broken.
     *
     * The count assertions below are scoped the same way, but cannot be
     * read off the endpoint's own sourceCount/ledgerCount/documentCount
     * fields, since those are unscoped totals across the whole table; they
     * are computed here directly against the same three tables, filtered
     * to the same id floor, so a corpus that is short some ledger rows
     * cannot pass this test merely because another suite's extra rows
     * elsewhere happen to even the raw totals back out.
     */
    @Test
    void aFullyMigratedSourceTableReconcilesClean() {
        ReconcileResult result = waitUntilSeededCorpusClean(Duration.ofMinutes(3));

        long scopedSourceCount = countBelowFloor(sourceJdbc, "files", "id");
        long scopedLedgerCount = countBelowFloor(targetJdbc, "migration_state", "source_id");
        long scopedDocumentCount = countBelowFloor(targetJdbc, "document", "source_id");
        assertEquals(scopedSourceCount, scopedLedgerCount,
                "expected the seeded corpus's source and ledger row counts to agree; got source="
                        + scopedSourceCount + " ledger=" + scopedLedgerCount);
        assertEquals(scopedLedgerCount, scopedDocumentCount,
                "expected the seeded corpus's ledger and document row counts to agree; got ledger="
                        + scopedLedgerCount + " document=" + scopedDocumentCount);

        assertTrue(belowReservedRange(result.checksumMismatches()).isEmpty(),
                "expected no checksum mismatches in the seeded corpus; got: " + result.checksumMismatches());
        assertTrue(belowReservedRange(result.ocrMismatches()).isEmpty(),
                "expected no OCR mismatches in the seeded corpus; got: " + result.ocrMismatches());
        assertTrue(belowReservedRange(result.missingObjects()).isEmpty(),
                "expected no missing objects in the seeded corpus; got: " + result.missingObjects());
        assertTrue(belowReservedRange(unreadableObjectIds(result)).isEmpty(),
                "expected no unreadable objects in the seeded corpus; got: " + result.unreadableObjects());
        assertTrue(belowReservedRange(result.missingDocuments()).isEmpty(),
                "expected no source ids missing a document row in the seeded corpus; got: "
                        + result.missingDocuments());
        assertTrue(belowReservedRange(result.orphanDocuments()).isEmpty(),
                "expected no orphan document rows in the seeded corpus; got: " + result.orphanDocuments());
        assertTrue(belowReservedRange(result.missingLedgerRows()).isEmpty(),
                "expected no source ids missing a ledger row in the seeded corpus; got: "
                        + result.missingLedgerRows());
        assertTrue(belowReservedRange(result.orphanLedgerRows()).isEmpty(),
                "expected no orphan ledger rows in the seeded corpus; got: " + result.orphanLedgerRows());
        assertTrue(belowReservedRange(permanentFailureIds(result)).isEmpty(),
                "expected no permanent failures in the seeded corpus; got: " + result.permanentFailures());
    }

    @Test
    void corruptedOcrTextIsDetectedAndClearedOnRevert() {
        long id = BASE_ID + 1;
        byte[] content = "Invoice   Number\t42".getBytes(StandardCharsets.UTF_8);
        insertCleanRow(id, content);
        String correctOcrText = OcrTextTransform.extractText(content);

        targetJdbc.update("UPDATE document SET ocr_text = ? WHERE source_id = ?", "WRONG", id);

        ReconcileResult corrupted = reconcile();
        assertFalse(corrupted.clean());
        assertTrue(corrupted.ocrMismatches().contains(id),
                "expected " + id + " in ocrMismatches, got " + corrupted.ocrMismatches());

        targetJdbc.update("UPDATE document SET ocr_text = ? WHERE source_id = ?", correctOcrText, id);

        ReconcileResult restored = reconcile();
        assertFalse(restored.ocrMismatches().contains(id),
                "reverting the OCR text must clear the mismatch for this id");
    }

    @Test
    void corruptedChecksumIsDetectedAndClearedOnRevert() {
        long id = BASE_ID + 2;
        byte[] content = "Checksum Target Document".getBytes(StandardCharsets.UTF_8);
        insertCleanRow(id, content);
        String correctChecksum = sha256Hex(content);

        targetJdbc.update("UPDATE document SET checksum_sha256 = ? WHERE source_id = ?",
                "0".repeat(64), id);

        ReconcileResult corrupted = reconcile();
        assertFalse(corrupted.clean());
        assertTrue(corrupted.checksumMismatches().contains(id),
                "expected " + id + " in checksumMismatches, got " + corrupted.checksumMismatches());

        targetJdbc.update("UPDATE document SET checksum_sha256 = ? WHERE source_id = ?", correctChecksum, id);

        ReconcileResult restored = reconcile();
        assertFalse(restored.checksumMismatches().contains(id),
                "reverting the checksum column must clear the mismatch for this id");
    }

    @Test
    void missingMinioObjectIsDetectedAndClearedOnRestore() {
        long id = BASE_ID + 3;
        byte[] content = "Object Store Target".getBytes(StandardCharsets.UTF_8);
        insertCleanRow(id, content);
        String objectKey = objectStore.keyFor(id);

        objectStore.delete(objectKey);

        ReconcileResult corrupted = reconcile();
        assertFalse(corrupted.clean());
        assertTrue(corrupted.missingObjects().contains(id),
                "expected " + id + " in missingObjects, got " + corrupted.missingObjects());

        objectStore.put(objectKey, content, "text/plain");

        ReconcileResult restored = reconcile();
        assertFalse(restored.missingObjects().contains(id),
                "restoring the object must clear it from missingObjects");
    }

    @Test
    void documentRowDeletedWhileSourceAndLedgerRemainIsReportedAsAMissingDocument() {
        long id = BASE_ID + 4;
        byte[] content = "Orphaned Ledger Row".getBytes(StandardCharsets.UTF_8);
        insertCleanRow(id, content);

        targetJdbc.update("DELETE FROM document WHERE source_id = ?", id);

        ReconcileResult result = reconcile();

        assertFalse(result.clean());
        assertNotEquals(result.sourceCount(), result.documentCount(),
                "a source row with no matching document row must break the count agreement clean() requires");
        assertTrue(result.missingDocuments().contains(id),
                "expected " + id + " in missingDocuments, got " + result.missingDocuments());
    }

    /**
     * Overwrites the actual bytes stored in MinIO for a document whose
     * checksum column is left untouched and correct, proving the checksum
     * check reads and hashes the real stored object rather than only ever
     * comparing the checksum column against itself; the missing-object
     * test above proves objectStore.get() is called, but not that its
     * result is ever compared against anything.
     */
    @Test
    void corruptedStoredObjectBytesIsDetectedAndClearedOnRestore() {
        long id = BASE_ID + 6;
        byte[] content = "Stored Bytes Target Document".getBytes(StandardCharsets.UTF_8);
        insertCleanRow(id, content);
        String objectKey = objectStore.keyFor(id);

        objectStore.put(objectKey, "These are not the original bytes at all".getBytes(StandardCharsets.UTF_8),
                "text/plain");

        ReconcileResult corrupted = reconcile();
        assertFalse(corrupted.clean());
        assertTrue(corrupted.checksumMismatches().contains(id),
                "expected " + id + " in checksumMismatches when the stored object bytes disagree with both "
                        + "the source blob and the checksum column, got " + corrupted.checksumMismatches());

        objectStore.put(objectKey, content, "text/plain");

        ReconcileResult restored = reconcile();
        assertFalse(restored.checksumMismatches().contains(id),
                "restoring the correct bytes in the object store must clear the mismatch for this id");
    }

    /**
     * Creates a migration_state row that is currently FAILED_PERMANENT
     * with no source row behind it at all, the established way a row
     * reaches that status, and proves permanentFailures actually reports
     * it with its recorded error; nothing else in this suite ever produces
     * a row in that status.
     */
    @Test
    void permanentFailureWithNoSourceRowIsReportedAndClearedOnRemoval() {
        long id = BASE_ID + 7;
        String error = "Source record no longer exists for a claimed id";

        // Idempotent seeding: this is a plain INSERT, not an upsert, so a
        // row an earlier aborted run left behind on this exact id (the
        // cleanup below never ran because that run never got there) would
        // otherwise fail this insert on a duplicate key before the test
        // ever gets to run.
        targetJdbc.update("DELETE FROM migration_state WHERE source_id = ?", id);
        targetJdbc.update("INSERT INTO migration_state (source_id, lane, status, source_version, "
                        + "last_error, consecutive_failures) VALUES (?, ?, ?, ?, ?, ?)",
                id, "cdc", "FAILED_PERMANENT", 0L, error, 5);

        ReconcileResult result = reconcile();

        assertFalse(result.clean());
        assertTrue(findPermanentFailure(result, id).isPresent(),
                "expected " + id + " in permanentFailures, got " + result.permanentFailures());
        assertEquals(error, findPermanentFailure(result, id).get().error());

        targetJdbc.update("DELETE FROM migration_state WHERE source_id = ?", id);

        ReconcileResult restored = reconcile();
        assertTrue(findPermanentFailure(restored, id).isEmpty(),
                "removing the row must clear it from permanentFailures");
    }

    /**
     * Constructs the exact scenario a cardinality-only check cannot see:
     * one real, already-migrated source row loses its document row (a
     * missing document), while a separate, unrelated id gains a document
     * row with no source row behind it (an orphan document). sourceCount,
     * ledgerCount, and documentCount all still agree with each other
     * throughout, since one row disappeared from the document side and
     * another took its place; only checking set membership by id, not
     * just the totals, can tell the two ids apart from a genuinely clean
     * migration.
     *
     * The missing-document side reuses an already-seeded real corpus id
     * rather than deleting a fresh MySQL row: deleting straight from MySQL
     * would be captured by the real Debezium connector and raced by the
     * live migrator-worker's own CDC consumer, which tombstones a deleted
     * source id's document and ledger rows together, undoing exactly the
     * condition this test needs to hold still long enough to observe.
     * Removing only this id's document row, leaving its real MySQL row
     * and its DONE ledger row untouched, produces the identical
     * missingDocuments condition without racing anything live; the only
     * thing that could ever re-create its document row on its own is the
     * backfill coordinator's periodic replanning, whose 30 second cycle is
     * comfortably longer than this test's own window before it restores
     * the row itself.
     *
     * The count assertions below compare against a baseline captured by
     * this test immediately before it makes any change, not against each
     * other in absolute terms: sourceCount, ledgerCount, and documentCount
     * are unscoped totals across the whole table (see the class-level
     * comment on reconcile() and the RESERVED_RANGE_FLOOR javadoc), so
     * another suite's own fixture, seeded under its own reserved range and
     * not yet cleaned up, can leave sourceCount and ledgerCount genuinely
     * unequal before this test ever runs. Diffing against this test's own
     * baseline is what proves the property this test actually exists to
     * prove, that removing one document row and adding an unrelated one
     * nets to zero change in documentCount, regardless of what documentCount
     * happened to be before either change.
     */
    @Test
    void aSourceRowMissingItsDocumentCompensatedByAnOrphanDocumentMustNotReadAsClean() {
        long realSeededId = 17L;
        long orphanId = BASE_ID + 5;

        ReconcileResult baseline = reconcile();
        long ledgerCountBefore = baseline.ledgerCount();
        long documentCountBefore = baseline.documentCount();

        Map<String, Object> originalDocument = targetJdbc.queryForMap(
                "SELECT filename, content_type, object_key, byte_size, checksum_sha256, ocr_text, "
                        + "ocr_confidence, ocr_page_count, ocr_vendor_job_id FROM document WHERE source_id = ?",
                realSeededId);

        targetJdbc.update("DELETE FROM document WHERE source_id = ?", realSeededId);

        // Deliberately no migration_state row for orphanId: orphanDocuments
        // is a source/document set-membership check, not a ledger one, so
        // a bare document row with nothing else backing it is enough to
        // trigger it, and leaving ledger untouched here is what keeps
        // ledgerCount itself unaffected by this scenario.
        //
        // The delete before it is idempotent seeding: this is a plain
        // INSERT, not an upsert, so a row an earlier aborted run left
        // behind on this exact orphanId (its own finally block below never
        // ran because that run never got there) would otherwise fail this
        // insert on a duplicate key before the test ever gets to run.
        targetJdbc.update("DELETE FROM document WHERE source_id = ?", orphanId);
        targetJdbc.update("INSERT INTO document (source_id, filename, content_type, object_key, byte_size, "
                        + "checksum_sha256, ocr_text, ocr_confidence, ocr_page_count, ocr_vendor_job_id) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                orphanId, "orphan-" + orphanId + ".txt", "text/plain", objectStore.keyFor(orphanId), 4,
                "0".repeat(64), "ORPHAN", 0.9, 1, "job_orphan_" + orphanId);

        try {
            ReconcileResult result = reconcile();

            assertEquals(ledgerCountBefore, result.ledgerCount(),
                    "this scenario never touches the ledger table, so ledgerCount must be exactly what this "
                            + "test observed before it made any change");
            assertEquals(documentCountBefore, result.documentCount(),
                    "this is exactly the compensating case: one row removed from the document table and one "
                            + "added elsewhere must net back to the documentCount this test observed before it "
                            + "made any change");
            assertFalse(result.clean(),
                    "a source row missing its document, exactly compensated by an unrelated orphan "
                            + "document row, must never be masked by matching counts alone");
            assertTrue(result.missingDocuments().contains(realSeededId),
                    "expected " + realSeededId + " in missingDocuments, got " + result.missingDocuments());
            assertTrue(result.orphanDocuments().contains(orphanId),
                    "expected " + orphanId + " in orphanDocuments, got " + result.orphanDocuments());
        } finally {
            targetJdbc.update("DELETE FROM document WHERE source_id = ?", orphanId);
            targetJdbc.update("INSERT INTO document (source_id, filename, content_type, object_key, byte_size, "
                            + "checksum_sha256, ocr_text, ocr_confidence, ocr_page_count, ocr_vendor_job_id) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    realSeededId, originalDocument.get("filename"), originalDocument.get("content_type"),
                    originalDocument.get("object_key"), originalDocument.get("byte_size"),
                    originalDocument.get("checksum_sha256"), originalDocument.get("ocr_text"),
                    originalDocument.get("ocr_confidence"), originalDocument.get("ocr_page_count"),
                    originalDocument.get("ocr_vendor_job_id"));
        }

        ReconcileResult restored = reconcile();
        assertFalse(restored.missingDocuments().contains(realSeededId),
                "restoring the document row must clear it from missingDocuments");
        assertFalse(restored.orphanDocuments().contains(orphanId),
                "removing the orphan row must clear it from orphanDocuments");
    }

    /**
     * The same scenario as the missing/orphan document test above, one
     * table over: one real, already-migrated source row loses its
     * migration_state row (a missing ledger row), while a separate,
     * unrelated id gains a migration_state row with no source row behind
     * it (an orphan ledger row). sourceCount and ledgerCount still agree
     * with each other throughout, since one row disappeared from the
     * ledger table and another took its place; a cardinality-only check
     * against the ledger table is exactly as blind to this as it was to
     * the document-table version.
     *
     * Neither side of this touches the MySQL files table, so there is no
     * Debezium event, and no live consumer, to race against here.
     *
     * As in the document-table version above, the count assertion compares
     * against a baseline this test captures for itself immediately before
     * making any change, not sourceCount against ledgerCount in absolute
     * terms: both are unscoped totals across the whole table, so another
     * suite's own not-yet-cleaned-up fixture can leave them genuinely
     * unequal before this test ever runs, which has nothing to do with
     * whether this test's own compensating change nets to zero.
     */
    @Test
    void aSourceRowMissingItsLedgerRowCompensatedByAnOrphanLedgerRowMustNotReadAsClean() {
        long realSeededId = 41L;
        long orphanId = BASE_ID + 8;

        long ledgerCountBefore = reconcile().ledgerCount();

        Map<String, Object> originalLedgerRow = targetJdbc.queryForMap(
                "SELECT lane, status, source_version, checksum_sha256, consecutive_failures, attempts, "
                        + "last_error FROM migration_state WHERE source_id = ?",
                realSeededId);

        targetJdbc.update("DELETE FROM migration_state WHERE source_id = ?", realSeededId);

        // Idempotent seeding: this is a plain INSERT, not an upsert, so a
        // row an earlier aborted run left behind on this exact orphanId
        // (its own finally block below never ran because that run never
        // got there) would otherwise fail this insert on a duplicate key
        // before the test ever gets to run.
        targetJdbc.update("DELETE FROM migration_state WHERE source_id = ?", orphanId);
        targetJdbc.update("INSERT INTO migration_state (source_id, lane, status, source_version, "
                        + "checksum_sha256, consecutive_failures) VALUES (?, ?, ?, ?, ?, ?)",
                orphanId, "backfill", "DONE", 0L, "0".repeat(64), 0);

        try {
            ReconcileResult result = reconcile();

            assertEquals(ledgerCountBefore, result.ledgerCount(),
                    "this is exactly the compensating case: one row removed from the ledger table and one "
                            + "added elsewhere must net back to the ledgerCount this test observed before it "
                            + "made any change");
            assertFalse(result.clean(),
                    "a source row missing its ledger row, exactly compensated by an unrelated orphan "
                            + "ledger row, must never be masked by matching counts alone");
            assertTrue(result.missingLedgerRows().contains(realSeededId),
                    "expected " + realSeededId + " in missingLedgerRows, got " + result.missingLedgerRows());
            assertTrue(result.orphanLedgerRows().contains(orphanId),
                    "expected " + orphanId + " in orphanLedgerRows, got " + result.orphanLedgerRows());
        } finally {
            targetJdbc.update("DELETE FROM migration_state WHERE source_id = ?", orphanId);
            targetJdbc.update("INSERT INTO migration_state (source_id, lane, status, source_version, "
                            + "checksum_sha256, consecutive_failures, attempts, last_error) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    realSeededId, originalLedgerRow.get("lane"), originalLedgerRow.get("status"),
                    originalLedgerRow.get("source_version"), originalLedgerRow.get("checksum_sha256"),
                    originalLedgerRow.get("consecutive_failures"), originalLedgerRow.get("attempts"),
                    originalLedgerRow.get("last_error"));
        }

        ReconcileResult restored = reconcile();
        assertFalse(restored.missingLedgerRows().contains(realSeededId),
                "restoring the ledger row must clear it from missingLedgerRows");
        assertFalse(restored.orphanLedgerRows().contains(orphanId),
                "removing the orphan row must clear it from orphanLedgerRows");
    }

    private ReconcileResult reconcile() {
        return httpClient.post().uri(reconcileUrl).retrieve().body(ReconcileResult.class);
    }

    private ReconcileResult waitUntilSeededCorpusClean(Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        ReconcileResult last = reconcile();
        while (!seededCorpusIsClean(last) && Instant.now().isBefore(deadline)) {
            sleep(Duration.ofSeconds(2));
            last = reconcile();
        }
        return last;
    }

    private static boolean seededCorpusIsClean(ReconcileResult result) {
        return belowReservedRange(result.checksumMismatches()).isEmpty()
                && belowReservedRange(result.ocrMismatches()).isEmpty()
                && belowReservedRange(result.missingObjects()).isEmpty()
                && belowReservedRange(unreadableObjectIds(result)).isEmpty()
                && belowReservedRange(result.missingDocuments()).isEmpty()
                && belowReservedRange(result.orphanDocuments()).isEmpty()
                && belowReservedRange(result.missingLedgerRows()).isEmpty()
                && belowReservedRange(result.orphanLedgerRows()).isEmpty()
                && belowReservedRange(permanentFailureIds(result)).isEmpty();
    }

    private static long countBelowFloor(JdbcTemplate jdbc, String table, String idColumn) {
        Long count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM " + table + " WHERE " + idColumn + " < ?", Long.class, RESERVED_RANGE_FLOOR);
        return count == null ? 0L : count;
    }

    private static List<Long> belowReservedRange(List<Long> ids) {
        List<Long> filtered = new ArrayList<>();
        for (Long id : ids) {
            if (id < RESERVED_RANGE_FLOOR) {
                filtered.add(id);
            }
        }
        return filtered;
    }

    private static List<Long> permanentFailureIds(ReconcileResult result) {
        List<Long> ids = new ArrayList<>();
        for (ReconcileResult.PermanentFailure failure : result.permanentFailures()) {
            ids.add(failure.id());
        }
        return ids;
    }

    private static List<Long> unreadableObjectIds(ReconcileResult result) {
        List<Long> ids = new ArrayList<>();
        for (ReconcileResult.UnreadableObject unreadable : result.unreadableObjects()) {
            ids.add(unreadable.id());
        }
        return ids;
    }

    private static Optional<ReconcileResult.PermanentFailure> findPermanentFailure(ReconcileResult result, long id) {
        for (ReconcileResult.PermanentFailure failure : result.permanentFailures()) {
            if (failure.id() == id) {
                return Optional.of(failure);
            }
        }
        return Optional.empty();
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Inserts a source row, a DONE ledger row, a matching document row, and
     * the actual object bytes in MinIO, all internally consistent, so a
     * test can corrupt exactly one of those four places on purpose and
     * know the reconciler flagged that corruption specifically.
     *
     * The ledger and document writes upsert rather than insert outright:
     * this id is a real row in the real MySQL files table the moment the
     * first statement below commits, so the live migrator-coordinator and
     * migrator-worker containers backing this same compose stack are free
     * to notice it and migrate it through the real pipeline before this
     * method's own writes run. Either writer reaching source_id first is
     * fine, since both are writing the same checksum and OCR text computed
     * from the same content; an outright INSERT would instead fail the
     * whole test on a duplicate key the moment the real pipeline won that
     * race, which is a timing accident, not a fixture problem.
     *
     * The delete right before that INSERT is a different kind of guard,
     * against a different kind of duplicate key: idempotent seeding for a
     * row an earlier aborted run left behind on this exact id, since
     * nothing but this method ever writes a source row under this
     * reserved range and a leftover row here would otherwise fail this
     * INSERT before the test ever gets to run, indistinguishable at that
     * point from the live-pipeline race described above.
     */
    private void insertCleanRow(long id, byte[] content) {
        wroteToSourceTable = true;
        String filename = "reconcile-" + id + ".txt";
        String contentType = "text/plain";
        sourceJdbc.update("DELETE FROM files WHERE id = ?", id);
        sourceJdbc.update("INSERT INTO files (id, filename, content_type, content, byte_size) "
                + "VALUES (?, ?, ?, ?, ?)", id, filename, contentType, content, content.length);

        String checksum = sha256Hex(content);
        String ocrText = OcrTextTransform.extractText(content);
        String objectKey = objectStore.keyFor(id);
        objectStore.put(objectKey, content, contentType);

        targetJdbc.update("INSERT INTO migration_state (source_id, lane, status, source_version, "
                + "checksum_sha256, consecutive_failures) VALUES (?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (source_id) DO UPDATE SET lane = EXCLUDED.lane, status = EXCLUDED.status, "
                + "source_version = EXCLUDED.source_version, checksum_sha256 = EXCLUDED.checksum_sha256, "
                + "consecutive_failures = EXCLUDED.consecutive_failures",
                id, "backfill", "DONE", 0L, checksum, 0);

        targetJdbc.update("INSERT INTO document (source_id, filename, content_type, object_key, byte_size, "
                + "checksum_sha256, ocr_text, ocr_confidence, ocr_page_count, ocr_vendor_job_id) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (source_id) DO UPDATE SET filename = EXCLUDED.filename, "
                + "content_type = EXCLUDED.content_type, object_key = EXCLUDED.object_key, "
                + "byte_size = EXCLUDED.byte_size, checksum_sha256 = EXCLUDED.checksum_sha256, "
                + "ocr_text = EXCLUDED.ocr_text, ocr_confidence = EXCLUDED.ocr_confidence, "
                + "ocr_page_count = EXCLUDED.ocr_page_count, ocr_vendor_job_id = EXCLUDED.ocr_vendor_job_id",
                id, filename, contentType, objectKey, content.length, checksum, ocrText, 0.95, 1,
                "job_reconcile_it_" + id);
    }

    private static String sha256Hex(byte[] content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content);
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
