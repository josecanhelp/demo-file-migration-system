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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the reconciler against the real migrator-worker process and the
 * real Postgres, MySQL, and MinIO docker compose starts, proving it can
 * both report a clean migration and, just as importantly, catch every kind
 * of corruption it claims to check for. A reconciler that cannot fail this
 * class is not verifying anything.
 *
 * Every corruption test writes its own row under a reserved id range, well
 * clear of the ids the real seeded migration uses, so it never disturbs
 * that migration's own counts other than by the exact corruption each test
 * introduces on purpose. Every row is removed again in @AfterEach, whether
 * or not the test passed.
 */
class ReconcileIT {

    private static final long BASE_ID = 9_700_000L;
    private static final String BUCKET = "documents";
    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int READ_TIMEOUT_MS = 60000;

    private static HikariDataSource targetDataSource;
    private static HikariDataSource sourceDataSource;
    private static JdbcTemplate targetJdbc;
    private static JdbcTemplate sourceJdbc;
    private static S3Client s3Client;
    private static ObjectStore objectStore;
    private static RestClient migratorClient;

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

        String migratorBaseUrl = System.getenv().getOrDefault("MIGRATOR_WORKER_URL", "http://localhost:8082");
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(CONNECT_TIMEOUT_MS);
        requestFactory.setReadTimeout(READ_TIMEOUT_MS);
        migratorClient = RestClient.builder()
                .baseUrl(migratorBaseUrl)
                .requestFactory(requestFactory)
                .build();
        migratorClient.get().uri("/actuator/health").retrieve().toBodilessEntity();
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
     * live migrator-worker container's own CDC nack backoff (10 seconds by
     * default): every insert or delete this class makes against the real
     * MySQL files table is a real row change, so the real Debezium
     * connector captures it, and that same container's CDC consumer,
     * sharing this database, can claim and process the id after this
     * method's first delete has already run, re-creating a migration_state
     * row for an id this suite just removed with no source row left to
     * back it. A single pass, or even a short one, can finish before that
     * consumer's next retry lands; sweeping past its backoff window is
     * what actually outlasts it, rather than merely reducing how often the
     * race is lost.
     */
    @AfterEach
    void cleanUpReservedRows() {
        // The extended sweep below only matters for a test that actually
        // wrote to the real MySQL files table; the read-only clean-run
        // test never did, so there is no Debezium event in flight for it
        // to race against and one plain delete pass is enough.
        Instant deadline = wroteToSourceTable ? Instant.now().plus(Duration.ofSeconds(25)) : Instant.now();
        do {
            targetJdbc.update("DELETE FROM document WHERE source_id >= ?", BASE_ID);
            targetJdbc.update("DELETE FROM migration_state WHERE source_id >= ?", BASE_ID);
            sourceJdbc.update("DELETE FROM files WHERE id >= ?", BASE_ID);
            if (Instant.now().isBefore(deadline)) {
                sleep(Duration.ofSeconds(2));
            }
        } while (Instant.now().isBefore(deadline));
        for (long id = BASE_ID + 1; id <= BASE_ID + 4; id++) {
            objectStore.delete(objectStore.keyFor(id));
        }
    }

    /**
     * The real seeded migration this docker compose stack already ran end
     * to end. Polls rather than asserting on the first response, since the
     * backfill lane may still be finishing when this suite starts.
     */
    @Test
    void aFullyMigratedSourceTableReconcilesClean() {
        ReconcileResult result = waitUntilClean(Duration.ofMinutes(3));

        assertTrue(result.clean(), "expected the already-completed migration to reconcile clean; got: " + result);
        assertTrue(result.checksumMismatches().isEmpty());
        assertTrue(result.ocrMismatches().isEmpty());
        assertTrue(result.missingObjects().isEmpty());
        assertTrue(result.permanentFailures().isEmpty());
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
    void documentRowDeletedWhileSourceAndLedgerRemainCausesACountMismatch() {
        long id = BASE_ID + 4;
        byte[] content = "Orphaned Ledger Row".getBytes(StandardCharsets.UTF_8);
        insertCleanRow(id, content);

        targetJdbc.update("DELETE FROM document WHERE source_id = ?", id);

        ReconcileResult result = reconcile();

        assertFalse(result.clean());
        assertNotEquals(result.sourceCount(), result.documentCount(),
                "a source row with no matching document row must break the count agreement clean() requires");
    }

    private ReconcileResult reconcile() {
        return migratorClient.post().uri("/internal/reconcile").retrieve().body(ReconcileResult.class);
    }

    private ReconcileResult waitUntilClean(Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        ReconcileResult last = reconcile();
        while (!last.clean() && Instant.now().isBefore(deadline)) {
            sleep(Duration.ofSeconds(2));
            last = reconcile();
        }
        return last;
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
     */
    private void insertCleanRow(long id, byte[] content) {
        wroteToSourceTable = true;
        String filename = "reconcile-" + id + ".txt";
        String contentType = "text/plain";
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
