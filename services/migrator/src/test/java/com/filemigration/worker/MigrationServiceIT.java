package com.filemigration.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.governor.TestGovernorFactory;
import com.filemigration.store.DocumentRepository;
import com.filemigration.store.EventRepository;
import com.filemigration.store.LedgerRepository;
import com.filemigration.store.ObjectStore;
import com.filemigration.store.SourceFileRepository;
import com.filemigration.vendor.OcrResult;
import com.filemigration.vendor.VendorClient;
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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises MigrationService end to end against the real Postgres, MySQL,
 * MinIO, and vendor-mock services docker compose starts, proving a source
 * row actually turns into an object in the target store plus a document
 * row carrying OCR metadata and a checksum that matches what was stored,
 * not merely that the collaborators can be called with fakes standing in
 * for them.
 *
 * Every row this test writes uses a source id at or above the reserved
 * range below, and every one of those rows is removed after each test. If
 * any real dependency is unreachable, connecting fails loudly here rather
 * than being swallowed into a skip.
 */
class MigrationServiceIT {

    private static final long BASE_ID = 9_000_000L;
    private static final String BUCKET = "documents";
    private static final long LEASE_SECONDS = 300L;
    private static final long CLAIM_RENEW_INTERVAL_SECONDS = 10L;
    private static final int WORKER_CONCURRENCY = 1;
    private static final int MAX_RETRY_ATTEMPTS = 5;

    private static HikariDataSource targetDataSource;
    private static HikariDataSource sourceDataSource;
    private static JdbcTemplate targetJdbc;
    private static JdbcTemplate sourceJdbc;
    private static S3Client s3Client;
    private static RestClient vendorAdminClient;
    private static MigrationService service;

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

        String vendorBaseUrl = System.getenv().getOrDefault("VENDOR_BASE_URL", "http://localhost:8088");
        vendorAdminClient = RestClient.create(vendorBaseUrl);
        vendorAdminClient.get().uri("/health").retrieve().toBodilessEntity();
        setVendorMode("healthy");

        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(2000);
        requestFactory.setReadTimeout(10000);
        RestClient vendorRestClient = RestClient.builder()
                .baseUrl(vendorBaseUrl)
                .requestFactory(requestFactory)
                .build();

        ObjectMapper objectMapper = new ObjectMapper();
        LedgerRepository ledger = new LedgerRepository(targetJdbc, LEASE_SECONDS);
        SourceFileRepository sourceRepo = new SourceFileRepository(sourceJdbc);
        ObjectStore objectStore = new ObjectStore(s3Client, BUCKET);
        DocumentRepository documentRepo = new DocumentRepository(targetJdbc);
        EventRepository eventRepo = new EventRepository(targetJdbc);
        VendorClient vendorClient = new VendorClient(vendorRestClient, objectMapper);
        service = new MigrationService(ledger, sourceRepo, objectStore, documentRepo, eventRepo, vendorClient,
                TestGovernorFactory.passthrough(), objectMapper, CLAIM_RENEW_INTERVAL_SECONDS, WORKER_CONCURRENCY,
                MAX_RETRY_ATTEMPTS);
    }

    @AfterAll
    static void disconnectAndResetVendor() {
        try {
            setVendorMode("healthy");
        } finally {
            if (service != null) {
                service.shutdown();
            }
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
    }

    @AfterEach
    void cleanUpReservedRows() {
        targetJdbc.update("DELETE FROM migration_event WHERE source_id >= ?", BASE_ID);
        targetJdbc.update("DELETE FROM document WHERE source_id >= ?", BASE_ID);
        targetJdbc.update("DELETE FROM migration_state WHERE source_id >= ?", BASE_ID);
        sourceJdbc.update("DELETE FROM files WHERE id >= ?", BASE_ID);
    }

    @Test
    void migratesARealFileFromSourceThroughToTheTargetStore() throws Exception {
        long id = BASE_ID + 1;
        byte[] content = "INVOICE 00000001".getBytes(StandardCharsets.UTF_8);
        insertSourceFile(id, "invoice.txt", "text/plain", content);
        seedPending(id, "backfill");

        MigrationOutcome outcome = service.migrate(List.of(id), "backfill");

        assertEquals(1, outcome.done());
        assertEquals(0, outcome.permanentFailures());
        assertEquals(0, outcome.retryable());

        byte[] stored = s3Client.getObject(GetObjectRequest.builder()
                .bucket(BUCKET)
                .key("files/" + id)
                .build()).readAllBytes();
        assertArrayEquals(content, stored, "the object written to MinIO must match the source content");

        String expectedChecksum = sha256Hex(content);
        Map<String, Object> document = targetJdbc.queryForMap(
                "SELECT filename, content_type, object_key, byte_size, checksum_sha256, ocr_text "
                        + "FROM document WHERE source_id = ?", id);
        assertEquals("invoice.txt", document.get("filename"));
        assertEquals("text/plain", document.get("content_type"));
        assertEquals("files/" + id, document.get("object_key"));
        assertEquals(content.length, document.get("byte_size"));
        assertEquals(expectedChecksum, document.get("checksum_sha256"));
        assertTrue(document.get("ocr_text") != null && !document.get("ocr_text").toString().isBlank());

        Map<String, Object> state = targetJdbc.queryForMap(
                "SELECT status, checksum_sha256 FROM migration_state WHERE source_id = ?", id);
        assertEquals("DONE", state.get("status"));
        assertEquals(expectedChecksum, state.get("checksum_sha256"));
    }

    @Test
    void resumesFromACachedOcrPayloadWithoutCallingTheVendorAgain() throws Exception {
        long id = BASE_ID + 2;
        byte[] content = "PRE-COMPUTED OCR".getBytes(StandardCharsets.UTF_8);
        insertSourceFile(id, "resumed.txt", "text/plain", content);
        seedPending(id, "backfill");
        // Simulate a worker that crashed after paying for OCR but before
        // writing the document row: the object already exists, and the
        // ledger already holds the OCR payload, but status is still
        // OCR_DONE rather than DONE.
        s3Client.putObject(PutObjectRequest.builder().bucket(BUCKET).key("files/" + id).contentType("text/plain")
                .build(), RequestBody.fromBytes(content));
        String cachedPayload = new ObjectMapper().writeValueAsString(
                new OcrResult(id, "PRE-COMPUTED OCR", 0.95, 1, "job-resumed"));
        // Backdate updated_at so this row's claim lease reads as expired,
        // matching what a real crash-then-resume looks like instead of a
        // worker still actively holding the row.
        targetJdbc.update("UPDATE migration_state SET status = 'OCR_DONE', ocr_payload = ?::jsonb, "
                + "updated_at = now() - interval '1 hour' WHERE source_id = ?", cachedPayload, id);

        MigrationOutcome outcome = service.migrate(List.of(id), "backfill");

        assertEquals(1, outcome.done());
        Map<String, Object> state = targetJdbc.queryForMap(
                "SELECT status FROM migration_state WHERE source_id = ?", id);
        assertEquals("DONE", state.get("status"));
        Map<String, Object> document = targetJdbc.queryForMap(
                "SELECT ocr_text, ocr_vendor_job_id FROM document WHERE source_id = ?", id);
        assertEquals("PRE-COMPUTED OCR", document.get("ocr_text"));
        assertEquals("job-resumed", document.get("ocr_vendor_job_id"));
    }

    private void insertSourceFile(long id, String filename, String contentType, byte[] content) {
        sourceJdbc.update("INSERT INTO files (id, filename, content_type, content, byte_size) "
                + "VALUES (?, ?, ?, ?, ?)", id, filename, contentType, content, content.length);
    }

    private void seedPending(long id, String lane) {
        LedgerRepository ledger = new LedgerRepository(targetJdbc, LEASE_SECONDS);
        ledger.seedPending(List.of(id), lane, Map.of(id, Instant.now()));
    }

    private static String sha256Hex(byte[] content) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(content);
        StringBuilder hex = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }

    private static void setVendorMode(String mode) {
        vendorAdminClient.post().uri("/admin/mode").body(Map.of("mode", mode)).retrieve().toBodilessEntity();
    }
}
