package com.filemigration.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.store.DocumentRepository;
import com.filemigration.store.EventRepository;
import com.filemigration.store.LedgerRepository;
import com.filemigration.store.ObjectStore;
import com.filemigration.store.SourceFileRepository;
import com.filemigration.vendor.VendorClient;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.web.client.RestClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Exercises the CDC lane end to end against the real stack docker compose
 * starts: a change made straight against MySQL, with no message this test
 * publishes itself, has to be picked up by the real Debezium connector,
 * carried across the real cdc.sourcedb.files topic, and driven through a
 * real CdcConsumer to a terminal state in the real Postgres and MinIO. If
 * Debezium, Kafka, or CdcConsumer are not actually wired up correctly,
 * this fails on a timeout rather than passing vacuously.
 *
 * This test drives its own dedicated CdcConsumer instance, reading the
 * real topic under a consumer group unique to this run, rather than
 * relying on the live migrator-worker container's "cdc" group to make
 * progress on its own. Two things make that necessary rather than merely
 * convenient. First, Kafka delivers every message on a topic to each
 * consumer group independently, so a private group here never contends
 * with, or depends on the pace of, whatever the live container or another
 * test is doing with the shared group. Second, and more importantly: any
 * suite that deletes a MySQL row very soon after creating it (several
 * already do, to keep a reserved id range clean) can race its own change
 * capture, so the row is gone by the time the create envelope is actually
 * read back out. That case can never resolve to DONE or FAILED_PERMANENT
 * on its own, so under the live container's retry-with-backoff consumer
 * it would hold up every id waiting behind it on the same partition
 * forever. Driving a private, disposable consumer instance directly, one
 * poll loop at a time, is immune to that: it acts on the ack/nack outcome
 * of one message only to decide whether this test's own condition is
 * satisfied yet, never by seeking backward and retrying, so an
 * unrelated, permanently unresolvable message from another suite is
 * simply passed over rather than blocking anything.
 *
 * Every row this test writes uses a filename carrying the "cdcit-" prefix
 * and is removed after each test by the exact id MySQL assigned it, never
 * by a broad filter or a truncate.
 */
class CdcIT {

    private static final String BUCKET = "documents";
    private static final String TOPIC = "cdc.sourcedb.files";
    private static final Duration DETECTION_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(2);
    private static final long LEASE_SECONDS = 300L;
    private static final long NACK_BACKOFF_SECONDS = 10L;
    private static final long CLAIM_RENEW_INTERVAL_SECONDS = 10L;
    private static final int WORKER_CONCURRENCY = 1;
    private static final Acknowledgment NO_OP_ACK = new Acknowledgment() {
        @Override
        public void acknowledge() {
        }

        @Override
        public void nack(Duration sleep) {
        }
    };

    private static HikariDataSource targetDataSource;
    private static HikariDataSource sourceDataSource;
    private static JdbcTemplate targetJdbc;
    private static JdbcTemplate sourceJdbc;
    private static S3Client s3Client;
    private static RestClient vendorAdminClient;
    private static MigrationService migrationService;
    private static CdcConsumer consumer;
    private static KafkaConsumer<String, String> rawConsumer;

    private final List<Long> insertedIds = new ArrayList<>();

    @BeforeAll
    static void connect() throws Exception {
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
        // Deliberately not wrapped in try/catch: if the target database is
        // not reachable, this throws and the whole class fails instead of
        // quietly reporting a pass with nothing exercised.
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
        migrationService = new MigrationService(ledger, sourceRepo, objectStore, documentRepo, eventRepo,
                vendorClient, objectMapper, CLAIM_RENEW_INTERVAL_SECONDS, WORKER_CONCURRENCY);
        consumer = new CdcConsumer(migrationService, ledger, objectStore, eventRepo, objectMapper,
                NACK_BACKOFF_SECONDS);

        String kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "cdc-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        rawConsumer = new KafkaConsumer<>(consumerProps);
        rawConsumer.subscribe(List.of(TOPIC));
    }

    @AfterAll
    static void disconnectAndResetVendor() {
        try {
            setVendorMode("healthy");
        } finally {
            if (rawConsumer != null) {
                rawConsumer.close();
            }
            if (migrationService != null) {
                migrationService.shutdown();
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
        for (Long id : insertedIds) {
            targetJdbc.update("DELETE FROM migration_event WHERE source_id = ?", id);
            targetJdbc.update("DELETE FROM document WHERE source_id = ?", id);
            targetJdbc.update("DELETE FROM migration_state WHERE source_id = ?", id);
            sourceJdbc.update("DELETE FROM files WHERE id = ?", id);
        }
        insertedIds.clear();
    }

    @Test
    void insertReachesDoneWithADocumentRowAndAMinioObjectWithinSeconds() throws Exception {
        byte[] content = ("CDCIT INSERT " + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        long id = insertFile("cdcit-insert-" + UUID.randomUUID(), "text/plain", content);

        driveConsumptionUntil(() -> isDone(id), DETECTION_TIMEOUT,
                "source_id " + id + " did not reach status DONE");

        Map<String, Object> document = targetJdbc.queryForMap("SELECT * FROM document WHERE source_id = ?", id);
        assertEquals("files/" + id, document.get("object_key"));
        assertEquals(extractText(content), document.get("ocr_text"));

        byte[] stored = getObject("files/" + id);
        assertArrayEquals(content, stored, "the object written to MinIO must match the source content");
    }

    @Test
    void updateChangesTheDocumentContentAndTheOcrText() throws Exception {
        byte[] original = ("CDCIT ORIGINAL " + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        long id = insertFile("cdcit-update-" + UUID.randomUUID(), "text/plain", original);
        driveConsumptionUntil(() -> isDone(id), DETECTION_TIMEOUT,
                "source_id " + id + " did not reach status DONE after insert");
        String originalOcrText = queryOcrTextOrNull(id);
        assertEquals(extractText(original), originalOcrText);

        byte[] updated = ("CDCIT UPDATED " + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        sourceJdbc.update("UPDATE files SET content = ?, byte_size = ? WHERE id = ?",
                updated, updated.length, id);

        driveConsumptionUntil(() -> {
            String current = queryOcrTextOrNull(id);
            return current != null && !Objects.equals(current, originalOcrText);
        }, DETECTION_TIMEOUT, "document.ocr_text for source_id " + id + " never changed from its original value");

        assertEquals(extractText(updated), queryOcrTextOrNull(id), "the OCR text must reflect the updated content");
        assertEquals("DONE", targetJdbc.queryForObject(
                "SELECT status FROM migration_state WHERE source_id = ?", String.class, id));

        byte[] stored = getObject("files/" + id);
        assertArrayEquals(updated, stored, "the object in MinIO must reflect the updated content");
    }

    @Test
    void deleteRemovesTheDocumentRowAndTheMinioObject() throws Exception {
        byte[] content = ("CDCIT DELETE " + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        long id = insertFile("cdcit-delete-" + UUID.randomUUID(), "text/plain", content);
        driveConsumptionUntil(() -> isDone(id), DETECTION_TIMEOUT,
                "source_id " + id + " did not reach status DONE after insert");
        // Confirmed present before the delete, so its absence afterward is
        // proof the delete actually did something rather than there being
        // nothing to remove in the first place.
        getObject("files/" + id);

        sourceJdbc.update("DELETE FROM files WHERE id = ?", id);

        String objectKey = "files/" + id;
        driveConsumptionUntil(() -> isGoneFromLedgerAndDocument(id) && isGoneFromObjectStore(objectKey),
                DETECTION_TIMEOUT,
                "source_id " + id + " still has a ledger row, a document row, or a MinIO object after deletion");
    }

    /**
     * Polls the real cdc.sourcedb.files topic and feeds every record
     * through this test's own CdcConsumer instance, exactly as the live
     * container's listener would, until the given condition is met or the
     * timeout elapses. A record naming an id this test does not recognize
     * is processed exactly the same way as one of its own: harmlessly,
     * since CdcConsumer's own broad catch already turns any failure into
     * a logged nack rather than an exception, and this loop does not
     * examine the ack/nack outcome at all, only whether its own condition
     * is now true.
     */
    private void driveConsumptionUntil(BooleanSupplier condition, Duration timeout, String timeoutMessage) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            ConsumerRecords<String, String> records = rawConsumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<String, String> record : records) {
                consumer.consume(record.value(), NO_OP_ACK);
            }
        }
        if (condition.getAsBoolean()) {
            return;
        }
        throw new AssertionError(timeoutMessage + " within " + timeout);
    }

    private boolean isDone(long id) {
        List<Map<String, Object>> rows = targetJdbc.queryForList(
                "SELECT status FROM migration_state WHERE source_id = ?", id);
        return !rows.isEmpty() && "DONE".equals(rows.get(0).get("status"));
    }

    private boolean isGoneFromLedgerAndDocument(long id) {
        Long stateCount = targetJdbc.queryForObject(
                "SELECT count(*) FROM migration_state WHERE source_id = ?", Long.class, id);
        Long documentCount = targetJdbc.queryForObject(
                "SELECT count(*) FROM document WHERE source_id = ?", Long.class, id);
        return stateCount != null && stateCount == 0 && documentCount != null && documentCount == 0;
    }

    private boolean isGoneFromObjectStore(String key) {
        try {
            s3Client.headObject(HeadObjectRequest.builder().bucket(BUCKET).key(key).build());
            return false;
        } catch (NoSuchKeyException e) {
            return true;
        }
    }

    private String queryOcrTextOrNull(long id) {
        List<Map<String, Object>> rows = targetJdbc.queryForList(
                "SELECT ocr_text FROM document WHERE source_id = ?", id);
        return rows.isEmpty() ? null : (String) rows.get(0).get("ocr_text");
    }

    private long insertFile(String filename, String contentType, byte[] content) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        sourceJdbc.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO files (filename, content_type, content, byte_size) VALUES (?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, filename);
            ps.setString(2, contentType);
            ps.setBytes(3, content);
            ps.setInt(4, content.length);
            return ps;
        }, keyHolder);
        long id = keyHolder.getKey().longValue();
        insertedIds.add(id);
        return id;
    }

    private byte[] getObject(String key) {
        try (ResponseInputStream<GetObjectResponse> response = s3Client.getObject(
                GetObjectRequest.builder().bucket(BUCKET).key(key).build())) {
            return response.readAllBytes();
        } catch (Exception e) {
            throw new RuntimeException("Failed to read object " + key + " from MinIO", e);
        }
    }

    /**
     * Mirrors vendor-mock's own OCR transform (uppercase, whitespace
     * collapsed, trimmed) so the test can assert on the exact text a real
     * OCR call against the real vendor-mock produces, not an approximation
     * of it.
     */
    private static String extractText(byte[] content) {
        return new String(content, StandardCharsets.UTF_8).toUpperCase().replaceAll("\\s+", " ").trim();
    }

    private static void setVendorMode(String mode) {
        vendorAdminClient.post().uri("/admin/mode").body(Map.of("mode", mode)).retrieve().toBodilessEntity();
    }
}
