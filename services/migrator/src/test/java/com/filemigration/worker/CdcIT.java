package com.filemigration.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.governor.TestGovernorFactory;
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
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
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
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the CDC lane end to end against the real stack docker compose
 * starts: a change made straight against MySQL, with no message this test
 * publishes itself, has to be picked up by the real Debezium connector,
 * carried across the real cdc.sourcedb.files topic, and driven through a
 * real CdcConsumer instance to a terminal state in the real Postgres and
 * MinIO, with the actual ack/nack decision it made along the way checked,
 * not merely the database state it left behind. If Debezium, Kafka, or
 * CdcConsumer's own processing are not actually wired up correctly, this
 * fails on a timeout or a failed assertion rather than passing vacuously.
 * Whether the real @KafkaListener annotation itself is bound to the
 * configured topic, group id, and ack mode is a separate concern this
 * class does not touch; {@code CdcListenerWiringTest} covers that.
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
 * Every poll loop below polls at least once before it ever looks at its
 * own success condition, so this test's own CdcConsumer instance always
 * gets at least one chance to act on a message before anything is
 * asserted about it. What the ack/nack assertions below examine is
 * strictly this consumer's own ack/nack decision, recorded at the moment
 * it made it; that is a fact this test can state honestly regardless of
 * who wins any given claim. Which of the two consumers actually finishes
 * a row, the live container's "cdc" group or this test's own instance,
 * is a separate question the exit condition on each poll loop answers by
 * reading the database, not something the ack/nack assertions claim to
 * settle.
 *
 * Every row this test writes uses a filename carrying the "cdcit-" prefix
 * and is removed after each test by the exact id MySQL assigned it, never
 * by a broad filter or a truncate.
 */
class CdcIT {

    private static final String BUCKET = "documents";
    private static final Duration DETECTION_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(2);
    private static final long LEASE_SECONDS = 300L;
    private static final long NACK_BACKOFF_SECONDS = 10L;
    private static final long CLAIM_RENEW_INTERVAL_SECONDS = 10L;
    private static final int WORKER_CONCURRENCY = 1;
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static HikariDataSource targetDataSource;
    private static HikariDataSource sourceDataSource;
    private static JdbcTemplate targetJdbc;
    private static JdbcTemplate sourceJdbc;
    private static S3Client s3Client;
    private static RestClient vendorAdminClient;
    private static MigrationService migrationService;
    private static CdcConsumer consumer;
    private static KafkaConsumer<String, String> rawConsumer;
    private static String topic;

    private final List<Long> insertedIds = new ArrayList<>();
    private final Map<Long, RecordingAcknowledgment> lastAckById = new HashMap<>();
    private final Map<Long, String> lastPayloadById = new HashMap<>();

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
                "jdbc:mysql://localhost:3306/sourcedb?useSSL=false&allowPublicKeyRetrieval=true&connectionTimeZone=UTC");
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

        LedgerRepository ledger = new LedgerRepository(targetJdbc, LEASE_SECONDS);
        SourceFileRepository sourceRepo = new SourceFileRepository(sourceJdbc);
        ObjectStore objectStore = new ObjectStore(s3Client, BUCKET);
        DocumentRepository documentRepo = new DocumentRepository(targetJdbc);
        EventRepository eventRepo = new EventRepository(targetJdbc);
        VendorClient vendorClient = new VendorClient(vendorRestClient, OBJECT_MAPPER);
        migrationService = new MigrationService(ledger, sourceRepo, objectStore, documentRepo, eventRepo,
                vendorClient, TestGovernorFactory.passthrough(), OBJECT_MAPPER, CLAIM_RENEW_INTERVAL_SECONDS,
                WORKER_CONCURRENCY, MAX_RETRY_ATTEMPTS);
        consumer = new CdcConsumer(migrationService, ledger, objectStore, eventRepo, sourceRepo, OBJECT_MAPPER,
                NACK_BACKOFF_SECONDS);

        // Read from the same application.yml key the real @KafkaListener
        // binds to (migrator.cdc.topic) instead of a literal duplicated
        // here, so a rename of that property is caught by this test
        // failing to find anything on the topic it expects, rather than
        // silently subscribing to a name nothing publishes to anymore.
        topic = resolveConfiguredTopic();

        String kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "cdc-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        rawConsumer = new KafkaConsumer<>(consumerProps);
        rawConsumer.subscribe(List.of(topic));
    }

    private static String resolveConfiguredTopic() throws Exception {
        List<PropertySource<?>> sources = new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml"));
        for (PropertySource<?> source : sources) {
            Object value = source.getProperty("migrator.cdc.topic");
            if (value != null) {
                return value.toString();
            }
        }
        throw new IllegalStateException("migrator.cdc.topic is not set in application.yml");
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
        lastAckById.clear();
        lastPayloadById.clear();
    }

    @Test
    void insertReachesDoneWithADocumentRowAndAMinioObjectWithinSeconds() throws Exception {
        byte[] content = ("CDCIT INSERT " + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        long id = insertFile("cdcit-insert-" + UUID.randomUUID(), "text/plain", content);

        driveConsumptionUntil(() -> isDone(id), DETECTION_TIMEOUT,
                "source_id " + id + " did not reach status DONE");

        assertAcknowledged(id, "the create envelope");

        Map<String, Object> document = targetJdbc.queryForMap("SELECT * FROM document WHERE source_id = ?", id);
        assertEquals("files/" + id, document.get("object_key"));
        assertEquals(extractText(content), document.get("ocr_text"));

        byte[] stored = getObject("files/" + id);
        assertArrayEquals(content, stored, "the object written to MinIO must match the source content");

        // column.exclude.list is what keeps multi-megabyte blobs off
        // Kafka; this is the one place that would notice a regression
        // that put the blob column back on the wire.
        JsonNode after = OBJECT_MAPPER.readTree(lastPayloadById.get(id)).get("after");
        assertFalse(after.has("content"), "the CDC envelope must never carry the blob column");
    }

    @Test
    void createEnvelopeCarriesTheSourceRowsCreatedAtIntoTheLedger() throws Exception {
        byte[] content = ("CDCIT CREATEDAT " + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        long id = insertFile("cdcit-createdat-" + UUID.randomUUID(), "text/plain", content);

        driveConsumptionUntil(() -> isDone(id), DETECTION_TIMEOUT,
                "source_id " + id + " did not reach status DONE");

        Timestamp sourceCreatedAt = sourceJdbc.queryForObject(
                "SELECT created_at FROM files WHERE id = ?", Timestamp.class, id);
        Timestamp ledgerCreatedAt = targetJdbc.queryForObject(
                "SELECT source_created_at FROM migration_state WHERE source_id = ?", Timestamp.class, id);
        assertNotNull(ledgerCreatedAt, "the CDC lane must populate source_created_at rather than leave it NULL");
        assertEquals(sourceCreatedAt.toInstant(), ledgerCreatedAt.toInstant(),
                "migration_state.source_created_at must match the MySQL row's own created_at exactly");
    }

    /**
     * The freshness gauge (see stats.js's SLA_QUERY in the control plane)
     * reads COALESCE(EXTRACT(EPOCH FROM (now() - MIN(source_created_at))),
     * 0) over outstanding cdc rows. With source_created_at always NULL,
     * that COALESCE silently substitutes 0 no matter how far behind a
     * file actually is. Vendor mode is set to "down" so this row's own
     * migration attempt keeps failing and it never reaches DONE, keeping
     * it in the gauge's scope for the query below to actually measure.
     *
     * Driven by calling this test's own CdcConsumer instance directly on
     * a hand-built envelope rather than through the real topic: that
     * consumer's raw Kafka reader always starts from the earliest offset
     * under a brand new group, so with the vendor down it would also
     * replay, and slowly retry, every other still-unresolved envelope
     * already sitting on the shared topic from other suites, turning
     * this test's timeout into a fight against unrelated backlog instead
     * of a check of the one thing it cares about. This still exercises
     * the real seedPending/CdcConsumer production code against the same
     * real Postgres instance the freshness gauge itself reads, only
     * without going through the topic to reach it.
     */
    @Test
    void anOutstandingRowWithAPopulatedSourceCreatedAtProducesANonZeroFreshnessLag() throws Exception {
        setVendorMode("down");
        try {
            byte[] content = ("CDCIT FRESHNESS " + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
            long id = insertFile("cdcit-freshness-" + UUID.randomUUID(), "text/plain", content);

            // No created_at in "after": this also proves the fallback to
            // reading it from the source row works end to end, against
            // the real MySQL connection, not only against a fake one.
            String payload = "{\"op\":\"c\",\"before\":null,\"after\":{\"id\":" + id + "},\"ts_ms\":"
                    + System.currentTimeMillis() + "}";
            consumer.consume(payload, new RecordingAcknowledgment());

            assertTrue(hasSourceCreatedAt(id), "the CDC lane must seed source_created_at even when the vendor "
                    + "is unreachable, since seeding happens before the vendor is ever called");
            assertFalse(isDone(id), "vendor mode 'down' must keep this row from ever reaching DONE, or it falls "
                    + "out of the freshness gauge's scope and this assertion means nothing");

            // Real, if small, elapsed time since the row was seeded, so the
            // lag below is not just an artifact of clock skew between this
            // process and Postgres.
            Thread.sleep(1500);

            Double lagSeconds = targetJdbc.queryForObject(
                    "SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MIN(source_created_at))), 0) "
                            + "FROM migration_state WHERE lane = 'cdc' AND status <> 'DONE' AND source_id = ?",
                    Double.class, id);
            assertTrue(lagSeconds != null && lagSeconds > 0,
                    "an outstanding cdc row with a populated source_created_at must report a non-zero freshness "
                            + "lag, not the 0 COALESCE silently substitutes for an all-NULL aggregate");
        } finally {
            setVendorMode("healthy");
        }
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

        assertAcknowledged(id, "the update envelope");
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

        assertAcknowledged(id, "the delete envelope");
    }

    private void assertAcknowledged(long id, String description) {
        RecordingAcknowledgment ack = lastAckById.get(id);
        assertTrue(ack != null && ack.acknowledged,
                description + " for id " + id + " must be acknowledged once its effect is visible");
        assertNull(ack.nackedWith, description + " for id " + id + " must not also carry a negative acknowledgment");
    }

    /**
     * Polls the real cdc.sourcedb.files topic at least once, feeding every
     * record through this test's own CdcConsumer instance and recording
     * the ack/nack decision it made, before ever looking at whether the
     * given condition is satisfied yet; only then does it check the
     * condition, and it keeps alternating the two until the condition is
     * true or the timeout elapses. Polling before checking is what makes
     * the ack/nack recorded here trustworthy: checking first would let a
     * condition already satisfied by the live container's own "cdc" group
     * end the loop before this test's consumer ever ran, leaving the
     * assertions that follow checking a stale or absent recording instead
     * of this call's own outcome.
     *
     * Every id this test itself created that has already seen a nack is
     * replayed against its own stored payload on each pass, the same
     * effect a real nack has in production: the live container's "cdc"
     * group consumes the same real topic this test does, under its own
     * group id, so it can legitimately win the race to claim a row this
     * test also just inserted, in which case this test's own attempt
     * correctly nacks (the row is not yet DONE from where this attempt is
     * standing) while the live container finishes it. Replaying converges
     * the recording this test asserts on to the truth once the row
     * settles, rather than asserting on whichever side happened to lose
     * an ordinary claim race. Only this test's own ids are ever replayed:
     * the topic also carries other suites' changes, including at least
     * one every run that never resolves (a row another suite deletes
     * before its own create envelope is read back), and replaying one of
     * those would re-seed and re-record a migration_state/migration_event
     * row for an id this test does not own and its own cleanup does not
     * know to remove.
     */
    private void driveConsumptionUntil(BooleanSupplier condition, Duration timeout, String timeoutMessage) {
        Instant deadline = Instant.now().plus(timeout);
        do {
            ConsumerRecords<String, String> records = rawConsumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<String, String> record : records) {
                processRecord(record.value());
            }
            replayNackedPayloads();
            if (condition.getAsBoolean()) {
                return;
            }
        } while (Instant.now().isBefore(deadline));
        throw new AssertionError(timeoutMessage + " within " + timeout);
    }

    private void replayNackedPayloads() {
        for (Long id : insertedIds) {
            RecordingAcknowledgment ack = lastAckById.get(id);
            if (ack != null && ack.nackedWith != null) {
                String payload = lastPayloadById.get(id);
                if (payload != null) {
                    processRecord(payload);
                }
            }
        }
    }

    private void processRecord(String payload) {
        RecordingAcknowledgment ack = new RecordingAcknowledgment();
        consumer.consume(payload, ack);
        Long id = extractId(payload);
        if (id != null) {
            lastAckById.put(id, ack);
            lastPayloadById.put(id, payload);
        }
    }

    /**
     * Parses just enough of the envelope to know which id it concerns,
     * reusing CdcConsumer's own envelope shape rather than a second,
     * possibly diverging, definition of it. A null value (Kafka's own
     * post-delete tombstone) or a message that fails to parse carries no
     * id to record anything against.
     */
    private Long extractId(String payload) {
        if (payload == null) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(payload, CdcConsumer.CdcEnvelope.class).sourceId();
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private boolean isDone(long id) {
        List<Map<String, Object>> rows = targetJdbc.queryForList(
                "SELECT status FROM migration_state WHERE source_id = ?", id);
        return !rows.isEmpty() && "DONE".equals(rows.get(0).get("status"));
    }

    private boolean hasSourceCreatedAt(long id) {
        List<Map<String, Object>> rows = targetJdbc.queryForList(
                "SELECT source_created_at FROM migration_state WHERE source_id = ?", id);
        return !rows.isEmpty() && rows.get(0).get("source_created_at") != null;
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

    private static final class RecordingAcknowledgment implements Acknowledgment {

        private boolean acknowledged;
        private Duration nackedWith;

        @Override
        public void acknowledge() {
            acknowledged = true;
        }

        @Override
        public void nack(Duration sleep) {
            nackedWith = sleep;
        }
    }
}
