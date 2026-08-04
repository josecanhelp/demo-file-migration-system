package com.filemigration.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.backfill.BackfillMessage;
import com.filemigration.governor.TestGovernorFactory;
import com.filemigration.model.BackfillRange;
import com.filemigration.store.BackfillCheckpointRepository;
import com.filemigration.store.DocumentRepository;
import com.filemigration.store.EventRepository;
import com.filemigration.store.LedgerRepository;
import com.filemigration.store.ObjectStore;
import com.filemigration.store.SourceFileRepository;
import com.filemigration.testsupport.IsolatedStackPreflight;
import com.filemigration.vendor.VendorClient;
import com.filemigration.worker.BackfillConsumer;
import com.filemigration.worker.MigrationService;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.web.client.RestClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the backfill lane end to end against the real Postgres,
 * MySQL, MinIO, Kafka, and vendor-mock services docker compose starts:
 * seeding a range through BackfillCoordinator really publishes to the
 * real topic, and BackfillConsumer really consumes what lands there and
 * drives it through to a document row and a MinIO object. If any real
 * dependency is unreachable, connecting fails loudly here rather than
 * being swallowed into a skip.
 *
 * Every row this test writes uses a source id at or above the reserved
 * range below, which sits well clear of both the small ranges other
 * repository tests use and anything the real range planner (which starts
 * counting at 1) would produce for a modestly sized source table. Every
 * one of those rows is removed both before and after each test: every
 * insert here is a plain INSERT on a fixed, reused id rather than an
 * upsert, so a row an earlier aborted or forcibly killed run left behind
 * (its own @AfterEach never having gotten to run) would otherwise collide
 * with this run's own insert on the same id instead of the test ever
 * getting to run. The vendor is reset to healthy after the class runs
 * whether or not a test failed.
 *
 * Publishing and consuming both use a topic of this test's own rather
 * than the real "files.backfill" topic the live coordinator and worker
 * containers use: Kafka delivers a message to every consumer group
 * subscribed to a topic independently, so publishing reserved test ids
 * to the real topic would have the real "backfill" group's live worker
 * racing this test to claim and process the very same ids. A dedicated
 * topic means this test never contends with, and never depends on,
 * whatever is actually running against the same compose network.
 */
class BackfillIT {

    private static final long BASE_ID = 9_500_000L;
    private static final String BUCKET = "documents";
    private static final long LEDGER_LEASE_SECONDS = 300L;
    private static final long CHECKPOINT_LEASE_SECONDS = 300L;
    private static final String TOPIC = "files.backfill.it";
    private static final int TEST_TOPIC_PARTITIONS = 1;
    private static final int TEST_VENDOR_BATCH_SIZE = 2;
    private static final long PLAN_INTERVAL_SECONDS = 30L;
    private static final long NACK_BACKOFF_SECONDS = 30L;
    private static final long CLAIM_RENEW_INTERVAL_SECONDS = 10L;
    private static final int WORKER_CONCURRENCY = 1;
    private static final int MAX_RETRY_ATTEMPTS = 5;

    private static HikariDataSource targetDataSource;
    private static HikariDataSource sourceDataSource;
    private static JdbcTemplate targetJdbc;
    private static JdbcTemplate sourceJdbc;
    private static S3Client s3Client;
    private static RestClient vendorAdminClient;
    private static ProducerFactory<String, String> producerFactory;
    private static KafkaTemplate<String, String> kafkaTemplate;
    private static BackfillCoordinator coordinator;
    private static BackfillConsumer consumer;
    private static MigrationService migrationService;
    private static ObjectMapper objectMapper;

    @BeforeAll
    static void connect() throws Exception {
        IsolatedStackPreflight.verify();
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

        objectMapper = new ObjectMapper();
        LedgerRepository ledger = new LedgerRepository(targetJdbc, LEDGER_LEASE_SECONDS);
        SourceFileRepository sourceRepo = new SourceFileRepository(sourceJdbc);
        ObjectStore objectStore = new ObjectStore(s3Client, BUCKET);
        DocumentRepository documentRepo = new DocumentRepository(targetJdbc);
        EventRepository eventRepo = new EventRepository(targetJdbc);
        VendorClient vendorClient = new VendorClient(vendorRestClient, objectMapper);
        migrationService = new MigrationService(ledger, sourceRepo, objectStore, documentRepo,
                eventRepo, vendorClient, TestGovernorFactory.passthrough(), objectMapper,
                CLAIM_RENEW_INTERVAL_SECONDS, WORKER_CONCURRENCY, MAX_RETRY_ATTEMPTS);
        BackfillCheckpointRepository checkpointRepo = new BackfillCheckpointRepository(targetJdbc,
                CHECKPOINT_LEASE_SECONDS);

        String kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        ensureTopicExists(kafkaBootstrapServers);
        Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);

        coordinator = new BackfillCoordinator(sourceRepo, checkpointRepo, ledger, eventRepo, kafkaTemplate,
                objectMapper, 1000L, TEST_VENDOR_BATCH_SIZE, TOPIC, PLAN_INTERVAL_SECONDS);
        consumer = new BackfillConsumer(migrationService, ledger, objectMapper, NACK_BACKOFF_SECONDS);
    }

    @AfterAll
    static void disconnectAndResetVendor() {
        try {
            setVendorMode("healthy");
        } finally {
            if (migrationService != null) {
                migrationService.shutdown();
            }
            if (producerFactory instanceof DefaultKafkaProducerFactory<String, String> factory) {
                factory.destroy();
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

    @BeforeEach
    @AfterEach
    void cleanUpReservedRows() {
        targetJdbc.update("DELETE FROM migration_event WHERE source_id >= ?", BASE_ID);
        // The range-level QUEUED event carries a null source_id (it
        // describes a whole range, not one file), so it is not covered
        // by the source_id >= filter above and needs its own match on
        // the range recorded in its detail.
        targetJdbc.update("DELETE FROM migration_event WHERE source_id IS NULL "
                + "AND detail->>'rangeStart' = ?", String.valueOf(BASE_ID));
        targetJdbc.update("DELETE FROM document WHERE source_id >= ?", BASE_ID);
        targetJdbc.update("DELETE FROM migration_state WHERE source_id >= ?", BASE_ID);
        targetJdbc.update("DELETE FROM backfill_checkpoint WHERE range_start >= ?", BASE_ID);
        sourceJdbc.update("DELETE FROM files WHERE id >= ?", BASE_ID);
    }

    @Test
    void aClaimedRangeIsSeededPublishedConsumedAndReachesDoneWithDocumentsAndObjects() throws Exception {
        long rangeStart = BASE_ID;
        long rangeEnd = BASE_ID + 999;
        List<Long> ids = List.of(BASE_ID + 1, BASE_ID + 2, BASE_ID + 3);
        Map<Long, byte[]> contentById = Map.of(
                ids.get(0), "INVOICE-A".getBytes(StandardCharsets.UTF_8),
                ids.get(1), "INVOICE-B".getBytes(StandardCharsets.UTF_8),
                ids.get(2), "INVOICE-C".getBytes(StandardCharsets.UTF_8));
        for (Long id : ids) {
            insertSourceFile(id, "invoice-" + id + ".txt", "text/plain", contentById.get(id));
        }
        // Simulate a range already claimed by an earlier claimNextRange()
        // call, since claim-loop concurrency is proven separately.
        targetJdbc.update("INSERT INTO backfill_checkpoint (range_start, range_end, status, claimed_at) "
                + "VALUES (?, ?, 'CLAIMED', now())", rangeStart, rangeEnd);

        // With a vendor batch size of 2 and 3 reserved ids, this must
        // publish across two Kafka messages, not one, exercising the
        // chunk boundary against a real broker.
        coordinator.processRange(new BackfillRange(rangeStart, rangeEnd));

        Map<String, Object> checkpointRow = targetJdbc.queryForMap(
                "SELECT status FROM backfill_checkpoint WHERE range_start = ? AND range_end = ?",
                rangeStart, rangeEnd);
        assertEquals("DONE", checkpointRow.get("status"));

        consumeUntilAllIdsSeen(new HashSet<>(ids), Duration.ofSeconds(60));

        for (Long id : ids) {
            Map<String, Object> state = targetJdbc.queryForMap(
                    "SELECT status, source_created_at FROM migration_state WHERE source_id = ?", id);
            assertEquals("DONE", state.get("status"), "id " + id + " must reach DONE");

            Timestamp sourceCreatedAt = sourceJdbc.queryForObject(
                    "SELECT created_at FROM files WHERE id = ?", Timestamp.class, id);
            assertEquals(sourceCreatedAt.toInstant(), ((Timestamp) state.get("source_created_at")).toInstant(),
                    "migration_state.source_created_at for id " + id + " must match the source row's created_at, "
                            + "not be left NULL");

            byte[] stored = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(BUCKET)
                    .key("files/" + id)
                    .build()).readAllBytes();
            assertTrue(java.util.Arrays.equals(contentById.get(id), stored),
                    "the object written to MinIO for id " + id + " must match the source content");
        }

        Long documentCount = targetJdbc.queryForObject(
                "SELECT count(*) FROM document WHERE source_id >= ? AND source_id <= ?", Long.class,
                rangeStart, rangeEnd);
        assertEquals(3L, documentCount);

        Long queuedEvents = targetJdbc.queryForObject(
                "SELECT count(*) FROM migration_event WHERE stage = 'QUEUED' AND lane = 'backfill' "
                        + "AND detail->>'rangeStart' = ?",
                Long.class, String.valueOf(rangeStart));
        assertEquals(1L, queuedEvents, "processing a range must record exactly one QUEUED range event");
    }

    /**
     * Reads from the real topic with a fresh, uniquely named consumer
     * group so this never picks up a stale committed offset from a
     * previous run and never contends with the real worker's "backfill"
     * group. The topic can carry unrelated messages from other runs of
     * the system, so anything not naming one of the ids this test cares
     * about is left alone rather than acknowledged.
     */
    private void consumeUntilAllIdsSeen(Set<Long> remaining, Duration timeout) throws Exception {
        String kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "backfill-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> rawConsumer = new KafkaConsumer<>(consumerProps)) {
            rawConsumer.subscribe(List.of(TOPIC));
            Instant deadline = Instant.now().plus(timeout);
            while (!remaining.isEmpty() && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = rawConsumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    BackfillMessage message = objectMapper.readValue(record.value(), BackfillMessage.class);
                    boolean matches = message.sourceIds().stream().anyMatch(remaining::contains);
                    if (matches) {
                        // A real Debezium connector also runs against this
                        // same MySQL and the same shared target Postgres
                        // now, so the CDC lane can legitimately win the
                        // race to claim one of these ids before this call
                        // does; when that happens BackfillConsumer finds it
                        // still unresolved and nacks. The status assertions
                        // below re-check the real database directly rather
                        // than trusting this call finished the migration
                        // itself, so nack only needs to not blow up here.
                        consumer.consume(record.value(), new Acknowledgment() {
                            @Override
                            public void acknowledge() {
                            }

                            @Override
                            public void nack(Duration sleep) {
                            }
                        });
                        remaining.removeAll(message.sourceIds());
                    }
                }
            }
            assertTrue(remaining.isEmpty(), "not every published id was consumed within the timeout: " + remaining);
        }
    }

    private void insertSourceFile(long id, String filename, String contentType, byte[] content) {
        sourceJdbc.update("INSERT INTO files (id, filename, content_type, content, byte_size) "
                + "VALUES (?, ?, ?, ?, ?)", id, filename, contentType, content, content.length);
    }

    private static void setVendorMode(String mode) {
        vendorAdminClient.post().uri("/admin/mode").body(Map.of("mode", mode)).retrieve().toBodilessEntity();
    }

    /**
     * Creates this test's own topic if it does not already exist, rather
     * than relying on broker auto-create to choose whatever partition
     * count and configuration the broker happens to default to.
     * Tolerates the topic already existing from an earlier run.
     */
    private static void ensureTopicExists(String bootstrapServers) throws Exception {
        Map<String, Object> adminProps = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (Admin admin = Admin.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, TEST_TOPIC_PARTITIONS, (short) 1)))
                    .all()
                    .get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
    }
}
