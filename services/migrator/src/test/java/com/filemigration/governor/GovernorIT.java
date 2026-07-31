package com.filemigration.governor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.backfill.BackfillMessage;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the Governor against the real stack docker compose starts: real
 * Postgres, MySQL, Kafka, MinIO, and vendor-mock, with the actual
 * production wiring of Governor, LaneRateLimiter, the vendor CircuitBreaker,
 * and BreakerListener acting on real, Spring-managed listener containers.
 * This is the test the project's headline demo depends on: kill the
 * vendor, watch the breaker trip and consumption pause, restore the
 * vendor, watch the backlog drain with zero files lost.
 *
 * Every id this test seeds uses its own reserved source id range, well
 * clear of every other IT's, and is removed after each test regardless of
 * outcome. The backfill and cdc topics and consumer groups are private to
 * this test class, distinct from the real "files.backfill"/"backfill" and
 * "cdc.sourcedb.files"/"cdc" names the live migrator-worker container
 * uses, so this test never contends with, or depends on the pace of,
 * whatever that container is doing against the same compose network. The
 * vendor itself, and the shared vendor circuit breaker this Spring context
 * builds, are not private the same way: every test method here shares one
 * running application context, so each one restores the vendor to healthy
 * and waits for the breaker to leave OPEN before finishing, whether it
 * passed or failed, so one test's chaos is never a later test's inherited
 * problem.
 */
@SpringBootTest(properties = {
        "MYSQL_HOST=localhost",
        "POSTGRES_HOST=localhost",
        "MINIO_ENDPOINT=http://localhost:9000",
        "VENDOR_BASE_URL=http://localhost:8088",
        "KAFKA_BOOTSTRAP_SERVERS=localhost:9092",
        "migrator.backfill.topic=files.backfill.governor-it",
        "migrator.backfill.group-id=backfill-governor-it",
        "migrator.backfill.topic-partitions=1",
        "migrator.cdc.topic=cdc.sourcedb.files.governor-it",
        "migrator.cdc.group-id=cdc-governor-it",
        "migrator.cdc.topic-partitions=1",
        "migrator.dlq.topic=files.dlq.governor-it",
        "migrator.dlq.topic-partitions=1",
        "migrator.worker-concurrency=1",
        "migrator.claim-lease-seconds=300",
        "migrator.backfill.nack-backoff-seconds=2",
        "migrator.cdc.nack-backoff-seconds=2",
        "migrator.max-retry-attempts=3",
        "migrator.breaker.failure-rate-threshold=50",
        "migrator.breaker.open-duration-seconds=8",
        "migrator.governor.retry-base-backoff-ms=50",
        // Shortened from Spring Kafka's 5s default so a container's poll
        // loop notices a pause() request quickly enough to have actually
        // entered the paused state well before open-duration-seconds
        // elapses and the breaker auto-transitions to HALF_OPEN; at the
        // default 5s poll timeout the two were close enough to race.
        "spring.kafka.listener.poll-timeout=1000"
})
@ActiveProfiles("worker")
class GovernorIT {

    private static final long BASE_ID = 9_600_000L;
    private static final Duration DETECTION_TIMEOUT = Duration.ofSeconds(40);
    private static final Duration RECOVERY_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(250);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    @Qualifier("targetJdbc")
    private JdbcTemplate targetJdbc;

    @Autowired
    @Qualifier("sourceJdbc")
    private JdbcTemplate sourceJdbc;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private CircuitBreaker vendorCircuitBreaker;

    private static RestClient vendorAdminClient;
    private final List<Long> seededIds = new ArrayList<>();

    @BeforeAll
    static void setUpVendorAdminClient() {
        vendorAdminClient = RestClient.create("http://localhost:8088");
        vendorAdminClient.get().uri("/health").retrieve().toBodilessEntity();
    }

    @AfterEach
    void cleanUpAndRestoreSharedState() {
        setVendorMode("healthy");
        // The breaker and the listener containers are shared across every
        // test method in this class (one Spring context for the whole
        // class), so a test that drove the breaker OPEN must not leave it,
        // or a paused container, behind for the next test to inherit.
        waitUntil(() -> vendorCircuitBreaker.getState() == CircuitBreaker.State.CLOSED, RECOVERY_TIMEOUT,
                "the vendor circuit breaker never returned to CLOSED after resetting the vendor to healthy");
        waitUntil(() -> registry.getListenerContainers().stream().noneMatch(MessageListenerContainer::isContainerPaused),
                RECOVERY_TIMEOUT, "a Kafka listener container never resumed after the breaker closed");

        for (Long id : seededIds) {
            targetJdbc.update("DELETE FROM migration_event WHERE source_id = ?", id);
            targetJdbc.update("DELETE FROM document WHERE source_id = ?", id);
            targetJdbc.update("DELETE FROM migration_state WHERE source_id = ?", id);
            sourceJdbc.update("DELETE FROM files WHERE id = ?", id);
        }
        seededIds.clear();
    }

    @AfterAll
    static void resetVendorEvenOnFailure() {
        setVendorMode("healthy");
    }

    /**
     * The headline scenario: kill the vendor, watch the breaker trip and
     * both listener containers actually pause while the backlog behind
     * them grows instead of being dropped, restore the vendor, watch the
     * breaker close, the containers resume, and every file reach DONE with
     * zero loss and zero files wrongly condemned to FAILED_PERMANENT along
     * the way.
     */
    @Test
    void breakerOpensOnOutagePausesBothLanesThenClosesAndDrainsWithZeroLoss() throws Exception {
        List<Long> ids = seedBackfillFiles(20, 0);
        setVendorMode("down");

        publishBackfillMessages(ids, 2);

        waitUntil(() -> vendorCircuitBreaker.getState() == CircuitBreaker.State.OPEN, DETECTION_TIMEOUT,
                "the vendor circuit breaker never opened after the vendor went down");
        waitUntil(() -> registry.getListenerContainers().stream().allMatch(MessageListenerContainer::isContainerPaused),
                DETECTION_TIMEOUT, "not every Kafka listener container paused after the breaker opened");
        assertTrue(countEvents("BREAKER_OPEN") >= 1, "a BREAKER_OPEN event must be recorded");

        // Simulate the backlog continuing to arrive while the vendor is
        // down: these ids are published only after both containers are
        // already confirmed paused, so nothing has any chance to consume
        // them before the lag measurement below.
        List<Long> extraIds = seedBackfillFiles(5, 100);
        int extraChunkSize = 2;
        long expectedExtraMessages = (extraIds.size() + extraChunkSize - 1) / extraChunkSize;
        long lagBeforeWait = totalLag("backfill-governor-it", "files.backfill.governor-it");
        publishBackfillMessages(extraIds, extraChunkSize);
        List<Long> allIds = new ArrayList<>(ids);
        allIds.addAll(extraIds);

        Thread.sleep(2_000);
        long lagAfterPublishingMore = totalLag("backfill-governor-it", "files.backfill.governor-it");
        // Lag is counted in Kafka offsets, i.e. messages, not source ids;
        // extraIds is chunked into expectedExtraMessages messages, so that
        // is the growth this asserts on, not the raw id count.
        assertTrue(lagAfterPublishingMore >= lagBeforeWait + expectedExtraMessages,
                "consumer lag must grow while paused, since a paused container fetches nothing to consume or drop; "
                        + "before=" + lagBeforeWait + " after=" + lagAfterPublishingMore
                        + " expectedExtraMessages=" + expectedExtraMessages);

        assertEquals(0, countStatus(allIds, "FAILED_PERMANENT"),
                "no id may be condemned to FAILED_PERMANENT purely because of a vendor outage");

        setVendorMode("healthy");

        waitUntil(() -> vendorCircuitBreaker.getState() == CircuitBreaker.State.CLOSED, RECOVERY_TIMEOUT,
                "the vendor circuit breaker never closed once the vendor recovered");
        waitUntil(() -> registry.getListenerContainers().stream().noneMatch(MessageListenerContainer::isContainerPaused),
                RECOVERY_TIMEOUT, "the Kafka listener containers never resumed once the breaker closed");
        assertTrue(countEvents("BREAKER_CLOSED") >= 1, "a BREAKER_CLOSED event must be recorded");

        waitUntil(() -> countStatus(allIds, "DONE") == allIds.size(), DETECTION_TIMEOUT,
                "every id must reach DONE once the vendor recovers and the backlog drains");
        assertEquals(0, countStatus(allIds, "FAILED_PERMANENT"), "zero files lost: none of them may end up "
                + "permanently failed once the outage is over");
    }

    /**
     * A file the vendor will never be able to process, empty content
     * triggering UNPROCESSABLE_DOCUMENT, must be dead-lettered after
     * exactly one attempt rather than retried, and must never look like a
     * vendor outage to the breaker.
     */
    @Test
    void genuinelyUnprocessableFileGoesToFailedPermanentAfterOneAttemptWithoutTrippingTheBreaker() {
        long id = BASE_ID + 900;
        seededIds.add(id);
        insertSourceFile(id, "empty.txt", "text/plain", new byte[0]);
        targetJdbc.update("INSERT INTO migration_state (source_id, lane, status) VALUES (?, ?, ?)",
                id, "backfill", "PENDING");
        setVendorMode("healthy");
        CircuitBreaker.State stateBefore = vendorCircuitBreaker.getState();

        publishBackfillMessages(List.of(id), 1);

        waitUntil(() -> "FAILED_PERMANENT".equals(statusOf(id)), DETECTION_TIMEOUT,
                "source_id " + id + " never reached FAILED_PERMANENT");
        assertEquals(1, targetJdbc.queryForObject(
                "SELECT attempts FROM migration_state WHERE source_id = ?", Integer.class, id),
                "an unprocessable document must fail on its first and only attempt");
        assertEquals(1, countEventsForId(id, "DLQ"));
        assertEquals(stateBefore, vendorCircuitBreaker.getState(),
                "a document the vendor rejects as unprocessable is not a vendor outage and must never move the "
                        + "breaker");
    }

    /**
     * THE WEDGE. A ledger row for a source id whose MySQL row does not
     * exist can never succeed no matter how many times it is retried:
     * MigrationService marks it FAILED_RETRYABLE every time, and before
     * this fix, claim() reclaimed a FAILED_RETRYABLE row unconditionally
     * forever, so a CdcConsumer message for an id like this would nack
     * forever and block every other change on the same partition behind
     * it. This drives exactly that id through the real CdcConsumer and
     * asserts it reaches FAILED_PERMANENT, with a DLQ event, within
     * MAX_RETRY_ATTEMPTS rather than nacking forever.
     */
    @Test
    void wedgeIdWithNoMatchingSourceRowReachesFailedPermanentWithinTheRetryCapInsteadOfNackingForever() {
        long id = BASE_ID + 950;
        seededIds.add(id);
        // Deliberately no insertSourceFile(id, ...): the MySQL row for
        // this id is never created, mirroring a row that has already been
        // deleted from the source by the time its change event is
        // processed.

        publishCdcCreateEnvelope(id);

        waitUntil(() -> "FAILED_PERMANENT".equals(statusOf(id)), DETECTION_TIMEOUT,
                "source_id " + id + " never reached FAILED_PERMANENT; it would otherwise nack forever and block "
                        + "every other change queued behind it on the same partition");
        assertEquals(1, countEventsForId(id, "DLQ"));
        Integer attempts = targetJdbc.queryForObject(
                "SELECT attempts FROM migration_state WHERE source_id = ?", Integer.class, id);
        assertTrue(attempts >= 3, "the id must have actually been retried up to the configured cap, not condemned "
                + "on the first attempt: attempts=" + attempts);
    }

    private List<Long> seedBackfillFiles(int count, long offset) {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long id = BASE_ID + offset + i;
            ids.add(id);
            seededIds.add(id);
            insertSourceFile(id, "outage-" + id + ".txt", "text/plain", ("GOVERNOR IT " + id).getBytes());
            targetJdbc.update("INSERT INTO migration_state (source_id, lane, status) VALUES (?, ?, ?)",
                    id, "backfill", "PENDING");
        }
        return ids;
    }

    private void publishBackfillMessages(List<Long> ids, int chunkSize) {
        for (int i = 0; i < ids.size(); i += chunkSize) {
            List<Long> chunk = ids.subList(i, Math.min(i + chunkSize, ids.size()));
            String payload = writeJson(new BackfillMessage("backfill", chunk));
            kafkaTemplate.send("files.backfill.governor-it", String.valueOf(chunk.get(0)), payload);
        }
    }

    private void publishCdcCreateEnvelope(long id) {
        String payload = "{\"op\":\"c\",\"before\":null,\"after\":{\"id\":" + id + ",\"filename\":\"wedge.txt\","
                + "\"content_type\":\"text/plain\",\"byte_size\":5},\"ts_ms\":" + System.currentTimeMillis() + "}";
        kafkaTemplate.send("cdc.sourcedb.files.governor-it", String.valueOf(id), payload);
    }

    private void insertSourceFile(long id, String filename, String contentType, byte[] content) {
        sourceJdbc.update("INSERT INTO files (id, filename, content_type, content, byte_size) "
                + "VALUES (?, ?, ?, ?, ?)", id, filename, contentType, content, content.length);
    }

    private String statusOf(long id) {
        List<String> statuses = targetJdbc.queryForList(
                "SELECT status FROM migration_state WHERE source_id = ?", String.class, id);
        return statuses.isEmpty() ? null : statuses.get(0);
    }

    private long countStatus(List<Long> ids, String status) {
        if (ids.isEmpty()) {
            return 0;
        }
        String placeholders = String.join(",", ids.stream().map(id -> "?").toList());
        List<Object> params = new ArrayList<>();
        params.add(status);
        params.addAll(ids);
        Long count = targetJdbc.queryForObject(
                "SELECT count(*) FROM migration_state WHERE status = ? AND source_id IN (" + placeholders + ")",
                Long.class, params.toArray());
        return count == null ? 0 : count;
    }

    private long countEvents(String stage) {
        Long count = targetJdbc.queryForObject(
                "SELECT count(*) FROM migration_event WHERE stage = ?", Long.class, stage);
        return count == null ? 0 : count;
    }

    private long countEventsForId(long id, String stage) {
        Long count = targetJdbc.queryForObject(
                "SELECT count(*) FROM migration_event WHERE source_id = ? AND stage = ?", Long.class, id, stage);
        return count == null ? 0 : count;
    }

    /**
     * Sum, across every partition of topic, of how far the end of the log
     * is ahead of what groupId has committed; an id with no committed
     * offset yet counts from the start of the log, exactly the backlog
     * BreakerListener pausing is meant to protect. Uses a throwaway
     * consumer and the admin client only to read positions, never to join
     * groupId itself.
     */
    private long totalLag(String groupId, String topic) {
        Map<String, Object> adminProps = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (Admin admin = Admin.create(adminProps);
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

            long lag = 0;
            for (TopicPartition partition : partitions) {
                long endOffset = endOffsets.getOrDefault(partition, 0L);
                org.apache.kafka.clients.consumer.OffsetAndMetadata committedOffset = committed.get(partition);
                long consumedUpTo = committedOffset == null ? 0L : committedOffset.offset();
                lag += Math.max(0, endOffset - consumedUpTo);
            }
            return lag;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to compute consumer lag for group " + groupId, e);
        }
    }

    private static String writeJson(BackfillMessage message) {
        try {
            return OBJECT_MAPPER.writeValueAsString(message);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize a BackfillMessage", e);
        }
    }

    private static void setVendorMode(String mode) {
        vendorAdminClient.post().uri("/admin/mode").body(Map.of("mode", mode)).retrieve().toBodilessEntity();
    }

    private static void waitUntil(BooleanSupplier condition, Duration timeout, String timeoutMessage) {
        Instant deadline = Instant.now().plus(timeout);
        do {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep(POLL_INTERVAL);
        } while (Instant.now().isBefore(deadline));
        throw new AssertionError(timeoutMessage + " within " + timeout);
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
