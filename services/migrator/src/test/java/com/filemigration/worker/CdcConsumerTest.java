package com.filemigration.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.governor.TestGovernorFactory;
import com.filemigration.model.FileRecord;
import com.filemigration.model.Stage;
import com.filemigration.model.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises how a Debezium envelope is routed: a create or read seeds and
 * migrates, an update resets the cached OCR result and migrates, a delete
 * reads its id from "before" (not "after", which is always null for a
 * delete) and removes both the ledger row and the object, and a
 * null-valued tombstone record is acknowledged without touching anything.
 * The ack/nack decisions mirror BackfillConsumerTest: fully resolved acks,
 * still-unresolved nacks, unparseable acks without ever calling migrate(),
 * and any exception from processing nacks with the configured backoff.
 */
class CdcConsumerTest {

    private static final long NACK_BACKOFF_SECONDS = 10L;
    private static final long CLAIM_RENEW_INTERVAL_SECONDS = 10L;
    private static final int WORKER_CONCURRENCY = 1;
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private FakeLedgerRepository ledger;
    private FakeSourceFileRepository sourceRepo;
    private FakeVendorClient vendorClient;
    private FakeObjectStore objectStore;
    private FakeEventRepository eventRepo;
    private MigrationService migrationService;
    private CdcConsumer consumer;

    @BeforeEach
    void setUp() {
        ledger = new FakeLedgerRepository();
        sourceRepo = new FakeSourceFileRepository();
        vendorClient = new FakeVendorClient();
        objectStore = new FakeObjectStore();
        eventRepo = new FakeEventRepository();
        migrationService = new MigrationService(ledger, sourceRepo, objectStore,
                new FakeDocumentRepository(), eventRepo, vendorClient, TestGovernorFactory.passthrough(),
                OBJECT_MAPPER, CLAIM_RENEW_INTERVAL_SECONDS, WORKER_CONCURRENCY, MAX_RETRY_ATTEMPTS);
        consumer = new CdcConsumer(migrationService, ledger, objectStore, eventRepo, OBJECT_MAPPER,
                NACK_BACKOFF_SECONDS);
    }

    @AfterEach
    void tearDown() {
        migrationService.shutdown();
    }

    @Test
    void createEnvelopeSeedsPendingAndMigratesToDone() {
        putSourceRecord(1L, "invoice-1.txt", "content one");
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(envelope("c", null, afterRow(1L)), ack);

        assertTrue(ack.acknowledged, "an id that reached DONE must be acknowledged");
        assertNull(ack.nackedWith);
        assertEquals(Status.DONE, ledger.statusOf(1L));
        assertEquals(1, eventRepo.countByStageAndId(Stage.CDC_CAPTURED, 1L),
                "a CDC_CAPTURED event must be recorded on receipt");
    }

    @Test
    void readEnvelopeBehavesLikeCreate() {
        putSourceRecord(2L, "invoice-2.txt", "content two");
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(envelope("r", null, afterRow(2L)), ack);

        assertTrue(ack.acknowledged);
        assertEquals(Status.DONE, ledger.statusOf(2L));
    }

    @Test
    void updateEnvelopeResetsCachedOcrThenMigratesAgain() {
        putSourceRecord(3L, "invoice-3.txt", "original content");
        ledger.presetState(3L, Status.DONE, "{\"id\":3,\"text\":\"stale\",\"confidence\":0.9,\"pageCount\":1,"
                + "\"jobId\":\"job-old\"}");
        putSourceRecord(3L, "invoice-3.txt", "updated content");
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(envelope("u", null, afterRow(3L)), ack);

        assertTrue(ack.acknowledged);
        assertEquals(Status.DONE, ledger.statusOf(3L));
        assertEquals(1, vendorClient.callCount(),
                "an update must clear the cached OCR payload so the vendor is called again rather than reusing it");
    }

    @Test
    void updateEnvelopeForAnIdTheLedgerHasNeverSeenIsSeededAndMigratedRatherThanAckedAwayWithNothingDone() {
        // No presetState/presetPending for id 9: the ledger has never
        // heard of it, exactly as if the backfill lane has not reached it
        // yet. resetForUpdate alone would match zero rows here.
        putSourceRecord(9L, "invoice-9.txt", "brand new content");
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(envelope("u", null, afterRow(9L)), ack);

        assertTrue(ack.acknowledged, "an update for a previously untracked id must still resolve to DONE");
        assertEquals(Status.DONE, ledger.statusOf(9L),
                "an update for an id the ledger has never seen must be seeded and migrated, not silently dropped");
        assertEquals(1, vendorClient.callCount());
    }

    /**
     * THE UPDATE WEDGE. No source record exists for id 11, so every
     * delivery of this update envelope fails the same structural way a
     * create envelope for a missing row does. What this test is actually
     * about is that a nack causes Kafka to redeliver the identical
     * envelope, carrying the identical version, and resetForUpdate must
     * never treat that redelivery as new work: if it reset
     * consecutive_failures on every delivery the way it once did, this id
     * could never reach the retry cap and would nack forever instead of
     * ever reaching FAILED_PERMANENT. Calling consume() again with the
     * exact same payload is what a real redelivery looks like from this
     * consumer's point of view.
     */
    @Test
    void repeatedDeliveryOfTheSameUpdateEnvelopeForAnIdWithNoMatchingSourceRowReachesFailedPermanentWithinTheCapInsteadOfNackingForever() {
        String sameEnvelope = envelope("u", null, afterRow(11L));
        boolean settled = false;

        for (int delivery = 1; delivery <= MAX_RETRY_ATTEMPTS + 2; delivery++) {
            RecordingAcknowledgment ack = new RecordingAcknowledgment();
            consumer.consume(sameEnvelope, ack);
            if (ack.acknowledged) {
                settled = true;
                break;
            }
            assertEquals(delivery, ledger.consecutiveFailuresOf(11L),
                    "redelivery " + delivery + " of the identical update envelope must accumulate consecutive "
                            + "failures rather than reset them back to zero, or this id could never reach the "
                            + "retry cap and would nack forever");
        }

        assertTrue(settled, "the id must eventually be acknowledged once it reaches a terminal state, not nack "
                + "forever");
        assertEquals(Status.FAILED_PERMANENT, ledger.statusOf(11L));
        assertEquals(1, eventRepo.countByStageAndId(Stage.DLQ, 11L));
    }

    @Test
    void deleteEnvelopeReadsIdFromBeforeAndTombstonesAndDeletesTheObject() {
        ledger.presetState(4L, Status.DONE, null);
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        // "after" is populated with a different id to prove the delete
        // path never reads it: if it did, id 999 would be tombstoned
        // instead of 4, and this assertion would fail.
        consumer.consume(envelope("d", beforeRow(4L), afterRow(999L)), ack);

        assertTrue(ack.acknowledged);
        assertFalse(ledger.exists(4L), "the ledger row must be gone after a delete");
        assertTrue(objectStore.wasDeleted("files/4"), "the object store entry must be deleted after a delete");
        assertEquals(0, vendorClient.callCount(), "a delete must never call the vendor");
        assertEquals(1, eventRepo.countByStageAndId(Stage.CDC_CAPTURED, 4L));
    }

    @Test
    void nullValuedTombstoneRecordIsAcknowledgedWithoutThrowingOrActing() {
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(null, ack);

        assertTrue(ack.acknowledged, "a null-valued Kafka tombstone must be acknowledged");
        assertNull(ack.nackedWith);
        assertTrue(eventRepo.events().isEmpty(), "a null value carries no id, so nothing should be recorded");
    }

    @Test
    void unparseableMessageIsAcknowledgedWithoutEverCallingMigrate() {
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume("this is not json", ack);

        assertTrue(ack.acknowledged, "an unparseable message must be acknowledged so it is never retried");
        assertNull(ack.nackedWith);
        assertEquals(0, vendorClient.callCount());
        assertTrue(eventRepo.events().isEmpty());
    }

    @Test
    void envelopeWithNoIdInTheExpectedFieldIsAcknowledgedWithoutActing() {
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(envelope("c", null, null), ack);

        assertTrue(ack.acknowledged, "a create with no usable id must be acknowledged, since retrying never helps");
        assertEquals(0, vendorClient.callCount());
        assertTrue(eventRepo.events().isEmpty());
    }

    @Test
    void negativelyAcknowledgesWithBackoffWhenTheIdIsStillUnresolvedAfterProcessing() {
        // A row still IN_FLIGHT from an earlier, now-dead attempt is not
        // claimable by the fake ledger's rules, so migrate() will skip it
        // rather than finishing it, leaving it unresolved.
        ledger.presetState(5L, Status.IN_FLIGHT, null);
        putSourceRecord(5L, "invoice-5.txt", "content five");
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(envelope("c", null, afterRow(5L)), ack);

        assertFalse(ack.acknowledged, "an id left unresolved after processing must not be acknowledged");
        assertEquals(Duration.ofSeconds(NACK_BACKOFF_SECONDS), ack.nackedWith);
        assertEquals(Status.IN_FLIGHT, ledger.statusOf(5L), "the still-claimed row is untouched, not lost");
    }

    @Test
    void negativelyAcknowledgesWithBackoffWhenMigrateThrows() {
        putSourceRecord(6L, "invoice-6.txt", "content six");
        objectStore.throwOnNextPut(new IllegalStateException("MinIO put failed"));
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(envelope("c", null, afterRow(6L)), ack);

        assertFalse(ack.acknowledged, "a failure processing the envelope must not be acknowledged");
        assertEquals(Duration.ofSeconds(NACK_BACKOFF_SECONDS), ack.nackedWith);
    }

    @Test
    void negativelyAcknowledgesWithBackoffWhenDeleteProcessingThrows() {
        ledger.presetState(7L, Status.DONE, null);
        objectStore.throwOnNextDelete(new IllegalStateException("MinIO delete failed"));
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(envelope("d", beforeRow(7L), null), ack);

        assertFalse(ack.acknowledged, "a failure deleting the object must not be acknowledged");
        assertEquals(Duration.ofSeconds(NACK_BACKOFF_SECONDS), ack.nackedWith);
    }

    @Test
    void negativelyAcknowledgesWithBackoffWhenFindUnresolvedThrows() {
        putSourceRecord(10L, "invoice-10.txt", "content ten");
        ledger.throwOnNextFindUnresolved(new IllegalStateException("Postgres blip"));
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(envelope("c", null, afterRow(10L)), ack);

        assertFalse(ack.acknowledged,
                "a failure checking whether the id is fully resolved must not be acknowledged; it must never "
                        + "reach the container's own error handling, which would commit the offset anyway");
        assertEquals(Duration.ofSeconds(NACK_BACKOFF_SECONDS), ack.nackedWith);
    }

    @Test
    void unrecognizedOpIsAcknowledgedWithoutActing() {
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(envelope("t", null, afterRow(8L)), ack);

        assertTrue(ack.acknowledged, "an op this consumer does not understand must be acknowledged, since "
                + "retrying it would never help");
        assertEquals(0, vendorClient.callCount());
    }

    /**
     * Same op, but this time the id already has a PENDING row sitting in
     * the ledger, unresolved from an earlier envelope for the same id.
     * Debezium emits op="t" for a MySQL TRUNCATE, so a row the ledger has
     * not yet finished with is a case this consumer will actually see. The
     * unrecognized-op branch never touches ledger state, so nothing about
     * that PENDING row resolves as a result of this envelope; acknowledging
     * immediately, rather than checking whether the id is resolved, is
     * what keeps this envelope from being redelivered and nacked forever.
     */
    @Test
    void unrecognizedOpIsAcknowledgedRatherThanNackLoopedWhenTheIdAlreadyHasAnUnresolvedLedgerRow() {
        ledger.presetPending(12L);
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(envelope("t", null, afterRow(12L)), ack);

        assertTrue(ack.acknowledged, "an unrecognized op must be acknowledged even when the id's existing ledger "
                + "row is still unresolved, since nack-looping it forever would never help");
        assertNull(ack.nackedWith);
        assertEquals(Status.PENDING, ledger.statusOf(12L), "the unrecognized op must not touch the existing row");
        assertEquals(0, vendorClient.callCount());
    }

    private void putSourceRecord(long id, String filename, String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        sourceRepo.put(new FileRecord(id, filename, "text/plain", bytes, bytes.length, Instant.now()));
    }

    private static String afterRow(long id) {
        return "{\"id\":" + id + ",\"filename\":\"invoice-" + id + ".txt\",\"content_type\":\"text/plain\","
                + "\"byte_size\":10}";
    }

    private static String beforeRow(long id) {
        return afterRow(id);
    }

    private static String envelope(String op, String beforeJson, String afterJson) {
        String before = beforeJson == null ? "null" : beforeJson;
        String after = afterJson == null ? "null" : afterJson;
        return "{\"op\":\"" + op + "\",\"before\":" + before + ",\"after\":" + after
                + ",\"ts_ms\":1700000000000,\"source\":{\"table\":\"files\"}}";
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
