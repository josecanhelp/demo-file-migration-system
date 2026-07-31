package com.filemigration.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.backfill.BackfillMessage;
import com.filemigration.governor.TestGovernorFactory;
import com.filemigration.model.FileRecord;
import com.filemigration.model.Status;
import com.filemigration.vendor.ErrorClass;
import com.filemigration.vendor.VendorException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the exact decision that caused ids to be stranded before this
 * fix: whether a batch is acknowledged, negatively acknowledged, or
 * acknowledged without ever reaching migrate() at all. A recording fake
 * for Acknowledgment is used so each case can assert on the actual call
 * BackfillConsumer made, not merely on the ledger state migrate() left
 * behind.
 */
class BackfillConsumerTest {

    private static final long NACK_BACKOFF_SECONDS = 30L;
    private static final long CLAIM_RENEW_INTERVAL_SECONDS = 10L;
    private static final int WORKER_CONCURRENCY = 1;
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private FakeLedgerRepository ledger;
    private FakeSourceFileRepository sourceRepo;
    private FakeVendorClient vendorClient;
    private FakeObjectStore objectStore;
    private MigrationService migrationService;
    private BackfillConsumer consumer;

    @BeforeEach
    void setUp() {
        ledger = new FakeLedgerRepository();
        sourceRepo = new FakeSourceFileRepository();
        vendorClient = new FakeVendorClient();
        objectStore = new FakeObjectStore();
        migrationService = new MigrationService(ledger, sourceRepo, objectStore,
                new FakeDocumentRepository(), new FakeEventRepository(), vendorClient,
                TestGovernorFactory.passthrough(), OBJECT_MAPPER,
                CLAIM_RENEW_INTERVAL_SECONDS, WORKER_CONCURRENCY, MAX_RETRY_ATTEMPTS);
        consumer = new BackfillConsumer(migrationService, ledger, OBJECT_MAPPER, NACK_BACKOFF_SECONDS);
    }

    @AfterEach
    void tearDown() {
        migrationService.shutdown();
    }

    @Test
    void acknowledgesOnceEveryIdInTheBatchReachesDone() throws JsonProcessingException {
        seedPending(1L, "invoice-1.txt", "content one");
        seedPending(2L, "invoice-2.txt", "content two");
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(message(1L, 2L), ack);

        assertTrue(ack.acknowledged, "every id resolved to DONE, so the batch must be acknowledged");
        assertNull(ack.nackedWith, "a fully resolved batch must not be negatively acknowledged");
        assertEquals(Status.DONE, ledger.statusOf(1L));
        assertEquals(Status.DONE, ledger.statusOf(2L));
    }

    @Test
    void negativelyAcknowledgesWithBackoffWhenAnIdIsStillUnresolvedAfterMigrateReturns() throws JsonProcessingException {
        seedPending(1L, "invoice-1.txt", "content one");
        // Simulates an id still owned by an earlier, now-dead attempt at
        // this same message: IN_FLIGHT is not claimable by the fake
        // ledger's claim() rules, so migrate() will skip it entirely
        // rather than finishing it.
        ledger.presetState(2L, Status.IN_FLIGHT, null);
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(message(1L, 2L), ack);

        assertFalse(ack.acknowledged, "a batch with an unresolved id must not be acknowledged");
        assertEquals(Duration.ofSeconds(NACK_BACKOFF_SECONDS), ack.nackedWith,
                "the batch must be negatively acknowledged with the configured backoff");
        assertEquals(Status.DONE, ledger.statusOf(1L), "the id that could be resolved still was");
        assertEquals(Status.IN_FLIGHT, ledger.statusOf(2L), "the still-claimed id is untouched, not lost");
    }

    @Test
    void negativelyAcknowledgesWithBackoffWhenTheVendorCallFails() throws JsonProcessingException {
        seedPending(1L, "invoice-1.txt", "content one");
        vendorClient.throwOnNextCall(new VendorException(ErrorClass.TRANSIENT, null, "vendor unreachable"));
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(message(1L), ack);

        assertFalse(ack.acknowledged, "a batch that fails calling the vendor must not be acknowledged");
        assertEquals(Duration.ofSeconds(NACK_BACKOFF_SECONDS), ack.nackedWith,
                "a vendor failure must be turned into a negative acknowledgment here rather than left to "
                        + "propagate into the container's own error handling");
    }

    @Test
    void negativelyAcknowledgesWithBackoffWhenMigrateThrowsAnInfrastructureException() throws JsonProcessingException {
        seedPending(1L, "invoice-1.txt", "content one");
        objectStore.throwOnNextPut(new IllegalStateException("MinIO put failed"));
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(message(1L), ack);

        assertFalse(ack.acknowledged,
                "a batch that fails for any reason other than an unparseable message must not be acknowledged");
        assertEquals(Duration.ofSeconds(NACK_BACKOFF_SECONDS), ack.nackedWith,
                "an infrastructure failure unrelated to the vendor call must also be turned into a negative "
                        + "acknowledgment here, not left to propagate into the container's own error handling");
    }

    @Test
    void negativelyAcknowledgesWithBackoffWhenFindUnresolvedThrows() throws JsonProcessingException {
        seedPending(1L, "invoice-1.txt", "content one");
        ledger.throwOnNextFindUnresolved(new IllegalStateException("Postgres blip"));
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume(message(1L), ack);

        assertFalse(ack.acknowledged,
                "a failure checking whether the batch is fully resolved must not be acknowledged; it must never "
                        + "reach the container's own error handling, which would commit the offset anyway");
        assertEquals(Duration.ofSeconds(NACK_BACKOFF_SECONDS), ack.nackedWith);
    }

    @Test
    void acknowledgesAnUnparseableMessageWithoutEverCallingMigrate() {
        RecordingAcknowledgment ack = new RecordingAcknowledgment();

        consumer.consume("this is not json", ack);

        assertTrue(ack.acknowledged, "an unparseable message must be acknowledged so it is never retried");
        assertNull(ack.nackedWith);
        assertEquals(0, vendorClient.callCount(), "an unparseable message must never reach the vendor call");
    }

    private void seedPending(long id, String filename, String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        sourceRepo.put(new FileRecord(id, filename, "text/plain", bytes, bytes.length, Instant.now()));
        ledger.presetPending(id);
    }

    private static String message(Long... ids) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(new BackfillMessage("backfill", List.of(ids)));
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
