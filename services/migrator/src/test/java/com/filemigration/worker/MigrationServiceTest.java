package com.filemigration.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.model.FileRecord;
import com.filemigration.model.Stage;
import com.filemigration.model.Status;
import com.filemigration.vendor.ErrorClass;
import com.filemigration.vendor.OcrResult;
import com.filemigration.vendor.VendorException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises MigrationService against in-memory fakes for every
 * collaborator, so its ordering guarantees can be checked without any
 * real database, object store, or vendor call.
 */
class MigrationServiceTest {

    private static final long CLAIM_RENEW_INTERVAL_SECONDS = 10L;
    private static final int WORKER_CONCURRENCY = 1;

    private FakeLedgerRepository ledger;
    private FakeSourceFileRepository sourceRepo;
    private FakeObjectStore objectStore;
    private FakeDocumentRepository documentRepo;
    private FakeEventRepository eventRepo;
    private FakeVendorClient vendorClient;
    private MigrationService service;

    @BeforeEach
    void setUp() {
        ledger = new FakeLedgerRepository();
        sourceRepo = new FakeSourceFileRepository();
        objectStore = new FakeObjectStore();
        documentRepo = new FakeDocumentRepository();
        eventRepo = new FakeEventRepository();
        vendorClient = new FakeVendorClient();
        service = new MigrationService(ledger, sourceRepo, objectStore, documentRepo, eventRepo, vendorClient,
                new ObjectMapper(), CLAIM_RENEW_INTERVAL_SECONDS, WORKER_CONCURRENCY);
    }

    @AfterEach
    void tearDown() {
        service.shutdown();
    }

    @Test
    void reprocessingAFileDoesNotCallVendorTwice() {
        seedPending(1L, "invoice.txt", "content one");

        MigrationOutcome first = service.migrate(List.of(1L), "backfill");
        MigrationOutcome second = service.migrate(List.of(1L), "backfill");

        assertEquals(1, first.done());
        assertEquals(0, second.done());
        assertEquals(1, second.skipped());
        assertEquals(1, vendorClient.callCount(), "claim blocks the second pass from reaching the vendor");
        assertEquals(1, documentRepo.documentCount(), "the second pass must upsert, not insert");
        assertEquals(Status.DONE, ledger.statusOf(1L));
    }

    @Test
    void cachedOcrPayloadSkipsVendorOnRetryAfterCrash() throws Exception {
        byte[] content = "INVOICE 00000001".getBytes(StandardCharsets.UTF_8);
        seedPending(1L, "invoice.txt", "INVOICE 00000001");
        String cachedPayload = new ObjectMapper().writeValueAsString(
                new OcrResult(1L, "INVOICE 00000001", 0.97, 1, "job-1"));
        ledger.presetState(1L, Status.OCR_DONE, cachedPayload);

        MigrationOutcome outcome = service.migrate(List.of(1L), "backfill");

        assertEquals(0, vendorClient.callCount());
        assertEquals(1, outcome.done());
        assertEquals(Status.DONE, ledger.statusOf(1L));
        assertEquals(1, documentRepo.documentCount());
        assertEquals("INVOICE 00000001", documentRepo.get(1L).ocrText());
        assertEquals(1, objectStore.putCount(),
                "the cached path must still write the blob, so the checksum matches what is stored");
        assertArrayEquals(content, objectStore.get("files/1"));
    }

    @Test
    void permanentVendorFailureMarksFailedPermanentAndDoesNotRethrow() {
        seedPending(5001L, "bad.txt", "whatever");
        vendorClient.throwOnNextCall(new VendorException(ErrorClass.PERMANENT, null, "unprocessable"));

        MigrationOutcome outcome = service.migrate(List.of(5001L), "backfill");

        assertEquals(1, outcome.permanentFailures());
        assertEquals(0, outcome.retryable());
        assertEquals(Status.FAILED_PERMANENT, ledger.statusOf(5001L));
        assertEquals(1, eventRepo.countByStageAndId(Stage.DLQ, 5001L));
        assertEquals(0, documentRepo.documentCount());
    }

    @Test
    void transientVendorFailureMarksFailedRetryableAndRethrows() {
        seedPending(6001L, "flaky.txt", "whatever");
        vendorClient.throwOnNextCall(new VendorException(ErrorClass.TRANSIENT, Duration.ofSeconds(1), "vendor down"));

        VendorException thrown = assertThrows(VendorException.class, () -> service.migrate(List.of(6001L), "cdc"));

        assertEquals(ErrorClass.TRANSIENT, thrown.errorClass());
        assertEquals(Status.FAILED_RETRYABLE, ledger.statusOf(6001L));
        assertEquals(1, eventRepo.countByStageAndId(Stage.RETRY, 6001L));
        assertEquals(0, documentRepo.documentCount());
    }

    @Test
    void mixedBatchSendsOnlyTheFreshFileToVendorAndBothReachDone() throws Exception {
        seedPending(2L, "fresh.txt", "fresh content");
        seedPending(1L, "cached.txt", "cached content");
        String cachedPayload = new ObjectMapper().writeValueAsString(
                new OcrResult(1L, "cached content", 0.9, 1, "job-1"));
        ledger.presetState(1L, Status.OCR_DONE, cachedPayload);

        MigrationOutcome outcome = service.migrate(List.of(1L, 2L), "backfill");

        assertEquals(2, outcome.done());
        assertEquals(1, vendorClient.callCount());
        assertEquals(List.of(2L), vendorClient.idsFromCall(0), "only the fresh file should reach the vendor");
        assertEquals(Status.DONE, ledger.statusOf(1L));
        assertEquals(Status.DONE, ledger.statusOf(2L));
        assertEquals(2, documentRepo.documentCount());
        assertEquals(2, objectStore.putCount(), "both the cached and the fresh file must have their blob written");
    }

    @Test
    void idsNotClaimedAreSkippedAndNeverSentToVendor() {
        seedPending(2L, "fresh.txt", "fresh content");
        ledger.presetState(1L, Status.DONE, null);

        MigrationOutcome outcome = service.migrate(List.of(1L, 2L), "backfill");

        assertEquals(1, outcome.done());
        assertEquals(1, outcome.skipped());
        assertEquals(1, vendorClient.callCount());
        assertEquals(List.of(2L), vendorClient.idsFromCall(0));
        assertFalse(eventRepo.events().stream().anyMatch(e -> Long.valueOf(1L).equals(e.sourceId())),
                "an id claim() did not return must never be touched");
    }

    @Test
    void claimedIdsAreRecordedBeforeAnyVendorCall() {
        seedPending(3L, "a.txt", "a");

        service.migrate(List.of(3L), "backfill");

        assertTrue(eventRepo.countByStageAndId(Stage.CLAIMED, 3L) >= 1);
        assertEquals(1, eventRepo.countByStageAndId(Stage.OCR_DONE, 3L));
        assertEquals(1, eventRepo.countByStageAndId(Stage.STORED, 3L));
    }

    @Test
    void vendorShortResponseMarksTheMissingIdRetryableAndPreservesTheInvariant() {
        seedPending(10L, "a.txt", "a content");
        seedPending(11L, "b.txt", "b content");
        vendorClient.omitFromResults(11L);

        MigrationOutcome outcome = service.migrate(List.of(10L, 11L), "backfill");

        assertEquals(1, outcome.done());
        assertEquals(0, outcome.skipped());
        assertEquals(0, outcome.permanentFailures());
        assertEquals(1, outcome.retryable());
        assertInvariant(2, outcome);
        assertEquals(Status.DONE, ledger.statusOf(10L));
        assertEquals(Status.FAILED_RETRYABLE, ledger.statusOf(11L));
        assertEquals(1, eventRepo.countByStageAndId(Stage.RETRY, 11L));
    }

    @Test
    void sourceRecordMissingForANeedsOcrIdIsMarkedRetryableAndPreservesTheInvariant() {
        ledger.presetPending(20L);
        seedPending(21L, "b.txt", "b content");

        MigrationOutcome outcome = service.migrate(List.of(20L, 21L), "backfill");

        assertEquals(1, outcome.done());
        assertEquals(1, outcome.retryable());
        assertInvariant(2, outcome);
        assertEquals(Status.FAILED_RETRYABLE, ledger.statusOf(20L));
        assertEquals(1, eventRepo.countByStageAndId(Stage.RETRY, 20L));
        assertEquals(1, vendorClient.callCount());
        assertFalse(vendorClient.idsFromCall(0).contains(20L),
                "an id missing its source record must never reach the vendor");
    }

    @Test
    void sourceRecordMissingForACachedIdIsMarkedRetryableAndPreservesTheInvariant() throws Exception {
        String cachedPayload = new ObjectMapper().writeValueAsString(
                new OcrResult(30L, "stale", 0.9, 1, "job-30"));
        ledger.presetState(30L, Status.OCR_DONE, cachedPayload);

        MigrationOutcome outcome = service.migrate(List.of(30L), "backfill");

        assertEquals(0, outcome.done());
        assertEquals(1, outcome.retryable());
        assertInvariant(1, outcome);
        assertEquals(Status.FAILED_RETRYABLE, ledger.statusOf(30L));
        assertEquals(0, vendorClient.callCount());
    }

    @Test
    void permanentFailureIsolatesThePoisonFileAndFinishesTheRestOfTheBatch() {
        seedPending(40L, "a.txt", "good a");
        seedPending(41L, "poison.txt", "bad");
        seedPending(42L, "c.txt", "good c");
        vendorClient.markPoison(41L);

        MigrationOutcome outcome = service.migrate(List.of(40L, 41L, 42L), "backfill");

        assertEquals(2, outcome.done());
        assertEquals(1, outcome.permanentFailures());
        assertEquals(0, outcome.retryable());
        assertInvariant(3, outcome);
        assertEquals(Status.DONE, ledger.statusOf(40L));
        assertEquals(Status.FAILED_PERMANENT, ledger.statusOf(41L));
        assertEquals(Status.DONE, ledger.statusOf(42L));
        assertEquals(1, eventRepo.countByStageAndId(Stage.DLQ, 41L));
        assertNull(documentRepo.get(41L), "the poisoned file must never get a document row");
        assertEquals(4, vendorClient.callCount(), "one whole-batch call plus one isolating retry per file");
    }

    private void seedPending(long id, String filename, String text) {
        byte[] content = text.getBytes(StandardCharsets.UTF_8);
        sourceRepo.put(new FileRecord(id, filename, "text/plain", content, content.length, Instant.now()));
        ledger.presetPending(id);
    }

    private static void assertInvariant(int totalRequested, MigrationOutcome outcome) {
        assertEquals(totalRequested,
                outcome.done() + outcome.skipped() + outcome.permanentFailures() + outcome.retryable(),
                "every id passed to migrate must be accounted for exactly once");
    }
}
