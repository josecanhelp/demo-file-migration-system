package com.filemigration.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.governor.Governor;
import com.filemigration.model.FileRecord;
import com.filemigration.model.Stage;
import com.filemigration.store.DocumentRepository;
import com.filemigration.store.EventRepository;
import com.filemigration.store.LedgerRepository;
import com.filemigration.store.ObjectStore;
import com.filemigration.store.SourceFileRepository;
import com.filemigration.vendor.ErrorClass;
import com.filemigration.vendor.OcrResult;
import com.filemigration.vendor.VendorClient;
import com.filemigration.vendor.VendorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Carries a batch of source ids from the source database through to the
 * target store: claim ownership, run OCR on whatever has not already had
 * it done, and write the result. The order these steps run in is what
 * lets a batch be resubmitted after a crash without paying for OCR twice
 * or writing a duplicate document row.
 *
 * Every id passed to {@link #migrate} lands in exactly one bucket of the
 * returned outcome: done, skipped, permanently failed, or retryable.
 * Nothing is ever left unaccounted for, since an id left untouched would
 * stay IN_FLIGHT with no way for a later call to notice it needs help.
 *
 * For as long as a call is working through the ids it claimed, it renews
 * only that exact set on a short interval, so the ledger's claim lease
 * only has to outlast a brief gap between renewals rather than however
 * long this whole call takes. Renewing is scoped strictly to the ids this
 * call itself claimed, never to whatever ids the caller originally asked
 * for: an id another, still-live attempt already owns is never touched
 * here, so renewing never extends a claim this call does not hold.
 */
@Service
public class MigrationService {

    private static final Logger log = LoggerFactory.getLogger(MigrationService.class);

    private final LedgerRepository ledger;
    private final SourceFileRepository sourceRepo;
    private final ObjectStore objectStore;
    private final DocumentRepository documentRepo;
    private final EventRepository eventRepo;
    private final VendorClient vendorClient;
    private final Governor governor;
    private final ObjectMapper objectMapper;
    private final Duration claimRenewInterval;
    private final int maxRetryAttempts;
    private final ScheduledExecutorService renewalExecutor;

    public MigrationService(LedgerRepository ledger, SourceFileRepository sourceRepo, ObjectStore objectStore,
            DocumentRepository documentRepo, EventRepository eventRepo, VendorClient vendorClient,
            Governor governor, ObjectMapper objectMapper,
            @Value("${migrator.claim-renew-interval-seconds}") long claimRenewIntervalSeconds,
            @Value("${migrator.worker-concurrency}") int workerConcurrency,
            @Value("${migrator.max-retry-attempts}") int maxRetryAttempts) {
        this.ledger = ledger;
        this.sourceRepo = sourceRepo;
        this.objectStore = objectStore;
        this.documentRepo = documentRepo;
        this.eventRepo = eventRepo;
        this.vendorClient = vendorClient;
        this.governor = governor;
        this.objectMapper = objectMapper;
        this.claimRenewInterval = Duration.ofSeconds(claimRenewIntervalSeconds);
        this.maxRetryAttempts = maxRetryAttempts;
        this.renewalExecutor = Executors.newScheduledThreadPool(Math.max(2, workerConcurrency), runnable -> {
            Thread thread = new Thread(runnable, "claim-renewer");
            thread.setDaemon(true);
            return thread;
        });
    }

    @PreDestroy
    public void shutdown() {
        renewalExecutor.shutdownNow();
    }

    /**
     * Migrates the given source ids for the given lane. An id this call
     * does not own, because another worker already claimed it or it is
     * already done, is counted as skipped and touched nowhere else. Every
     * id this call does own is either finished, marked permanently
     * failed, or marked retryable and left claimable again for a later
     * attempt: done + skipped + permanentFailures + retryable always adds
     * up to the number of ids passed in.
     *
     * Before claiming anything, whichever given id is FAILED_RETRYABLE and
     * has already used up every attempt it is allowed is moved straight to
     * FAILED_PERMANENT and dead-lettered instead: claim() itself would
     * otherwise reclaim it unconditionally forever, which is exactly how a
     * single id with no way to ever succeed, for example one whose source
     * row is permanently gone, ends up nacking forever and blocking every
     * other id queued behind it on the same partition.
     */
    public MigrationOutcome migrate(List<Long> sourceIds, String lane) {
        List<LedgerRepository.ExceededAttempt> exceeded = ledger.failExceededAttempts(sourceIds, maxRetryAttempts);
        for (LedgerRepository.ExceededAttempt id : exceeded) {
            deadLetter(id.sourceId(), lane, "MAX_RETRY_ATTEMPTS_EXCEEDED", id.attempts(), id.lastError());
        }

        List<Long> claimed = ledger.claim(sourceIds);
        int skipped = sourceIds.size() - claimed.size() - exceeded.size();
        if (claimed.isEmpty()) {
            return new MigrationOutcome(0, skipped, exceeded.size(), 0);
        }

        // Scheduled against claimed, and only claimed: an id from
        // sourceIds that this call did not just claim is, by definition,
        // still owned by someone else's live attempt or already done,
        // and renewing it here would extend a claim this call has no
        // right to extend.
        ScheduledFuture<?> renewal = renewalExecutor.scheduleAtFixedRate(() -> renewClaims(claimed),
                claimRenewInterval.toSeconds(), claimRenewInterval.toSeconds(), TimeUnit.SECONDS);
        try {
            for (Long id : claimed) {
                eventRepo.record(id, Stage.CLAIMED, lane, null);
            }

            Map<Long, String> cachedPayloads = ledger.findCachedOcrPayloads(claimed);
            List<Long> cachedIds = new ArrayList<>();
            List<Long> needsOcr = new ArrayList<>();
            for (Long id : claimed) {
                if (cachedPayloads.containsKey(id)) {
                    cachedIds.add(id);
                } else {
                    needsOcr.add(id);
                }
            }

            // Metadata is fetched for every claimed id, cached or not,
            // since the document row needs filename, content type, and a
            // checksum of the current content regardless of whether OCR
            // runs again this time.
            Map<Long, FileRecord> recordsById = new HashMap<>();
            for (FileRecord record : sourceRepo.findByIds(claimed)) {
                recordsById.put(record.id(), record);
            }

            int done = 0;
            // Seeded with what failExceededAttempts already condemned
            // above, so the two returns below never have to add it back
            // in separately.
            int permanentFailures = exceeded.size();
            int retryable = 0;

            for (Long id : cachedIds) {
                FileRecord record = recordsById.get(id);
                if (record == null) {
                    markRetryable(id, "Source record no longer exists for a claimed id with a cached OCR payload",
                            lane);
                    retryable++;
                    continue;
                }
                OcrResult cached = readOcrPayload(cachedPayloads.get(id));
                finishDocument(record, cached, lane);
                done++;
            }

            List<FileRecord> freshRecords = new ArrayList<>();
            for (Long id : needsOcr) {
                FileRecord record = recordsById.get(id);
                if (record == null) {
                    markRetryable(id, "Source record no longer exists for a claimed id", lane);
                    retryable++;
                    continue;
                }
                freshRecords.add(record);
            }

            if (!freshRecords.isEmpty()) {
                Map<Long, OcrResult> results;
                try {
                    results = governor.execute(lane, () -> vendorClient.ocrBatch(freshRecords));
                } catch (VendorException e) {
                    if (e.errorClass() == ErrorClass.PERMANENT) {
                        IsolationResult isolation = isolatePermanentFailure(freshRecords, lane);
                        done += isolation.done();
                        permanentFailures += isolation.permanentFailures();
                        retryable += isolation.retryable();
                        if (isolation.toRethrow() != null) {
                            throw isolation.toRethrow();
                        }
                        return new MigrationOutcome(done, skipped, permanentFailures, retryable);
                    }
                    for (FileRecord record : freshRecords) {
                        markRetryable(record.id(), e.getMessage(), lane);
                    }
                    throw e;
                }

                for (FileRecord record : freshRecords) {
                    OcrResult result = results.get(record.id());
                    if (result == null) {
                        markRetryable(record.id(), "Vendor response did not include a result for this id", lane);
                        retryable++;
                        continue;
                    }
                    ledger.saveOcrPayload(record.id(), writeOcrPayload(result));
                    eventRepo.record(record.id(), Stage.OCR_DONE, lane, null);
                    finishDocument(record, result, lane);
                    done++;
                }
            }

            return new MigrationOutcome(done, skipped, permanentFailures, retryable);
        } finally {
            renewal.cancel(false);
        }
    }

    private void renewClaims(List<Long> ids) {
        try {
            ledger.renewClaims(ids);
        } catch (RuntimeException e) {
            log.warn("Failed to renew the claim lease for {} id(s); it may expire and become reclaimable "
                    + "while this call is still legitimately in progress", ids.size(), e);
        }
    }

    /**
     * Handles a PERMANENT vendor error for a batch that held more than one
     * file. The real vendor rejects the whole HTTP call the moment any one
     * document in it is unprocessable, so the batch is re-sent one file at
     * a time to find out which one is actually poison rather than
     * condemning every file that happened to share the request. A batch of
     * exactly one file is already isolated, so it is failed directly
     * without a second call. If an individual retry comes back with
     * anything other than PERMANENT, that file and every file not yet
     * retried are marked retryable and the exception is handed back to the
     * caller to rethrow, since only PERMANENT is ever safe to absorb here.
     */
    private IsolationResult isolatePermanentFailure(List<FileRecord> freshRecords, String lane) {
        if (freshRecords.size() == 1) {
            FileRecord record = freshRecords.get(0);
            ledger.markFailed(record.id(), "Vendor rejected this file", true);
            deadLetter(record.id(), lane, ErrorClass.PERMANENT.name(), ledger.attemptsOf(record.id()),
                    "Vendor rejected this file");
            return new IsolationResult(0, 1, 0, null);
        }

        int done = 0;
        int permanentFailures = 0;
        int retryable = 0;
        for (int i = 0; i < freshRecords.size(); i++) {
            FileRecord record = freshRecords.get(i);
            Map<Long, OcrResult> singleResult;
            try {
                singleResult = governor.execute(lane, () -> vendorClient.ocrBatch(List.of(record)));
            } catch (VendorException individual) {
                if (individual.errorClass() != ErrorClass.PERMANENT) {
                    for (int j = i; j < freshRecords.size(); j++) {
                        markRetryable(freshRecords.get(j).id(), individual.getMessage(), lane);
                        retryable++;
                    }
                    return new IsolationResult(done, permanentFailures, retryable, individual);
                }
                ledger.markFailed(record.id(), individual.getMessage(), true);
                deadLetter(record.id(), lane, ErrorClass.PERMANENT.name(), ledger.attemptsOf(record.id()),
                        individual.getMessage());
                permanentFailures++;
                continue;
            }
            OcrResult result = singleResult.get(record.id());
            if (result == null) {
                markRetryable(record.id(), "Vendor response did not include a result for this id", lane);
                retryable++;
                continue;
            }
            ledger.saveOcrPayload(record.id(), writeOcrPayload(result));
            eventRepo.record(record.id(), Stage.OCR_DONE, lane, null);
            finishDocument(record, result, lane);
            done++;
        }
        return new IsolationResult(done, permanentFailures, retryable, null);
    }

    private void markRetryable(long id, String message, String lane) {
        ledger.markFailed(id, message, false);
        eventRepo.record(id, Stage.RETRY, lane, null);
    }

    /**
     * Records the DLQ stage for an id the caller has already marked
     * FAILED_PERMANENT, both in migration_event and on the files.dlq Kafka
     * topic via {@link Governor#deadLetter}, so a permanently failed file
     * is visible in both places regardless of which of the paths inside
     * this class that lead to FAILED_PERMANENT called it.
     */
    private void deadLetter(long id, String lane, String errorClass, int attempts, String lastError) {
        eventRepo.record(id, Stage.DLQ, lane, writeDlqDetail(errorClass, attempts, lastError));
        governor.deadLetter(id, lane, errorClass, attempts, lastError);
    }

    private String writeDlqDetail(String errorClass, int attempts, String lastError) {
        try {
            return objectMapper.writeValueAsString(Map.of(
                    "errorClass", errorClass,
                    "attempts", attempts,
                    "lastError", lastError == null ? "" : lastError));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize a DLQ event detail", e);
        }
    }

    private void finishDocument(FileRecord record, OcrResult ocr, String lane) {
        String objectKey = objectStore.keyFor(record.id());
        // Written unconditionally, cached OCR result or not, so the
        // checksum below always describes the bytes that just landed in
        // the object store rather than an assumption about what an
        // earlier attempt may have stored there.
        objectStore.put(objectKey, record.content(), record.contentType());
        String checksum = sha256Hex(record.content());
        documentRepo.upsert(record.id(), record.filename(), record.contentType(), objectKey, record.byteSize(),
                checksum, ocr.text(), ocr.confidence(), ocr.pageCount(), ocr.jobId());
        ledger.markDone(record.id(), checksum);
        eventRepo.record(record.id(), Stage.STORED, lane, null);
    }

    private String writeOcrPayload(OcrResult result) {
        try {
            return objectMapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OCR result for id " + result.id(), e);
        }
    }

    private OcrResult readOcrPayload(String json) {
        try {
            return objectMapper.readValue(json, OcrResult.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to parse cached OCR payload: " + json, e);
        }
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

    /**
     * Running tally from isolating a poisoned batch: how many files were
     * finished, how many were individually condemned, how many were
     * marked retryable, and, if isolation had to stop early because an
     * individual retry came back with a non-permanent error, the
     * exception the caller must rethrow.
     */
    private record IsolationResult(int done, int permanentFailures, int retryable, VendorException toRethrow) {
    }
}
