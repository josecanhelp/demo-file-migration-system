package com.filemigration.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Carries a batch of source ids from the source database through to the
 * target store: claim ownership, run OCR on whatever has not already had
 * it done, and write the result. The order these steps run in is what
 * lets a batch be resubmitted after a crash without paying for OCR twice
 * or writing a duplicate document row.
 */
@Service
public class MigrationService {

    private final LedgerRepository ledger;
    private final SourceFileRepository sourceRepo;
    private final ObjectStore objectStore;
    private final DocumentRepository documentRepo;
    private final EventRepository eventRepo;
    private final VendorClient vendorClient;
    private final ObjectMapper objectMapper;

    public MigrationService(LedgerRepository ledger, SourceFileRepository sourceRepo, ObjectStore objectStore,
            DocumentRepository documentRepo, EventRepository eventRepo, VendorClient vendorClient,
            ObjectMapper objectMapper) {
        this.ledger = ledger;
        this.sourceRepo = sourceRepo;
        this.objectStore = objectStore;
        this.documentRepo = documentRepo;
        this.eventRepo = eventRepo;
        this.vendorClient = vendorClient;
        this.objectMapper = objectMapper;
    }

    /**
     * Migrates the given source ids for the given lane. An id this call
     * does not own, because another worker already claimed it or it is
     * already done, is counted as skipped and touched nowhere else. Every
     * id this call does own is either finished, or, if the vendor call
     * for it fails, left claimable again for a later attempt.
     */
    public MigrationOutcome migrate(List<Long> sourceIds, String lane) {
        List<Long> claimed = ledger.claim(sourceIds);
        int skipped = sourceIds.size() - claimed.size();
        if (claimed.isEmpty()) {
            return new MigrationOutcome(0, skipped, 0, 0);
        }
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

        // Metadata is fetched for every claimed id, cached or not, since
        // the document row needs filename, content type, and a checksum
        // of the current content regardless of whether OCR runs again
        // this time.
        Map<Long, FileRecord> recordsById = new HashMap<>();
        for (FileRecord record : sourceRepo.findByIds(claimed)) {
            recordsById.put(record.id(), record);
        }

        int done = 0;
        int permanentFailures = 0;
        int retryable = 0;

        for (Long id : cachedIds) {
            FileRecord record = recordsById.get(id);
            if (record == null) {
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
                continue;
            }
            objectStore.put(objectStore.keyFor(record.id()), record.content(), record.contentType());
            freshRecords.add(record);
        }

        if (!freshRecords.isEmpty()) {
            Map<Long, OcrResult> results;
            try {
                results = vendorClient.ocrBatch(freshRecords);
            } catch (VendorException e) {
                boolean permanent = e.errorClass() == ErrorClass.PERMANENT;
                for (FileRecord record : freshRecords) {
                    ledger.markFailed(record.id(), e.getMessage(), permanent);
                    eventRepo.record(record.id(), permanent ? Stage.DLQ : Stage.RETRY, lane, null);
                }
                if (permanent) {
                    permanentFailures += freshRecords.size();
                    return new MigrationOutcome(done, skipped, permanentFailures, retryable);
                }
                retryable += freshRecords.size();
                throw e;
            }

            for (FileRecord record : freshRecords) {
                OcrResult result = results.get(record.id());
                if (result == null) {
                    continue;
                }
                ledger.saveOcrPayload(record.id(), writeOcrPayload(result));
                eventRepo.record(record.id(), Stage.OCR_DONE, lane, null);
                finishDocument(record, result, lane);
                done++;
            }
        }

        return new MigrationOutcome(done, skipped, permanentFailures, retryable);
    }

    private void finishDocument(FileRecord record, OcrResult ocr, String lane) {
        String checksum = sha256Hex(record.content());
        String objectKey = objectStore.keyFor(record.id());
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
}
