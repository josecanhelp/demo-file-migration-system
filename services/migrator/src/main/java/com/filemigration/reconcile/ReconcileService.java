package com.filemigration.reconcile;

import com.filemigration.model.FileRecord;
import com.filemigration.store.DocumentRepository;
import com.filemigration.store.LedgerRepository;
import com.filemigration.store.ObjectStore;
import com.filemigration.store.SourceFileRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Answers whether a migration is actually complete and correct by checking
 * the target store against the source database directly, rather than
 * trusting the columns the migration pipeline itself wrote, and rather
 * than trusting row counts alone: sourceCount, ledgerCount, and
 * documentCount can agree while a deleted source row and a separate
 * un-migrated source row cancel each other out in the totals, so every
 * check here is by id, not by cardinality.
 *
 * Six checks: row counts across the three tables (necessary but never
 * sufficient on their own, per above); which source ids have no matching
 * document row at all (missingDocuments) and which document ids have no
 * matching source row (orphanDocuments); the source blob's checksum
 * against both document.checksum_sha256 and the bytes actually stored in
 * the object store (a missing object goes in missingObjects, one that
 * exists but could not be read for some other reason goes in
 * unreadableObjects, neither is folded into checksumMismatches); the OCR
 * text recomputed from the source blob against document.ocr_text; and any
 * row currently FAILED_PERMANENT.
 *
 * The document-table walk (checksum, OCR, and orphanDocuments) and the
 * source-table walk (missingDocuments) each proceed a page at a time,
 * fetching only that page's source blobs or ids, so memory use stays
 * bounded by the configured page size regardless of how many rows either
 * table holds.
 */
@Profile("worker")
@Service
public class ReconcileService {

    private final SourceFileRepository sourceRepo;
    private final LedgerRepository ledger;
    private final DocumentRepository documentRepo;
    private final ObjectStore objectStore;
    private final int batchSize;

    public ReconcileService(SourceFileRepository sourceRepo, LedgerRepository ledger,
            DocumentRepository documentRepo, ObjectStore objectStore,
            @Value("${migrator.reconcile.batch-size}") int batchSize) {
        this.sourceRepo = sourceRepo;
        this.ledger = ledger;
        this.documentRepo = documentRepo;
        this.objectStore = objectStore;
        this.batchSize = batchSize;
    }

    public ReconcileResult reconcile() {
        long sourceCount = sourceRepo.countAll();
        long ledgerCount = ledger.countAll();
        long documentCount = documentRepo.countAll();

        List<ReconcileResult.PermanentFailure> permanentFailures = new ArrayList<>();
        for (LedgerRepository.PermanentFailure failure : ledger.findPermanentFailures()) {
            permanentFailures.add(new ReconcileResult.PermanentFailure(failure.sourceId(), failure.lastError()));
        }

        List<Long> checksumMismatches = new ArrayList<>();
        List<Long> ocrMismatches = new ArrayList<>();
        List<Long> missingObjects = new ArrayList<>();
        List<Long> orphanDocuments = new ArrayList<>();
        List<ReconcileResult.UnreadableObject> unreadableObjects = new ArrayList<>();
        long rowsExamined = 0;

        long cursor = 0;
        while (true) {
            List<DocumentRepository.DocumentRow> page = documentRepo.findPage(cursor, batchSize);
            if (page.isEmpty()) {
                break;
            }

            List<Long> ids = new ArrayList<>(page.size());
            for (DocumentRepository.DocumentRow row : page) {
                ids.add(row.sourceId());
            }
            Map<Long, FileRecord> sourceById = new HashMap<>();
            for (FileRecord record : sourceRepo.findByIds(ids)) {
                sourceById.put(record.id(), record);
            }

            for (DocumentRepository.DocumentRow row : page) {
                FileRecord source = sourceById.get(row.sourceId());
                if (source == null) {
                    // This document row's source id no longer exists in
                    // the source table at all: an orphan, distinct from a
                    // checksum or OCR mismatch, since there is no blob
                    // left to recompute either of those from. Recorded
                    // explicitly rather than silently skipped, since a
                    // count-only comparison can hide exactly this behind
                    // a compensating missingDocuments entry elsewhere.
                    orphanDocuments.add(row.sourceId());
                    continue;
                }

                rowsExamined++;

                String sourceChecksum = sha256Hex(source.content());
                boolean checksumMismatch = !sourceChecksum.equalsIgnoreCase(row.checksumSha256());

                try {
                    byte[] stored = objectStore.get(row.objectKey());
                    String storedChecksum = sha256Hex(stored);
                    if (!sourceChecksum.equals(storedChecksum)) {
                        checksumMismatch = true;
                    }
                } catch (NoSuchKeyException e) {
                    missingObjects.add(row.sourceId());
                } catch (SdkException e) {
                    // Anything else the object store call could throw:
                    // recorded with its message and the scan continues,
                    // rather than one unreadable object aborting the
                    // whole reconcile pass and losing every result
                    // gathered so far behind a bare server error.
                    unreadableObjects.add(
                            new ReconcileResult.UnreadableObject(row.sourceId(), String.valueOf(e.getMessage())));
                }

                if (checksumMismatch) {
                    checksumMismatches.add(row.sourceId());
                }

                String expectedOcrText = OcrTextTransform.extractText(source.content());
                if (!expectedOcrText.equals(row.ocrText())) {
                    ocrMismatches.add(row.sourceId());
                }
            }

            cursor = page.get(page.size() - 1).sourceId();
            if (page.size() < batchSize) {
                break;
            }
        }

        List<Long> missingDocuments = findMissingDocuments();

        boolean clean = sourceCount == ledgerCount
                && ledgerCount == documentCount
                && checksumMismatches.isEmpty()
                && ocrMismatches.isEmpty()
                && missingObjects.isEmpty()
                && missingDocuments.isEmpty()
                && orphanDocuments.isEmpty()
                && unreadableObjects.isEmpty()
                && permanentFailures.isEmpty();

        return new ReconcileResult(sourceCount, ledgerCount, documentCount, checksumMismatches, ocrMismatches,
                missingObjects, missingDocuments, orphanDocuments, unreadableObjects, permanentFailures,
                rowsExamined, clean);
    }

    /**
     * Every source id with no matching document row, found by walking the
     * source table's id space a page at a time (ids only, no blobs) and
     * checking each page's ids against the document table. This is the
     * other half of the set-membership check the document-table walk
     * above cannot do on its own: that walk only ever visits ids that
     * already have a document row, so a source id with none is invisible
     * to it no matter how many document rows are examined. sourceCount
     * agreeing with documentCount says nothing about this by itself, since
     * this id's absence can be, and in the exact scenario this check
     * exists for, is, exactly cancelled out by some other document row
     * with no source id of its own.
     */
    private List<Long> findMissingDocuments() {
        List<Long> missingDocuments = new ArrayList<>();
        long maxSourceId = sourceRepo.maxId();
        long rangeStart = 0;
        while (rangeStart < maxSourceId) {
            long rangeEnd = Math.min(rangeStart + batchSize, maxSourceId);
            List<Long> idsInRange = sourceRepo.findIdsInRange(rangeStart + 1, rangeEnd);
            if (!idsInRange.isEmpty()) {
                Set<Long> haveDocument = new HashSet<>(documentRepo.existingIdsAmong(idsInRange));
                for (Long id : idsInRange) {
                    if (!haveDocument.contains(id)) {
                        missingDocuments.add(id);
                    }
                }
            }
            rangeStart = rangeEnd;
        }
        return missingDocuments;
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
