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

import java.io.UncheckedIOException;
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
 * documentCount can agree while a deleted row and a separate un-migrated
 * row cancel each other out in the totals, on either the document table
 * or the ledger table, so every one of those two tables is checked
 * against the source table by id, not only by cardinality.
 *
 * Six checks: row counts across the three tables (necessary but never
 * sufficient on their own, per above); which source ids have no matching
 * document row and which document ids have no matching source row
 * (missingDocuments / orphanDocuments); which source ids have no matching
 * migration_state row and which migration_state ids have no matching
 * source row (missingLedgerRows / orphanLedgerRows); the source blob's
 * checksum against both document.checksum_sha256 and the bytes actually
 * stored in the object store (a missing object goes in missingObjects,
 * one that exists but could not be read for some other reason goes in
 * unreadableObjects, neither is folded into checksumMismatches); the OCR
 * text recomputed from the source blob against document.ocr_text; and any
 * row currently FAILED_PERMANENT.
 *
 * Every walk below (the document-table walk for checksum, OCR, and
 * orphanDocuments; the source-table walk for missingDocuments and
 * missingLedgerRows; the ledger-table walk for orphanLedgerRows) proceeds
 * a page at a time over ids actually present in the table it walks,
 * rather than over a fixed slice of numeric id space, so memory use and
 * round trips both stay bounded by the configured page size and how many
 * rows that table actually holds, regardless of how sparse or how large
 * its id range is.
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
                } catch (SdkException | UncheckedIOException e) {
                    // Anything else the object store call could throw,
                    // including a mid-read I/O failure surfacing as
                    // UncheckedIOException rather than an SdkException:
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

        SourceSideGaps sourceSideGaps = findSourceSideGaps();
        List<Long> orphanLedgerRows = findOrphanLedgerRows();

        boolean clean = sourceCount == ledgerCount
                && ledgerCount == documentCount
                && checksumMismatches.isEmpty()
                && ocrMismatches.isEmpty()
                && missingObjects.isEmpty()
                && sourceSideGaps.missingDocuments.isEmpty()
                && orphanDocuments.isEmpty()
                && sourceSideGaps.missingLedgerRows.isEmpty()
                && orphanLedgerRows.isEmpty()
                && unreadableObjects.isEmpty()
                && permanentFailures.isEmpty();

        return new ReconcileResult(sourceCount, ledgerCount, documentCount, checksumMismatches, ocrMismatches,
                missingObjects, sourceSideGaps.missingDocuments, orphanDocuments, sourceSideGaps.missingLedgerRows,
                orphanLedgerRows, unreadableObjects, permanentFailures, rowsExamined, clean);
    }

    /**
     * Walks the source table's actual rows a page at a time (ids only, no
     * blobs), checking each page's ids against both the document table
     * and the ledger table in one pass, since both are the same kind of
     * check against the same source-id space: a source id absent from
     * this list. Neither list can be derived from the document-table walk
     * above, since that walk, and any walk of the ledger table on its
     * own, only ever visits ids that already have a row there; a source
     * id with none is invisible to either no matter how many of their own
     * rows are examined. sourceCount agreeing with documentCount or with
     * ledgerCount says nothing about this by itself, since this id's
     * absence can be, and in the exact scenario these checks exist for,
     * is, exactly cancelled out by some other row with no source id of
     * its own.
     */
    private SourceSideGaps findSourceSideGaps() {
        SourceSideGaps gaps = new SourceSideGaps();
        long cursor = 0;
        while (true) {
            List<Long> page = sourceRepo.findIdsPage(cursor, batchSize);
            if (page.isEmpty()) {
                break;
            }
            Set<Long> haveDocument = new HashSet<>(documentRepo.existingIdsAmong(page));
            Set<Long> haveLedgerRow = new HashSet<>(ledger.existingIdsAmong(page));
            for (Long id : page) {
                if (!haveDocument.contains(id)) {
                    gaps.missingDocuments.add(id);
                }
                if (!haveLedgerRow.contains(id)) {
                    gaps.missingLedgerRows.add(id);
                }
            }
            cursor = page.get(page.size() - 1);
            if (page.size() < batchSize) {
                break;
            }
        }
        return gaps;
    }

    /**
     * Every migration_state id with no matching source row, found by
     * walking the ledger table's actual rows a page at a time and
     * checking each page's ids against the source table. The document
     * table's own walk already catches a document row with no source id
     * (orphanDocuments); this is the same check against the ledger table,
     * which nothing else here walks on its own.
     */
    private List<Long> findOrphanLedgerRows() {
        List<Long> orphanLedgerRows = new ArrayList<>();
        long cursor = 0;
        while (true) {
            List<Long> page = ledger.findIdsPage(cursor, batchSize);
            if (page.isEmpty()) {
                break;
            }
            Set<Long> haveSource = new HashSet<>(sourceRepo.existingIdsAmong(page));
            for (Long id : page) {
                if (!haveSource.contains(id)) {
                    orphanLedgerRows.add(id);
                }
            }
            cursor = page.get(page.size() - 1);
            if (page.size() < batchSize) {
                break;
            }
        }
        return orphanLedgerRows;
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
     * The two lists {@link #findSourceSideGaps} produces in a single walk
     * of the source table: a source id with no document row, and a
     * source id with no ledger row.
     */
    private static final class SourceSideGaps {
        private final List<Long> missingDocuments = new ArrayList<>();
        private final List<Long> missingLedgerRows = new ArrayList<>();
    }
}
