package com.filemigration.reconcile;

import com.filemigration.model.FileRecord;
import com.filemigration.store.DocumentRepository;
import com.filemigration.store.LedgerRepository;
import com.filemigration.store.ObjectStore;
import com.filemigration.store.SourceFileRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Answers whether a migration is actually complete and correct by checking
 * the target store against the source database directly, rather than
 * trusting the columns the migration pipeline itself wrote. Four checks:
 * row counts across the three tables, the source blob's checksum against
 * both document.checksum_sha256 and the bytes actually stored in the
 * object store (a missing object is reported separately, in
 * missingObjects, rather than folded into checksumMismatches), the OCR
 * text recomputed from the source blob against document.ocr_text, and any
 * row currently FAILED_PERMANENT.
 *
 * The checksum and OCR checks walk document rows a page at a time,
 * fetching only that page's source blobs, so memory use stays bounded by
 * the configured page size regardless of how many rows the table holds.
 */
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
                rowsExamined++;
                FileRecord source = sourceById.get(row.sourceId());
                if (source == null) {
                    // The source row for this document is gone; the count
                    // check above already reflects that discrepancy, and
                    // there is no blob left here to recompute a checksum
                    // or OCR text from.
                    continue;
                }

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

        boolean clean = sourceCount == ledgerCount
                && ledgerCount == documentCount
                && checksumMismatches.isEmpty()
                && ocrMismatches.isEmpty()
                && missingObjects.isEmpty()
                && permanentFailures.isEmpty();

        return new ReconcileResult(sourceCount, ledgerCount, documentCount, checksumMismatches, ocrMismatches,
                missingObjects, permanentFailures, rowsExamined, clean);
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
