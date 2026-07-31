package com.filemigration.reconcile;

import java.util.List;

/**
 * The outcome of one reconciliation pass. Matching row counts across the
 * three tables are necessary but never sufficient for clean: a deleted
 * source row and a separate un-migrated source row can cancel each other
 * out in the totals while still leaving both wrong, which is exactly what
 * the four id-level lists below exist to catch, by set membership rather
 * than cardinality, on both tables the source table backs: document and
 * ledger alike. clean is true only when sourceCount, ledgerCount, and
 * documentCount all agree, and checksumMismatches, ocrMismatches,
 * missingObjects, missingDocuments, orphanDocuments, missingLedgerRows,
 * orphanLedgerRows, and unreadableObjects are all empty, and no row is
 * currently FAILED_PERMANENT.
 *
 * missingDocuments lists a source id with no matching document row at all,
 * and orphanDocuments lists a document id with no matching source row.
 * missingLedgerRows lists a source id with no matching migration_state
 * row, and orphanLedgerRows lists a migration_state id with no matching
 * source row: the same pair of checks, against the ledger instead of the
 * document table. missingObjects lists a document row whose object key
 * was not found in the object store, and unreadableObjects lists one
 * whose object could not be read for any other reason, each paired with
 * the error message. rowsExamined counts only document rows for which the
 * checksum and OCR checks actually ran, i.e. those with a matching source
 * row, since counts alone cannot show whether every row was covered.
 */
public record ReconcileResult(
        long sourceCount,
        long ledgerCount,
        long documentCount,
        List<Long> checksumMismatches,
        List<Long> ocrMismatches,
        List<Long> missingObjects,
        List<Long> missingDocuments,
        List<Long> orphanDocuments,
        List<Long> missingLedgerRows,
        List<Long> orphanLedgerRows,
        List<UnreadableObject> unreadableObjects,
        List<PermanentFailure> permanentFailures,
        long rowsExamined,
        boolean clean
) {

    public record PermanentFailure(long id, String error) {
    }

    public record UnreadableObject(long id, String error) {
    }
}
