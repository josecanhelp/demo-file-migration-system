package com.filemigration.reconcile;

import java.util.List;

/**
 * The outcome of one reconciliation pass. Matching row counts across the
 * three tables are necessary but never sufficient for clean: a deleted
 * source row and a separate un-migrated source row can cancel each other
 * out in the totals while still leaving both wrong, which is exactly what
 * missingDocuments and orphanDocuments exist to catch by id rather than by
 * cardinality. clean is true only when sourceCount, ledgerCount, and
 * documentCount all agree, and checksumMismatches, ocrMismatches,
 * missingObjects, missingDocuments, orphanDocuments, and unreadableObjects
 * are all empty, and no row is currently FAILED_PERMANENT.
 *
 * missingDocuments lists a source id with no matching document row at all
 * (the document-row loop never visits it, since it walks the document
 * table). orphanDocuments lists a document id with no matching source row
 * (the row is visited, but there is no source content left to check it
 * against). missingObjects lists a document row whose object key was not
 * found in the object store, and unreadableObjects lists one whose object
 * could not be read for any other reason, each paired with the error
 * message. rowsExamined counts only document rows for which the checksum
 * and OCR checks actually ran, i.e. those with a matching source row,
 * since counts alone cannot show whether every row was covered.
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
