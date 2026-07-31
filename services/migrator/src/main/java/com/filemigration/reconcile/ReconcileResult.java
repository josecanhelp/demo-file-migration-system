package com.filemigration.reconcile;

import java.util.List;

/**
 * The outcome of one reconciliation pass. clean is true only when every
 * count agrees, checksumMismatches, ocrMismatches, and missingObjects are
 * all empty, and no row is currently FAILED_PERMANENT; rowsExamined
 * reports how many document rows were visited across every page the
 * checksum and OCR checks walked, since counts alone cannot show whether
 * every row was covered.
 */
public record ReconcileResult(
        long sourceCount,
        long ledgerCount,
        long documentCount,
        List<Long> checksumMismatches,
        List<Long> ocrMismatches,
        List<Long> missingObjects,
        List<PermanentFailure> permanentFailures,
        long rowsExamined,
        boolean clean
) {

    public record PermanentFailure(long id, String error) {
    }
}
