package com.filemigration.model;

/**
 * Lifecycle status of a single source file as tracked in migration_state.
 * A file moves PENDING to IN_FLIGHT to OCR_DONE to DONE on the happy path,
 * or into one of the FAILED_ states when the vendor call or the write to
 * the target store cannot complete.
 */
public enum Status {
    PENDING,
    IN_FLIGHT,
    OCR_DONE,
    DONE,
    FAILED_RETRYABLE,
    FAILED_PERMANENT
}
