package com.filemigration.model;

/**
 * A point a file passes through on its way from the source database to the
 * target store. Each transition is recorded in migration_event so the path
 * a given file took can be replayed after the fact.
 */
public enum Stage {
    CDC_CAPTURED,
    QUEUED,
    CLAIMED,
    OCR_DONE,
    STORED,
    RETRY,
    DLQ,
    BREAKER_OPEN,
    BREAKER_CLOSED
}
