package com.filemigration.worker;

/**
 * Tally of what happened to one batch of ids passed to
 * {@link MigrationService#migrate}: how many reached DONE, how many were
 * not owned by this call at all, and how many failed the vendor call,
 * split by whether that failure is worth retrying.
 */
public record MigrationOutcome(int done, int skipped, int permanentFailures, int retryable) {
}
