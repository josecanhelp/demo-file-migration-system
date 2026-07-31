package com.filemigration.vendor;

import java.time.Duration;

/**
 * Thrown when a vendor OCR call does not complete successfully. Carries
 * the classification a caller needs to decide what happens next: dead
 * letter a permanent failure, wait out retryAfter for a rate limit, or
 * leave a transient failure to later retry and circuit breaking logic.
 * retryAfter is null whenever the vendor did not provide one.
 */
public class VendorException extends RuntimeException {

    private final ErrorClass errorClass;
    private final Duration retryAfter;

    public VendorException(ErrorClass errorClass, Duration retryAfter, String message) {
        super(message);
        this.errorClass = errorClass;
        this.retryAfter = retryAfter;
    }

    public ErrorClass errorClass() {
        return errorClass;
    }

    public Duration retryAfter() {
        return retryAfter;
    }
}
