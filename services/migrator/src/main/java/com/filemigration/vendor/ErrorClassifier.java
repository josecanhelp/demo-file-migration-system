package com.filemigration.vendor;

/**
 * Turns an HTTP status code (and, where available, the vendor's error body
 * code) from a failed vendor call into the error class that decides what
 * happens to the batch next.
 */
public final class ErrorClassifier {

    private ErrorClassifier() {
    }

    public static ErrorClass classify(int status, String bodyCode) {
        if (status == 429) return ErrorClass.RATE_LIMITED;
        if (status >= 400 && status < 500) return ErrorClass.PERMANENT;
        return ErrorClass.TRANSIENT;   // 5xx, and 0 meaning no response
    }
}
