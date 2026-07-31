package com.filemigration.vendor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ErrorClassifierTest {
  @Test void permanentForUnprocessableDocument() {
    assertEquals(ErrorClass.PERMANENT, ErrorClassifier.classify(400, "UNPROCESSABLE_DOCUMENT"));
  }
  @Test void permanentForAnyFourHundredExceptRateLimit() {
    assertEquals(ErrorClass.PERMANENT, ErrorClassifier.classify(422, null));
    assertEquals(ErrorClass.PERMANENT, ErrorClassifier.classify(413, null));
  }
  @Test void rateLimitedForTooManyRequests() {
    assertEquals(ErrorClass.RATE_LIMITED, ErrorClassifier.classify(429, null));
  }
  @Test void transientForServerErrors() {
    assertEquals(ErrorClass.TRANSIENT, ErrorClassifier.classify(500, null));
    assertEquals(ErrorClass.TRANSIENT, ErrorClassifier.classify(503, null));
  }
  @Test void transientForNoResponse() {
    assertEquals(ErrorClass.TRANSIENT, ErrorClassifier.classify(0, null));
  }
}
