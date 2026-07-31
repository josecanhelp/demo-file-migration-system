package com.filemigration.vendor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.model.FileRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Exercises VendorClient against the real vendor-mock service rather than a
 * stub, so the classification it produces is proven against the vendor's
 * actual wire behavior in every chaos mode, not against an assumption of
 * what that behavior is. If vendor-mock is not reachable, connecting fails
 * loudly here instead of being swallowed into a skip.
 *
 * The chaos mode is flipped before each case that needs it and always put
 * back to healthy afterward, whether or not the case passed, so a failure
 * partway through never leaves the vendor stuck in a failure mode for
 * whatever runs next.
 */
class VendorClientIT {

    private static final String BASE_URL =
            System.getenv().getOrDefault("VENDOR_BASE_URL", "http://localhost:8088");
    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int READ_TIMEOUT_MS = 10000;

    private static RestClient adminClient;
    private static VendorClient vendorClient;

    @BeforeAll
    static void connect() {
        adminClient = RestClient.create(BASE_URL);
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(CONNECT_TIMEOUT_MS);
        requestFactory.setReadTimeout(READ_TIMEOUT_MS);
        RestClient restClient = RestClient.builder()
                .baseUrl(BASE_URL)
                .requestFactory(requestFactory)
                .build();
        vendorClient = new VendorClient(restClient, new ObjectMapper());

        // Deliberately not wrapped in try/catch: if vendor-mock is not
        // reachable, this throws and the whole class fails instead of
        // quietly reporting a pass with nothing exercised.
        adminClient.get().uri("/health").retrieve().toBodilessEntity();
    }

    @AfterEach
    void resetToHealthy() {
        setMode("healthy");
    }

    @Test
    void healthyModeReturnsResultsForEachDocument() {
        setMode("healthy");
        FileRecord first = fileRecord(1001L, "hello world");
        FileRecord second = fileRecord(1002L, "another document");

        Map<Long, OcrResult> results = vendorClient.ocrBatch(List.of(first, second));

        assertEquals(2, results.size());
        assertEquals(1001L, results.get(1001L).id());
        assertFalse(results.get(1001L).text().isBlank());
        assertEquals("HELLO WORLD", results.get(1001L).text());
        assertEquals(1002L, results.get(1002L).id());
        assertFalse(results.get(1002L).text().isBlank());
        assertEquals("ANOTHER DOCUMENT", results.get(1002L).text());
    }

    @Test
    void rateLimitedModeThrowsWithRetryAfterOfTwoSeconds() {
        setMode("rate_limited");
        FileRecord file = fileRecord(2001L, "content");

        VendorException exception = assertThrows(VendorException.class,
                () -> vendorClient.ocrBatch(List.of(file)));

        assertEquals(ErrorClass.RATE_LIMITED, exception.errorClass());
        assertEquals(Duration.ofSeconds(2), exception.retryAfter());
    }

    @Test
    void erroringModeThrowsTransient() {
        setMode("erroring");
        FileRecord file = fileRecord(3001L, "content");

        VendorException exception = assertThrows(VendorException.class,
                () -> vendorClient.ocrBatch(List.of(file)));

        assertEquals(ErrorClass.TRANSIENT, exception.errorClass());
    }

    @Test
    void downModeThrowsTransientWithNoHttpResponse() {
        setMode("down");
        FileRecord file = fileRecord(4001L, "content");

        VendorException exception = assertThrows(VendorException.class,
                () -> vendorClient.ocrBatch(List.of(file)));

        assertEquals(ErrorClass.TRANSIENT, exception.errorClass());
    }

    @Test
    void emptyContentThrowsPermanentForUnprocessableDocument() {
        setMode("healthy");
        FileRecord file = fileRecord(5001L, "");

        VendorException exception = assertThrows(VendorException.class,
                () -> vendorClient.ocrBatch(List.of(file)));

        assertEquals(ErrorClass.PERMANENT, exception.errorClass());
    }

    private static void setMode(String mode) {
        adminClient.post()
                .uri("/admin/mode")
                .body(Map.of("mode", mode))
                .retrieve()
                .toBodilessEntity();
    }

    private static FileRecord fileRecord(long id, String text) {
        byte[] content = text.getBytes(StandardCharsets.UTF_8);
        return new FileRecord(id, "doc-" + id + ".txt", "text/plain", content, content.length, Instant.now());
    }
}
