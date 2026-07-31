package com.filemigration.vendor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.model.FileRecord;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Calls the vendor OCR endpoint for a batch of files and returns the
 * result keyed by source id. Makes exactly one call per invocation: it
 * never retries and never swallows a failure, since deciding whether and
 * when to retry belongs to the governor, not the client.
 */
@Component
public class VendorClient {

    private static final String BATCH_PATH = "/v1/ocr/batch";

    private final RestClient restClient;
    private final ObjectMapper objectMapper;

    public VendorClient(RestClient vendorRestClient, ObjectMapper objectMapper) {
        this.restClient = vendorRestClient;
        this.objectMapper = objectMapper;
    }

    public Map<Long, OcrResult> ocrBatch(List<FileRecord> files) {
        List<DocumentPayload> documents = files.stream()
                .map(file -> new DocumentPayload(file.id(), Base64.getEncoder().encodeToString(file.content())))
                .toList();

        BatchResponse response;
        try {
            response = restClient.post()
                    .uri(BATCH_PATH)
                    .body(new BatchRequest(documents))
                    .retrieve()
                    .body(BatchResponse.class);
        } catch (RestClientResponseException e) {
            // A real HTTP response came back carrying an error status.
            throw toVendorException(e);
        } catch (RestClientException e) {
            // No usable HTTP response: a connection failure, a connection
            // reset partway through the response (vendor-mock's "down"
            // mode), or a read timeout on a hung socket. Depending on
            // exactly where the failure happens, the underlying request
            // factory surfaces this as a ResourceAccessException or as a
            // plainer RestClientException wrapping an IOException; both
            // mean the same thing here, so both are caught by this one
            // clause. The classifier treats status 0 as transient.
            throw new VendorException(ErrorClassifier.classify(0, null), null,
                    "Vendor call produced no response: " + e.getMessage());
        }

        if (response == null || response.results() == null) {
            throw new VendorException(ErrorClass.TRANSIENT, null, "Vendor returned an empty batch response");
        }

        Map<Long, OcrResult> byId = new HashMap<>();
        for (ResultPayload result : response.results()) {
            byId.put(result.id(),
                    new OcrResult(result.id(), result.text(), result.confidence(), result.pageCount(), result.jobId()));
        }
        return byId;
    }

    private VendorException toVendorException(RestClientResponseException e) {
        String bodyCode = extractBodyCode(e.getResponseBodyAsString());
        ErrorClass errorClass = ErrorClassifier.classify(e.getStatusCode().value(), bodyCode);
        Duration retryAfter = parseRetryAfter(e.getResponseHeaders());
        String detail = bodyCode != null ? bodyCode : e.getStatusText();
        return new VendorException(errorClass, retryAfter,
                "Vendor call failed with status " + e.getStatusCode().value() + ": " + detail);
    }

    private String extractBodyCode(String body) {
        if (body == null || body.isBlank()) {
            return null;
        }
        try {
            return objectMapper.readValue(body, ErrorBody.class).code();
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    // Retry-After arrives as delta-seconds, e.g. "2". Absent or unparseable
    // values leave retryAfter null rather than failing the call.
    private Duration parseRetryAfter(HttpHeaders headers) {
        if (headers == null) {
            return null;
        }
        String value = headers.getFirst(HttpHeaders.RETRY_AFTER);
        if (value == null) {
            return null;
        }
        try {
            return Duration.ofSeconds(Long.parseLong(value.trim()));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private record BatchRequest(List<DocumentPayload> documents) {
    }

    private record DocumentPayload(long id, String contentBase64) {
    }

    private record BatchResponse(List<ResultPayload> results) {
    }

    private record ResultPayload(long id, String text, double confidence, int pageCount, String jobId) {
    }

    private record ErrorBody(String code) {
    }
}
