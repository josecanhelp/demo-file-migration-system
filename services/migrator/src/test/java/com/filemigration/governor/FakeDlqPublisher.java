package com.filemigration.governor;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * In-memory stand-in for DlqPublisher. Records every publish call instead
 * of touching Kafka, so a test can assert exactly what would have been
 * dead-lettered.
 */
public class FakeDlqPublisher extends DlqPublisher {

    private final List<Published> published = new ArrayList<>();

    public FakeDlqPublisher() {
        super(null, new ObjectMapper(), "files.dlq");
    }

    public List<Published> published() {
        return published;
    }

    public boolean wasPublished(long sourceId) {
        return published.stream().anyMatch(p -> p.sourceId() == sourceId);
    }

    @Override
    public void publish(long sourceId, String lane, String errorClass, int attempts, String lastError) {
        published.add(new Published(sourceId, lane, errorClass, attempts, lastError));
    }

    public record Published(long sourceId, String lane, String errorClass, int attempts, String lastError) {
    }
}
