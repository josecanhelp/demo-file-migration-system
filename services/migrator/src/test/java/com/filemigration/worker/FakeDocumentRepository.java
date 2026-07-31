package com.filemigration.worker;

import com.filemigration.store.DocumentRepository;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * In-memory stand-in for DocumentRepository. Upserts by source id, so
 * writing the same id twice still leaves exactly one row, matching the
 * real table's ON CONFLICT behavior.
 */
class FakeDocumentRepository extends DocumentRepository {

    private final Map<Long, Row> rows = new LinkedHashMap<>();

    FakeDocumentRepository() {
        super(null);
    }

    int documentCount() {
        return rows.size();
    }

    Row get(long sourceId) {
        return rows.get(sourceId);
    }

    @Override
    public void upsert(long sourceId, String filename, String contentType, String objectKey, int byteSize,
            String checksumSha256, String ocrText, double ocrConfidence, int ocrPageCount, String ocrVendorJobId) {
        rows.put(sourceId, new Row(filename, contentType, objectKey, byteSize, checksumSha256, ocrText,
                ocrConfidence, ocrPageCount, ocrVendorJobId));
    }

    record Row(String filename, String contentType, String objectKey, int byteSize, String checksumSha256,
            String ocrText, double ocrConfidence, int ocrPageCount, String ocrVendorJobId) {
    }
}
