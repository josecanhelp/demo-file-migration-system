package com.filemigration.store;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * The document table: one row per migrated file, holding where its blob
 * landed in the target store and the OCR result attached to it. A source
 * file can be migrated more than once, for example after its content
 * changes and it is reprocessed, so writes here always upsert rather than
 * insert.
 */
@Repository
public class DocumentRepository {

    private static final String UPSERT_SQL =
            "INSERT INTO document (source_id, filename, content_type, object_key, byte_size, "
            + "checksum_sha256, ocr_text, ocr_confidence, ocr_page_count, ocr_vendor_job_id, migrated_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now()) "
            + "ON CONFLICT (source_id) DO UPDATE SET "
            + "filename = EXCLUDED.filename, "
            + "content_type = EXCLUDED.content_type, "
            + "object_key = EXCLUDED.object_key, "
            + "byte_size = EXCLUDED.byte_size, "
            + "checksum_sha256 = EXCLUDED.checksum_sha256, "
            + "ocr_text = EXCLUDED.ocr_text, "
            + "ocr_confidence = EXCLUDED.ocr_confidence, "
            + "ocr_page_count = EXCLUDED.ocr_page_count, "
            + "ocr_vendor_job_id = EXCLUDED.ocr_vendor_job_id, "
            + "migrated_at = now()";

    private final JdbcTemplate targetJdbc;

    public DocumentRepository(@Qualifier("targetJdbc") JdbcTemplate targetJdbc) {
        this.targetJdbc = targetJdbc;
    }

    /**
     * Writes the fully migrated record for a source file: where its blob
     * landed, the checksum of that blob, and the OCR result attached to
     * it. Reprocessing the same source id overwrites the existing row
     * rather than creating a second one.
     */
    public void upsert(long sourceId, String filename, String contentType, String objectKey, int byteSize,
            String checksumSha256, String ocrText, double ocrConfidence, int ocrPageCount, String ocrVendorJobId) {
        targetJdbc.update(UPSERT_SQL, sourceId, filename, contentType, objectKey, byteSize, checksumSha256,
                ocrText, ocrConfidence, ocrPageCount, ocrVendorJobId);
    }
}
