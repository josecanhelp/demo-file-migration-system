package com.filemigration.store;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;

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

    /**
     * Total row count in document, used by the reconciler to compare
     * against the source and ledger row counts.
     */
    public long countAll() {
        Long result = targetJdbc.queryForObject("SELECT COUNT(*) FROM document", Long.class);
        return result == null ? 0L : result;
    }

    /**
     * One page of document rows ordered by source_id ascending, starting
     * strictly after afterId. Used by the reconciler to walk the whole
     * table in fixed-size pages rather than loading every row, and every
     * blob it points at, into memory at once.
     */
    public List<DocumentRow> findPage(long afterId, int limit) {
        return targetJdbc.query(
                "SELECT source_id, checksum_sha256, ocr_text, object_key FROM document "
                        + "WHERE source_id > ? ORDER BY source_id LIMIT ?",
                (rs, rowNum) -> new DocumentRow(rs.getLong("source_id"), rs.getString("checksum_sha256"),
                        rs.getString("ocr_text"), rs.getString("object_key")),
                afterId, limit);
    }

    /**
     * Which of the given ids currently have a document row. Used by the
     * reconciler to find a source id with no matching document row at
     * all: any id passed in that does not come back here never made it
     * into this table, regardless of what the row counts alone say.
     */
    public List<Long> existingIdsAmong(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }
        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        String sql = "SELECT source_id FROM document WHERE source_id IN (" + placeholders + ")";
        return targetJdbc.query(sql, (rs, rowNum) -> rs.getLong("source_id"), ids.toArray());
    }

    /**
     * The subset of a document row the reconciler needs: enough to
     * recompute and compare its checksum and OCR text against the source
     * blob without pulling the whole row.
     */
    public record DocumentRow(long sourceId, String checksumSha256, String ocrText, String objectKey) {
    }
}
