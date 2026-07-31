package com.filemigration.store;

import com.filemigration.model.Status;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The migration_state table: one row per source file, tracking where it is
 * in the pipeline and guarding against two workers processing the same
 * file at once.
 */
@Repository
public class LedgerRepository {

    // The RETURNING clause reports exactly which ids this statement moved
    // into IN_FLIGHT. A row already IN_FLIGHT or further along matches
    // neither status in the WHERE clause, so it is silently left out of
    // the result instead of being claimed a second time. That is what
    // lets several workers pull from the same batch of ids without two of
    // them ever processing the same file at once.
    private static final String CLAIM_SQL =
            "UPDATE migration_state\n"
            + "   SET status = 'IN_FLIGHT', attempts = attempts + 1, updated_at = now()\n"
            + " WHERE source_id = ANY(?)\n"
            + "   AND status IN ('PENDING', 'FAILED_RETRYABLE')\n"
            + "RETURNING source_id";

    private final JdbcTemplate targetJdbc;

    public LedgerRepository(@Qualifier("targetJdbc") JdbcTemplate targetJdbc) {
        this.targetJdbc = targetJdbc;
    }

    /**
     * Inserts a PENDING row per id for the given lane, leaving any id
     * already tracked untouched. Returns the number of rows actually
     * inserted, which can be fewer than ids.size() when some are already
     * seeded.
     */
    public int seedPending(List<Long> ids, String lane, Map<Long, Instant> createdAt) {
        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        String sql = "INSERT INTO migration_state (source_id, lane, status, source_created_at) "
                + "VALUES (?, ?, ?, ?) "
                + "ON CONFLICT (source_id) DO NOTHING";
        List<Object[]> batchArgs = new ArrayList<>(ids.size());
        for (Long id : ids) {
            Instant instant = createdAt == null ? null : createdAt.get(id);
            batchArgs.add(new Object[] {
                    id,
                    lane,
                    Status.PENDING.name(),
                    instant == null ? null : Timestamp.from(instant)
            });
        }
        int[] results = targetJdbc.batchUpdate(sql, batchArgs);
        int inserted = 0;
        for (int result : results) {
            if (result > 0) {
                inserted += result;
            }
        }
        return inserted;
    }

    /**
     * Attempts to move each given id from PENDING or FAILED_RETRYABLE into
     * IN_FLIGHT. Only the ids this call actually transitioned come back;
     * an id already claimed by another worker is silently dropped. Callers
     * must treat the returned list, not the input list, as the set of ids
     * they now own.
     */
    public List<Long> claim(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }
        Long[] idArray = ids.toArray(new Long[0]);
        return targetJdbc.execute((ConnectionCallback<List<Long>>) connection -> {
            Array sqlArray = connection.createArrayOf("bigint", idArray);
            try (PreparedStatement ps = connection.prepareStatement(CLAIM_SQL)) {
                ps.setArray(1, sqlArray);
                try (ResultSet rs = ps.executeQuery()) {
                    List<Long> claimed = new ArrayList<>();
                    while (rs.next()) {
                        claimed.add(rs.getLong("source_id"));
                    }
                    return claimed;
                }
            }
        });
    }

    /**
     * Stores the vendor OCR result for a claimed file and marks it
     * OCR_DONE, so a crash after this point does not pay for OCR again.
     */
    public void saveOcrPayload(long id, String json) {
        targetJdbc.update(
                "UPDATE migration_state SET ocr_payload = ?::jsonb, status = ?, updated_at = now() "
                        + "WHERE source_id = ?",
                json, Status.OCR_DONE.name(), id);
    }

    /**
     * Marks a file fully migrated, recording the checksum of the blob that
     * was written to the target store.
     */
    public void markDone(long id, String checksum) {
        targetJdbc.update(
                "UPDATE migration_state SET status = ?, checksum_sha256 = ?, updated_at = now() "
                        + "WHERE source_id = ?",
                Status.DONE.name(), checksum, id);
    }

    /**
     * Marks a file failed, retryable or permanent depending on the error
     * classification the caller already made.
     */
    public void markFailed(long id, String error, boolean permanent) {
        Status status = permanent ? Status.FAILED_PERMANENT : Status.FAILED_RETRYABLE;
        targetJdbc.update(
                "UPDATE migration_state SET status = ?, last_error = ?, updated_at = now() "
                        + "WHERE source_id = ?",
                status.name(), error, id);
    }

    /**
     * Puts a file back to PENDING after its source row changed, bumping
     * its version and dropping any cached OCR result, since that result
     * describes content that no longer matches what is in the source
     * table.
     */
    public void resetForUpdate(long id, long version) {
        targetJdbc.update(
                "UPDATE migration_state SET status = ?, source_version = ?, ocr_payload = NULL, "
                        + "last_error = NULL, updated_at = now() WHERE source_id = ?",
                Status.PENDING.name(), version, id);
    }

    /**
     * Removes the ledger and migrated-document rows for a source file that
     * no longer exists. Both deletes commit or roll back together, so a
     * failure partway through never leaves a migration_state row with no
     * matching document, or the reverse. Event history is left in place as
     * an audit trail.
     */
    @Transactional("targetTransactionManager")
    public void tombstone(long id) {
        targetJdbc.update("DELETE FROM document WHERE source_id = ?", id);
        targetJdbc.update("DELETE FROM migration_state WHERE source_id = ?", id);
    }
}
