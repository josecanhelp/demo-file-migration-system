package com.filemigration.store;

import com.filemigration.model.Status;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The migration_state table: one row per source file, tracking where it is
 * in the pipeline. A claimed row is only safe from a second worker for as
 * long as its lease lasts; once the lease expires the row becomes
 * claimable again on the assumption that whatever held it crashed.
 */
@Repository
public class LedgerRepository {

    // A row moves into IN_FLIGHT in one of two cases: it is new work
    // (PENDING or FAILED_RETRYABLE), or it is already IN_FLIGHT or
    // OCR_DONE but has sat there longer than its claim lease, meaning
    // whatever worker had it has either crashed or long since finished
    // without updating it. A row still within its lease is left alone, so
    // a worker actively handling it is not interrupted mid-flight.
    //
    // This recovers from a crash at any point in the pipeline, not only
    // the window right after saveOcrPayload, but it does not guarantee
    // exactly one owner at every instant: nothing renews the lease while
    // work is in progress, so a row whose lease happens to expire while a
    // worker is still legitimately processing it can be claimed a second
    // time. What it does guarantee is that no claimed file is ever
    // stranded forever.
    private static final String CLAIM_SQL =
            "UPDATE migration_state\n"
            + "   SET status = 'IN_FLIGHT', attempts = attempts + 1, updated_at = now()\n"
            + " WHERE source_id = ANY(?)\n"
            + "   AND (\n"
            + "     status IN ('PENDING', 'FAILED_RETRYABLE')\n"
            + "     OR (status IN ('IN_FLIGHT', 'OCR_DONE') AND updated_at < now() - make_interval(secs => ?))\n"
            + "   )\n"
            + "RETURNING source_id";

    private final JdbcTemplate targetJdbc;
    private final long claimLeaseSeconds;

    public LedgerRepository(@Qualifier("targetJdbc") JdbcTemplate targetJdbc,
            @Value("${migrator.claim-lease-seconds}") long claimLeaseSeconds) {
        this.targetJdbc = targetJdbc;
        this.claimLeaseSeconds = claimLeaseSeconds;
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
     * Attempts to move each given id into IN_FLIGHT: unconditionally for
     * PENDING or FAILED_RETRYABLE rows, and for IN_FLIGHT or OCR_DONE rows
     * whose claim lease has expired. Only the ids this call actually
     * transitioned come back; an id currently owned by a live claim, or
     * already DONE, is silently dropped. Callers must treat the returned
     * list, not the input list, as the set of ids they now own.
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
                ps.setLong(2, claimLeaseSeconds);
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
     * Looks up the saved OCR payload for each given id that has one.
     * Ids with no payload yet are simply absent from the returned map, so
     * a caller can tell a file that already has a cached OCR result apart
     * from one that still needs the vendor called for it.
     */
    public Map<Long, String> findCachedOcrPayloads(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyMap();
        }
        Long[] idArray = ids.toArray(new Long[0]);
        return targetJdbc.execute((ConnectionCallback<Map<Long, String>>) connection -> {
            Array sqlArray = connection.createArrayOf("bigint", idArray);
            String sql = "SELECT source_id, ocr_payload FROM migration_state "
                    + "WHERE source_id = ANY(?) AND ocr_payload IS NOT NULL";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setArray(1, sqlArray);
                try (ResultSet rs = ps.executeQuery()) {
                    Map<Long, String> payloads = new HashMap<>();
                    while (rs.next()) {
                        payloads.put(rs.getLong("source_id"), rs.getString("ocr_payload"));
                    }
                    return payloads;
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
