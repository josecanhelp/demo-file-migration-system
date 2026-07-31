package com.filemigration.store;

import com.filemigration.model.FileRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * Reads file blobs and identifiers from the MySQL source database. This is
 * the only component that queries the files table directly; everything
 * else asks for records by id.
 */
@Repository
public class SourceFileRepository {

    private static final RowMapper<FileRecord> FILE_RECORD_MAPPER = (rs, rowNum) -> new FileRecord(
            rs.getLong("id"),
            rs.getString("filename"),
            rs.getString("content_type"),
            rs.getBytes("content"),
            rs.getInt("byte_size"),
            toInstant(rs.getTimestamp("created_at"))
    );

    private final JdbcTemplate sourceJdbc;

    public SourceFileRepository(@Qualifier("sourceJdbc") JdbcTemplate sourceJdbc) {
        this.sourceJdbc = sourceJdbc;
    }

    /**
     * Fetches full records, blob included, for the given ids. Returns
     * fewer records than requested if some ids no longer exist.
     */
    public List<FileRecord> findByIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }
        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        String sql = "SELECT id, filename, content_type, content, byte_size, created_at "
                + "FROM files WHERE id IN (" + placeholders + ")";
        return sourceJdbc.query(sql, FILE_RECORD_MAPPER, ids.toArray());
    }

    /**
     * Lists ids in an inclusive range, ascending. Used by the backfill
     * coordinator to slice the source table into claimable ranges.
     */
    public List<Long> findIdsInRange(long start, long end) {
        String sql = "SELECT id FROM files WHERE id >= ? AND id <= ? ORDER BY id";
        return sourceJdbc.query(sql, (rs, rowNum) -> rs.getLong("id"), start, end);
    }

    /**
     * Highest id currently present in the source table, or 0 if the table
     * is empty. Used to bound backfill ranges.
     */
    public long maxId() {
        Long result = sourceJdbc.queryForObject("SELECT COALESCE(MAX(id), 0) FROM files", Long.class);
        return result == null ? 0L : result;
    }

    /**
     * One page of ids actually present in the source table, ordered
     * ascending, starting strictly after afterId. Unlike findIdsInRange,
     * this walks actual rows rather than a fixed slice of the numeric id
     * space, so a table whose ids have large gaps costs one round trip
     * per batchSize rows that actually exist, not per batchSize of raw id
     * span. Used by the reconciler to walk the whole source table looking
     * for an id with no matching ledger or document row.
     */
    public List<Long> findIdsPage(long afterId, int limit) {
        return sourceJdbc.query(
                "SELECT id FROM files WHERE id > ? ORDER BY id LIMIT ?",
                (rs, rowNum) -> rs.getLong("id"), afterId, limit);
    }

    /**
     * Which of the given ids currently exist in the source table. Used by
     * the reconciler to find a ledger or document id with no matching
     * source row, without pulling the blob findByIds would.
     */
    public List<Long> existingIdsAmong(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }
        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        String sql = "SELECT id FROM files WHERE id IN (" + placeholders + ")";
        return sourceJdbc.query(sql, (rs, rowNum) -> rs.getLong("id"), ids.toArray());
    }

    /**
     * Total row count in the source table, used by the reconciler to
     * compare against the ledger and document row counts.
     */
    public long countAll() {
        Long result = sourceJdbc.queryForObject("SELECT COUNT(*) FROM files", Long.class);
        return result == null ? 0L : result;
    }

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
