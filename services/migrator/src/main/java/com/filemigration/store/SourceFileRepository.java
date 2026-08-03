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
import java.util.Optional;

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
     * Ids and their created_at in an inclusive range, ascending, without
     * fetching the blob. Used by the backfill coordinator to slice the
     * source table into claimable ranges and to seed each ledger row with
     * the age of the file it tracks, which is what lets the freshness
     * gauge measure real lag on the backfill lane instead of always
     * reading zero.
     */
    public List<IdCreatedAt> findIdsWithCreatedAtInRange(long start, long end) {
        String sql = "SELECT id, created_at FROM files WHERE id >= ? AND id <= ? ORDER BY id";
        return sourceJdbc.query(sql,
                (rs, rowNum) -> new IdCreatedAt(rs.getLong("id"), toInstant(rs.getTimestamp("created_at"))),
                start, end);
    }

    /**
     * The created_at of one source row, or empty if the id no longer
     * exists. Used as a fallback when a CDC envelope's own payload does
     * not carry created_at, so the ledger still gets a value at the cost
     * of one extra round trip rather than being left NULL.
     */
    public Optional<Instant> findCreatedAtById(long id) {
        List<Instant> found = sourceJdbc.query(
                "SELECT created_at FROM files WHERE id = ?",
                (rs, rowNum) -> toInstant(rs.getTimestamp("created_at")), id);
        return found.isEmpty() ? Optional.empty() : Optional.ofNullable(found.get(0));
    }

    /**
     * One source id paired with its created_at.
     */
    public record IdCreatedAt(long id, Instant createdAt) {
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
