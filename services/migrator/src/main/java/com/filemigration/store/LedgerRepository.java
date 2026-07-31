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
    // A worker that is still actively working a row keeps its lease alive
    // by renewing it (see renewClaims), touching updated_at well before
    // the lease would otherwise expire. That is what lets the lease be
    // sized to a small multiple of the renewal interval rather than the
    // worst case processing time for an entire batch: a row only ever
    // sits unrenewed for the length of one real crash, not for however
    // long the batch it was in takes to finish. What this guarantees is
    // that no claimed file is ever stranded forever, and that detecting a
    // crash takes on the order of the lease, not the order of a batch.
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
     * Moves whichever of the given ids is PENDING or FAILED_RETRYABLE with
     * consecutive_failures already at or past the cap straight to
     * FAILED_PERMANENT, without touching last_error: the message already
     * there is the real reason this id keeps failing, and overwriting it
     * with "exceeded max attempts" would throw that reason away right when
     * a caller most needs it for a dead-letter record. Must run before
     * {@link #claim}, since claim's own WHERE clause reclaims a PENDING or
     * FAILED_RETRYABLE row unconditionally; calling this first is what
     * stops an id from being retried forever once it has already used up
     * every consecutive failure it is allowed.
     *
     * Gates on consecutive_failures, never on the lifetime attempts
     * column: attempts increments on every claim regardless of outcome,
     * including a file that keeps getting legitimately updated and
     * succeeding, or an id reclaimed repeatedly while the vendor circuit
     * breaker is flapping open and closed during an outage. Neither of
     * those is a reason to ever condemn a file. consecutive_failures only
     * increments for a failure that retrying can never fix on its own (for
     * example the source row is gone) and is reset on every success and by
     * {@link #resetForUpdate}, so only a run of that specific kind of
     * failure, never ordinary lifetime activity or vendor unavailability,
     * can trip this cap.
     *
     * PENDING is included, not only FAILED_RETRYABLE, because
     * {@link #resetForUpdate} forces a row back to PENDING before an
     * updated CDC envelope is migrated again, which would otherwise let an
     * id dodge this cap indefinitely if update envelopes for it kept
     * arriving: consecutive_failures keeps whatever value it already had,
     * but the status this method matches on never sits at FAILED_RETRYABLE
     * long enough to be caught. This branch is not merely a defensive
     * fallback: {@link #resetForUpdate} only clears consecutive_failures
     * when the incoming version actually advances past what is already
     * stored, so a redelivery of the same failing update envelope, which
     * is exactly what a nack produces, carries the same version every
     * time and leaves the row PENDING with its count intact. This check is
     * what eventually condemns that row once the count reaches the cap,
     * the same as it would for a FAILED_RETRYABLE row that never got
     * reset at all.
     */
    public List<ExceededAttempt> failExceededAttempts(List<Long> ids, int maxConsecutiveFailures) {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }
        Long[] idArray = ids.toArray(new Long[0]);
        String sql = "UPDATE migration_state\n"
                + "   SET status = 'FAILED_PERMANENT', updated_at = now()\n"
                + " WHERE source_id = ANY(?)\n"
                + "   AND status IN ('PENDING', 'FAILED_RETRYABLE')\n"
                + "   AND consecutive_failures >= ?\n"
                + "RETURNING source_id, attempts, last_error";
        return targetJdbc.execute((ConnectionCallback<List<ExceededAttempt>>) connection -> {
            Array sqlArray = connection.createArrayOf("bigint", idArray);
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setArray(1, sqlArray);
                ps.setInt(2, maxConsecutiveFailures);
                try (ResultSet rs = ps.executeQuery()) {
                    List<ExceededAttempt> exceeded = new ArrayList<>();
                    while (rs.next()) {
                        exceeded.add(new ExceededAttempt(rs.getLong("source_id"), rs.getInt("attempts"),
                                rs.getString("last_error")));
                    }
                    return exceeded;
                }
            }
        });
    }

    /**
     * The current lifetime attempts count for one id, used only to enrich
     * a dead-letter record with how many times this id was ever claimed
     * before the failure that finally sent it to FAILED_PERMANENT. This is
     * the lifetime counter, not the consecutive-failures count the retry
     * cap itself reads. Returns 0 for an id with no row rather than
     * failing, since a caller building a best-effort dead-letter record
     * should not itself fail over this.
     */
    public int attemptsOf(long id) {
        List<Integer> attempts = targetJdbc.queryForList(
                "SELECT attempts FROM migration_state WHERE source_id = ?", Integer.class, id);
        return attempts.isEmpty() ? 0 : attempts.get(0);
    }

    /**
     * Refreshes updated_at for whichever of the given ids is currently
     * IN_FLIGHT, so a worker still actively processing a batch keeps its
     * claim from expiring out from under it. An id that is not IN_FLIGHT,
     * whether because it was never claimed by this attempt or has already
     * moved past it, is left untouched. Meant to be called on a much
     * shorter interval than the claim lease, so a live worker's ids never
     * come close to expiring while a dead one's do so quickly.
     */
    public int renewClaims(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        Long[] idArray = ids.toArray(new Long[0]);
        return targetJdbc.execute((ConnectionCallback<Integer>) connection -> {
            Array sqlArray = connection.createArrayOf("bigint", idArray);
            String sql = "UPDATE migration_state SET updated_at = now() "
                    + "WHERE source_id = ANY(?) AND status = 'IN_FLIGHT'";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setArray(1, sqlArray);
                return ps.executeUpdate();
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
     * Returns whichever of the given ids has not yet reached a terminal
     * status (DONE or FAILED_PERMANENT). claim() silently drops an id
     * already owned by a live, unexpired claim without saying whether
     * that claim ever actually finished it, which is not enough for a
     * caller deciding whether it is safe to stop tracking a batch: an id
     * left IN_FLIGHT by a worker that crashed moments ago is still owned,
     * by the same reasoning, for as long as its lease has left to run, so
     * it comes back from this method as unresolved rather than silently
     * being treated the same as an id that is genuinely done.
     */
    public List<Long> findUnresolved(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }
        Long[] idArray = ids.toArray(new Long[0]);
        return targetJdbc.execute((ConnectionCallback<List<Long>>) connection -> {
            Array sqlArray = connection.createArrayOf("bigint", idArray);
            String sql = "SELECT source_id FROM migration_state "
                    + "WHERE source_id = ANY(?) AND status NOT IN ('DONE', 'FAILED_PERMANENT')";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setArray(1, sqlArray);
                try (ResultSet rs = ps.executeQuery()) {
                    List<Long> unresolved = new ArrayList<>();
                    while (rs.next()) {
                        unresolved.add(rs.getLong("source_id"));
                    }
                    return unresolved;
                }
            }
        });
    }

    /**
     * Stores the vendor OCR result for a claimed file and marks it
     * OCR_DONE, so a crash after this point does not pay for OCR again.
     * Clears consecutive_failures, since a vendor call that actually
     * returned a result for this id is forward progress, not a failure.
     */
    public void saveOcrPayload(long id, String json) {
        targetJdbc.update(
                "UPDATE migration_state SET ocr_payload = ?::jsonb, status = ?, "
                        + "consecutive_failures = 0, updated_at = now() WHERE source_id = ?",
                json, Status.OCR_DONE.name(), id);
    }

    /**
     * Marks a file fully migrated, recording the checksum of the blob that
     * was written to the target store, and clears consecutive_failures:
     * whatever run of failures this id may have had before, reaching DONE
     * is unambiguous success.
     */
    public void markDone(long id, String checksum) {
        targetJdbc.update(
                "UPDATE migration_state SET status = ?, checksum_sha256 = ?, "
                        + "consecutive_failures = 0, updated_at = now() WHERE source_id = ?",
                Status.DONE.name(), checksum, id);
    }

    /**
     * Marks a file failed, retryable or permanent depending on the error
     * classification the caller already made. countsTowardRetryCap decides
     * whether this failure increments consecutive_failures, the counter
     * {@link #failExceededAttempts} reads: the caller should pass true
     * only for a failure that retrying can never fix on its own (the
     * source row is gone, the vendor's response omitted this id), and
     * false for a vendor TRANSIENT or RATE_LIMITED failure, since the
     * circuit breaker and pausing already protect that case and a vendor
     * outage, however long, must never condemn a file through this
     * counter climbing on its own. Ignored when permanent is true, since a
     * permanent failure is already terminal and the cap has nothing left
     * to decide.
     */
    public void markFailed(long id, String error, boolean permanent, boolean countsTowardRetryCap) {
        Status status = permanent ? Status.FAILED_PERMANENT : Status.FAILED_RETRYABLE;
        String sql = !permanent && countsTowardRetryCap
                ? "UPDATE migration_state SET status = ?, last_error = ?, "
                        + "consecutive_failures = consecutive_failures + 1, updated_at = now() WHERE source_id = ?"
                : "UPDATE migration_state SET status = ?, last_error = ?, updated_at = now() WHERE source_id = ?";
        targetJdbc.update(sql, status.name(), error, id);
    }

    /**
     * Puts a file back to PENDING after its source row changed, bumping
     * its version and dropping any cached OCR result, since that result
     * describes content that no longer matches what is in the source
     * table. consecutive_failures is cleared only when version actually
     * advances past whatever is already stored in source_version, never
     * unconditionally: a genuinely new update is new work, and whatever
     * run of failures the row had before this content changed says
     * nothing about whether the new content will fail the same way, so
     * that case does get a fresh budget. But CdcConsumer nacks a failing
     * envelope rather than dropping it, and Kafka redelivers the identical
     * message, carrying the identical version, until it is finally
     * acknowledged; resetting on every one of those redeliveries would
     * hold consecutive_failures at zero or one forever and the row could
     * never reach {@link #failExceededAttempts}'s cap no matter how many
     * times the same update fails, nacking forever instead of ever
     * reaching FAILED_PERMANENT. Comparing against the stored version
     * before overwriting it is what tells a redelivery of the same
     * envelope apart from an actually new one.
     */
    public void resetForUpdate(long id, long version) {
        targetJdbc.update(
                "UPDATE migration_state SET status = ?, source_version = ?, ocr_payload = NULL, last_error = NULL, "
                        + "consecutive_failures = CASE WHEN ? > source_version THEN 0 ELSE consecutive_failures END, "
                        + "updated_at = now() WHERE source_id = ?",
                Status.PENDING.name(), version, version, id);
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

    /**
     * One id {@link #failExceededAttempts} moved to FAILED_PERMANENT,
     * carrying the attempts count and last_error already on that row at
     * the moment it was moved, so a caller can build a dead-letter record
     * without a second query.
     */
    public record ExceededAttempt(long sourceId, int attempts, String lastError) {
    }

    /**
     * Total row count in migration_state, used by the reconciler to
     * compare against the source and document row counts.
     */
    public long countAll() {
        Long result = targetJdbc.queryForObject("SELECT COUNT(*) FROM migration_state", Long.class);
        return result == null ? 0L : result;
    }

    /**
     * One page of source ids currently tracked in migration_state, ordered
     * ascending, starting strictly after afterId. Used by the reconciler
     * to walk the whole ledger table in fixed-size pages looking for a row
     * with no matching source id, without loading every row into memory
     * at once.
     */
    public List<Long> findIdsPage(long afterId, int limit) {
        return targetJdbc.query(
                "SELECT source_id FROM migration_state WHERE source_id > ? ORDER BY source_id LIMIT ?",
                (rs, rowNum) -> rs.getLong("source_id"), afterId, limit);
    }

    /**
     * Which of the given ids currently have a migration_state row. Used
     * by the reconciler to find a source id with no ledger row at all.
     */
    public List<Long> existingIdsAmong(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }
        Long[] idArray = ids.toArray(new Long[0]);
        return targetJdbc.execute((ConnectionCallback<List<Long>>) connection -> {
            Array sqlArray = connection.createArrayOf("bigint", idArray);
            String sql = "SELECT source_id FROM migration_state WHERE source_id = ANY(?)";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setArray(1, sqlArray);
                try (ResultSet rs = ps.executeQuery()) {
                    List<Long> existing = new ArrayList<>();
                    while (rs.next()) {
                        existing.add(rs.getLong("source_id"));
                    }
                    return existing;
                }
            }
        });
    }

    /**
     * Every id whose current status is FAILED_PERMANENT, with the error
     * recorded on that row. Reads migration_state directly rather than
     * migration_event: resetForUpdate can revive a FAILED_PERMANENT row
     * back to PENDING, so a past DLQ event is not proof an id is still
     * bad, only migration_state.status reflects whether it currently is.
     */
    public List<PermanentFailure> findPermanentFailures() {
        return targetJdbc.query(
                "SELECT source_id, last_error FROM migration_state WHERE status = ? ORDER BY source_id",
                (rs, rowNum) -> new PermanentFailure(rs.getLong("source_id"), rs.getString("last_error")),
                Status.FAILED_PERMANENT.name());
    }

    /**
     * One id currently FAILED_PERMANENT, with the error last recorded
     * against it.
     */
    public record PermanentFailure(long sourceId, String lastError) {
    }
}
