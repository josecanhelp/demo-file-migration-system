package com.filemigration.worker;

import com.filemigration.model.Status;
import com.filemigration.store.LedgerRepository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory stand-in for LedgerRepository that mirrors the claimability
 * rules of the real migration_state table: a row is claimable while
 * PENDING or FAILED_RETRYABLE, and OCR_DONE is treated as always
 * claimable here to model a row whose claim lease has already expired
 * after a crash. The real repository only reclaims an OCR_DONE or
 * IN_FLIGHT row once its lease has actually expired, which is what
 * LedgerRepositoryIT exercises against a real Postgres instance. Also
 * fakes seedPending, resetForUpdate, and tombstone, so a consumer test
 * driving those directly (rather than through a coordinator) does not
 * need its own subclass.
 */
class FakeLedgerRepository extends LedgerRepository {

    private final Map<Long, Row> rows = new LinkedHashMap<>();
    private RuntimeException throwOnNextFindUnresolved;

    FakeLedgerRepository() {
        super(null, 300L);
    }

    void presetState(long id, Status status, String ocrPayload) {
        rows.put(id, new Row(status, ocrPayload));
    }

    void presetPending(long id) {
        presetState(id, Status.PENDING, null);
    }

    void presetFailedRetryable(long id, int attempts, String lastError) {
        Row row = new Row(Status.FAILED_RETRYABLE, null);
        row.attempts = attempts;
        row.consecutiveFailures = attempts;
        row.lastError = lastError;
        rows.put(id, row);
    }

    int consecutiveFailuresOf(long id) {
        Row row = rows.get(id);
        return row == null ? 0 : row.consecutiveFailures;
    }

    void throwOnNextFindUnresolved(RuntimeException exception) {
        this.throwOnNextFindUnresolved = exception;
    }

    Status statusOf(long id) {
        Row row = rows.get(id);
        return row == null ? null : row.status;
    }

    String lastErrorOf(long id) {
        Row row = rows.get(id);
        return row == null ? null : row.lastError;
    }

    boolean exists(long id) {
        return rows.containsKey(id);
    }

    @Override
    public int seedPending(List<Long> ids, String lane, Map<Long, Instant> createdAt) {
        int inserted = 0;
        for (Long id : ids) {
            if (!rows.containsKey(id)) {
                rows.put(id, new Row(Status.PENDING, null));
                inserted++;
            }
        }
        return inserted;
    }

    @Override
    public void resetForUpdate(long id, long version) {
        Row row = rows.computeIfAbsent(id, unused -> new Row(Status.PENDING, null));
        row.status = Status.PENDING;
        row.ocrPayload = null;
        row.lastError = null;
        row.consecutiveFailures = 0;
    }

    @Override
    public void tombstone(long id) {
        rows.remove(id);
    }

    @Override
    public List<ExceededAttempt> failExceededAttempts(List<Long> ids, int maxConsecutiveFailures) {
        List<ExceededAttempt> exceeded = new ArrayList<>();
        for (Long id : ids) {
            Row row = rows.get(id);
            boolean candidate = row != null && (row.status == Status.PENDING || row.status == Status.FAILED_RETRYABLE);
            if (candidate && row.consecutiveFailures >= maxConsecutiveFailures) {
                row.status = Status.FAILED_PERMANENT;
                exceeded.add(new ExceededAttempt(id, row.attempts, row.lastError));
            }
        }
        return exceeded;
    }

    @Override
    public int attemptsOf(long id) {
        Row row = rows.get(id);
        return row == null ? 0 : row.attempts;
    }

    @Override
    public List<Long> claim(List<Long> ids) {
        List<Long> claimed = new ArrayList<>();
        for (Long id : ids) {
            Row row = rows.computeIfAbsent(id, unused -> new Row(Status.PENDING, null));
            if (row.status == Status.PENDING || row.status == Status.FAILED_RETRYABLE
                    || row.status == Status.OCR_DONE) {
                row.status = Status.IN_FLIGHT;
                row.attempts++;
                claimed.add(id);
            }
        }
        return claimed;
    }

    @Override
    public Map<Long, String> findCachedOcrPayloads(List<Long> ids) {
        Map<Long, String> payloads = new HashMap<>();
        for (Long id : ids) {
            Row row = rows.get(id);
            if (row != null && row.ocrPayload != null) {
                payloads.put(id, row.ocrPayload);
            }
        }
        return payloads;
    }

    @Override
    public void saveOcrPayload(long id, String json) {
        Row row = rows.computeIfAbsent(id, unused -> new Row(Status.IN_FLIGHT, null));
        row.ocrPayload = json;
        row.status = Status.OCR_DONE;
        row.consecutiveFailures = 0;
    }

    @Override
    public void markDone(long id, String checksum) {
        Row row = rows.computeIfAbsent(id, unused -> new Row(Status.IN_FLIGHT, null));
        row.status = Status.DONE;
        row.checksum = checksum;
        row.consecutiveFailures = 0;
    }

    @Override
    public void markFailed(long id, String error, boolean permanent, boolean countsTowardRetryCap) {
        Row row = rows.computeIfAbsent(id, unused -> new Row(Status.IN_FLIGHT, null));
        row.status = permanent ? Status.FAILED_PERMANENT : Status.FAILED_RETRYABLE;
        row.lastError = error;
        if (!permanent && countsTowardRetryCap) {
            row.consecutiveFailures++;
        }
    }

    @Override
    public List<Long> findUnresolved(List<Long> ids) {
        if (throwOnNextFindUnresolved != null) {
            RuntimeException toThrow = throwOnNextFindUnresolved;
            throwOnNextFindUnresolved = null;
            throw toThrow;
        }
        List<Long> unresolved = new ArrayList<>();
        for (Long id : ids) {
            Row row = rows.get(id);
            // No row at all, whether because it was never seeded or
            // because tombstone() just removed it, mirrors the real
            // query's WHERE source_id = ANY(ids): an id with no matching
            // row is simply absent from the result set, not unresolved.
            if (row == null) {
                continue;
            }
            if (row.status != Status.DONE && row.status != Status.FAILED_PERMANENT) {
                unresolved.add(id);
            }
        }
        return unresolved;
    }

    private static final class Row {
        private Status status;
        private String ocrPayload;
        private String checksum;
        private String lastError;
        private int attempts;
        private int consecutiveFailures;

        private Row(Status status, String ocrPayload) {
            this.status = status;
            this.ocrPayload = ocrPayload;
        }
    }
}
