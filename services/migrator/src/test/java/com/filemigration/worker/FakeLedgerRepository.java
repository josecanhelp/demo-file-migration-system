package com.filemigration.worker;

import com.filemigration.model.Status;
import com.filemigration.store.LedgerRepository;

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
 * LedgerRepositoryIT exercises against a real Postgres instance.
 */
class FakeLedgerRepository extends LedgerRepository {

    private final Map<Long, Row> rows = new LinkedHashMap<>();

    FakeLedgerRepository() {
        super(null, 300L);
    }

    void presetState(long id, Status status, String ocrPayload) {
        rows.put(id, new Row(status, ocrPayload));
    }

    void presetPending(long id) {
        presetState(id, Status.PENDING, null);
    }

    Status statusOf(long id) {
        Row row = rows.get(id);
        return row == null ? null : row.status;
    }

    String lastErrorOf(long id) {
        Row row = rows.get(id);
        return row == null ? null : row.lastError;
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
    }

    @Override
    public void markDone(long id, String checksum) {
        Row row = rows.computeIfAbsent(id, unused -> new Row(Status.IN_FLIGHT, null));
        row.status = Status.DONE;
        row.checksum = checksum;
    }

    @Override
    public void markFailed(long id, String error, boolean permanent) {
        Row row = rows.computeIfAbsent(id, unused -> new Row(Status.IN_FLIGHT, null));
        row.status = permanent ? Status.FAILED_PERMANENT : Status.FAILED_RETRYABLE;
        row.lastError = error;
    }

    @Override
    public List<Long> findUnresolved(List<Long> ids) {
        List<Long> unresolved = new ArrayList<>();
        for (Long id : ids) {
            Status status = statusOf(id);
            if (status != Status.DONE && status != Status.FAILED_PERMANENT) {
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

        private Row(Status status, String ocrPayload) {
            this.status = status;
            this.ocrPayload = ocrPayload;
        }
    }
}
