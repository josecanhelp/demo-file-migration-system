package com.filemigration.store;

import com.filemigration.model.Stage;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * The migration_event table: an append-only record of every stage a source
 * file has passed through, used to reconstruct a file's path after the
 * fact.
 */
@Repository
public class EventRepository {

    private final JdbcTemplate targetJdbc;

    public EventRepository(@Qualifier("targetJdbc") JdbcTemplate targetJdbc) {
        this.targetJdbc = targetJdbc;
    }

    /**
     * Appends one event row. sourceId may be null for events that describe
     * something other than a single file (for example a backfill range).
     * detailJson may be null when a stage carries no extra detail.
     */
    public void record(Long sourceId, Stage stage, String lane, String detailJson) {
        targetJdbc.update(
                "INSERT INTO migration_event (source_id, stage, lane, detail) "
                        + "VALUES (?, ?, ?, ?::jsonb)",
                sourceId, stage.name(), lane, detailJson);
    }

    /**
     * Appends one event row per id in sourceIds, all carrying the same
     * stage, lane, and detail, in a single round trip. Used when one thing
     * that happened (a backfill range being scanned, a chunk being
     * published) resolves into the same stage for every file it covered, so
     * each of those files gets its own row exactly as it would from a loop
     * over {@link #record(Long, Stage, String, String)}, without a
     * round trip per id. A no-op for an empty list.
     */
    public void recordBatch(List<Long> sourceIds, Stage stage, String lane, String detailJson) {
        if (sourceIds.isEmpty()) {
            return;
        }
        List<Object[]> batchArgs = new ArrayList<>(sourceIds.size());
        for (Long sourceId : sourceIds) {
            batchArgs.add(new Object[] { sourceId, stage.name(), lane, detailJson });
        }
        targetJdbc.batchUpdate(
                "INSERT INTO migration_event (source_id, stage, lane, detail) "
                        + "VALUES (?, ?, ?, ?::jsonb)",
                batchArgs);
    }
}
