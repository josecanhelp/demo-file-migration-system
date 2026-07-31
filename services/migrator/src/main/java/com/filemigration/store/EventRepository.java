package com.filemigration.store;

import com.filemigration.model.Stage;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

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
}
