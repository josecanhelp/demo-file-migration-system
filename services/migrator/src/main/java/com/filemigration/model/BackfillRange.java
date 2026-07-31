package com.filemigration.model;

/**
 * One inclusive slice of source ids the backfill coordinator plans to walk
 * through, tracked in backfill_checkpoint. A range moves from PENDING to
 * CLAIMED while a coordinator is seeding and publishing it, then to DONE
 * once every chunk in it has been published.
 */
public record BackfillRange(long rangeStart, long rangeEnd) {
}
