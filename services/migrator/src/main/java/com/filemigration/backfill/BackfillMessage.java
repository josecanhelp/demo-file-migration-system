package com.filemigration.backfill;

import java.util.List;

/**
 * The Kafka message contract for the backfill topic: which lane a batch of
 * ids belongs to and the ids themselves. Serialized and deserialized as
 * plain JSON so the wire format has no dependency on either side's
 * classpath.
 */
public record BackfillMessage(String lane, List<Long> sourceIds) {
}
