package com.filemigration.worker;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.model.Stage;
import com.filemigration.store.EventRepository;
import com.filemigration.store.LedgerRepository;
import com.filemigration.store.ObjectStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Consumes Debezium change envelopes for sourcedb.files and drives each one
 * through {@link MigrationService} the moment it lands, rather than waiting
 * for the backfill lane to sweep past that id. A create or read envelope
 * seeds a PENDING row and migrates it; an update resets the row so it is
 * re-OCR'd against the new content and migrates it; a delete removes the
 * row and its document from both stores. An envelope is only acknowledged
 * once whatever it named has actually reached a terminal state, mirroring
 * BackfillConsumer: any failure other than a message this consumer cannot
 * even parse comes back for another try instead of being silently dropped.
 *
 * Debezium always excludes the blob column from what it publishes here
 * (see column.exclude.list in the connector config), so every envelope
 * this class handles is small; the blob itself is fetched from the source
 * database by id, the same way the backfill lane does it.
 */
@Component
@Profile("worker")
public class CdcConsumer {

    private static final Logger log = LoggerFactory.getLogger(CdcConsumer.class);
    private static final String LANE = "cdc";

    private static final String OP_CREATE = "c";
    private static final String OP_READ = "r";
    private static final String OP_UPDATE = "u";
    private static final String OP_DELETE = "d";

    private final MigrationService migrationService;
    private final LedgerRepository ledger;
    private final ObjectStore objectStore;
    private final EventRepository eventRepo;
    private final ObjectMapper objectMapper;
    private final Duration nackBackoff;

    public CdcConsumer(MigrationService migrationService, LedgerRepository ledger, ObjectStore objectStore,
            EventRepository eventRepo, ObjectMapper objectMapper,
            @Value("${migrator.cdc.nack-backoff-seconds}") long nackBackoffSeconds) {
        this.migrationService = migrationService;
        this.ledger = ledger;
        this.objectStore = objectStore;
        this.eventRepo = eventRepo;
        this.objectMapper = objectMapper;
        this.nackBackoff = Duration.ofSeconds(nackBackoffSeconds);
    }

    @KafkaListener(
            topics = "${migrator.cdc.topic}",
            groupId = "${migrator.cdc.group-id}",
            concurrency = "${migrator.worker-concurrency}")
    public void consume(String payload, Acknowledgment acknowledgment) {
        if (payload == null) {
            // Debezium publishes a null-valued tombstone record right
            // after a delete envelope for the same key, purely so Kafka
            // log compaction can eventually drop both records. The delete
            // itself was already carried out when the 'd' envelope with
            // op=d arrived; there is nothing left to do here.
            acknowledgment.acknowledge();
            return;
        }

        CdcEnvelope envelope;
        try {
            envelope = objectMapper.readValue(payload, CdcEnvelope.class);
        } catch (JsonProcessingException e) {
            log.error("CDC message could not be parsed, acknowledging it since retrying would never succeed. "
                    + "Raw payload: {}", payload, e);
            acknowledgment.acknowledge();
            return;
        }

        Long id = envelope.sourceId();
        if (id == null) {
            log.error("CDC envelope with op '{}' carried no usable id, acknowledging it since retrying would "
                    + "never succeed. Raw payload: {}", envelope.op(), payload);
            acknowledgment.acknowledge();
            return;
        }

        eventRepo.record(id, Stage.CDC_CAPTURED, LANE, null);

        try {
            switch (envelope.op()) {
                case OP_CREATE, OP_READ -> {
                    ledger.seedPending(List.of(id), LANE, null);
                    migrationService.migrate(List.of(id), LANE);
                }
                case OP_UPDATE -> {
                    ledger.resetForUpdate(id, envelope.version());
                    migrationService.migrate(List.of(id), LANE);
                }
                case OP_DELETE -> {
                    ledger.tombstone(id);
                    objectStore.delete(objectStore.keyFor(id));
                }
                default -> log.error("CDC envelope for id {} carried an unrecognized op '{}', acknowledging it "
                        + "since retrying would never help", id, envelope.op());
            }
        } catch (Exception e) {
            // Deliberately broad, mirroring BackfillConsumer: a vendor
            // failure, a database problem, an object store problem, or
            // anything else this branch can throw must never let this
            // envelope be acknowledged. Letting it propagate would hand
            // the retry decision to the container's own generic error
            // handling, which gives up quickly and commits the offset
            // anyway, losing this change during any outage longer than a
            // couple of seconds.
            log.warn("CDC envelope for id {} (op {}) failed processing ({}: {}); retrying in {}",
                    id, envelope.op(), e.getClass().getSimpleName(), e.getMessage(), nackBackoff);
            acknowledgment.nack(nackBackoff);
            return;
        }

        List<Long> unresolved = ledger.findUnresolved(List.of(id));
        if (!unresolved.isEmpty()) {
            log.warn("CDC id {} still unresolved after processing, most likely still claimed by an earlier "
                    + "attempt that never finished; retrying in {}", id, nackBackoff);
            acknowledgment.nack(nackBackoff);
            return;
        }
        acknowledgment.acknowledge();
    }

    /**
     * The Debezium change envelope for one row. Only the fields this
     * consumer actually acts on are modeled; everything else Debezium
     * includes (source metadata, transaction info, and so on) is ignored.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record CdcEnvelope(String op, JsonNode before, JsonNode after, @JsonProperty("ts_ms") Long tsMs) {

        /**
         * The id a change concerns. Every op except a delete carries the
         * row in "after"; a delete's "after" is always null, since the row
         * is gone, so its id has to come from "before" instead, the row as
         * it looked the moment it was removed.
         */
        Long sourceId() {
            JsonNode row = OP_DELETE.equals(op) ? before : after;
            if (row == null || !row.hasNonNull("id")) {
                return null;
            }
            return row.get("id").asLong();
        }

        /**
         * A marker for how recent this change is, used to bump the
         * ledger's source_version on an update. Debezium's own capture
         * timestamp is used rather than anything derived from the row
         * itself, since it increases with every event regardless of what
         * changed.
         */
        long version() {
            return tsMs == null ? 0L : tsMs;
        }
    }
}
