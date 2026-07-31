package com.filemigration.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.backfill.BackfillMessage;
import com.filemigration.store.LedgerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Consumes batches of source ids from the backfill topic and runs them
 * through {@link MigrationService}. The offset for a message is only
 * committed once every id it named has actually reached a terminal
 * state: if migrate() throws because the vendor call came back
 * rate-limited or transiently failed, this method throws too, and if
 * migrate() returns without throwing but some id it was given is still
 * owned by a claim from an earlier, now-dead attempt at this same
 * message, this method throws anyway. Either way nothing is
 * acknowledged, so the same message is redelivered and gets another
 * attempt instead of a batch that only got partway done being forgotten.
 */
@Component
@Profile("worker")
public class BackfillConsumer {

    private static final Logger log = LoggerFactory.getLogger(BackfillConsumer.class);

    private final MigrationService migrationService;
    private final LedgerRepository ledger;
    private final ObjectMapper objectMapper;

    public BackfillConsumer(MigrationService migrationService, LedgerRepository ledger, ObjectMapper objectMapper) {
        this.migrationService = migrationService;
        this.ledger = ledger;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${migrator.backfill.topic}",
            groupId = "${migrator.backfill.group-id}",
            concurrency = "${migrator.worker-concurrency}")
    public void consume(String payload, Acknowledgment acknowledgment) {
        BackfillMessage message = readMessage(payload);
        MigrationOutcome outcome = migrationService.migrate(message.sourceIds(), message.lane());
        log.info("Backfill batch of {} id(s): done={} skipped={} permanentFailures={} retryable={}",
                message.sourceIds().size(), outcome.done(), outcome.skipped(), outcome.permanentFailures(),
                outcome.retryable());

        List<Long> unresolved = ledger.findUnresolved(message.sourceIds());
        if (!unresolved.isEmpty()) {
            throw new IllegalStateException("Backfill batch left " + unresolved.size()
                    + " id(s) still unresolved after migrate() returned, most likely still claimed by an "
                    + "earlier attempt at this message that never finished; not acknowledging so it is "
                    + "redelivered: " + unresolved);
        }
        acknowledgment.acknowledge();
    }

    private BackfillMessage readMessage(String payload) {
        try {
            return objectMapper.readValue(payload, BackfillMessage.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to parse a backfill message: " + payload, e);
        }
    }
}
