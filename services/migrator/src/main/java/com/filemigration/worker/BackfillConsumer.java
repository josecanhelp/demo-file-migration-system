package com.filemigration.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.backfill.BackfillMessage;
import com.filemigration.store.LedgerRepository;
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
 * Consumes batches of source ids from the backfill topic and runs them
 * through {@link MigrationService}. A message is only acknowledged once
 * every id it named has actually reached a terminal state. It is
 * negatively acknowledged, with a backoff, instead in two cases: some id
 * is still owned by a claim from an earlier, now-dead attempt at this
 * same message, or migrate() itself threw for any reason, a failed
 * vendor call, a database problem, an object store problem, anything at
 * all. Either way the same message comes back for another try instead of
 * a batch that only got partway done, or never started, being forgotten.
 * A message that cannot even be parsed is the one case acknowledged
 * immediately instead: retrying it would never succeed, so leaving it
 * unacknowledged would only block every message behind it on the same
 * partition forever.
 */
@Component
@Profile("worker")
public class BackfillConsumer {

    private static final Logger log = LoggerFactory.getLogger(BackfillConsumer.class);

    private final MigrationService migrationService;
    private final LedgerRepository ledger;
    private final ObjectMapper objectMapper;
    private final Duration unresolvedBackoff;

    public BackfillConsumer(MigrationService migrationService, LedgerRepository ledger, ObjectMapper objectMapper,
            @Value("${migrator.backfill.nack-backoff-seconds}") long nackBackoffSeconds) {
        this.migrationService = migrationService;
        this.ledger = ledger;
        this.objectMapper = objectMapper;
        this.unresolvedBackoff = Duration.ofSeconds(nackBackoffSeconds);
    }

    @KafkaListener(
            topics = "${migrator.backfill.topic}",
            groupId = "${migrator.backfill.group-id}",
            concurrency = "${migrator.worker-concurrency}")
    public void consume(String payload, Acknowledgment acknowledgment) {
        BackfillMessage message;
        try {
            message = objectMapper.readValue(payload, BackfillMessage.class);
        } catch (JsonProcessingException e) {
            log.error("Backfill message could not be parsed, acknowledging it since retrying would never "
                    + "succeed. Raw payload: {}", payload, e);
            acknowledgment.acknowledge();
            return;
        }

        MigrationOutcome outcome;
        List<Long> unresolved;
        try {
            outcome = migrationService.migrate(message.sourceIds(), message.lane());
            unresolved = ledger.findUnresolved(message.sourceIds());
        } catch (Exception e) {
            // Deliberately broad: a vendor failure, a database problem, an
            // object store problem, or anything else migrate() or
            // findUnresolved() can throw must never let this message be
            // acknowledged. Letting any of them propagate past here would
            // hand the retry decision to the container's own generic error
            // handling, which gives up after a handful of quick attempts
            // and commits the offset anyway, exactly how a batch would go
            // missing during an outage lasting longer than a couple of
            // seconds.
            log.warn("Backfill batch of {} id(s) failed processing ({}: {}); retrying in {}",
                    message.sourceIds().size(), e.getClass().getSimpleName(), e.getMessage(), unresolvedBackoff);
            acknowledgment.nack(unresolvedBackoff);
            return;
        }
        log.info("Backfill batch of {} id(s): done={} skipped={} permanentFailures={} retryable={}",
                message.sourceIds().size(), outcome.done(), outcome.skipped(), outcome.permanentFailures(),
                outcome.retryable());

        if (!unresolved.isEmpty()) {
            log.warn("Backfill batch left {} id(s) still unresolved after migrate() returned, most likely "
                    + "still claimed by an earlier attempt at this message that never finished; retrying "
                    + "in {}: {}", unresolved.size(), unresolvedBackoff, unresolved);
            acknowledgment.nack(unresolvedBackoff);
            return;
        }
        acknowledgment.acknowledge();
    }
}
