package com.filemigration.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.backfill.BackfillMessage;
import com.filemigration.store.LedgerRepository;
import com.filemigration.vendor.VendorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Consumes batches of source ids from the backfill topic and runs them
 * through {@link MigrationService}. A message is only acknowledged once
 * every id it named has actually reached a terminal state. It is
 * negatively acknowledged, with a backoff, instead of acknowledged in two
 * cases: some id is still owned by a claim from an earlier, now-dead
 * attempt at this same message, or the vendor call itself failed in a way
 * that is worth trying again later. Either way the same message comes
 * back for another try instead of a batch that only got partway done, or
 * never started, being forgotten. Nothing from this method is ever left
 * to fall through to the container's own error handling: a vendor
 * failure is caught and turned into a negative acknowledgment right here,
 * since letting it propagate would hand the retry decision to a generic
 * handler that gives up after a handful of quick attempts and commits the
 * offset anyway, which is exactly how a batch would go missing during a
 * vendor outage longer than a couple of seconds. A message that cannot
 * even be parsed is acknowledged immediately instead: retrying it would
 * never succeed, so leaving it unacknowledged would only block every
 * message behind it on the same partition forever.
 *
 * While a batch is being worked, this renews the claim on whichever of
 * its ids are actually in flight on a short interval, so the claim lease
 * only has to outlast a brief gap between renewals rather than however
 * long the whole batch takes to process.
 */
@Component
@Profile("worker")
public class BackfillConsumer {

    private static final Logger log = LoggerFactory.getLogger(BackfillConsumer.class);

    private final MigrationService migrationService;
    private final LedgerRepository ledger;
    private final ObjectMapper objectMapper;
    private final Duration unresolvedBackoff;
    private final Duration claimRenewInterval;
    private final ScheduledExecutorService renewalExecutor;

    public BackfillConsumer(MigrationService migrationService, LedgerRepository ledger, ObjectMapper objectMapper,
            @Value("${migrator.backfill.nack-backoff-seconds}") long nackBackoffSeconds,
            @Value("${migrator.claim-renew-interval-seconds}") long claimRenewIntervalSeconds,
            @Value("${migrator.worker-concurrency}") int workerConcurrency) {
        this.migrationService = migrationService;
        this.ledger = ledger;
        this.objectMapper = objectMapper;
        this.unresolvedBackoff = Duration.ofSeconds(nackBackoffSeconds);
        this.claimRenewInterval = Duration.ofSeconds(claimRenewIntervalSeconds);
        this.renewalExecutor = Executors.newScheduledThreadPool(Math.max(2, workerConcurrency), runnable -> {
            Thread thread = new Thread(runnable, "backfill-claim-renewer");
            thread.setDaemon(true);
            return thread;
        });
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

        ScheduledFuture<?> renewal = renewalExecutor.scheduleAtFixedRate(
                () -> renewClaims(message.sourceIds()),
                claimRenewInterval.toSeconds(), claimRenewInterval.toSeconds(), TimeUnit.SECONDS);
        try {
            MigrationOutcome outcome;
            try {
                outcome = migrationService.migrate(message.sourceIds(), message.lane());
            } catch (VendorException e) {
                log.warn("Backfill batch of {} id(s) failed calling the vendor ({}); retrying in {}",
                        message.sourceIds().size(), e.getMessage(), unresolvedBackoff);
                acknowledgment.nack(unresolvedBackoff);
                return;
            }
            log.info("Backfill batch of {} id(s): done={} skipped={} permanentFailures={} retryable={}",
                    message.sourceIds().size(), outcome.done(), outcome.skipped(), outcome.permanentFailures(),
                    outcome.retryable());

            List<Long> unresolved = ledger.findUnresolved(message.sourceIds());
            if (!unresolved.isEmpty()) {
                log.warn("Backfill batch left {} id(s) still unresolved after migrate() returned, most likely "
                        + "still claimed by an earlier attempt at this message that never finished; retrying "
                        + "in {}: {}", unresolved.size(), unresolvedBackoff, unresolved);
                acknowledgment.nack(unresolvedBackoff);
                return;
            }
            acknowledgment.acknowledge();
        } finally {
            renewal.cancel(false);
        }
    }

    private void renewClaims(List<Long> ids) {
        try {
            ledger.renewClaims(ids);
        } catch (RuntimeException e) {
            log.warn("Failed to renew the backfill claim lease for {} id(s); it may expire and become "
                    + "reclaimable while this batch is still legitimately in progress", ids.size(), e);
        }
    }
}
