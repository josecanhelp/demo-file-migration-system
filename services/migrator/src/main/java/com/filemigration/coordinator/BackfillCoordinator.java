package com.filemigration.coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.backfill.BackfillMessage;
import com.filemigration.backfill.Chunker;
import com.filemigration.model.BackfillRange;
import com.filemigration.model.Stage;
import com.filemigration.store.BackfillCheckpointRepository;
import com.filemigration.store.EventRepository;
import com.filemigration.store.LedgerRepository;
import com.filemigration.store.SourceFileRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Divides the source table into fixed-size id ranges, seeds a ledger row
 * for every id in each range, and publishes those ids to the backfill
 * topic in vendor-sized chunks. Runs for the life of the process: it
 * repeatedly checks how far the source table has grown, plans any ranges
 * that do not exist yet, drains every claimable range it finds, and then
 * sleeps for a configured interval before checking again. This is what
 * lets a source table that is still empty, or still being seeded, when
 * the coordinator starts catch up on its own the moment rows show up,
 * with no restart needed.
 *
 * Planning happens on a background thread rather than during startup, so
 * a source table large enough to take a while to plan does not hold up
 * the application context or the health endpoint that depends on it.
 */
@Component
@Profile("coordinator")
public class BackfillCoordinator {

    private static final Logger log = LoggerFactory.getLogger(BackfillCoordinator.class);
    private static final String LANE = "backfill";
    private static final long PUBLISH_TIMEOUT_SECONDS = 30;

    private final SourceFileRepository sourceRepo;
    private final BackfillCheckpointRepository checkpointRepo;
    private final LedgerRepository ledger;
    private final EventRepository eventRepo;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final long rangeSize;
    private final int vendorBatchSize;
    private final String topic;
    private final long planIntervalSeconds;
    private boolean announcedComplete;

    public BackfillCoordinator(SourceFileRepository sourceRepo, BackfillCheckpointRepository checkpointRepo,
            LedgerRepository ledger, EventRepository eventRepo, KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${migrator.backfill.range-size}") long rangeSize,
            @Value("${migrator.vendor.batch-size}") int vendorBatchSize,
            @Value("${migrator.backfill.topic}") String topic,
            @Value("${migrator.backfill.plan-interval-seconds}") long planIntervalSeconds) {
        this.sourceRepo = sourceRepo;
        this.checkpointRepo = checkpointRepo;
        this.ledger = ledger;
        this.eventRepo = eventRepo;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.rangeSize = rangeSize;
        this.vendorBatchSize = vendorBatchSize;
        this.topic = topic;
        this.planIntervalSeconds = planIntervalSeconds;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        Thread planningThread = new Thread(this::runPlanningLoop, "backfill-coordinator");
        planningThread.setDaemon(true);
        planningThread.start();
    }

    private void runPlanningLoop() {
        while (true) {
            try {
                planAndDrainOnce();
            } catch (RuntimeException e) {
                // A range left CLAIMED here is not lost: once its lease
                // expires it becomes claimable again on a later pass of
                // this same loop.
                log.error("A backfill range failed; it will become reclaimable once its lease expires and a "
                        + "later planning pass will retry it", e);
            }
            sleepInterval();
        }
    }

    private void planAndDrainOnce() {
        long maxId = sourceRepo.maxId();
        if (maxId <= 0) {
            log.info("Source table has no rows yet; backfill coordinator will check again in {}s",
                    planIntervalSeconds);
            return;
        }

        int plannedRanges = checkpointRepo.planRanges(maxId, rangeSize);
        if (plannedRanges > 0) {
            log.info("Backfill planning covers ids 1 through {}, inserted {} new range(s)", maxId, plannedRanges);
            announcedComplete = false;
        }

        boolean processedAnyRange = false;
        while (true) {
            checkpointRepo.reapExpiredClaims();
            Optional<BackfillRange> claimed = checkpointRepo.claimNextRange();
            if (claimed.isEmpty()) {
                break;
            }
            processRange(claimed.get());
            processedAnyRange = true;
        }

        // Logged once on the pass that actually reaches "nothing left",
        // not on every idle pass afterward: with nothing new to plan and
        // nothing left to claim, every subsequent pass would otherwise
        // repeat the exact same line forever.
        if (processedAnyRange) {
            announcedComplete = false;
        }
        if (!announcedComplete) {
            log.info("backfill planning complete, checking again in {}s", planIntervalSeconds);
            announcedComplete = true;
        }
    }

    private void sleepInterval() {
        try {
            TimeUnit.SECONDS.sleep(planIntervalSeconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Package-private rather than private so an integration test can drive
    // exactly one range through the real pipeline (seed, publish, record,
    // mark done) without going through the claim loop, which is exercised
    // separately against a reserved range of its own.
    void processRange(BackfillRange range) {
        List<Long> ids = sourceRepo.findIdsInRange(range.rangeStart(), range.rangeEnd());
        List<List<Long>> chunks = Chunker.chunk(ids, vendorBatchSize);
        int published = 0;
        for (List<Long> chunk : chunks) {
            // Seeding must commit before the matching chunk is published:
            // otherwise a consumer could pull the message and try to
            // claim ids the ledger does not know about yet.
            ledger.seedPending(chunk, LANE, null);
            publish(chunk);
            published += chunk.size();
        }
        eventRepo.record(null, Stage.QUEUED, LANE, writeRangeDetail(range, ids.size()));
        checkpointRepo.markDone(range.rangeStart(), range.rangeEnd());
        log.info("Backfill range {}-{} queued {} id(s) across {} chunk(s)",
                range.rangeStart(), range.rangeEnd(), published, chunks.size());
    }

    private void publish(List<Long> ids) {
        String payload = writeMessage(ids);
        // Keyed by the chunk's first id rather than left null: an
        // unkeyed record lets Kafka's sticky partitioner land an entire
        // burst of sends on the same partition, which would mean a
        // stuck message blocks the whole lane instead of a share of it.
        String key = String.valueOf(ids.get(0));
        try {
            kafkaTemplate.send(topic, key, payload).get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing a backfill chunk to " + topic, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Failed to publish a backfill chunk to " + topic, e);
        }
    }

    private String writeMessage(List<Long> ids) {
        try {
            return objectMapper.writeValueAsString(new BackfillMessage(LANE, ids));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize a backfill message", e);
        }
    }

    private String writeRangeDetail(BackfillRange range, int idCount) {
        try {
            return objectMapper.writeValueAsString(Map.of(
                    "rangeStart", range.rangeStart(),
                    "rangeEnd", range.rangeEnd(),
                    "idCount", idCount));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize a backfill range event detail", e);
        }
    }
}
