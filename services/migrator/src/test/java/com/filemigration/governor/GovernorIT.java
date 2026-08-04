package com.filemigration.governor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.filemigration.backfill.BackfillMessage;
import com.filemigration.store.ObjectStore;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the Governor against the real stack docker compose starts: real
 * Postgres, MySQL, Kafka, MinIO, and vendor-mock, with the actual
 * production wiring of Governor, LaneRateLimiter, the vendor CircuitBreaker,
 * and BreakerListener acting on real, Spring-managed listener containers.
 * This is the test the project's headline demo depends on: kill the
 * vendor, watch the breaker trip and consumption pause, restore the
 * vendor, watch the backlog drain with zero files lost.
 *
 * migrator.worker-concurrency is deliberately set higher than either lane's
 * single topic partition: a concurrency thread with no partition assigned
 * sits idle and never registers as paused the way MessageListenerContainer
 * .isContainerPaused() reports it (that flag needs every child thread to
 * individually confirm it, which an idle thread never does), which is
 * exactly the configuration under which BreakerListener once got stuck
 * permanently paused, since it only called resume() when isContainerPaused()
 * already read true. Running every test in this class under that
 * configuration is what lets a regression of that bug actually fail a test
 * here instead of only ever showing up in a live, manually driven chaos
 * run. Assertions in this class that need to observe pause or resume
 * happening use isPauseRequested() instead of isContainerPaused() for
 * exactly that reason: pause()/resume() set it immediately and
 * synchronously on every container, parent and child alike, regardless of
 * whether a child has any partition assigned to actually act on it, so it
 * is the one signal here that is never at the mercy of an idle thread's
 * poll loop.
 *
 * Every id this test seeds uses its own reserved source id range, well
 * clear of every other IT's. Within that range, BASE_ID itself moves on
 * every run (see nextRunSlot()), rotating through a fixed set of
 * non-overlapping slots so that two runs can never insert into the same
 * source id, even if one of them was killed before it ever reached its
 * own cleanup. Cleanup after each test still runs regardless of outcome,
 * but only after confirming the backfill and cdc lanes have actually
 * finished committing everything published during that test: deleting a
 * ledger or source row while a message naming it can still be redelivered
 * is what manufactures a "Source record no longer exists" failure that
 * counts toward the retry cap for no real reason, so cleanup drains each
 * lane first, deletes, and then confirms the range actually stays empty
 * before moving on. The backfill, cdc, and dlq topics and consumer groups
 * are private to this test class, distinct from the real names the live
 * migrator-worker container uses, so this test never contends with, or
 * depends on the pace of, whatever that container is doing against the
 * same compose network. The vendor itself, and the shared vendor circuit
 * breaker this Spring context builds, are not private the same way: every
 * test method here shares one running application context, so each one
 * restores the vendor to healthy and waits for the breaker to leave OPEN
 * before finishing, whether it passed or failed, so one test's chaos is
 * never a later test's inherited problem.
 */
@SpringBootTest(properties = {
        "MYSQL_HOST=localhost",
        "POSTGRES_HOST=localhost",
        "MINIO_ENDPOINT=http://localhost:9000",
        "VENDOR_BASE_URL=http://localhost:8088",
        "KAFKA_BOOTSTRAP_SERVERS=localhost:9092",
        "migrator.backfill.topic=files.backfill.governor-it",
        "migrator.backfill.group-id=backfill-governor-it",
        "migrator.backfill.topic-partitions=1",
        "migrator.cdc.topic=cdc.sourcedb.files.governor-it",
        "migrator.cdc.group-id=cdc-governor-it",
        "migrator.cdc.topic-partitions=1",
        "migrator.dlq.topic=files.dlq.governor-it",
        "migrator.dlq.topic-partitions=1",
        "migrator.worker-concurrency=3",
        "migrator.claim-lease-seconds=300",
        "migrator.backfill.nack-backoff-seconds=2",
        "migrator.cdc.nack-backoff-seconds=2",
        "migrator.max-retry-attempts=3",
        "migrator.breaker.failure-rate-threshold=50",
        "migrator.breaker.open-duration-seconds=5",
        "migrator.governor.retry-base-backoff-ms=50",
        // Shortened from Spring Kafka's 5s default so a container's poll
        // loop notices a pause() request quickly. Kept even though
        // BreakerListener no longer waits on isContainerPaused() to act,
        // since a faster reaction to a pause request is still a good idea
        // on its own.
        "spring.kafka.listener.poll-timeout=1000"
})
@ActiveProfiles("worker")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GovernorIT {

    private static final String BACKFILL_TOPIC = "files.backfill.governor-it";
    private static final String BACKFILL_GROUP_ID = "backfill-governor-it";
    private static final String CDC_TOPIC = "cdc.sourcedb.files.governor-it";
    private static final String CDC_GROUP_ID = "cdc-governor-it";

    /** The lowest id this class ever reserves; ReconcileIT's own range starts at 9,700,000. */
    private static final long RANGE_FLOOR = 9_600_000L;
    private static final long RANGE_CEILING = 9_700_000L;
    /** The widest span of ids, relative to BASE_ID, any test method in this class seeds. */
    private static final long RUN_SLOT_WIDTH = 1_000L;
    private static final long RUN_SLOT_COUNT = (RANGE_CEILING - RANGE_FLOOR) / RUN_SLOT_WIDTH;
    /**
     * Where a run's own id range starts within [RANGE_FLOOR, RANGE_CEILING).
     * Computed once per run by rotating through RUN_SLOT_COUNT fixed slots
     * (see nextRunSlot()), so a run started immediately after a prior one
     * was killed, before that prior run's own cleanup ever ran, still gets
     * a slot the prior run never touched rather than reusing it.
     */
    private static final long BASE_ID = RANGE_FLOOR + nextRunSlot() * RUN_SLOT_WIDTH;

    private static final Duration DETECTION_TIMEOUT = Duration.ofSeconds(40);
    private static final Duration RECOVERY_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(250);
    /** How long cleanup waits for both lanes to finish committing every message before it is safe to delete. */
    private static final Duration LANE_DRAIN_TIMEOUT = Duration.ofSeconds(40);
    /** How long the range this test just cleaned must read empty, continuously, before cleanup calls it done. */
    private static final Duration CLEANUP_STABILITY_WINDOW = Duration.ofSeconds(2);
    /**
     * Bounds how long cleanup will keep re-deleting a reappearing row
     * before failing loudly instead of hanging. Every insert or delete
     * this class makes against the real MySQL files table is a real row
     * change, so the real Debezium connector captures it independently of
     * this class's own private topics, and the real, always-running
     * migrator-worker container can claim and reprocess an id after this
     * method's own delete already ran, the same real-pipeline race
     * ReconcileIT's own cleanup ceiling accounts for. That container's own
     * nack backoff and retry cap (10 seconds and 5 attempts by default)
     * put a single stale envelope's own retry chain at roughly 50 seconds;
     * this leaves margin against more than one envelope for the same id
     * still being queued behind it.
     */
    private static final Duration CLEANUP_CEILING = Duration.ofSeconds(120);
    private static final Duration CLEANUP_POLL_INTERVAL = Duration.ofMillis(500);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Picks this run's slot by reading the slot the previous run recorded
     * in a file under this module's build directory, advancing it by one,
     * and writing the new value back before anything else in this class
     * runs. The file survives an aborted or killed run, since it is
     * written here, before BASE_ID is ever used to seed or clean a row,
     * not at the end of a run; the next run to start reads whatever the
     * last run to reach this point already wrote and moves past it. Only
     * a build directory problem (the module was never compiled, or the
     * file cannot be read or written for some other reason) falls back to
     * a slot derived from the JVM's own clock, which is not guaranteed to
     * differ from whatever slot came before, but is different from a
     * fixed constant that would collide on every single run.
     */
    private static long nextRunSlot() {
        Path counterFile = Path.of("target", "governor-it-run-slot");
        try {
            Files.createDirectories(counterFile.getParent());
            long previousSlot = Files.exists(counterFile)
                    ? Long.parseLong(Files.readString(counterFile).trim())
                    : -1;
            long slot = Math.floorMod(previousSlot + 1, RUN_SLOT_COUNT);
            Files.writeString(counterFile, Long.toString(slot));
            return slot;
        } catch (IOException | NumberFormatException e) {
            return Math.floorMod(System.nanoTime(), RUN_SLOT_COUNT);
        }
    }

    @Autowired
    @Qualifier("targetJdbc")
    private JdbcTemplate targetJdbc;

    @Autowired
    @Qualifier("sourceJdbc")
    private JdbcTemplate sourceJdbc;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private CircuitBreaker vendorCircuitBreaker;

    @Autowired
    private ObjectStore objectStore;

    private static RestClient vendorAdminClient;
    private final List<Long> seededIds = new ArrayList<>();

    @BeforeAll
    static void setUpVendorAdminClient() {
        vendorAdminClient = RestClient.create("http://localhost:8088");
        vendorAdminClient.get().uri("/health").retrieve().toBodilessEntity();
    }

    @AfterEach
    void cleanUpAndRestoreSharedState() {
        // The row cleanup below must run even if restoring the shared
        // vendor/breaker/container state below fails or times out: that
        // shared state is irrelevant to whether this method leaves rows
        // behind for the next run to collide with, and a waitUntil
        // failure here must never be the reason a leftover row survives
        // to poison a later run the way an after-the-fact-only cleanup
        // would.
        try {
            setVendorMode("healthy");
            // The breaker and the listener containers are shared across
            // every test method in this class (one Spring context for the
            // whole class), so a test that drove the breaker OPEN must not
            // leave it, or a paused container, behind for the next test to
            // inherit.
            waitUntil(() -> vendorCircuitBreaker.getState() == CircuitBreaker.State.CLOSED, RECOVERY_TIMEOUT,
                    "the vendor circuit breaker never returned to CLOSED after resetting the vendor to healthy");
            waitUntil(
                    () -> registry.getListenerContainers().stream().noneMatch(MessageListenerContainer::isPauseRequested),
                    RECOVERY_TIMEOUT, "a Kafka listener container never had its pause request cleared after the "
                            + "breaker closed");
        } finally {
            cleanReservedRunRangeDeterministically();
        }
    }

    /**
     * Removes every row a seeded id could have left behind across every
     * table and store this class touches: migration_event, document, and
     * migration_state in the target Postgres, the source row in MySQL, and
     * its MinIO object, if any. Called only before a test seeds an id, so
     * a row an earlier aborted or forcibly killed run left behind, using
     * an id this run is about to insert into for the first time, never
     * collides with this run's own insert; BASE_ID moving on every run
     * (see nextRunSlot()) already keeps that collision from happening in
     * the ordinary case, so this is a second, cheap layer under that, not
     * the only thing standing between two runs. Deleting a row, or an
     * object, that never existed is a no-op on every one of these stores,
     * so this is safe to call unconditionally. Cleanup after a test runs,
     * in cleanReservedRunRangeDeterministically below, does not use this:
     * it deletes by range rather than by id, after first confirming
     * nothing can still redeliver into that range.
     */
    private void cleanRowsForId(long id) {
        targetJdbc.update("DELETE FROM migration_event WHERE source_id = ?", id);
        targetJdbc.update("DELETE FROM document WHERE source_id = ?", id);
        targetJdbc.update("DELETE FROM migration_state WHERE source_id = ?", id);
        sourceJdbc.update("DELETE FROM files WHERE id = ?", id);
        objectStore.delete(objectStore.keyFor(id));
    }

    /**
     * The deterministic replacement for deleting a seeded id's rows the
     * moment a test finishes. Order matters here: deleting a ledger or
     * source row while a backfill or cdc message naming it can still be
     * redelivered is exactly what turns an ordinary cleanup into a
     * manufactured "Source record no longer exists for a claimed id"
     * failure, since that redelivery would seed a fresh row, find nothing
     * in MySQL behind it, and count the resulting failure toward the
     * retry cap. Draining both lanes first, so nothing naming any id in
     * this run's range can still be in flight, then deleting by range, and
     * then confirming the range actually stays empty rather than trusting
     * a single pass, is the same three-step shape ReconcileIT's own
     * cleanup already uses for the identical reason.
     *
     * Deliberately not wrapped to force the delete through if the drain
     * itself times out: a lane that never drains means something is
     * genuinely still in flight, and deleting anyway is exactly the
     * unsafe move this method exists to avoid. Letting that failure
     * propagate leaves this run's range dirty, but BASE_ID moving on the
     * next run keeps that from colliding with anything, and
     * noRowsSurviveInThisRunsReservedIdRange still reports it plainly
     * once every test has run.
     */
    private void cleanReservedRunRangeDeterministically() {
        waitForLanesToDrain();
        deleteReservedRunRange();
        waitForReservedRunRangeToStayGone();
        for (Long id : seededIds) {
            objectStore.delete(objectStore.keyFor(id));
        }
        seededIds.clear();
    }

    /**
     * Waits until neither this class's private backfill nor cdc consumer
     * group has anything left uncommitted on its topic. A record that is
     * still bouncing through a nack-and-backoff cycle, or one that simply
     * has not been fetched yet, both show up here the same way: as lag
     * greater than zero, since only a successful acknowledge advances a
     * group's committed offset. Both topics carry only this class's own
     * traffic (see the class-level comment), so zero lag on each is a
     * direct answer to "can anything published during the test that just
     * finished still be redelivered," not an approximation of it.
     */
    private void waitForLanesToDrain() {
        waitUntil(() -> totalLag(BACKFILL_GROUP_ID, BACKFILL_TOPIC) == 0 && totalLag(CDC_GROUP_ID, CDC_TOPIC) == 0,
                LANE_DRAIN_TIMEOUT, "the backfill or cdc lane still had an uncommitted message on its topic; "
                        + "deleting this run's rows now would risk a redelivery reseeding one of them with "
                        + "nothing left in MySQL behind it");
    }

    private void deleteReservedRunRange() {
        targetJdbc.update("DELETE FROM migration_event WHERE source_id >= ? AND source_id < ?", BASE_ID,
                BASE_ID + RUN_SLOT_WIDTH);
        targetJdbc.update("DELETE FROM document WHERE source_id >= ? AND source_id < ?", BASE_ID,
                BASE_ID + RUN_SLOT_WIDTH);
        targetJdbc.update("DELETE FROM migration_state WHERE source_id >= ? AND source_id < ?", BASE_ID,
                BASE_ID + RUN_SLOT_WIDTH);
        // The MySQL source row is deleted last, and only once
        // waitForLanesToDrain already confirmed nothing can redeliver into
        // this range: a message naming one of these ids that is still in
        // flight when this row disappears is what manufactures the
        // structural "Source record no longer exists" failure in the
        // first place.
        sourceJdbc.update("DELETE FROM files WHERE id >= ? AND id < ?", BASE_ID, BASE_ID + RUN_SLOT_WIDTH);
    }

    private boolean reservedRunRangeHasRows() {
        Long ledgerRows = targetJdbc.queryForObject(
                "SELECT COUNT(*) FROM migration_state WHERE source_id >= ? AND source_id < ?", Long.class, BASE_ID,
                BASE_ID + RUN_SLOT_WIDTH);
        Long documentRows = targetJdbc.queryForObject(
                "SELECT COUNT(*) FROM document WHERE source_id >= ? AND source_id < ?", Long.class, BASE_ID,
                BASE_ID + RUN_SLOT_WIDTH);
        Long eventRows = targetJdbc.queryForObject(
                "SELECT COUNT(*) FROM migration_event WHERE source_id >= ? AND source_id < ?", Long.class, BASE_ID,
                BASE_ID + RUN_SLOT_WIDTH);
        Long sourceRows = sourceJdbc.queryForObject(
                "SELECT COUNT(*) FROM files WHERE id >= ? AND id < ?", Long.class, BASE_ID,
                BASE_ID + RUN_SLOT_WIDTH);
        return (ledgerRows != null && ledgerRows > 0) || (documentRows != null && documentRows > 0)
                || (eventRows != null && eventRows > 0) || (sourceRows != null && sourceRows > 0);
    }

    /**
     * Polls this run's reserved range until it has read empty for a full
     * CLEANUP_STABILITY_WINDOW, re-deleting immediately the moment a row
     * reappears and starting that window over. waitForLanesToDrain already
     * makes a reappearance here unlikely, but does not make it impossible,
     * since a message could still be mid-flight in a way lag alone does
     * not catch; this is the backstop for that gap, bounded by
     * CLEANUP_CEILING so a row that keeps coming back well past any
     * plausible redelivery fails this loudly instead of leaving the range
     * dirty for whatever test or run comes next.
     */
    private void waitForReservedRunRangeToStayGone() {
        Instant ceiling = Instant.now().plus(CLEANUP_CEILING);
        while (true) {
            Instant windowStart = Instant.now();
            boolean reappeared = false;
            while (Duration.between(windowStart, Instant.now()).compareTo(CLEANUP_STABILITY_WINDOW) < 0) {
                if (Instant.now().isAfter(ceiling)) {
                    throw new IllegalStateException("rows in this run's reserved id range [" + BASE_ID + ", "
                            + (BASE_ID + RUN_SLOT_WIDTH) + ") were still being recreated "
                            + CLEANUP_CEILING.getSeconds() + " seconds after cleanup started; a redelivery landed "
                            + "far later than expected, or something outside this test's own lanes is writing "
                            + "into its reserved range");
                }
                sleep(CLEANUP_POLL_INTERVAL);
                if (reservedRunRangeHasRows()) {
                    deleteReservedRunRange();
                    reappeared = true;
                    break;
                }
            }
            if (!reappeared) {
                return;
            }
        }
    }

    @AfterAll
    static void resetVendorEvenOnFailure() {
        setVendorMode("healthy");
    }

    /**
     * The suite-wide guarantee every per-test cleanup above exists to
     * uphold: once every test method in this class has run its own
     * cleanup, this run's slice of the reserved id range must hold zero
     * rows in every store this class ever writes to, not merely the ids a
     * passing test happened to check. A row surviving here means some
     * test's cleanup lost a race against a Kafka message still in flight
     * for one of its ids, and the id is the specific evidence of which
     * test and which store.
     */
    @AfterAll
    void noRowsSurviveInThisRunsReservedIdRange() {
        assertEquals(0, countInRunRange(targetJdbc, "migration_state", "source_id"),
                "migration_state must hold zero rows in this run's reserved id range once every test's cleanup "
                        + "has finished");
        assertEquals(0, countInRunRange(targetJdbc, "document", "source_id"),
                "document must hold zero rows in this run's reserved id range once every test's cleanup has "
                        + "finished");
        assertEquals(0, countInRunRange(targetJdbc, "migration_event", "source_id"),
                "migration_event must hold zero rows in this run's reserved id range once every test's cleanup "
                        + "has finished");
        assertEquals(0, countInRunRange(sourceJdbc, "files", "id"),
                "the MySQL files table must hold zero rows in this run's reserved id range once every test's "
                        + "cleanup has finished");
    }

    private long countInRunRange(JdbcTemplate jdbc, String table, String idColumn) {
        Long count = jdbc.queryForObject("SELECT count(*) FROM " + table + " WHERE " + idColumn + " >= ? AND "
                + idColumn + " < ?", Long.class, BASE_ID, BASE_ID + RUN_SLOT_WIDTH);
        return count == null ? 0 : count;
    }

    /**
     * The headline scenario: kill the vendor, watch the breaker trip and
     * both listener containers actually get told to pause while the
     * backlog behind them grows instead of being dropped, restore the
     * vendor, watch the breaker close, the containers get told to resume,
     * and every file reach DONE with zero loss and zero files wrongly
     * condemned to FAILED_PERMANENT along the way.
     */
    @Test
    void breakerOpensOnOutagePausesBothLanesThenClosesAndDrainsWithZeroLoss() throws Exception {
        long breakerOpenEventsBefore = countEvents("BREAKER_OPEN");
        List<Long> ids = seedBackfillFiles(20, 0);
        setVendorMode("down");

        publishBackfillMessages(ids, 2);

        waitUntil(() -> vendorCircuitBreaker.getState() == CircuitBreaker.State.OPEN, DETECTION_TIMEOUT,
                "the vendor circuit breaker never opened after the vendor went down");
        waitUntil(() -> registry.getListenerContainers().stream().allMatch(MessageListenerContainer::isPauseRequested),
                DETECTION_TIMEOUT, "not every Kafka listener container was actually told to pause after the "
                        + "breaker opened");
        assertTrue(countEvents("BREAKER_OPEN") > breakerOpenEventsBefore,
                "a BREAKER_OPEN event must be recorded for this outage specifically, not merely have existed "
                        + "already from some earlier run");

        // Simulate the backlog continuing to arrive while the vendor is
        // down: these ids are published only after both containers are
        // already confirmed told to pause, so nothing has any chance to
        // consume them before the lag measurement below.
        List<Long> extraIds = seedBackfillFiles(5, 100);
        int extraChunkSize = 2;
        long expectedExtraMessages = (extraIds.size() + extraChunkSize - 1) / extraChunkSize;
        long lagBeforeWait = totalLag("backfill-governor-it", "files.backfill.governor-it");
        publishBackfillMessages(extraIds, extraChunkSize);
        List<Long> allIds = new ArrayList<>(ids);
        allIds.addAll(extraIds);

        Thread.sleep(2_000);
        long lagAfterPublishingMore = totalLag("backfill-governor-it", "files.backfill.governor-it");
        // Lag is counted in Kafka offsets, i.e. messages, not source ids;
        // extraIds is chunked into expectedExtraMessages messages, so that
        // is the growth this asserts on, not the raw id count.
        assertTrue(lagAfterPublishingMore >= lagBeforeWait + expectedExtraMessages,
                "consumer lag must grow while paused, since a paused container fetches nothing to consume or "
                        + "drop; before=" + lagBeforeWait + " after=" + lagAfterPublishingMore
                        + " expectedExtraMessages=" + expectedExtraMessages);

        assertEquals(0, countStatus(allIds, "FAILED_PERMANENT"),
                "no id may be condemned to FAILED_PERMANENT purely because of a vendor outage");

        long breakerClosedEventsBefore = countEvents("BREAKER_CLOSED");
        setVendorMode("healthy");

        waitUntil(() -> vendorCircuitBreaker.getState() == CircuitBreaker.State.CLOSED, RECOVERY_TIMEOUT,
                "the vendor circuit breaker never closed once the vendor recovered");
        waitUntil(() -> registry.getListenerContainers().stream().noneMatch(MessageListenerContainer::isPauseRequested),
                RECOVERY_TIMEOUT, "the Kafka listener containers were never told to resume once the breaker closed");
        assertTrue(countEvents("BREAKER_CLOSED") > breakerClosedEventsBefore,
                "a BREAKER_CLOSED event must be recorded for this recovery specifically");

        waitUntil(() -> countStatus(allIds, "DONE") == allIds.size(), DETECTION_TIMEOUT,
                "every id must reach DONE once the vendor recovers and the backlog drains");
        assertEquals(0, countStatus(allIds, "FAILED_PERMANENT"), "zero files lost: none of them may end up "
                + "permanently failed once the outage is over");
    }

    /**
     * THE HEADLINE CLAIM, proven directly: an outage held open for well
     * longer than a single breaker open-duration window (open-duration-
     * seconds is 5 here, so 65 seconds is roughly 13 open/half-open/
     * reopen cycles) must never condemn a single file to FAILED_PERMANENT,
     * no matter how many times the breaker flaps. Status is polled
     * throughout the outage, not only checked once at the end, so a
     * file that was condemned partway through and only later noticed
     * cannot slip past this test. Before the fix this test exists to
     * prove, every reclaim during every flap burned one attempt against
     * the same counter the retry cap read, so an outage this long would
     * have converted every one of these ids to FAILED_PERMANENT well
     * before the vendor ever came back.
     */
    @Test
    void sustainedOutageLongerThanTheBreakerFlapWindowLosesZeroFiles() throws Exception {
        List<Long> ids = seedBackfillFiles(4, 300);
        setVendorMode("down");
        publishBackfillMessages(ids, 2);

        waitUntil(() -> vendorCircuitBreaker.getState() == CircuitBreaker.State.OPEN, DETECTION_TIMEOUT,
                "the vendor circuit breaker never opened after the vendor went down");

        Instant outageDeadline = Instant.now().plus(Duration.ofSeconds(65));
        int checks = 0;
        while (Instant.now().isBefore(outageDeadline)) {
            assertEquals(0, countStatus(ids, "FAILED_PERMANENT"),
                    "zero ids may be condemned to FAILED_PERMANENT while the outage is still ongoing, no matter "
                            + "how many times the breaker has already flapped open and closed");
            checks++;
            Thread.sleep(2_000);
        }
        assertTrue(checks >= 10, "the outage window must actually have been held open long enough for several "
                + "breaker flap cycles to occur; only checked " + checks + " times");

        setVendorMode("healthy");

        waitUntil(() -> vendorCircuitBreaker.getState() == CircuitBreaker.State.CLOSED, RECOVERY_TIMEOUT,
                "the vendor circuit breaker never closed once the vendor recovered");
        waitUntil(() -> countStatus(ids, "DONE") == ids.size(), DETECTION_TIMEOUT,
                "every id must reach DONE once the vendor recovers, even after an outage longer than several "
                        + "breaker flap cycles");
        assertEquals(0, countStatus(ids, "FAILED_PERMANENT"),
                "zero files lost: an outage of any length must never condemn a file through the retry cap");
    }

    /**
     * A file legitimately updated several times, every update succeeding,
     * must never be condemned by the retry cap, even once its lifetime
     * attempts count climbs past MAX_RETRY_ATTEMPTS (3 for this test
     * class) purely from ordinary successful reclaims. Before the fix
     * this test exists to prove, the cap read the lifetime attempts
     * column directly, so the fifth update here, which resets the row to
     * PENDING with attempts already at or past the cap, would have been
     * condemned to FAILED_PERMANENT on the spot with no diagnostic,
     * despite never having actually failed once.
     */
    @Test
    void aFileUpdatedRepeatedlyWithEverySuccessIsNeverCondemnedByTheRetryCap() {
        long id = BASE_ID + 200;
        seededIds.add(id);
        cleanRowsForId(id);
        insertSourceFile(id, "update-test.txt", "text/plain", "version 0".getBytes(StandardCharsets.UTF_8));

        publishCdcCreateEnvelope(id);
        waitUntil(() -> "DONE".equals(statusOf(id)), DETECTION_TIMEOUT,
                "source_id " + id + " never reached DONE after its initial create");

        for (int i = 1; i <= 5; i++) {
            byte[] content = ("version " + i).getBytes(StandardCharsets.UTF_8);
            sourceJdbc.update("UPDATE files SET content = ?, byte_size = ? WHERE id = ?", content, content.length,
                    id);
            publishCdcUpdateEnvelope(id, i);
            int round = i;
            waitUntil(() -> "DONE".equals(statusOf(id)) && ("VERSION " + round).equals(queryOcrTextOrNull(id)),
                    DETECTION_TIMEOUT, "source_id " + id + " never reached DONE reflecting update " + round);
        }

        Integer attempts = targetJdbc.queryForObject(
                "SELECT attempts FROM migration_state WHERE source_id = ?", Integer.class, id);
        Integer consecutiveFailures = targetJdbc.queryForObject(
                "SELECT consecutive_failures FROM migration_state WHERE source_id = ?", Integer.class, id);
        assertTrue(attempts > 3, "lifetime attempts must have climbed past the retry cap of 3 through ordinary "
                + "successful reclaims for this test to actually prove anything: attempts=" + attempts);
        assertEquals(0, consecutiveFailures, "a file that never actually failed must have zero consecutive "
                + "failures regardless of how high its lifetime attempts count climbs");
        assertEquals("DONE", statusOf(id), "a file updated repeatedly with every update succeeding must end "
                + "DONE, never condemned by the retry cap");
    }

    /**
     * A file the vendor will never be able to process, empty content
     * triggering UNPROCESSABLE_DOCUMENT, must be dead-lettered after
     * exactly one attempt rather than retried, must never look like a
     * vendor outage to the breaker, and must actually land a record on
     * the real files.dlq topic, not only in migration_event.
     */
    @Test
    void genuinelyUnprocessableFileGoesToFailedPermanentAfterOneAttemptWithoutTrippingTheBreaker() {
        long id = BASE_ID + 900;
        seededIds.add(id);
        cleanRowsForId(id);
        insertSourceFile(id, "empty.txt", "text/plain", new byte[0]);
        targetJdbc.update("INSERT INTO migration_state (source_id, lane, status) VALUES (?, ?, ?)",
                id, "backfill", "PENDING");
        setVendorMode("healthy");
        CircuitBreaker.State stateBefore = vendorCircuitBreaker.getState();

        publishBackfillMessages(List.of(id), 1);

        waitUntil(() -> "FAILED_PERMANENT".equals(statusOf(id)), DETECTION_TIMEOUT,
                "source_id " + id + " never reached FAILED_PERMANENT");
        assertEquals(1, targetJdbc.queryForObject(
                "SELECT attempts FROM migration_state WHERE source_id = ?", Integer.class, id),
                "an unprocessable document must fail on its first and only attempt");
        assertEquals(1, countEventsForId(id, "DLQ"));
        assertEquals(stateBefore, vendorCircuitBreaker.getState(),
                "a document the vendor rejects as unprocessable is not a vendor outage and must never move the "
                        + "breaker");

        Map<String, Object> dlqRecord = pollForDlqRecord(id, Duration.ofSeconds(15));
        assertEquals(id, ((Number) dlqRecord.get("sourceId")).longValue());
        assertEquals("backfill", dlqRecord.get("lane"));
        assertEquals("PERMANENT", dlqRecord.get("errorClass"));
        assertEquals("Vendor rejected this file", dlqRecord.get("lastError"));
    }

    /**
     * THE WEDGE. A ledger row for a source id whose MySQL row does not
     * exist can never succeed no matter how many times it is retried:
     * MigrationService marks it FAILED_RETRYABLE every time, and before
     * this fix, claim() reclaimed a FAILED_RETRYABLE row unconditionally
     * forever, so a CdcConsumer message for an id like this would nack
     * forever and block every other change on the same partition behind
     * it. This drives exactly that id through the real CdcConsumer and
     * asserts it reaches FAILED_PERMANENT, with a DLQ event, within
     * MAX_RETRY_ATTEMPTS rather than nacking forever.
     */
    @Test
    void wedgeIdWithNoMatchingSourceRowReachesFailedPermanentWithinTheRetryCapInsteadOfNackingForever() {
        long id = BASE_ID + 950;
        seededIds.add(id);
        cleanRowsForId(id);
        // Deliberately no insertSourceFile(id, ...): the MySQL row for
        // this id is never created, mirroring a row that has already been
        // deleted from the source by the time its change event is
        // processed.

        publishCdcCreateEnvelope(id);

        waitUntil(() -> "FAILED_PERMANENT".equals(statusOf(id)), DETECTION_TIMEOUT,
                "source_id " + id + " never reached FAILED_PERMANENT; it would otherwise nack forever and block "
                        + "every other change queued behind it on the same partition");
        assertEquals(1, countEventsForId(id, "DLQ"));
        Integer consecutiveFailures = targetJdbc.queryForObject(
                "SELECT consecutive_failures FROM migration_state WHERE source_id = ?", Integer.class, id);
        assertTrue(consecutiveFailures >= 3, "the id must have actually been retried up to the configured cap, "
                + "not condemned on the first attempt: consecutive_failures=" + consecutiveFailures);
    }

    /**
     * THE UPDATE WEDGE, the same class of defect as THE WEDGE above, but
     * for an op=u envelope rather than op=c. No source row exists for this
     * id, so every delivery of this single update envelope fails the same
     * structural way; a nack causes Kafka to redeliver the identical
     * envelope, carrying the identical version, and CdcConsumer resets
     * this row back to PENDING via resetForUpdate before every one of
     * those redeliveries is migrated again. Before this fix,
     * resetForUpdate cleared consecutive_failures unconditionally on every
     * one of those redeliveries, so this id could never accumulate enough
     * consecutive failures to reach the cap and would nack forever instead
     * of ever reaching FAILED_PERMANENT, even though the cap's own PENDING
     * branch exists specifically to catch a row reset by an update. A
     * single publish is enough here; CdcConsumer's own nack-and-retry loop
     * is what supplies the repeated redelivery.
     */
    @Test
    void wedgeUpdateEnvelopeWithNoMatchingSourceRowReachesFailedPermanentWithinTheRetryCapInsteadOfNackingForever() {
        long id = BASE_ID + 970;
        seededIds.add(id);
        cleanRowsForId(id);
        // Deliberately no insertSourceFile(id, ...): mirrors a source row
        // already gone by the time this update envelope is processed.

        publishCdcUpdateEnvelope(id, 1);

        waitUntil(() -> "FAILED_PERMANENT".equals(statusOf(id)), DETECTION_TIMEOUT,
                "source_id " + id + " never reached FAILED_PERMANENT; an update envelope for a row with no "
                        + "matching source id is redelivered with the same version on every nack, and would "
                        + "otherwise nack forever instead of ever accumulating enough consecutive failures to "
                        + "reach the cap");
        assertEquals(1, countEventsForId(id, "DLQ"));
        Integer consecutiveFailures = targetJdbc.queryForObject(
                "SELECT consecutive_failures FROM migration_state WHERE source_id = ?", Integer.class, id);
        assertTrue(consecutiveFailures >= 3, "the id must have actually been retried up to the configured cap, "
                + "not condemned on the first attempt: consecutive_failures=" + consecutiveFailures);
    }

    private List<Long> seedBackfillFiles(int count, long offset) {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long id = BASE_ID + offset + i;
            ids.add(id);
            seededIds.add(id);
            // Idempotent seeding: BASE_ID moving on every run keeps a
            // fresh run from ever choosing an id a prior run is still
            // using, but a run whose own slot wrapped back around to one
            // used RUN_SLOT_COUNT runs ago, and was killed before its own
            // @AfterEach ran, could still have left a row behind on this
            // exact id. Without this, the INSERT below would fail with a
            // duplicate key on migration_state instead of this test ever
            // getting to run.
            cleanRowsForId(id);
            insertSourceFile(id, "outage-" + id + ".txt", "text/plain",
                    ("GOVERNOR IT " + id).getBytes(StandardCharsets.UTF_8));
            targetJdbc.update("INSERT INTO migration_state (source_id, lane, status) VALUES (?, ?, ?)",
                    id, "backfill", "PENDING");
        }
        return ids;
    }

    private void publishBackfillMessages(List<Long> ids, int chunkSize) {
        for (int i = 0; i < ids.size(); i += chunkSize) {
            List<Long> chunk = ids.subList(i, Math.min(i + chunkSize, ids.size()));
            String payload = writeJson(new BackfillMessage("backfill", chunk));
            kafkaTemplate.send("files.backfill.governor-it", String.valueOf(chunk.get(0)), payload);
        }
    }

    private void publishCdcCreateEnvelope(long id) {
        String payload = "{\"op\":\"c\",\"before\":null,\"after\":{\"id\":" + id + ",\"filename\":\"wedge.txt\","
                + "\"content_type\":\"text/plain\",\"byte_size\":5},\"ts_ms\":" + System.currentTimeMillis() + "}";
        kafkaTemplate.send("cdc.sourcedb.files.governor-it", String.valueOf(id), payload);
    }

    private void publishCdcUpdateEnvelope(long id, long version) {
        String payload = "{\"op\":\"u\",\"before\":null,\"after\":{\"id\":" + id + ",\"filename\":\"update-test.txt\","
                + "\"content_type\":\"text/plain\",\"byte_size\":5},\"ts_ms\":" + version + "}";
        kafkaTemplate.send("cdc.sourcedb.files.governor-it", String.valueOf(id), payload);
    }

    private void insertSourceFile(long id, String filename, String contentType, byte[] content) {
        sourceJdbc.update("INSERT INTO files (id, filename, content_type, content, byte_size) "
                + "VALUES (?, ?, ?, ?, ?)", id, filename, contentType, content, content.length);
    }

    private String statusOf(long id) {
        List<String> statuses = targetJdbc.queryForList(
                "SELECT status FROM migration_state WHERE source_id = ?", String.class, id);
        return statuses.isEmpty() ? null : statuses.get(0);
    }

    private String queryOcrTextOrNull(long id) {
        List<String> texts = targetJdbc.queryForList(
                "SELECT ocr_text FROM document WHERE source_id = ?", String.class, id);
        return texts.isEmpty() ? null : texts.get(0);
    }

    private long countStatus(List<Long> ids, String status) {
        if (ids.isEmpty()) {
            return 0;
        }
        String placeholders = String.join(",", ids.stream().map(id -> "?").toList());
        List<Object> params = new ArrayList<>();
        params.add(status);
        params.addAll(ids);
        Long count = targetJdbc.queryForObject(
                "SELECT count(*) FROM migration_state WHERE status = ? AND source_id IN (" + placeholders + ")",
                Long.class, params.toArray());
        return count == null ? 0 : count;
    }

    private long countEvents(String stage) {
        Long count = targetJdbc.queryForObject(
                "SELECT count(*) FROM migration_event WHERE stage = ?", Long.class, stage);
        return count == null ? 0 : count;
    }

    private long countEventsForId(long id, String stage) {
        Long count = targetJdbc.queryForObject(
                "SELECT count(*) FROM migration_event WHERE source_id = ? AND stage = ?", Long.class, id, stage);
        return count == null ? 0 : count;
    }

    /**
     * Sum, across every partition of topic, of how far the end of the log
     * is ahead of what groupId has committed; an id with no committed
     * offset yet counts from the start of the log, exactly the backlog
     * BreakerListener pausing is meant to protect. Uses a throwaway
     * consumer and the admin client only to read positions, never to join
     * groupId itself.
     */
    private long totalLag(String groupId, String topic) {
        Map<String, Object> adminProps = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (Admin admin = Admin.create(adminProps);
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

            long lag = 0;
            for (TopicPartition partition : partitions) {
                long endOffset = endOffsets.getOrDefault(partition, 0L);
                org.apache.kafka.clients.consumer.OffsetAndMetadata committedOffset = committed.get(partition);
                long consumedUpTo = committedOffset == null ? 0L : committedOffset.offset();
                lag += Math.max(0, endOffset - consumedUpTo);
            }
            return lag;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to compute consumer lag for group " + groupId, e);
        }
    }

    /**
     * Polls the real files.dlq topic (private to this test class) from
     * its beginning, under a fresh, disposable consumer group, until a
     * record whose sourceId field matches id arrives, and returns its
     * payload parsed as a map. This is what proves DlqPublisher actually
     * publishes to the real topic, not only that Governor.deadLetter
     * delegates to it, which GovernorTest already covers with a fake.
     */
    private Map<String, Object> pollForDlqRecord(long id, Duration timeout) {
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.GROUP_ID_CONFIG, "governor-it-dlq-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of("files.dlq.governor-it"));
            Instant deadline = Instant.now().plus(timeout);
            while (Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> payload = OBJECT_MAPPER.readValue(record.value(), Map.class);
                    if (payload.get("sourceId") != null && ((Number) payload.get("sourceId")).longValue() == id) {
                        return payload;
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read a DLQ record for source id " + id, e);
        }
        throw new AssertionError("No DLQ record for source id " + id + " arrived on files.dlq.governor-it within "
                + timeout);
    }

    private static String writeJson(BackfillMessage message) {
        try {
            return OBJECT_MAPPER.writeValueAsString(message);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize a BackfillMessage", e);
        }
    }

    private static void setVendorMode(String mode) {
        vendorAdminClient.post().uri("/admin/mode").body(Map.of("mode", mode)).retrieve().toBodilessEntity();
    }

    private static void waitUntil(BooleanSupplier condition, Duration timeout, String timeoutMessage) {
        Instant deadline = Instant.now().plus(timeout);
        do {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep(POLL_INTERVAL);
        } while (Instant.now().isBefore(deadline));
        throw new AssertionError(timeoutMessage + " within " + timeout);
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
