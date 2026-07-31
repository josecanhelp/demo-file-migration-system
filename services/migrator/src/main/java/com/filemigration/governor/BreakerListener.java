package com.filemigration.governor;

import com.filemigration.model.Stage;
import com.filemigration.store.EventRepository;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Reacts to the vendor circuit breaker's state by pausing or resuming every
 * Kafka listener container, both the backfill and the cdc lane, rather than
 * letting either one keep polling into a vendor that has already tripped
 * the breaker. Pausing, instead of letting each poll fail and get nacked,
 * is what keeps offsets from advancing during an outage: a paused container
 * simply stops fetching, so the backlog behind it grows on the broker
 * instead of being repeatedly claimed, failed, and nacked for no purpose
 * while the vendor is known to be down.
 *
 * Also resumes on the breaker's move to HALF_OPEN, not only on CLOSED: the
 * breaker can only discover the vendor has recovered by actually letting a
 * handful of trial calls through, and none of the paused containers can
 * offer it one while they stay paused. Resuming on HALF_OPEN is only that,
 * letting a handful of calls through to see what happens, not a
 * declaration that the vendor is healthy again; if those calls fail, the
 * breaker reopens and this listener pauses again exactly as it would from
 * a first outage.
 *
 * Registered only under the worker profile, since it exists to act on
 * listener containers that only run there; the coordinator profile never
 * registers a KafkaListener for either lane.
 */
@Component
@Profile("worker")
public class BreakerListener {

    private static final Logger log = LoggerFactory.getLogger(BreakerListener.class);

    private final KafkaListenerEndpointRegistry registry;
    private final EventRepository eventRepo;

    public BreakerListener(CircuitBreaker vendorCircuitBreaker, KafkaListenerEndpointRegistry registry,
            EventRepository eventRepo) {
        this.registry = registry;
        this.eventRepo = eventRepo;
        vendorCircuitBreaker.getEventPublisher().onStateTransition(event -> {
            CircuitBreaker.State toState = event.getStateTransition().getToState();
            if (toState == CircuitBreaker.State.OPEN) {
                onOpen();
            } else if (toState == CircuitBreaker.State.HALF_OPEN) {
                resumeListeners("half-open, letting trial call(s) through");
            } else if (toState == CircuitBreaker.State.CLOSED) {
                resumeListeners("closed");
                eventRepo.record(null, Stage.BREAKER_CLOSED, null, null);
            }
        });
    }

    private void onOpen() {
        int count = 0;
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            container.pause();
            count++;
        }
        log.warn("Vendor circuit breaker opened; requested pause on {} Kafka listener container(s)", count);
        eventRepo.record(null, Stage.BREAKER_OPEN, null, null);
    }

    /**
     * Calls resume() unconditionally rather than checking isContainerPaused()
     * first: for a ConcurrentMessageListenerContainer, that flag only
     * reports true once every one of its child threads has individually
     * registered as paused, which never happens for a thread with no
     * partition assigned (concurrency configured higher than the topic's
     * partition count leaves such threads sitting idle). Gating resume()
     * on that flag left an idle thread's container permanently stuck
     * paused the one time this was driven through worker-concurrency
     * greater than one against a live broker: pause() had genuinely been
     * called, but resume() was then skipped forever because
     * isContainerPaused() never read true. resume() on an already-running
     * container is documented as a no-op, so calling it unconditionally
     * here is always safe.
     */
    private void resumeListeners(String reason) {
        int count = 0;
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            container.resume();
            count++;
        }
        log.info("Vendor circuit breaker {}; requested resume on {} Kafka listener container(s)", reason, count);
    }
}
