package com.filemigration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.test.context.ActiveProfiles;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Confirms the real @KafkaListener annotation on CdcConsumer registered a
 * container bound to the topic and group id application.yml configures,
 * running with the configured concurrency and acknowledging manually
 * rather than on a fixed schedule. None of that is visible to a test that
 * only calls CdcConsumer's consume() method directly: a typo in
 * migrator.cdc.topic, migrator.cdc.group-id, or spring.kafka.listener.ack-mode
 * would leave the annotation wired to the wrong thing while every such
 * test kept passing, since consume() itself never changes. Loading the
 * real Spring context under the worker profile is what actually exercises
 * the annotation.
 *
 * The Kafka bootstrap address is overridden to a loopback address that
 * resolves with nothing listening on it, the same way MigratorApplicationTests
 * does, since confirming this registration needs no running broker: a
 * listener container is fully configured, including its ack mode, before
 * it ever tries to connect, and connecting happens later on its own
 * background thread rather than blocking context startup.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=localhost:19092",
        "spring.kafka.admin.operation-timeout=2s"
})
@ActiveProfiles("worker")
class CdcListenerWiringTest {

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Value("${migrator.cdc.topic}")
    private String expectedTopic;

    @Value("${migrator.cdc.group-id}")
    private String expectedGroupId;

    @Value("${migrator.worker-concurrency}")
    private int expectedConcurrency;

    @SuppressWarnings("rawtypes")
    @Test
    void cdcListenerIsBoundToTheConfiguredTopicGroupConcurrencyAndAckMode() {
        Optional<ConcurrentMessageListenerContainer> cdcContainer = registry.getListenerContainers().stream()
                .filter(ConcurrentMessageListenerContainer.class::isInstance)
                .map(ConcurrentMessageListenerContainer.class::cast)
                .filter(container -> expectedGroupId.equals(container.getGroupId()))
                .findFirst();

        assertTrue(cdcContainer.isPresent(),
                "no registered listener container has group id '" + expectedGroupId + "'; is CdcConsumer's "
                        + "@KafkaListener still active under the worker profile?");
        ConcurrentMessageListenerContainer container = cdcContainer.get();

        assertTrue(Arrays.asList(container.getContainerProperties().getTopics()).contains(expectedTopic),
                "the '" + expectedGroupId + "' listener container is not bound to topic '" + expectedTopic + "'");
        assertEquals(expectedConcurrency, container.getConcurrency(),
                "the CDC listener must run with migrator.worker-concurrency, not a hardcoded thread count");
        assertEquals(ContainerProperties.AckMode.MANUAL_IMMEDIATE, container.getContainerProperties().getAckMode(),
                "the CDC listener must acknowledge manually and immediately, matching the nack-with-backoff "
                        + "retry decision CdcConsumer itself makes, not commit on a fixed schedule");
    }
}
