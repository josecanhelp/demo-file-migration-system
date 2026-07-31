package com.filemigration.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Overrides the default listener container factory so a batch this
 * process cannot yet finish is retried until it can be, rather than given
 * up on after a handful of attempts. Spring Kafka's own default error
 * handling redelivers a failed record only a few times before logging it
 * as abandoned and moving on, which would quietly defeat the whole point
 * of manual acknowledgment: a batch left unresolved because it is still
 * owned by an earlier attempt's claim lease needs to keep coming back
 * until that lease has actually expired and the batch can be finished for
 * real, which can take longer than a few quick retries.
 */
@Configuration
@Profile("worker")
public class KafkaConsumerConfig {

    private static final long RETRY_INTERVAL_MS = 1000L;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(
                new DefaultErrorHandler(new FixedBackOff(RETRY_INTERVAL_MS, FixedBackOff.UNLIMITED_ATTEMPTS)));
        return factory;
    }
}
