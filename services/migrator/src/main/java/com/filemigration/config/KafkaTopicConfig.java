package com.filemigration.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the backfill, CDC, and dead-letter topics so a fresh cluster has
 * them the moment anything starts, rather than depending on a manual
 * creation step or on broker auto-creation choosing whatever partition
 * count the broker happens to default to. Registered under both the worker
 * and the coordinator profile since either one might be the first thing to
 * come up against a brand new broker, and creating a topic that already
 * exists with the same configuration is a no-op.
 *
 * Giving the CDC and dead-letter topics more than one partition matters for
 * the same reason the backfill topic has several: a single row that cannot
 * be resolved only holds up the one partition it lands on, not every other
 * row's events waiting behind it.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic filesBackfillTopic(
            @Value("${migrator.backfill.topic}") String topic,
            @Value("${migrator.backfill.topic-partitions}") int partitions) {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic cdcFilesTopic(
            @Value("${migrator.cdc.topic}") String topic,
            @Value("${migrator.cdc.topic-partitions}") int partitions) {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic filesDlqTopic(
            @Value("${migrator.dlq.topic}") String topic,
            @Value("${migrator.dlq.topic-partitions}") int partitions) {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(1)
                .build();
    }
}
