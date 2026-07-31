package com.filemigration.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the backfill topic so a fresh cluster has it the moment
 * anything starts, rather than depending on a manual creation step or on
 * broker auto-creation choosing whatever partition count the broker
 * happens to default to. Registered under both the worker and the
 * coordinator profile since either one might be the first thing to come
 * up against a brand new broker, and creating a topic that already exists
 * with the same configuration is a no-op.
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
}
