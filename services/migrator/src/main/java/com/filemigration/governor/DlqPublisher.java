package com.filemigration.governor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Publishes one record per permanently failed id to the files.dlq topic, so
 * a file that will never be retried again is visible somewhere a nack loop
 * can never surface it: migration_event already carries the same DLQ stage,
 * this exists for whatever downstream consumer wants to act on a dead
 * letter without polling Postgres for it.
 */
@Component
public class DlqPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public DlqPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper,
            @Value("${migrator.dlq.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    /**
     * Keyed by source id so every dead letter for the same file lands on
     * the same partition, the same reasoning the other topics in this
     * system key on an id for.
     */
    public void publish(long sourceId, String lane, String errorClass, int attempts, String lastError) {
        kafkaTemplate.send(topic, String.valueOf(sourceId), writePayload(sourceId, lane, errorClass, attempts,
                lastError));
    }

    private String writePayload(long sourceId, String lane, String errorClass, int attempts, String lastError) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("sourceId", sourceId);
        payload.put("lane", lane);
        payload.put("errorClass", errorClass);
        payload.put("attempts", attempts);
        payload.put("lastError", lastError);
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize a DLQ payload for source id " + sourceId, e);
        }
    }
}
