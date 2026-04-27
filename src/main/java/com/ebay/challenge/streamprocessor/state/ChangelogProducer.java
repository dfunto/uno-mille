package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.ChangelogEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Writes state-change events to Kafka changelog topics for crash recovery.
 * Each event is serialized to JSON and sent to a changelog topic.
 * Changelog topics use delete-based retention (no compaction), so all events
 * within the retention window are preserved for full state rebuild on restart.
 *
 * @see ChangelogReplayer
 * @see ChangelogEvent
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ChangelogProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Write an event to its changelog topic.
     *
     * @param event the event to persist; must implement {@link ChangelogEvent}
     * @throws RuntimeException if JSON serialization fails
     */
    public void write(ChangelogEvent event) {
        String key = event.getChangelogKey();
        String topic = event.getChangelogTopic();
        try {
            String value = objectMapper.writeValueAsString(event);
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic,
                    event.getPartition(),
                    key,
                    value
            );
            kafkaTemplate.send(record);
            log.debug("Wrote {} to changelog topic {} partition {}", key, topic, event.getPartition());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event for changelog", e);
        }
    }
}
