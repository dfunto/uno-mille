package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.ChangelogEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ChangelogProducer {

    private static final String CHANGELOG_SUFFIX = "-changelog";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void write(ChangelogEvent event) {
        String key = event.getChangelogKey();
        String topic = event.getSourceTopic() + CHANGELOG_SUFFIX;
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
