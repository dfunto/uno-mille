package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.ChangelogEvent;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Rebuilds in-memory state from Kafka changelog topics on application startup.
 * Implements SmartLifecycle with the earliest phase (Integer.MIN_VALUE)
 * to ensure state is fully restored before the main KafkaListener consumers
 * start processing new events. The listeners are configured with autoStartup = false
 * and are started by this component after replay completes.
 * Replay flow:
 * 1. Create a temporary KafkaConsumer with manual partition assignment (no group ID)
 * 2. Read each changelog topic from beginning to end
 * 3. Deserialize events and populate ClickStateStore / PageViewStore directly
 * 4. Start all registered KafkaListener containers
 * Because events are written directly to the stores (bypassing JoinEngine),
 * no changelog writes occur during replay — avoiding duplicate records.
 *
 * @see ChangelogProducer
 */
@Slf4j
@Component
public class ChangelogReplayer implements SmartLifecycle {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(2);

    private final ClickStateStore clickStore;
    private final PageViewStore pageViewStore;
    private final ObjectMapper objectMapper;
    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final String bootstrapServers;
    private final String clicksTopic;
    private final String pageViewsTopic;

    private volatile boolean running = false;

    public ChangelogReplayer(ClickStateStore clickStore,
                             PageViewStore pageViewStore,
                             ObjectMapper objectMapper,
                             KafkaListenerEndpointRegistry listenerRegistry,
                             @Value("${kafka.bootstrap-servers:localhost:29092}") String bootstrapServers,
                             @Value("${kafka.topics.ad-clicks:ad_clicks}") String clicksTopic,
                             @Value("${kafka.topics.page-views:page_views}") String pageViewsTopic) {
        this.clickStore = clickStore;
        this.pageViewStore = pageViewStore;
        this.objectMapper = objectMapper;
        this.listenerRegistry = listenerRegistry;
        this.bootstrapServers = bootstrapServers;
        this.clicksTopic = clicksTopic + ChangelogEvent.CHANGELOG_SUFFIX;
        this.pageViewsTopic = pageViewsTopic + ChangelogEvent.CHANGELOG_SUFFIX;
    }

    @Override
    public void start() {
        log.info("Starting changelog replay...");
        running = true;

        try (KafkaConsumer<String, String> consumer = createConsumer()) {
            replayTopic(consumer, clicksTopic, AdClickEvent.class);
        }

        try (KafkaConsumer<String, String> consumer = createConsumer()) {
            replayTopic(consumer, pageViewsTopic, PageViewEvent.class);
        }

        log.info("Changelog replay complete. Starting main consumers...");
        listenerRegistry.getListenerContainerIds().forEach(id -> {
            log.info("Starting listener container: {}", id);
            listenerRegistry.getListenerContainer(id).start();
        });
    }

    /**
     * Replay a single changelog topic into the appropriate state store.
     * Reads all records from offset 0 up to the end offsets captured at the start of replay.
     *
     * @param consumer  the Kafka consumer (already created, not yet assigned)
     * @param topic     the changelog topic name
     * @param eventType the event class to deserialize into
     */
    private <T> void replayTopic(KafkaConsumer<String, String> consumer, String topic, Class<T> eventType) {
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        if (partitions == null || partitions.isEmpty()) {
            log.warn("Changelog topic {} does not exist or has no partitions, skipping replay", topic);
            return;
        }

        List<TopicPartition> topicPartitions = partitions.stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .collect(Collectors.toList());

        consumer.assign(topicPartitions);
        consumer.seekToBeginning(topicPartitions);

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
        boolean allCaughtUp = endOffsets.values().stream().allMatch(offset -> offset == 0);
        if (allCaughtUp) {
            log.info("Changelog topic {} is empty, nothing to replay", topic);
            return;
        }

        int count = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
            if (records.isEmpty()) {
                break;
            }
            for (ConsumerRecord<String, String> record : records) {
                try {
                    T event = objectMapper.readValue(record.value(), eventType);
                    String sourceTopic = topic.endsWith(ChangelogEvent.CHANGELOG_SUFFIX)
                            ? topic.substring(0, topic.length() - ChangelogEvent.CHANGELOG_SUFFIX.length())
                            : topic;
                    if (event instanceof AdClickEvent click) {
                        click.setTopic(sourceTopic);
                        click.setPartition(record.partition());
                        click.setOffset(record.offset());
                        clickStore.addClick(click);
                    } else if (event instanceof PageViewEvent pageView) {
                        pageView.setTopic(sourceTopic);
                        pageView.setPartition(record.partition());
                        pageView.setOffset(record.offset());
                        pageViewStore.addPageView(pageView);
                    }
                    count++;
                } catch (Exception e) {
                    log.error("Failed to replay record from {} partition {} offset {}: {}",
                            topic, record.partition(), record.offset(), e.getMessage());
                }
            }

            // Check if we've reached the end offsets
            boolean done = topicPartitions.stream().allMatch(tp ->
                    consumer.position(tp) >= endOffsets.get(tp));
            if (done) {
                break;
            }
        }

        log.info("Replayed {} events from {}", count, topic);
    }

    /**
     * Create a short-lived Kafka consumer for changelog replay.
     * Uses no group ID (manual partition assignment) to avoid interfering
     * with the main consumer group's offsets.
     */
    private KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        return new KafkaConsumer<>(props);
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE;
    }

}
