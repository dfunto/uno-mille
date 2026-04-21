package com.ebay.challenge.streamprocessor.state;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks watermarks per partition to handle out-of-order events.
 * Watermark represents the point in event-time up to which we believe we have seen all events.
 * Events arriving with event_time < watermark - allowedLateness are considered too late.
 * Keys are "{topic}:{partition}"
 */
@Slf4j
@Component
public class WatermarkTracker {

    @Getter
    private final Duration allowedLateness;
    private final ConcurrentHashMap<String, Instant> watermarks = new ConcurrentHashMap<>();

    public WatermarkTracker(@Value("${watermark.allowed-lateness-minutes:15}") int allowedLatenessMinutes) {
        this.allowedLateness = Duration.ofMinutes(allowedLatenessMinutes);
        log.info("Initialized WatermarkTracker with allowed lateness: {} minutes", allowedLatenessMinutes);
    }

    /**
     * Update watermark for a partition based on observed event time.
     * Watermark advances monotonically (never goes backward).
     * Watermark advancement:
     * - Update partition watermark if event time is later than current watermark
     * - Ensure watermark never goes backward
     * - Handle concurrent updates
     *
     * @param key watermark key in {topic}:{partition} format
     * @param eventTime the event timestamp
     */
    public void updateWatermark(String key, Instant eventTime) {
        log.debug("Updating watermark for key {} with event time {}", key, eventTime);
        watermarks.merge(
            key,
            eventTime,
            (current, incoming) -> incoming.isAfter(current) ? incoming : current
        );
    }

    /**
     * Get current watermark for a partition.
     * Watermark retrieval:
     * - Return current watermark for the partition
     * - Return Instant.MIN if partition has no watermark yet
     *
     * @param key watermark key in {topic}:{partition} format
     * @return the current watermark, or Instant.MIN if not yet initialized
     */
    public Instant getWatermark(String key) {
        return watermarks.getOrDefault(key, Instant.MIN);
    }

    /**
     * Check if an event is too late (beyond allowed lateness).
     * Late event detection
     * - Calculate cutoff time as: watermark - allowedLateness
     * - Return true if event is before cutoff time
     * - Handle case when watermark is not yet initialized
     *
     * @param key watermark key in {topic}:{partition} format
     * @param eventTime the event timestamp
     * @return true if the event is too late and should be dropped
     */
    public boolean isTooLate(String key, Instant eventTime) {
        Instant watermark = getWatermark(key);
        return !watermark.equals(Instant.MIN) && eventTime.isBefore(watermark.minus(allowedLateness));
    }


    /**
     * Find the minimum watermark across all partitions for a given topic.
     * Used for state eviction: the minimum watermark represents the oldest event time
     * we still need to retain state for, ensuring no partition is evicted ahead of others.
     *
     * @param topic the topic name (e.g. "ad_clicks", "page_views")
     * @return the minimum watermark across all partitions, or Instant.MIN if none found
     */
    public Instant findMinWatermark(String topic){
        return watermarks.entrySet().stream()
                .filter(e -> e.getKey().startsWith(topic + ":"))
                .map(Map.Entry::getValue)
                .min(Comparator.naturalOrder())
                .orElse(Instant.MIN);
    }
}
