package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores ad click events partitioned by user_id for efficient windowed joins.
 * Thread-safe implementation with per-user locking for fine-grained concurrency.
 * Implements state eviction to prevent unbounded memory growth.
 */
@Slf4j
@Component
public class ClickStateStore {

    // UserId : Sorted set of clicks (sorted by event time ascending)
    private final ConcurrentHashMap<String, TreeSet<AdClickEvent>> state = new ConcurrentHashMap<>();

    /**
     * Add a click event to the state store.
     * Click storage (thread-safe)
     * - Use locks for thread safety
     * - Store clicks sorted by event time (most recent first)
     * - Handle concurrent access properly
     *
     * @param click the ad click event
     */
    public void addClick(AdClickEvent click) {
        log.debug("Adding click {} for user {}", click.getClickId(), click.getUserId());
        TreeSet<AdClickEvent> userEvents = state.computeIfAbsent(
            click.getUserId(),
            k -> new TreeSet<>(Comparator.comparing(AdClickEvent::getEventTime))
        );
        synchronized (userEvents) {
            userEvents.add(click);
        }
    }

    /**
     * Find the most recent click for a user within the attribution window.
     * Attribution logic:
     * - Search for clicks in window: [pageViewTime - 30 minutes, pageViewTime]
     * - Return the most recent click within the window
     * - Return null if no click found
     *
     * @param userId the user ID
     * @param pageViewTime the page view event time
     * @return the most recent click within 30 minutes before the page view, or null if none found
     */
    public Optional<AdClickEvent> findAttributableClick(String userId, Instant pageViewTime, Duration attributionWindow) {
        log.debug("Finding attributable click for {} at time {}", userId, pageViewTime);

        TreeSet<AdClickEvent> userEvents = this.state.get(userId);
        if (userEvents == null)
            return Optional.empty();

        Instant windowStart = pageViewTime.minus(attributionWindow);
        AdClickEvent lowerBound = AdClickEvent.builder().eventTime(windowStart).build();
        AdClickEvent upperBound = AdClickEvent.builder().eventTime(pageViewTime).build();

        synchronized (userEvents){
            NavigableSet<AdClickEvent> windowClicks = userEvents.subSet(
                    lowerBound, true,
                    upperBound,true
            );
            return windowClicks.isEmpty() ? Optional.empty() : Optional.of(windowClicks.last());
        }
    }

    /**
     * Evict old clicks that are beyond the retention window.
     * Prevents unbounded memory growth.
     * State eviction:
     * - Remove clicks older than the cutoff time
     * - Clean up empty user entries
     * - Return count of evicted clicks
     *
     * @param cutoffTime clicks older than this time should be evicted
     * @return number of clicks evicted
     */
    public int evictOldClicks(Instant cutoffTime) {
        log.debug("Evicting clicks older than {}", cutoffTime);

        AdClickEvent cutoff = AdClickEvent.builder().eventTime(cutoffTime).build();
        int counter = 0;
        for (Map.Entry<String, TreeSet<AdClickEvent>> entry : state.entrySet()) {
            TreeSet<AdClickEvent> clicks = entry.getValue();
            synchronized (clicks) {
                NavigableSet<AdClickEvent> old = clicks.headSet(cutoff, false);
                counter += old.size();
                old.clear();
            }
        }
        return counter;
    }
}
