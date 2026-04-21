package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
@Component
public class PageViewStore {

    // UserId : Sorted set of page views (sorted by event time ascending)
    private final ConcurrentHashMap<String, TreeSet<PageViewEvent>> state = new ConcurrentHashMap<>();

    public void addPageView(PageViewEvent pageView) {
        log.debug("Adding page view {} for user {}", pageView.getEventId(), pageView.getUserId());
        TreeSet<PageViewEvent> pageViewEvents = state.computeIfAbsent(
                pageView.getUserId(),
                k -> new TreeSet<>(Comparator.comparing(PageViewEvent::getEventTime))
        );
        synchronized (pageViewEvents) {
            pageViewEvents.add(pageView);
        }
    }

    public List<PageViewEvent> findUserPageViews(String userId, Instant windowStart, Instant cutoffTime){
        log.debug("Looking for page views for user {}", userId);
        TreeSet<PageViewEvent> userPages = state.get(userId);
        if (userPages == null)
            return Collections.emptyList();

        // Click must be BEFORE the page view, and within the window:
        // page view must be in (windowStart, cutoffTime)
        PageViewEvent lowerBound = PageViewEvent.builder().eventTime(windowStart).build();
        PageViewEvent upperBound = PageViewEvent.builder().eventTime(cutoffTime).build();

        synchronized(userPages) {
            NavigableSet<PageViewEvent> pageViews = userPages.subSet(
                lowerBound, false,
                upperBound, true
            );
            return new ArrayList<>(pageViews);
        }
    }

    /**
     * Evict old pages that are beyond the retention window.
     * Prevents unbounded memory growth.
     * @param cutoffTime pages older than this time should be evicted
     * @return number of pages evicted
     */
    public int evictOldPages(Instant cutoffTime) {
        log.debug("Evicting pages older than {}", cutoffTime);

        PageViewEvent cutoff = PageViewEvent.builder().eventTime(cutoffTime).build();
        int counter = 0;
        for (Map.Entry<String, TreeSet<PageViewEvent>> entry : state.entrySet()) {
            TreeSet<PageViewEvent> pages = entry.getValue();
            synchronized (pages) {
                NavigableSet<PageViewEvent> old = pages.headSet(cutoff, false);
                counter += old.size();
                old.clear();
            }
        }
        return counter;
    }
}
