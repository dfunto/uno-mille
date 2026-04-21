package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.sink.SqliteSink;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.PageViewStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Core join engine that performs windowed attribution joins between page views and ad clicks.
 * Join semantics:
 * - For each page_view, find the most recent ad_click for the same user
 *   within 30 minutes before the page view (in event time)
 * - Handle out-of-order arrivals through watermark tracking
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JoinEngine {

    static final Duration ATTRIBUTION_WINDOW = Duration.ofMinutes(30);

    private final ClickStateStore clickStore;
    private final PageViewStore pageStore;
    private final WatermarkTracker watermarkTracker;
    private final SqliteSink outputSink;

    /**
     * Process an ad click event.
     * Store the click in state for future attribution.
     * Processing logic:
     * - Check if event is too late using watermarkTracker
     * - Store the click in clickStore
     * - Update watermark for the partition
     * - Trigger reprocess of buffered pages for user
     *
     * @param click the ad click event
     */
    public void processClick(AdClickEvent click) {
        log.debug("Processing click: {}", click.getClickId());
        boolean isEventLate = watermarkTracker.isTooLate(click.getWatermarkKey(), click.getEventTime());
        if (isEventLate) {
            // # TODO Implement dead letter queue?
            log.warn("Click event: {} is too late, dropping.", click.getClickId());
            return;
        }
        clickStore.addClick(click);
        watermarkTracker.updateWatermark(click.getWatermarkKey(), click.getEventTime());
        reprocessBufferedPageViews(click.getUserId(), click.getEventTime());
    }

    /**
     * Process a page view event.
     * Find matching click and emit attributed page view.
     * Processing logic:
     * - Check if event is too late using watermarkTracker
     * - Buffer page view in event of late click
     * - Find attributable click from clickStore
     * - Create and emit AttributedPageView
     * - Update watermark for the partition
     *
     * @param pageView the page view event
     */
    public void processPageView(PageViewEvent pageView) {
        log.info("Processing page view: {}", pageView.getEventId());
        boolean isEventLate = watermarkTracker.isTooLate(pageView.getWatermarkKey(), pageView.getEventTime());
        if (isEventLate) {
            log.warn("Page view event: {} is too late, dropping.", pageView.getEventId());
            return;
        }
        pageStore.addPageView(pageView);
        Optional<AdClickEvent> clickEvent = clickStore.findAttributableClick(
                pageView.getUserId(),
                pageView.getEventTime().minus(ATTRIBUTION_WINDOW),
                pageView.getEventTime()
        );
        outputSink.write(AttributedPageView.from(pageView, clickEvent.orElse(null)));

        watermarkTracker.updateWatermark(pageView.getWatermarkKey(), pageView.getEventTime());
    }


    /**
     * Re-attribute page views that arrived before this click (out-of-order delivery).
     * Window: page views strictly after the click and within the attribution window.
     *
     * @param userId the user to reprocess buffered pages
     * @param eventTime the click event time that triggered the reprocess
     */
    private void reprocessBufferedPageViews(String userId, Instant eventTime) {
        log.debug("Checking buffered pages for userId {} in case of late click", userId);
        List<PageViewEvent> pageViews = pageStore.findUserPageViews(
                userId,
                eventTime,
                eventTime.plus(ATTRIBUTION_WINDOW)
        );
        for (PageViewEvent pageView : pageViews) {
            Optional<AdClickEvent> recentClick = clickStore.findAttributableClick(
                    pageView.getUserId(),
                    pageView.getEventTime().minus(ATTRIBUTION_WINDOW),
                    pageView.getEventTime()
            );
            if (recentClick.isEmpty())
                continue;

            outputSink.write(AttributedPageView.from(pageView, recentClick.get()));
            log.info("Re-attributed page view {} due to late click {}", pageView.getEventId(), recentClick.get().getClickId());
        }
    }

    /**
     * Scheduled task to evict old events from state.
     * Runs every 30 seconds to prevent unbounded memory growth.
     * State eviction logic:
     * - Evict clicks older than the min watermark for all
     * - Use clickStore.evictOldClicks() with appropriate cutoff time
     */
    @Scheduled(fixedRate = 30000)
    public void evictOldState() {
        log.info("Running state eviction");

        Instant minClickWatermark = watermarkTracker.findMinWatermark(AdClickEvent.TOPIC);
        if (minClickWatermark.isAfter(Instant.MIN)) {
            int clicksEvicted = clickStore.evictOldClicks(minClickWatermark);
            log.info("Evicted {} clicks older than {}", clicksEvicted, minClickWatermark);
        }

        Instant minPageWatermark = watermarkTracker.findMinWatermark(PageViewEvent.TOPIC);
        if (minPageWatermark.isAfter(Instant.MIN)) {
            int pagesEvicted = pageStore.evictOldPages(minPageWatermark);
            log.info("Evicted {} pages older than {}", pagesEvicted, minPageWatermark);
        }
    }
}
