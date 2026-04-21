package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.sink.FileSink;
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
    private final FileSink outputSink;

    /**
     * Process an ad click event.
     * Store the click in state for future attribution.
     * Processing logic:
     * - Check if event is too late using watermarkTracker
     * - Store the click in clickStore
     * - Update watermark for the partition
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
        reprocessBufferedPageViews(click);
    }

    /**
     * Re-attribute page views that arrived before this click (out-of-order delivery).
     * Window: page views strictly after the click and within the attribution window.
    */
    private void reprocessBufferedPageViews(AdClickEvent click) {
        log.debug("Checking buffered pages for click {} re-attribution due to late click", click.getClickId());
        List<PageViewEvent> pageViews = pageStore.findUserPageViews(
                click.getUserId(),
                click.getEventTime(),
                click.getEventTime().plus(ATTRIBUTION_WINDOW)
        );
        for (PageViewEvent pageView : pageViews) {
            Optional<AdClickEvent> bestClick = clickStore.findAttributableClick(pageView.getUserId(), pageView.getEventTime(), ATTRIBUTION_WINDOW);
            if (bestClick.isEmpty())
                continue;
            outputSink.write(AttributedPageView.from(pageView, bestClick.get()));
            log.info("Re-attributed page view {} due to late click {}", pageView.getEventId(), click.getClickId());
        }
    }

    /**
     * Process a page view event.
     * Find matching click and emit attributed page view.
     * Processing logic:
     * - Check if event is too late using watermarkTracker
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
            // # TODO Implement dead letter queue?
            log.warn("Page view event: {} is too late, dropping.", pageView.getEventId());
            return;
        }
        pageStore.addPageView(pageView);

        Optional<AdClickEvent> clickEvent = clickStore.findAttributableClick(pageView.getUserId(), pageView.getEventTime(), ATTRIBUTION_WINDOW);
        outputSink.write(AttributedPageView.from(pageView, clickEvent.orElse(null)));

        watermarkTracker.updateWatermark(pageView.getWatermarkKey(), pageView.getEventTime());
    }

    /**
     * Scheduled task to evict old events from state.
     * Runs every 30 seconds to prevent unbounded memory growth.
     * State eviction logic:
     * - Evict clicks older than the watermark cutoff
     * - Use clickStore.evictOldClicks() with appropriate cutoff time
     */
    @Scheduled(fixedRate = 30000)
    public void evictOldState() {
        log.info("Running state eviction");

        Instant minClickWatermark = watermarkTracker.findMinWatermark(AdClickEvent.TOPIC);
        int clicksEvicted = clickStore.evictOldClicks(minClickWatermark);
        log.info("Evicted {} clicks older than {}", clicksEvicted, minClickWatermark);

        Instant minPageWatermark = watermarkTracker.findMinWatermark(PageViewEvent.TOPIC);
        int pagesEvicted = pageStore.evictOldPages(minPageWatermark);
        log.info("Evicted {} pages older than {}", pagesEvicted, minPageWatermark);
    }
}
