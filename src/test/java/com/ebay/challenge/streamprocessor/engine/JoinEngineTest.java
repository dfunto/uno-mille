package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.dashboard.EventBroadcaster;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.sink.InMemoryOutputSink;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.PageViewStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import com.ebay.challenge.streamprocessor.state.ChangelogProducer;


/**
 * Tests for the JoinEngine covering all required cases:
 * - Events in order click then page view
 * - Out-of-order events, click after page view
 * - Multiple clicks in window (should the pick latest)
 * - Click outside attribution window (no attribution)
 * - Drop Late events
 * - No click for a page view
 */
class JoinEngineTest {

    private JoinEngine joinEngine;
    private InMemoryOutputSink outputSink;

    @BeforeEach
    void setUp() {
        ClickStateStore clickStore = new ClickStateStore();
        PageViewStore pageStore = new PageViewStore();
        EventBroadcaster broadcaster = new EventBroadcaster(new com.fasterxml.jackson.databind.ObjectMapper());
        WatermarkTracker watermarkTracker = new WatermarkTracker(15, broadcaster);
        outputSink = new InMemoryOutputSink();
        joinEngine = new JoinEngine(clickStore, pageStore, watermarkTracker, outputSink, broadcaster, mock(ChangelogProducer.class), "ad_clicks", "page_views");
    }

    @Test
    void clickBeforePageView() {
        AdClickEvent click = AdClickEvent.builder().topic("ad_clicks")
                .userId("user_1")
                .eventTime(Instant.parse("2024-01-01T12:05:00Z"))
                .campaignId("campaign_A")
                .clickId("click_1")
                .partition(0)
                .offset(0)
                .build();

        PageViewEvent pageView = PageViewEvent.builder().topic("page_views")
                .userId("user_1")
                .eventTime(Instant.parse("2024-01-01T12:10:00Z"))
                .url("https://example.com/p1")
                .eventId("pv_1")
                .partition(0)
                .offset(0)
                .build();

        joinEngine.processClick(click);
        joinEngine.processPageView(pageView);

        AttributedPageView result = outputSink.get("pv_1");
        assertNotNull(result);
        assertEquals("click_1", result.getAttributedClickId());
        assertEquals("campaign_A", result.getAttributedCampaignId());
    }

    @Test
    void clickAfterPageView() {
        // Click event time is BEFORE page view event time,
        // but the click ARRIVES after the page view (out-of-order processing)
        AdClickEvent click = AdClickEvent.builder().topic("ad_clicks")
                .userId("user_2")
                .eventTime(Instant.parse("2024-01-01T12:05:00Z"))
                .campaignId("campaign_B")
                .clickId("click_2")
                .partition(0)
                .offset(0)
                .build();

        PageViewEvent pageView = PageViewEvent.builder().topic("page_views")
                .userId("user_2")
                .eventTime(Instant.parse("2024-01-01T12:10:00Z"))
                .url("https://example.com/p1")
                .eventId("pv_2")
                .partition(0)
                .offset(0)
                .build();

        // Page view processed first, then the late click triggers re-attribution
        joinEngine.processPageView(pageView);
        joinEngine.processClick(click);

        AttributedPageView result = outputSink.get("pv_2");
        assertNotNull(result);
        assertEquals("click_2", result.getAttributedClickId());
        assertEquals("campaign_B", result.getAttributedCampaignId());
    }

    @Test
    void multipleClicksInWindow() {
        AdClickEvent clickA = AdClickEvent.builder().topic("ad_clicks")
                .userId("user_3")
                .eventTime(Instant.parse("2024-01-01T12:20:00Z"))
                .campaignId("campaign_C")
                .clickId("click_3a")
                .partition(0)
                .offset(0)
                .build();

        AdClickEvent clickB = AdClickEvent.builder().topic("ad_clicks")
                .userId("user_3")
                .eventTime(Instant.parse("2024-01-01T12:25:00Z"))
                .campaignId("campaign_D")
                .clickId("click_3b")
                .partition(0)
                .offset(1)
                .build();

        PageViewEvent pageView = PageViewEvent.builder().topic("page_views")
                .userId("user_3")
                .eventTime(Instant.parse("2024-01-01T12:30:00Z"))
                .url("https://example.com/product3")
                .eventId("pv_3")
                .partition(0)
                .offset(0)
                .build();

        joinEngine.processClick(clickA);
        joinEngine.processClick(clickB);
        joinEngine.processPageView(pageView);

        AttributedPageView result = outputSink.get("pv_3");
        assertNotNull(result);
        assertEquals("click_3b", result.getAttributedClickId());
        assertEquals("campaign_D", result.getAttributedCampaignId());
    }

    @Test
    void clickOutsideAttributionWindow() {
        AdClickEvent click = AdClickEvent.builder().topic("ad_clicks")
                .userId("user_4")
                .eventTime(Instant.parse("2024-01-01T12:35:00Z"))
                .campaignId("campaign_E")
                .clickId("click_4")
                .partition(0)
                .offset(0)
                .build();

        PageViewEvent pageView = PageViewEvent.builder().topic("page_views")
                .userId("user_4")
                .eventTime(Instant.parse("2024-01-01T13:10:00Z")) // 35 minutes later, outside 30-min window
                .url("https://example.com/product4")
                .eventId("pv_4")
                .partition(0)
                .offset(0)
                .build();

        joinEngine.processClick(click);
        joinEngine.processPageView(pageView);

        AttributedPageView result = outputSink.get("pv_4");
        assertNotNull(result);
        assertNull(result.getAttributedClickId());
        assertNull(result.getAttributedCampaignId());
    }

    @Test
    void dropLateEvent() {
        AdClickEvent onTimeClick = AdClickEvent.builder().topic("ad_clicks")
                .userId("user_other")
                .eventTime(Instant.parse("2024-01-01T12:45:00Z"))
                .campaignId("campaign_X")
                .clickId("click_ontime")
                .partition(0)
                .offset(0)
                .build();
        joinEngine.processClick(onTimeClick);

        AdClickEvent lateClick = AdClickEvent.builder().topic("ad_clicks")
                .userId("user_5")
                .eventTime(Instant.parse("2024-01-01T12:20:00Z"))
                .campaignId("campaign_F")
                .clickId("click_5")
                .partition(0)
                .offset(1)
                .build();
        joinEngine.processClick(lateClick);

        PageViewEvent pageView = PageViewEvent.builder().topic("page_views")
                .userId("user_5")
                .eventTime(Instant.parse("2024-01-01T12:45:00Z"))
                .url("https://example.com/product5")
                .eventId("pv_5")
                .partition(0)
                .offset(0)
                .build();
        joinEngine.processPageView(pageView);

        AttributedPageView result = outputSink.get("pv_5");
        assertNotNull(result);
        assertNull(result.getAttributedClickId());
        assertNull(result.getAttributedCampaignId());
    }

    @Test
    void noClickForPageView() {
        PageViewEvent pageView = PageViewEvent.builder().topic("page_views")
                .userId("user_6")
                .eventTime(Instant.parse("2024-01-01T13:20:00Z"))
                .url("https://example.com/product6")
                .eventId("pv_6")
                .partition(0)
                .offset(0)
                .build();

        joinEngine.processPageView(pageView);

        AttributedPageView result = outputSink.get("pv_6");
        assertNotNull(result);
        assertNull(result.getAttributedClickId());
        assertNull(result.getAttributedCampaignId());
    }

}
