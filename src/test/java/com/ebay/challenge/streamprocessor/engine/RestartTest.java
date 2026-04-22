package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.sink.InMemoryOutputSink;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.PageViewStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simulates a processor restart (e.g. pod eviction).
 * Scenario:
 * 1. Process a batch of events and "commit" their offsets.
 * 2. Simulate a crash — all in-memory state is lost.
 * 3. Rebuild the engine and replay from the last committed offsets.
 * 4. Process remaining events.
 * 5. Verify final output is correct — same as if no crash had occurred.
 */
class RestartTest {

    private JoinEngine createEngine(InMemoryOutputSink sink) {
        return new JoinEngine(
                new ClickStateStore(),
                new PageViewStore(),
                new WatermarkTracker(15),
                sink
        );
    }

    @Test
    void restartFromCommittedOffsets_producesCorrectResults() {
        // --- Batch 1: events that are processed and committed before crash ---
        AdClickEvent click1 = AdClickEvent.builder()
                .userId("user_1")
                .eventTime(Instant.parse("2024-01-01T12:00:00Z"))
                .campaignId("campaign_A")
                .clickId("click_1")
                .partition(0).offset(0)
                .build();

        PageViewEvent pv1 = PageViewEvent.builder()
                .userId("user_1")
                .eventTime(Instant.parse("2024-01-01T12:05:00Z"))
                .url("https://example.com/p1")
                .eventId("pv_1")
                .partition(0).offset(0)
                .build();

        AdClickEvent click2 = AdClickEvent.builder()
                .userId("user_2")
                .eventTime(Instant.parse("2024-01-01T12:10:00Z"))
                .campaignId("campaign_B")
                .clickId("click_2")
                .partition(0).offset(1)
                .build();

        // --- Batch 2: events that were NOT committed before crash ---
        PageViewEvent pv2 = PageViewEvent.builder()
                .userId("user_2")
                .eventTime(Instant.parse("2024-01-01T12:15:00Z"))
                .url("https://example.com/p2")
                .eventId("pv_2")
                .partition(0).offset(1)
                .build();

        AdClickEvent click3 = AdClickEvent.builder()
                .userId("user_3")
                .eventTime(Instant.parse("2024-01-01T12:20:00Z"))
                .campaignId("campaign_C")
                .clickId("click_3")
                .partition(0).offset(2)
                .build();

        PageViewEvent pv3 = PageViewEvent.builder()
                .userId("user_3")
                .eventTime(Instant.parse("2024-01-01T12:25:00Z"))
                .url("https://example.com/p3")
                .eventId("pv_3")
                .partition(0).offset(2)
                .build();

        // ======= Phase 1: process batch 1, then crash =======
        InMemoryOutputSink sink1 = new InMemoryOutputSink();
        JoinEngine engine1 = createEngine(sink1);

        engine1.processClick(click1);
        engine1.processPageView(pv1);
        engine1.processClick(click2);
        // Offsets committed here: ad_clicks offset=1, page_views offset=0.

        // Verify pre-crash output
        AttributedPageView result1 = sink1.get("pv_1");
        assertNotNull(result1);
        assertEquals("click_1", result1.getAttributedClickId());

        // ======= CRASH — all in-memory state is lost =======
        // engine1, sink1, and all state stores are gone.

        // ======= Phase 2: restart with fresh state =======
        InMemoryOutputSink sink2 = new InMemoryOutputSink();
        JoinEngine engine2 = createEngine(sink2);

        // Replay from last committed offsets (at-least-once delivery).
        // Re-process batch 1 events that were already committed:
        engine2.processClick(click1);
        engine2.processPageView(pv1);
        engine2.processClick(click2);

        // Now process batch 2 events (not yet committed before crash):
        engine2.processPageView(pv2);
        engine2.processClick(click3);
        engine2.processPageView(pv3);

        // ======= Verify: all results are correct =======
        // pv_1 attributed to click_1
        AttributedPageView restarted1 = sink2.get("pv_1");
        assertNotNull(restarted1);
        assertEquals("click_1", restarted1.getAttributedClickId());
        assertEquals("campaign_A", restarted1.getAttributedCampaignId());

        // pv_2 attributed to click_2
        AttributedPageView restarted2 = sink2.get("pv_2");
        assertNotNull(restarted2);
        assertEquals("click_2", restarted2.getAttributedClickId());
        assertEquals("campaign_B", restarted2.getAttributedCampaignId());

        // pv_3 attributed to click_3
        AttributedPageView restarted3 = sink2.get("pv_3");
        assertNotNull(restarted3);
        assertEquals("click_3", restarted3.getAttributedClickId());
        assertEquals("campaign_C", restarted3.getAttributedCampaignId());
    }
}
