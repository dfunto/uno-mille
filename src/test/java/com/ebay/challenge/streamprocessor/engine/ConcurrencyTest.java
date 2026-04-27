package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.dashboard.EventBroadcaster;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.sink.InMemoryOutputSink;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.ChangelogProducer;
import com.ebay.challenge.streamprocessor.state.PageViewStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;


public class ConcurrencyTest {

    private JoinEngine joinEngine;
    private InMemoryOutputSink outputSink;

    @BeforeEach
    void setUp() {
        ClickStateStore clickStore = new ClickStateStore();
        PageViewStore pageViewStore = new PageViewStore();
        EventBroadcaster broadcaster = new EventBroadcaster(new com.fasterxml.jackson.databind.ObjectMapper());
        WatermarkTracker watermarkTracker = new WatermarkTracker(2, broadcaster);
        outputSink = new InMemoryOutputSink();
        joinEngine = new JoinEngine(clickStore, pageViewStore, watermarkTracker, outputSink, broadcaster, mock(ChangelogProducer.class));
    }

    @RepeatedTest(3) // Try to catch race conditions
    void concurrentPartitionProcessing() throws Exception {
        int numPartitions = 4;
        int eventsPerPartition = 50;
        ExecutorService executor = Executors.newFixedThreadPool(numPartitions);
        CountDownLatch latch = new CountDownLatch(numPartitions);

        List<Future<?>> futures = new ArrayList<>();

        for (int p = 0; p < numPartitions; p++) {
            final int partition = p;
            futures.add(executor.submit(() -> {
                try {
                    latch.countDown();
                    latch.await(); // Start all partitions simultaneously

                    Instant base = Instant.parse("2024-01-01T12:00:00Z");

                    for (int i = 0; i < eventsPerPartition; i++) {
                        String userId = "user_p" + partition + "_" + i;
                        Instant clickTime = base.plusSeconds(i * 60L);
                        Instant pvTime = clickTime.plusSeconds(300); // 5 min after click

                        joinEngine.processClick(AdClickEvent.builder()
                                .userId(userId).clickId("click_" + partition + "_" + i)
                                .campaignId("campaign_" + partition)
                                .eventTime(clickTime).partition(partition).offset(i)
                                .build());

                        joinEngine.processPageView(PageViewEvent.builder()
                                .userId(userId).eventId("pv_" + partition + "_" + i)
                                .url("https://example.com/p" + partition)
                                .eventTime(pvTime).partition(partition).offset(i)
                                .build());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();

        // Verify all page views were attributed correctly
        int expectedTotal = numPartitions * eventsPerPartition;
        assertEquals(expectedTotal, outputSink.size(),
                "All page views should produce output");

        for (int p = 0; p < numPartitions; p++) {
            for (int i = 0; i < eventsPerPartition; i++) {
                AttributedPageView result = outputSink.get("pv_" + p + "_" + i);
                assertNotNull(result, "Missing output for pv_" + p + "_" + i);
                assertEquals("campaign_" + p, result.getAttributedCampaignId(),
                        "Wrong attribution for pv_" + p + "_" + i);
            }
        }
    }

    @RepeatedTest(3)
    void concurrentClicksAndPageViewsForSameUser() throws Exception {
        // Multiple threads process clicks and page views for the same user concurrently
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(1);

        Instant base = Instant.parse("2024-01-01T12:00:00Z");
        String userId = "shared_user";
        int numEvents = 20;

        List<Future<?>> futures = new ArrayList<>();

        // Thread 1: sends clicks
        futures.add(executor.submit(() -> {
            try { latch.await(); } catch (InterruptedException e) { return; }
            for (int i = 0; i < numEvents; i++) {
                joinEngine.processClick(AdClickEvent.builder()
                        .userId(userId).clickId("click_" + i).campaignId("campaign_" + i)
                        .eventTime(base.plusSeconds(i * 120L)).partition(0).offset(i)
                        .build());
            }
        }));

        // Thread 2: sends page views
        futures.add(executor.submit(() -> {
            try { latch.await(); } catch (InterruptedException e) { return; }
            for (int i = 0; i < numEvents; i++) {
                joinEngine.processPageView(PageViewEvent.builder()
                        .userId(userId).eventId("pv_" + i)
                        .url("https://example.com/p" + i)
                        .eventTime(base.plusSeconds(i * 120L + 60)).partition(0).offset(i)
                        .build());
            }
        }));

        latch.countDown(); // Start both threads
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        executor.shutdown();

        // All page views should have been written (with or without attribution)
        for (int i = 0; i < numEvents; i++) {
            AttributedPageView result = outputSink.get("pv_" + i);
            assertNotNull(result, "Missing output for pv_" + i);
        }
    }
}
