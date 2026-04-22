package com.ebay.challenge.streamprocessor.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WatermarkTrackerTest {

    private WatermarkTracker watermarkTracker;

    @BeforeEach
    void setUp() {
        watermarkTracker = new WatermarkTracker(2);
    }

    @Test
    void advancesWatermarkWhenEventIsNewer() {
        Instant t10 = Instant.parse("2024-01-01T12:10:00Z");
        Instant t20 = Instant.parse("2024-01-01T12:20:00Z");

        watermarkTracker.updateWatermark("topic:0", t10);
        watermarkTracker.updateWatermark("topic:0", t20);

        assertEquals(t20, watermarkTracker.getWatermark("topic:0"));
    }

    @Test
    void doesNotGoBackwardWhenEventIsOlder() {
        Instant t20 = Instant.parse("2024-01-01T12:20:00Z");
        Instant t10 = Instant.parse("2024-01-01T12:10:00Z");

        watermarkTracker.updateWatermark("topic:0", t20);
        watermarkTracker.updateWatermark("topic:0", t10);

        assertEquals(t20, watermarkTracker.getWatermark("topic:0"));
    }

    @Test
    void doesNotGoBackwardWhenEventIsEqual() {
        Instant t20 = Instant.parse("2024-01-01T12:20:00Z");

        watermarkTracker.updateWatermark("topic:0", t20);
        watermarkTracker.updateWatermark("topic:0", t20);

        assertEquals(t20, watermarkTracker.getWatermark("topic:0"));
    }

    @Test
    void tracksPartitionsIndependently() {
        Instant t10 = Instant.parse("2024-01-01T12:10:00Z");
        Instant t20 = Instant.parse("2024-01-01T12:20:00Z");

        watermarkTracker.updateWatermark("topic:0", t20);
        watermarkTracker.updateWatermark("topic:1", t10);

        assertEquals(t20, watermarkTracker.getWatermark("topic:0"));
        assertEquals(t10, watermarkTracker.getWatermark("topic:1"));
    }
}