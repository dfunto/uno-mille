package com.ebay.challenge.streamprocessor.dashboard;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;
import java.util.Map;

public record DashboardEvent(
        EventType type,
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant timestamp,
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Instant processedAt,
        Map<String, Object> data
) {
    public DashboardEvent(EventType type, Instant timestamp, Map<String, Object> data) {
        this(type, timestamp, Instant.now(), data);
    }
    public enum EventType {
        CLICK, PAGE_VIEW, ATTRIBUTION, LATE_EVENT, WATERMARK
    }
}
