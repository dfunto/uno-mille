package com.ebay.challenge.streamprocessor.dashboard;

import com.ebay.challenge.streamprocessor.dashboard.DashboardEvent.EventType;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@Component
public class EventBroadcaster {

    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    private final ObjectMapper objectMapper;

    public EventBroadcaster(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void addEmitter(SseEmitter emitter) {
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));
    }

    public void broadcastClick(AdClickEvent click) {
        broadcast(new DashboardEvent(EventType.CLICK, click.getEventTime(), Map.of(
                "clickId", click.getClickId(),
                "userId", click.getUserId(),
                "campaignId", click.getCampaignId())));
    }

    public void broadcastPageView(PageViewEvent pageView) {
        broadcast(new DashboardEvent(EventType.PAGE_VIEW, pageView.getEventTime(), Map.of(
                "eventId", pageView.getEventId(),
                "userId", pageView.getUserId(),
                "url", pageView.getUrl())));
    }

    public void broadcastAttribution(AttributedPageView attributed) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("pageViewId", attributed.getPageViewId());
        data.put("userId", attributed.getUserId());
        data.put("attributed", attributed.getAttributedClickId() != null);
        data.put("clickId", attributed.getAttributedClickId());
        data.put("campaignId", attributed.getAttributedCampaignId());
        broadcast(new DashboardEvent(EventType.ATTRIBUTION, attributed.getEventTime(), data));
    }

    public void broadcastLateClick(AdClickEvent click) {
        broadcast(new DashboardEvent(EventType.LATE_EVENT, click.getEventTime(), Map.of(
                "sourceType", "CLICK",
                "clickId", click.getClickId(),
                "userId", click.getUserId(),
                "campaignId", click.getCampaignId())));
    }

    public void broadcastLatePageView(PageViewEvent pageView) {
        broadcast(new DashboardEvent(EventType.LATE_EVENT, pageView.getEventTime(), Map.of(
                "sourceType", "PAGE_VIEW",
                "eventId", pageView.getEventId(),
                "userId", pageView.getUserId(),
                "url", pageView.getUrl())));
    }

    public void broadcastWatermark(String key, Instant watermarkValue) {
        broadcast(new DashboardEvent(EventType.WATERMARK, watermarkValue, Map.of(
                "key", key,
                "watermark", watermarkValue.toString())));
    }

    private void broadcast(DashboardEvent event) {
        if (emitters.isEmpty()) {
            return;
        }
        String json;
        try {
            json = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            log.warn("Failed to serialize dashboard event", e);
            return;
        }
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name(event.type().name())
                        .data(json));
            } catch (Exception e) {
                emitters.remove(emitter);
            }
        }
    }
}
