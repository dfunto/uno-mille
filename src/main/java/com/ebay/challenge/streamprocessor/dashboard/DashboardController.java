package com.ebay.challenge.streamprocessor.dashboard;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/events")
public class DashboardController {

    private final EventBroadcaster broadcaster;

    public DashboardController(EventBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @GetMapping("/stream")
    public SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(5 * 60 * 1000L); // 5 minutes
        broadcaster.addEmitter(emitter);
        return emitter;
    }
}
