package com.ebay.challenge.streamprocessor.dashboard;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class DashboardController {

    private final EventBroadcaster broadcaster;
    private final String dbPath;

    public DashboardController(EventBroadcaster broadcaster,
                               @Value("${output.database.path:./output/page_views.db}") String dbPath) {
        this.broadcaster = broadcaster;
        this.dbPath = dbPath;
    }

    @GetMapping("/events/stream")
    public SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(5 * 60 * 1000L); // 5 minutes
        broadcaster.addEmitter(emitter);
        return emitter;
    }

    @GetMapping("/output")
    public List<Map<String, String>> getAttributedPageViews() throws SQLException {
        File dbFile = new File(dbPath);
        if (!dbFile.exists()) {
            return List.of();
        }

        List<Map<String, String>> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT page_view_id, user_id, event_time, url, attributed_campaign_id, attributed_click_id FROM attributed_page_views ORDER BY user_id")) {
            while (rs.next()) {
                Map<String, String> row = new LinkedHashMap<>();
                row.put("page_view_id", rs.getString("page_view_id"));
                row.put("user_id", rs.getString("user_id"));
                row.put("event_time", rs.getString("event_time"));
                row.put("url", rs.getString("url"));
                row.put("attributed_campaign_id", rs.getString("attributed_campaign_id"));
                row.put("attributed_click_id", rs.getString("attributed_click_id"));
                rows.add(row);
            }
        }
        return rows;
    }
}