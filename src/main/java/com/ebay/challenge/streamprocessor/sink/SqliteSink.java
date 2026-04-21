package com.ebay.challenge.streamprocessor.sink;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.sql.*;

/**
 * SQLite-based output sink for attributed page view records.
 * Uses INSERT OR REPLACE on page_view_id (PRIMARY KEY) for upsert semantics,
 * allowing re-attribution corrections to overwrite earlier null-attributed records.
 * Thread-safe: synchronized write serializes concurrent partition threads (SQLite single-writer constraint).
 */
@Slf4j
@Component
public class SqliteSink implements OutputSink {

    private final String filePath;
    private Connection connection;

    public SqliteSink(@Value("${output.database.path:./output/page_views.db}") String filePath) {
        this.filePath = filePath;
    }

    @PostConstruct
    public void init() {
        try {
            File dbFile = new File(filePath);
            dbFile.getParentFile().mkdirs();

            connection = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getAbsolutePath());
            connection.setAutoCommit(true);
            createTable();
            log.info("Initialized SQLite output sink at {}", filePath);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize SQLite Sink", e);
        }
    }

    private void createTable() throws SQLException {
        String sql = """
            CREATE TABLE IF NOT EXISTS attributed_page_views (
                page_view_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                event_time TEXT NOT NULL,
                url TEXT NOT NULL,
                attributed_campaign_id TEXT,
                attributed_click_id TEXT
            )
            """;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    @Override
    public synchronized void write(AttributedPageView record) {
        String sql = """
            INSERT OR REPLACE INTO attributed_page_views
            (page_view_id, user_id, event_time, url, attributed_campaign_id, attributed_click_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, record.getPageViewId());
            ps.setString(2, record.getUserId());
            ps.setString(3, record.getEventTime().toString());
            ps.setString(4, record.getUrl());
            ps.setString(5, record.getAttributedCampaignId());
            ps.setString(6, record.getAttributedClickId());
            ps.executeUpdate();
            log.debug("Wrote attributed page view: {} -> campaign={}, click={}",
                    record.getPageViewId(), record.getAttributedCampaignId(), record.getAttributedClickId());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to write attributed page view: " + record.getPageViewId(), e);
        }
    }
}