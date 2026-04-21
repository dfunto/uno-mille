package com.ebay.challenge.streamprocessor.sink;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Writes attributed page view records to a local file in JSONL format (one JSON record per line).
 * Thread-safe: synchronized write ensures multiple partition threads don't interleave output.
 * File is recreated (truncated) on every startup. Flush-per-write ensures records are visible immediately.
 */
@Slf4j
@Component
public class FileSink implements OutputSink {

    private final ObjectMapper objectMapper;
    private final BufferedWriter writer;
    private final String filePath;

    public FileSink(
            ObjectMapper objectMapper,
            @Value("${output.file.path:./output/attributed_page_views.jsonl}") String filePath
    ) throws IOException {
        this.objectMapper = objectMapper;
        this.filePath = filePath;
        Path path = Path.of(filePath);
        Files.createDirectories(path.getParent());
        this.writer = new BufferedWriter(new FileWriter(path.toFile(), false));
        log.info("Initialized FileSink at {}", filePath);
    }

    @Override
    public synchronized void write(AttributedPageView record) {
        try {
            writer.write(objectMapper.writeValueAsString(record));
            writer.newLine();
            writer.flush();
            log.debug("Wrote attributed page view: {} -> campaign={}, click={}",
                    record.getPageViewId(), record.getAttributedCampaignId(), record.getAttributedClickId());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write attributed page view: " + record.getPageViewId(), e);
        }
    }

    @PreDestroy
    public void close() {
        try {
            writer.close();
            log.info("Closed FileSink at {}", filePath);
        } catch (IOException e) {
            log.warn("Failed to close FileSink", e);
        }
    }
}