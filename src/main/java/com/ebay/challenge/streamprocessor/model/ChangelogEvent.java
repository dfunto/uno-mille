package com.ebay.challenge.streamprocessor.model;

public interface ChangelogEvent {
    String getChangelogKey();
    String getChangelogTopic();
    int getPartition();
}
