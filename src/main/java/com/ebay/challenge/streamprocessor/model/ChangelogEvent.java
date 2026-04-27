package com.ebay.challenge.streamprocessor.model;

public interface ChangelogEvent {
    String CHANGELOG_SUFFIX = "-changelog";
    String getChangelogKey();
    String getTopic();
    int getPartition();

    default String getChangelogTopic() {
        return getTopic() + CHANGELOG_SUFFIX;
    }
}
