package com.ebay.challenge.streamprocessor.sink;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryOutputSink implements OutputSink {

    private final Map<String, AttributedPageView> state = new ConcurrentHashMap<>();

    @Override
    public void write(AttributedPageView record) {
        state.put(record.getPageViewId(), record);
    }

    public AttributedPageView get(String pageViewId) {
        return state.get(pageViewId);
    }

    public int size() {
        return state.size();
    }
}
