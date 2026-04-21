package com.ebay.challenge.streamprocessor.sink;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;

public interface OutputSink {

    void write(AttributedPageView record);
}
