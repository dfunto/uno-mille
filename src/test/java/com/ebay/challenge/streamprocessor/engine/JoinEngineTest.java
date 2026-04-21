package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JoinEngineTest {

    @Mock
    private ClickStateStore clickStore;

    @Mock
    private WatermarkTracker watermarkTracker;

    @InjectMocks
    private JoinEngine joinEngine;

    @Test
    void processClick_storesClickAndUpdatesWatermark_whenEventIsOnTime() {
        AdClickEvent click = AdClickEvent.builder()
                .clickId("click_1")
                .userId("user_1")
                .partition(0)
                .eventTime(Instant.now())
                .build();
        when(watermarkTracker.isTooLate(click.getWatermarkKey(), click.getEventTime())).thenReturn(false);

        joinEngine.processClick(click);
        verify(clickStore).addClick(click);
        verify(watermarkTracker).updateWatermark(click.getWatermarkKey(), click.getEventTime());
    }

    @Test
    void processClick_skipsClickAndDoesNotUpdateWatermark_whenEventIsLate() {
        AdClickEvent click = AdClickEvent.builder()
                .clickId("click_late")
                .userId("user_1")
                .partition(0)
                .eventTime(Instant.now().minusSeconds(3600))
                .build();

        when(watermarkTracker.isTooLate(click.getWatermarkKey(), click.getEventTime())).thenReturn(true);

        joinEngine.processClick(click);

        verify(clickStore, never()).addClick(any());
        verify(watermarkTracker, never()).updateWatermark(anyString(), any());
    }

    @Test
    void processClick_updatesWatermarkAfterStoringClick() {
        AdClickEvent click = AdClickEvent.builder()
                .clickId("click_1")
                .userId("user_1")
                .partition(0)
                .eventTime(Instant.now())
                .build();

        when(watermarkTracker.isTooLate(click.getWatermarkKey(), click.getEventTime())).thenReturn(false);

        InOrder order = inOrder(clickStore, watermarkTracker);
        joinEngine.processClick(click);

        order.verify(clickStore).addClick(click);
        order.verify(watermarkTracker).updateWatermark(click.getWatermarkKey(), click.getEventTime());
    }
}