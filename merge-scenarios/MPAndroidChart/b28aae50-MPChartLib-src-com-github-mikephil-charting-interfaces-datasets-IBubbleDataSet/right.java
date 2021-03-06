package com.github.mikephil.charting.interfaces.datasets;

import com.github.mikephil.charting.data.BubbleEntry;

public interface IBubbleDataSet extends IBarLineScatterCandleBubbleDataSet<BubbleEntry> {

    void setHighlightCircleWidth(float width);

    float getXMax();

    float getXMin();

    float getMaxSize();

    boolean isNormalizeSizeEnabled();

    float getHighlightCircleWidth();
}