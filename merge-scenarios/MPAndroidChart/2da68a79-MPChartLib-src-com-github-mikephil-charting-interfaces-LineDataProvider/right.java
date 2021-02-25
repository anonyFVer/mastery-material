package com.github.mikephil.charting.interfaces;

import com.github.mikephil.charting.data.LineData;
import com.github.mikephil.charting.utils.FillFormatter;

public interface LineDataProvider extends BarLineScatterCandleBubbleDataProvider {

    LineData getLineData();

    void setFillFormatter(FillFormatter formatter);

    FillFormatter getFillFormatter();
}