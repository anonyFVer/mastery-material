package com.github.mikephil.charting.utils;

import com.github.mikephil.charting.data.LineData;
import com.github.mikephil.charting.data.LineDataSet;
import com.github.mikephil.charting.interfaces.LineDataProvider;

public interface FillFormatter {

    float getFillLinePosition(LineDataSet dataSet, LineDataProvider dataProvider);
}