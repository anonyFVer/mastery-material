package com.github.mikephil.charting.utils;

import com.github.mikephil.charting.data.LineData;
import com.github.mikephil.charting.data.LineDataSet;

public interface FillFormatter {

    float getFillLinePosition(LineDataSet dataSet, LineData data, float chartMaxY, float chartMinY);
}