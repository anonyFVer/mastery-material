package com.github.mikephil.charting.interfaces.datasets;

import android.graphics.Typeface;
import com.github.mikephil.charting.components.YAxis;
import com.github.mikephil.charting.data.DataSet;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.formatter.ValueFormatter;
import java.util.List;

public interface IDataSet<T extends Entry> {

    float getYMin();

    float getYMax();

    int getEntryCount();

    void calcMinMax(int start, int end);

    T getEntryForXIndex(int xIndex);

    T getEntryForXIndex(int xIndex, DataSet.Rounding rounding);

    T getEntryForIndex(int index);

    int getEntryIndex(int xIndex, DataSet.Rounding rounding);

    int getEntryIndex(T e);

    float getYValForXIndex(int xIndex);

    int getIndexInEntries(int xIndex);

    boolean addEntry(T e);

    boolean removeEntry(T e);

    void addEntryOrdered(T e);

    boolean removeFirst();

    boolean removeLast();

    boolean removeEntry(int xIndex);

    boolean contains(T entry);

    void clear();

    String getLabel();

    void setLabel(String label);

    YAxis.AxisDependency getAxisDependency();

    void setAxisDependency(YAxis.AxisDependency dependency);

    List<Integer> getColors();

    int getColor();

    int getColor(int index);

    boolean isHighlightEnabled();

    void setHighlightEnabled(boolean enabled);

    void setValueFormatter(ValueFormatter f);

    ValueFormatter getValueFormatter();

    void setValueTextColor(int color);

    void setValueTextColors(List<Integer> colors);

    void setValueTypeface(Typeface tf);

    void setValueTextSize(float size);

    int getValueTextColor();

    int getValueTextColor(int index);

    Typeface getValueTypeface();

    float getValueTextSize();

    void setDrawValues(boolean enabled);

    boolean isDrawValuesEnabled();

    void setVisible(boolean visible);

    boolean isVisible();
}