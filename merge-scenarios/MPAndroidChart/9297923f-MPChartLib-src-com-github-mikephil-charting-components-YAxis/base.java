package com.github.mikephil.charting.components;

import android.graphics.Paint;
import com.github.mikephil.charting.formatter.DefaultValueFormatter;
import com.github.mikephil.charting.formatter.DefaultYAxisValueFormatter;
import com.github.mikephil.charting.utils.Utils;
import com.github.mikephil.charting.formatter.YAxisValueFormatter;

public class YAxis extends AxisBase {

    protected YAxisValueFormatter mYAxisValueFormatter;

    public float[] mEntries = new float[] {};

    public int mEntryCount;

    public int mDecimals;

    private int mLabelCount = 6;

    private boolean mDrawTopYLabelEntry = true;

    protected boolean mShowOnlyMinMax = false;

    protected boolean mInverted = false;

    protected boolean mStartAtZero = true;

    protected boolean mForceLabels = false;

    protected float mCustomAxisMin = Float.NaN;

    protected float mCustomAxisMax = Float.NaN;

    protected float mSpacePercentTop = 10f;

    protected float mSpacePercentBottom = 10f;

    public float mAxisMaximum = 0f;

    public float mAxisMinimum = 0f;

    public float mAxisRange = 0f;

    private YAxisLabelPosition mPosition = YAxisLabelPosition.OUTSIDE_CHART;

    public enum YAxisLabelPosition {

        OUTSIDE_CHART, INSIDE_CHART
    }

    private AxisDependency mAxisDependency;

    public enum AxisDependency {

        LEFT, RIGHT
    }

    public YAxis() {
        super();
        this.mAxisDependency = AxisDependency.LEFT;
        this.mYOffset = 0f;
    }

    public YAxis(AxisDependency position) {
        super();
        this.mAxisDependency = position;
        this.mYOffset = 0f;
    }

    public AxisDependency getAxisDependency() {
        return mAxisDependency;
    }

    public YAxisLabelPosition getLabelPosition() {
        return mPosition;
    }

    public void setPosition(YAxisLabelPosition pos) {
        mPosition = pos;
    }

    public boolean isDrawTopYLabelEntryEnabled() {
        return mDrawTopYLabelEntry;
    }

    public void setDrawTopYLabelEntry(boolean enabled) {
        mDrawTopYLabelEntry = enabled;
    }

    public void setLabelCount(int count, boolean force) {
        if (count > 25)
            count = 25;
        if (count < 2)
            count = 2;
        mLabelCount = count;
        mForceLabels = force;
    }

    public int getLabelCount() {
        return mLabelCount;
    }

    public boolean isForceLabelsEnabled() {
        return mForceLabels;
    }

    public void setShowOnlyMinMax(boolean enabled) {
        mShowOnlyMinMax = enabled;
    }

    public boolean isShowOnlyMinMaxEnabled() {
        return mShowOnlyMinMax;
    }

    public void setInverted(boolean enabled) {
        mInverted = enabled;
    }

    public boolean isInverted() {
        return mInverted;
    }

    public void setStartAtZero(boolean enabled) {
        this.mStartAtZero = enabled;
    }

    public boolean isStartAtZeroEnabled() {
        return mStartAtZero;
    }

    public float getAxisMinValue() {
        return mCustomAxisMin;
    }

    public void setAxisMinValue(float min) {
        mCustomAxisMin = min;
    }

    public void resetAxisMinValue() {
        mCustomAxisMin = Float.NaN;
    }

    public float getAxisMaxValue() {
        return mCustomAxisMax;
    }

    public void setAxisMaxValue(float max) {
        mCustomAxisMax = max;
    }

    public void resetAxisMaxValue() {
        mCustomAxisMax = Float.NaN;
    }

    public void setSpaceTop(float percent) {
        mSpacePercentTop = percent;
    }

    public float getSpaceTop() {
        return mSpacePercentTop;
    }

    public void setSpaceBottom(float percent) {
        mSpacePercentBottom = percent;
    }

    public float getSpaceBottom() {
        return mSpacePercentBottom;
    }

    public float getRequiredWidthSpace(Paint p) {
        p.setTextSize(mTextSize);
        String label = getLongestLabel();
        return (float) Utils.calcTextWidth(p, label) + getXOffset() * 2f;
    }

    public float getRequiredHeightSpace(Paint p) {
        p.setTextSize(mTextSize);
        String label = getLongestLabel();
        return (float) Utils.calcTextHeight(p, label) + getYOffset() * 2f;
    }

    @Override
    public String getLongestLabel() {
        String longest = "";
        for (int i = 0; i < mEntries.length; i++) {
            String text = getFormattedLabel(i);
            if (longest.length() < text.length())
                longest = text;
        }
        return longest;
    }

    public String getFormattedLabel(int index) {
        if (index < 0 || index >= mEntries.length)
            return "";
        else
            return getValueFormatter().getFormattedValue(mEntries[index], this);
    }

    public void setValueFormatter(YAxisValueFormatter f) {
        if (f == null)
            mYAxisValueFormatter = new DefaultYAxisValueFormatter(mDecimals);
        else
            mYAxisValueFormatter = f;
    }

    public YAxisValueFormatter getValueFormatter() {
        if (mYAxisValueFormatter == null)
            mYAxisValueFormatter = new DefaultYAxisValueFormatter(mDecimals);
        return mYAxisValueFormatter;
    }

    public boolean needsDefaultFormatter() {
        if (mYAxisValueFormatter == null)
            return true;
        if (mYAxisValueFormatter instanceof DefaultValueFormatter)
            return true;
        return false;
    }

    public boolean needsOffset() {
        if (isEnabled() && isDrawLabelsEnabled() && getLabelPosition() == YAxisLabelPosition.OUTSIDE_CHART)
            return true;
        else
            return false;
    }
}