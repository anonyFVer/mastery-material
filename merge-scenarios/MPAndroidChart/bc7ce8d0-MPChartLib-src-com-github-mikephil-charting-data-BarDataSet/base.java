package com.github.mikephil.charting.data;

import android.graphics.Color;
import com.github.mikephil.charting.interfaces.datasets.IBarDataSet;
import java.util.ArrayList;
import java.util.List;

public class BarDataSet extends BarLineScatterCandleBubbleDataSet<BarEntry> implements IBarDataSet {

    private float mBarSpace = 0.15f;

    private int mStackSize = 1;

    private int mBarShadowColor = Color.rgb(215, 215, 215);

    private int mHighLightAlpha = 120;

    private int mEntryCountStacks = 0;

    private String[] mStackLabels = new String[] { "Stack" };

    public BarDataSet(List<BarEntry> yVals, String label) {
        super(yVals, label);
        mHighLightColor = Color.rgb(0, 0, 0);
        calcStackSize(yVals);
        calcEntryCountIncludingStacks(yVals);
    }

    @Override
    public DataSet<BarEntry> copy() {
        List<BarEntry> yVals = new ArrayList<BarEntry>();
        for (int i = 0; i < mYVals.size(); i++) {
            yVals.add(((BarEntry) mYVals.get(i)).copy());
        }
        BarDataSet copied = new BarDataSet(yVals, getLabel());
        copied.mColors = mColors;
        copied.mStackSize = mStackSize;
        copied.mBarSpace = mBarSpace;
        copied.mBarShadowColor = mBarShadowColor;
        copied.mStackLabels = mStackLabels;
        copied.mHighLightColor = mHighLightColor;
        copied.mHighLightAlpha = mHighLightAlpha;
        return copied;
    }

    private void calcEntryCountIncludingStacks(List<BarEntry> yVals) {
        mEntryCountStacks = 0;
        for (int i = 0; i < yVals.size(); i++) {
            float[] vals = yVals.get(i).getVals();
            if (vals == null)
                mEntryCountStacks++;
            else
                mEntryCountStacks += vals.length;
        }
    }

    private void calcStackSize(List<BarEntry> yVals) {
        for (int i = 0; i < yVals.size(); i++) {
            float[] vals = yVals.get(i).getVals();
            if (vals != null && vals.length > mStackSize)
                mStackSize = vals.length;
        }
    }

    @Override
    public void calcMinMax(int start, int end) {
        if (mYVals == null)
            return;
        final int yValCount = mYVals.size();
        if (yValCount == 0)
            return;
        int endValue;
        if (end == 0 || end >= yValCount)
            endValue = yValCount - 1;
        else
            endValue = end;
        mYMin = Float.MAX_VALUE;
        mYMax = -Float.MAX_VALUE;
        for (int i = start; i <= endValue; i++) {
            BarEntry e = mYVals.get(i);
            if (e != null && !Float.isNaN(e.getVal())) {
                if (e.getVals() == null) {
                    if (e.getVal() < mYMin)
                        mYMin = e.getVal();
                    if (e.getVal() > mYMax)
                        mYMax = e.getVal();
                } else {
                    if (-e.getNegativeSum() < mYMin)
                        mYMin = -e.getNegativeSum();
                    if (e.getPositiveSum() > mYMax)
                        mYMax = e.getPositiveSum();
                }
            }
        }
        if (mYMin == Float.MAX_VALUE) {
            mYMin = 0.f;
            mYMax = 0.f;
        }
    }

    @Override
    public int getStackSize() {
        return mStackSize;
    }

    @Override
    public boolean isStacked() {
        return mStackSize > 1 ? true : false;
    }

    public int getEntryCountStacks() {
        return mEntryCountStacks;
    }

    public float getBarSpacePercent() {
        return mBarSpace * 100f;
    }

    @Override
    public float getBarSpace() {
        return mBarSpace;
    }

    public void setBarSpacePercent(float percent) {
        mBarSpace = percent / 100f;
    }

    public void setBarShadowColor(int color) {
        mBarShadowColor = color;
    }

    @Override
    public int getBarShadowColor() {
        return mBarShadowColor;
    }

    public void setHighLightAlpha(int alpha) {
        mHighLightAlpha = alpha;
    }

    @Override
    public int getHighLightAlpha() {
        return mHighLightAlpha;
    }

    public void setStackLabels(String[] labels) {
        mStackLabels = labels;
    }

    @Override
    public String[] getStackLabels() {
        return mStackLabels;
    }
}