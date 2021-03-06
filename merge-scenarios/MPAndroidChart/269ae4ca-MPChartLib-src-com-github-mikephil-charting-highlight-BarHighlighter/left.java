package com.github.mikephil.charting.highlight;

import com.github.mikephil.charting.components.YAxis;
import com.github.mikephil.charting.data.BarDataSet;
import com.github.mikephil.charting.data.BarEntry;
import com.github.mikephil.charting.interfaces.datainterfaces.datasets.IBarDataSet;
import com.github.mikephil.charting.interfaces.dataprovider.BarDataProvider;

public class BarHighlighter extends ChartHighlighter<BarDataProvider> {

    public BarHighlighter(BarDataProvider chart) {
        super(chart);
    }

    @Override
    public Highlight getHighlight(float x, float y) {
        Highlight h = super.getHighlight(x, y);
        if (h == null)
            return h;
        else {
            IBarDataSet set = mChart.getBarData().getDataSetByIndex(h.getDataSetIndex());
            if (set.isStacked()) {
                float[] pts = new float[2];
                pts[1] = y;
                mChart.getTransformer(set.getAxisDependency()).pixelsToValue(pts);
                return getStackedHighlight(h, set, h.getXIndex(), h.getDataSetIndex(), pts[1]);
            } else
                return h;
        }
    }

    @Override
    protected int getXIndex(float x) {
        if (!mChart.getBarData().isGrouped()) {
            return super.getXIndex(x);
        } else {
            float baseNoSpace = getBase(x);
            int setCount = mChart.getBarData().getDataSetCount();
            int xIndex = (int) baseNoSpace / setCount;
            int valCount = mChart.getData().getXValCount();
            if (xIndex < 0)
                xIndex = 0;
            else if (xIndex >= valCount)
                xIndex = valCount - 1;
            return xIndex;
        }
    }

    @Override
    protected int getDataSetIndex(int xIndex, float x, float y) {
        if (!mChart.getBarData().isGrouped()) {
            return 0;
        } else {
            float baseNoSpace = getBase(x);
            int setCount = mChart.getBarData().getDataSetCount();
            int dataSetIndex = (int) baseNoSpace % setCount;
            if (dataSetIndex < 0)
                dataSetIndex = 0;
            else if (dataSetIndex >= setCount)
                dataSetIndex = setCount - 1;
            return dataSetIndex;
        }
    }

    protected Highlight getStackedHighlight(Highlight old, IBarDataSet set, int xIndex, int dataSetIndex, double yValue) {
        BarEntry entry = set.getEntryForXIndex(xIndex);
        if (entry == null || entry.getVals() == null)
            return old;
        Range[] ranges = getRanges(entry);
        int stackIndex = getClosestStackIndex(ranges, (float) yValue);
        Highlight h = new Highlight(xIndex, dataSetIndex, stackIndex, ranges[stackIndex]);
        return h;
    }

    protected int getClosestStackIndex(Range[] ranges, float value) {
        if (ranges == null)
            return 0;
        int stackIndex = 0;
        for (Range range : ranges) {
            if (range.contains(value))
                return stackIndex;
            else
                stackIndex++;
        }
        int length = ranges.length - 1;
        return (value > ranges[length].to) ? length : 0;
    }

    protected float getBase(float x) {
        float[] pts = new float[2];
        pts[0] = x;
        mChart.getTransformer(YAxis.AxisDependency.LEFT).pixelsToValue(pts);
        float xVal = pts[0];
        int setCount = mChart.getBarData().getDataSetCount();
        int steps = (int) ((float) xVal / ((float) setCount + mChart.getBarData().getGroupSpace()));
        float groupSpaceSum = mChart.getBarData().getGroupSpace() * (float) steps;
        float baseNoSpace = (float) xVal - groupSpaceSum;
        return baseNoSpace;
    }

    protected Range[] getRanges(BarEntry entry) {
        float[] values = entry.getVals();
        if (values == null)
            return null;
        float negRemain = -entry.getNegativeSum();
        float posRemain = 0f;
        Range[] ranges = new Range[values.length];
        for (int i = 0; i < ranges.length; i++) {
            float value = values[i];
            if (value < 0) {
                ranges[i] = new Range(negRemain, negRemain + Math.abs(value));
                negRemain += Math.abs(value);
            } else {
                ranges[i] = new Range(posRemain, posRemain + value);
                posRemain += value;
            }
        }
        return ranges;
    }
}