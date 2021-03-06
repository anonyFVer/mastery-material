package com.github.mikephil.charting.data;

import android.graphics.Typeface;
import android.util.Log;
import com.github.mikephil.charting.components.YAxis.AxisDependency;
import com.github.mikephil.charting.formatter.ValueFormatter;
import com.github.mikephil.charting.highlight.Highlight;
import com.github.mikephil.charting.interfaces.datasets.IDataSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class ChartData<T extends IDataSet<? extends Entry>> {

    protected float mYMax = 0.0f;

    protected float mYMin = 0.0f;

    protected float mXMax = 0f;

    protected float mXMin = 0f;

    protected float mLeftAxisMax = 0.0f;

    protected float mLeftAxisMin = 0.0f;

    protected float mRightAxisMax = 0.0f;

    protected float mRightAxisMin = 0.0f;

    private int mYValCount = 0;

    private float mXValMaximumLength = 0;

    protected List<XAxisValue> mXVals;

    protected List<T> mDataSets;

    public ChartData() {
        mXVals = new ArrayList<XAxisValue>();
        mDataSets = new ArrayList<T>();
    }

    public ChartData(T... dataSets) {
        mDataSets = Arrays.asList(dataSets);
        init();
    }

    public ChartData(List<XAxisValue> xVals) {
        this.mXVals = xVals;
        this.mDataSets = new ArrayList<T>();
        init();
    }

    public ChartData(XAxisValue[] xVals) {
        this.mXVals = arrayToList(xVals);
        this.mDataSets = new ArrayList<T>();
        init();
    }

    public ChartData(List<XAxisValue> xVals, List<T> sets) {
        this.mXVals = xVals;
        this.mDataSets = sets;
        init();
    }

    public ChartData(XAxisValue[] xVals, List<T> sets) {
        this.mXVals = arrayToList(xVals);
        this.mDataSets = sets;
        init();
    }

    private List<XAxisValue> arrayToList(XAxisValue[] array) {
        return Arrays.asList(array);
    }

    protected void init() {
        checkLegal();
        calcYValueCount();
        calcMinMax(0, mYValCount);
        calcXValMaximumLength();
    }

    private void calcXValMaximumLength() {
        if (mXVals.size() <= 0) {
            mXValMaximumLength = 1;
            return;
        }
        int max = 1;
        for (int i = 0; i < mXVals.size(); i++) {
            int length = mXVals.get(i).getLabel().length();
            if (length > max)
                max = length;
        }
        mXValMaximumLength = max;
    }

    private void checkLegal() {
        if (mDataSets == null)
            return;
        if (this instanceof ScatterData || this instanceof CombinedData)
            return;
        for (int i = 0; i < mDataSets.size(); i++) {
            if (mDataSets.get(i).getEntryCount() > mXVals.size()) {
            }
        }
    }

    public void notifyDataChanged() {
        init();
    }

    public void calcMinMax(int start, int end) {
        if (mDataSets == null || mDataSets.size() < 1) {
            mYMax = 0f;
            mYMin = 0f;
            mXMax = 0f;
            mXMin = 0f;
        } else {
            mYMin = Float.MAX_VALUE;
            mYMax = -Float.MAX_VALUE;
            mXMin = Float.MAX_VALUE;
            mXMax = -Float.MAX_VALUE;
            for (int i = 0; i < mDataSets.size(); i++) {
                IDataSet set = mDataSets.get(i);
                set.calcMinMax(start, end);
                if (set.getYMin() < mYMin)
                    mYMin = set.getYMin();
                if (set.getYMax() > mYMax)
                    mYMax = set.getYMax();
                if (set.getXMin() < mXMin)
                    mXMin = set.getXMin();
                if (set.getXMax() > mXMax)
                    mXMax = set.getXMax();
            }
            if (mYMin == Float.MAX_VALUE) {
                mYMin = 0.f;
                mYMax = 0.f;
            }
            T firstLeft = getFirstLeft();
            if (firstLeft != null) {
                mLeftAxisMax = firstLeft.getYMax();
                mLeftAxisMin = firstLeft.getYMin();
                for (IDataSet dataSet : mDataSets) {
                    if (dataSet.getAxisDependency() == AxisDependency.LEFT) {
                        if (dataSet.getYMin() < mLeftAxisMin)
                            mLeftAxisMin = dataSet.getYMin();
                        if (dataSet.getYMax() > mLeftAxisMax)
                            mLeftAxisMax = dataSet.getYMax();
                    }
                }
            }
            T firstRight = getFirstRight();
            if (firstRight != null) {
                mRightAxisMax = firstRight.getYMax();
                mRightAxisMin = firstRight.getYMin();
                for (IDataSet dataSet : mDataSets) {
                    if (dataSet.getAxisDependency() == AxisDependency.RIGHT) {
                        if (dataSet.getYMin() < mRightAxisMin)
                            mRightAxisMin = dataSet.getYMin();
                        if (dataSet.getYMax() > mRightAxisMax)
                            mRightAxisMax = dataSet.getYMax();
                    }
                }
            }
            handleEmptyAxis(firstLeft, firstRight);
        }
    }

    protected void calcYValueCount() {
        mYValCount = 0;
        if (mDataSets == null)
            return;
        int count = 0;
        for (int i = 0; i < mDataSets.size(); i++) {
            count += mDataSets.get(i).getEntryCount();
        }
        mYValCount = count;
    }

    public int getDataSetCount() {
        if (mDataSets == null)
            return 0;
        return mDataSets.size();
    }

    public float getYMin() {
        return mYMin;
    }

    public float getYMin(AxisDependency axis) {
        if (axis == AxisDependency.LEFT)
            return mLeftAxisMin;
        else
            return mRightAxisMin;
    }

    public float getYMax() {
        return mYMax;
    }

    public float getYMax(AxisDependency axis) {
        if (axis == AxisDependency.LEFT)
            return mLeftAxisMax;
        else
            return mRightAxisMax;
    }

    public float getXMin() {
        return mXMin;
    }

    public float getXMax() {
        return mXMax;
    }

    public float getXValMaximumLength() {
        return mXValMaximumLength;
    }

    public int getYValCount() {
        return mYValCount;
    }

    public List<XAxisValue> getXVals() {
        return mXVals;
    }

    public void addXValue(XAxisValue xVal) {
        if (xVal != null && xVal.getLabel().length() > mXValMaximumLength)
            mXValMaximumLength = xVal.getLabel().length();
        mXVals.add(xVal);
    }

    public void removeXValue(int index) {
        mXVals.remove(index);
    }

    public List<T> getDataSets() {
        return mDataSets;
    }

    protected int getDataSetIndexByLabel(List<T> dataSets, String label, boolean ignorecase) {
        if (ignorecase) {
            for (int i = 0; i < dataSets.size(); i++) if (label.equalsIgnoreCase(dataSets.get(i).getLabel()))
                return i;
        } else {
            for (int i = 0; i < dataSets.size(); i++) if (label.equals(dataSets.get(i).getLabel()))
                return i;
        }
        return -1;
    }

    public int getXValCount() {
        return mXVals.size();
    }

    protected String[] getDataSetLabels() {
        String[] types = new String[mDataSets.size()];
        for (int i = 0; i < mDataSets.size(); i++) {
            types[i] = mDataSets.get(i).getLabel();
        }
        return types;
    }

    public Entry getEntryForHighlight(Highlight highlight) {
        if (highlight.getDataSetIndex() >= mDataSets.size())
            return null;
        else
            return mDataSets.get(highlight.getDataSetIndex()).getEntryForXIndex(highlight.getXIndex());
    }

    public T getDataSetByLabel(String label, boolean ignorecase) {
        int index = getDataSetIndexByLabel(mDataSets, label, ignorecase);
        if (index < 0 || index >= mDataSets.size())
            return null;
        else
            return mDataSets.get(index);
    }

    public T getDataSetByIndex(int index) {
        if (mDataSets == null || index < 0 || index >= mDataSets.size())
            return null;
        return mDataSets.get(index);
    }

    public void addDataSet(T d) {
        if (d == null)
            return;
        mYValCount += d.getEntryCount();
        if (mDataSets.size() <= 0) {
            mYMax = d.getYMax();
            mYMin = d.getYMin();
            if (d.getAxisDependency() == AxisDependency.LEFT) {
                mLeftAxisMax = d.getYMax();
                mLeftAxisMin = d.getYMin();
            } else {
                mRightAxisMax = d.getYMax();
                mRightAxisMin = d.getYMin();
            }
        } else {
            if (mYMax < d.getYMax())
                mYMax = d.getYMax();
            if (mYMin > d.getYMin())
                mYMin = d.getYMin();
            if (d.getAxisDependency() == AxisDependency.LEFT) {
                if (mLeftAxisMax < d.getYMax())
                    mLeftAxisMax = d.getYMax();
                if (mLeftAxisMin > d.getYMin())
                    mLeftAxisMin = d.getYMin();
            } else {
                if (mRightAxisMax < d.getYMax())
                    mRightAxisMax = d.getYMax();
                if (mRightAxisMin > d.getYMin())
                    mRightAxisMin = d.getYMin();
            }
        }
        mDataSets.add(d);
        handleEmptyAxis(getFirstLeft(), getFirstRight());
    }

    private void handleEmptyAxis(T firstLeft, T firstRight) {
        if (firstLeft == null) {
            mLeftAxisMax = mRightAxisMax;
            mLeftAxisMin = mRightAxisMin;
        } else if (firstRight == null) {
            mRightAxisMax = mLeftAxisMax;
            mRightAxisMin = mLeftAxisMin;
        }
    }

    public boolean removeDataSet(T d) {
        if (d == null)
            return false;
        boolean removed = mDataSets.remove(d);
        if (removed) {
            mYValCount -= d.getEntryCount();
            calcMinMax(0, mYValCount);
        }
        return removed;
    }

    public boolean removeDataSet(int index) {
        if (index >= mDataSets.size() || index < 0)
            return false;
        T set = mDataSets.get(index);
        return removeDataSet(set);
    }

    public void addEntry(Entry e, int dataSetIndex) {
        if (mDataSets.size() > dataSetIndex && dataSetIndex >= 0) {
            IDataSet set = mDataSets.get(dataSetIndex);
            if (!set.addEntry(e))
                return;
            float val = e.getVal();
            if (mYValCount == 0) {
                mYMin = val;
                mYMax = val;
                if (set.getAxisDependency() == AxisDependency.LEFT) {
                    mLeftAxisMax = e.getVal();
                    mLeftAxisMin = e.getVal();
                } else {
                    mRightAxisMax = e.getVal();
                    mRightAxisMin = e.getVal();
                }
            } else {
                if (mYMax < val)
                    mYMax = val;
                if (mYMin > val)
                    mYMin = val;
                if (set.getAxisDependency() == AxisDependency.LEFT) {
                    if (mLeftAxisMax < e.getVal())
                        mLeftAxisMax = e.getVal();
                    if (mLeftAxisMin > e.getVal())
                        mLeftAxisMin = e.getVal();
                } else {
                    if (mRightAxisMax < e.getVal())
                        mRightAxisMax = e.getVal();
                    if (mRightAxisMin > e.getVal())
                        mRightAxisMin = e.getVal();
                }
            }
            mYValCount += 1;
            handleEmptyAxis(getFirstLeft(), getFirstRight());
        } else {
            Log.e("addEntry", "Cannot add Entry because dataSetIndex too high or too low.");
        }
    }

    public boolean removeEntry(Entry e, int dataSetIndex) {
        if (e == null || dataSetIndex >= mDataSets.size())
            return false;
        IDataSet set = mDataSets.get(dataSetIndex);
        if (set != null) {
            boolean removed = set.removeEntry(e);
            if (removed) {
                mYValCount -= 1;
                calcMinMax(0, mYValCount);
            }
            return removed;
        } else
            return false;
    }

    public boolean removeEntry(int xIndex, int dataSetIndex) {
        if (dataSetIndex >= mDataSets.size())
            return false;
        IDataSet dataSet = mDataSets.get(dataSetIndex);
        Entry e = dataSet.getEntryForXIndex(xIndex);
        if (e == null || e.getXIndex() != xIndex)
            return false;
        return removeEntry(e, dataSetIndex);
    }

    public T getDataSetForEntry(Entry e) {
        if (e == null)
            return null;
        for (int i = 0; i < mDataSets.size(); i++) {
            T set = mDataSets.get(i);
            for (int j = 0; j < set.getEntryCount(); j++) {
                if (e.equalTo(set.getEntryForXIndex(e.getXIndex())))
                    return set;
            }
        }
        return null;
    }

    public int[] getColors() {
        if (mDataSets == null)
            return null;
        int clrcnt = 0;
        for (int i = 0; i < mDataSets.size(); i++) {
            clrcnt += mDataSets.get(i).getColors().size();
        }
        int[] colors = new int[clrcnt];
        int cnt = 0;
        for (int i = 0; i < mDataSets.size(); i++) {
            List<Integer> clrs = mDataSets.get(i).getColors();
            for (Integer clr : clrs) {
                colors[cnt] = clr;
                cnt++;
            }
        }
        return colors;
    }

    public int getIndexOfDataSet(T dataSet) {
        for (int i = 0; i < mDataSets.size(); i++) {
            if (mDataSets.get(i) == dataSet)
                return i;
        }
        return -1;
    }

    public T getFirstLeft() {
        for (T dataSet : mDataSets) {
            if (dataSet.getAxisDependency() == AxisDependency.LEFT)
                return dataSet;
        }
        return null;
    }

    public T getFirstRight() {
        for (T dataSet : mDataSets) {
            if (dataSet.getAxisDependency() == AxisDependency.RIGHT)
                return dataSet;
        }
        return null;
    }

    public static List<XAxisValue> generateXVals(int from, int to) {
        List<XAxisValue> xvals = new ArrayList<XAxisValue>();
        for (int i = from; i < to; i++) {
            xvals.add(new XAxisValue(i, i + ""));
        }
        return xvals;
    }

    public void setValueFormatter(ValueFormatter f) {
        if (f == null)
            return;
        else {
            for (IDataSet set : mDataSets) {
                set.setValueFormatter(f);
            }
        }
    }

    public void setValueTextColor(int color) {
        for (IDataSet set : mDataSets) {
            set.setValueTextColor(color);
        }
    }

    public void setValueTextColors(List<Integer> colors) {
        for (IDataSet set : mDataSets) {
            set.setValueTextColors(colors);
        }
    }

    public void setValueTypeface(Typeface tf) {
        for (IDataSet set : mDataSets) {
            set.setValueTypeface(tf);
        }
    }

    public void setValueTextSize(float size) {
        for (IDataSet set : mDataSets) {
            set.setValueTextSize(size);
        }
    }

    public void setDrawValues(boolean enabled) {
        for (IDataSet set : mDataSets) {
            set.setDrawValues(enabled);
        }
    }

    public void setHighlightEnabled(boolean enabled) {
        for (IDataSet set : mDataSets) {
            set.setHighlightEnabled(enabled);
        }
    }

    public boolean isHighlightEnabled() {
        for (IDataSet set : mDataSets) {
            if (!set.isHighlightEnabled())
                return false;
        }
        return true;
    }

    public void clearValues() {
        mDataSets.clear();
        notifyDataChanged();
    }

    public boolean contains(T dataSet) {
        for (T set : mDataSets) {
            if (set.equals(dataSet))
                return true;
        }
        return false;
    }
}