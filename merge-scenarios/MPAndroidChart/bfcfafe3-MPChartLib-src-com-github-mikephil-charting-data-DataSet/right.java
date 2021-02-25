package com.github.mikephil.charting.data;

import android.content.Context;
import android.graphics.Color;
import android.graphics.Typeface;
import com.github.mikephil.charting.components.YAxis.AxisDependency;
import com.github.mikephil.charting.utils.ColorTemplate;
import com.github.mikephil.charting.formatter.DefaultValueFormatter;
import com.github.mikephil.charting.utils.Utils;
import com.github.mikephil.charting.formatter.ValueFormatter;
import java.util.ArrayList;
import java.util.List;

public abstract class DataSet<T extends Entry> {

    protected List<Integer> mColors = null;

    protected List<T> mYVals = null;

    protected float mYMax = 0.0f;

    protected float mYMin = 0.0f;

    private float mYValueSum = 0f;

    protected int mLastStart = 0;

    protected int mLastEnd = 0;

    private String mLabel = "DataSet";

    private boolean mVisible = true;

    protected boolean mDrawValues = true;

    private int mValueColor = Color.BLACK;

    private float mValueTextSize = 17f;

    private Typeface mValueTypeface;

    protected transient ValueFormatter mValueFormatter;

    protected AxisDependency mAxisDependency = AxisDependency.LEFT;

    protected boolean mHighlightEnabled = true;

    public DataSet(List<T> yVals, String label) {
        this.mLabel = label;
        this.mYVals = yVals;
        if (mYVals == null)
            mYVals = new ArrayList<T>();
        mColors = new ArrayList<Integer>();
        mColors.add(Color.rgb(140, 234, 255));
        calcMinMax(mLastStart, mLastEnd);
        calcYValueSum();
    }

    public void notifyDataSetChanged() {
        calcMinMax(mLastStart, mLastEnd);
        calcYValueSum();
    }

    protected void calcMinMax(int start, int end) {
        final int yValCount = mYVals.size();
        if (yValCount == 0)
            return;
        int endValue;
        if (end == 0 || end >= yValCount)
            endValue = yValCount - 1;
        else
            endValue = end;
        mLastStart = start;
        mLastEnd = endValue;
        mYMin = Float.MAX_VALUE;
        mYMax = -Float.MAX_VALUE;
        for (int i = start; i <= endValue; i++) {
            Entry e = mYVals.get(i);
            if (e != null && !Float.isNaN(e.getVal())) {
                if (e.getVal() < mYMin)
                    mYMin = e.getVal();
                if (e.getVal() > mYMax)
                    mYMax = e.getVal();
            }
        }
        if (mYMin == Float.MAX_VALUE) {
            mYMin = 0.f;
            mYMax = 0.f;
        }
    }

    private void calcYValueSum() {
        mYValueSum = 0;
        for (int i = 0; i < mYVals.size(); i++) {
            Entry e = mYVals.get(i);
            if (e != null)
                mYValueSum += e.getVal();
        }
    }

    public float getAverage() {
        return (float) getYValueSum() / (float) getValueCount();
    }

    public int getEntryCount() {
        return mYVals.size();
    }

    public float getYValForXIndex(int xIndex) {
        Entry e = getEntryForXIndex(xIndex);
        if (e != null && e.getXIndex() == xIndex)
            return e.getVal();
        else
            return Float.NaN;
    }

    public T getEntryForXIndex(int x) {
        return getEntryForXIndex(x, Rounding.CLOSEST);
    }

    public T getEntryForXIndex(int x, Rounding rounding) {
        int index = getEntryIndex(x, rounding);
        if (index > -1)
            return mYVals.get(index);
        return null;
    }

    public int getEntryIndex(int x, Rounding rounding) {
        int low = 0;
        int high = mYVals.size() - 1;
        int closest = -1;
        while (low <= high) {
            int m = (high + low) / 2;
            if (x == mYVals.get(m).getXIndex()) {
                while (m > 0 && mYVals.get(m - 1).getXIndex() == x) m--;
                return m;
            }
            if (x > mYVals.get(m).getXIndex())
                low = m + 1;
            else
                high = m - 1;
            closest = m;
        }
        if (closest != -1) {
            int closestXIndex = mYVals.get(closest).getXIndex();
            if (rounding == Rounding.UP) {
                if (closestXIndex < x && closest < mYVals.size() - 1) {
                    ++closest;
                }
            } else if (rounding == Rounding.DOWN) {
                if (closestXIndex > x && closest > 0) {
                    --closest;
                }
            }
        }
        return closest;
    }

    public List<T> getEntriesForXIndex(int x) {
        List<T> entries = new ArrayList<T>();
        int low = 0;
        int high = mYVals.size() - 1;
        while (low <= high) {
            int m = (high + low) / 2;
            T entry = mYVals.get(m);
            if (x == entry.getXIndex()) {
                while (m > 0 && mYVals.get(m - 1).getXIndex() == x) m--;
                high = mYVals.size();
                for (; m < high; m++) {
                    entry = mYVals.get(m);
                    if (entry.getXIndex() == x) {
                        entries.add(entry);
                    } else {
                        break;
                    }
                }
            }
            if (x > entry.getXIndex())
                low = m + 1;
            else
                high = m - 1;
        }
        return entries;
    }

    public List<T> getYVals() {
        return mYVals;
    }

    public float getYValueSum() {
        return mYValueSum;
    }

    public float getYMin() {
        return mYMin;
    }

    public float getYMax() {
        return mYMax;
    }

    public int getValueCount() {
        return mYVals.size();
    }

    public int getIndexInEntries(int xIndex) {
        for (int i = 0; i < mYVals.size(); i++) {
            if (xIndex == mYVals.get(i).getXIndex())
                return i;
        }
        return -1;
    }

    public abstract DataSet<T> copy();

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append(toSimpleString());
        for (int i = 0; i < mYVals.size(); i++) {
            buffer.append(mYVals.get(i).toString() + " ");
        }
        return buffer.toString();
    }

    public String toSimpleString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("DataSet, label: " + (mLabel == null ? "" : mLabel) + ", entries: " + mYVals.size() + "\n");
        return buffer.toString();
    }

    public void setLabel(String label) {
        mLabel = label;
    }

    public String getLabel() {
        return mLabel;
    }

    public void setVisible(boolean visible) {
        mVisible = visible;
    }

    public boolean isVisible() {
        return mVisible;
    }

    public AxisDependency getAxisDependency() {
        return mAxisDependency;
    }

    public void setAxisDependency(AxisDependency dependency) {
        mAxisDependency = dependency;
    }

    public void setDrawValues(boolean enabled) {
        this.mDrawValues = enabled;
    }

    public boolean isDrawValuesEnabled() {
        return mDrawValues;
    }

    @SuppressWarnings("unchecked")
    public void addEntry(Entry e) {
        if (e == null)
            return;
        float val = e.getVal();
        if (mYVals == null) {
            mYVals = new ArrayList<T>();
        }
        if (mYVals.size() == 0) {
            mYMax = val;
            mYMin = val;
        } else {
            if (mYMax < val)
                mYMax = val;
            if (mYMin > val)
                mYMin = val;
        }
        mYValueSum += val;
        mYVals.add((T) e);
    }

    @SuppressWarnings("unchecked")
    public void addEntryOrdered(Entry e) {
        if (e == null)
            return;
        float val = e.getVal();
        if (mYVals == null) {
            mYVals = new ArrayList<T>();
        }
        if (mYVals.size() == 0) {
            mYMax = val;
            mYMin = val;
        } else {
            if (mYMax < val)
                mYMax = val;
            if (mYMin > val)
                mYMin = val;
        }
        mYValueSum += val;
        if (mYVals.size() > 0 && mYVals.get(mYVals.size() - 1).getXIndex() > e.getXIndex()) {
            int closestIndex = getEntryIndex(e.getXIndex(), Rounding.UP);
            mYVals.add(closestIndex, (T) e);
            return;
        }
        mYVals.add((T) e);
    }

    public boolean removeEntry(T e) {
        if (e == null)
            return false;
        boolean removed = mYVals.remove(e);
        if (removed) {
            float val = e.getVal();
            mYValueSum -= val;
            calcMinMax(mLastStart, mLastEnd);
        }
        return removed;
    }

    public boolean removeEntry(int xIndex) {
        T e = getEntryForXIndex(xIndex);
        return removeEntry(e);
    }

    public boolean removeFirst() {
        T entry = mYVals.remove(0);
        boolean removed = entry != null;
        if (removed) {
            float val = entry.getVal();
            mYValueSum -= val;
            calcMinMax(mLastStart, mLastEnd);
        }
        return removed;
    }

    public boolean removeLast() {
        if (mYVals.size() <= 0)
            return false;
        T entry = mYVals.remove(mYVals.size() - 1);
        boolean removed = entry != null;
        if (removed) {
            float val = entry.getVal();
            mYValueSum -= val;
            calcMinMax(mLastStart, mLastEnd);
        }
        return removed;
    }

    public void setColors(List<Integer> colors) {
        this.mColors = colors;
    }

    public void setColors(int[] colors) {
        this.mColors = ColorTemplate.createColors(colors);
    }

    public void setColors(int[] colors, Context c) {
        List<Integer> clrs = new ArrayList<Integer>();
        for (int color : colors) {
            clrs.add(c.getResources().getColor(color));
        }
        mColors = clrs;
    }

    public void addColor(int color) {
        if (mColors == null)
            mColors = new ArrayList<Integer>();
        mColors.add(color);
    }

    public void setColor(int color) {
        resetColors();
        mColors.add(color);
    }

    public List<Integer> getColors() {
        return mColors;
    }

    public int getColor(int index) {
        return mColors.get(index % mColors.size());
    }

    public int getColor() {
        return mColors.get(0);
    }

    public void resetColors() {
        mColors = new ArrayList<Integer>();
    }

    public void setHighlightEnabled(boolean enabled) {
        mHighlightEnabled = enabled;
    }

    public boolean isHighlightEnabled() {
        return mHighlightEnabled;
    }

    public int getEntryPosition(Entry e) {
        for (int i = 0; i < mYVals.size(); i++) {
            if (e.equalTo(mYVals.get(i)))
                return i;
        }
        return -1;
    }

    public void setValueFormatter(ValueFormatter f) {
        if (f == null)
            return;
        else
            mValueFormatter = f;
    }

    public ValueFormatter getValueFormatter() {
        if (mValueFormatter == null)
            return new DefaultValueFormatter(1);
        return mValueFormatter;
    }

    public boolean needsDefaultFormatter() {
        if (mValueFormatter == null)
            return true;
        if (mValueFormatter instanceof DefaultValueFormatter)
            return true;
        return false;
    }

    public void setValueTextColor(int color) {
        mValueColor = color;
    }

    public int getValueTextColor() {
        return mValueColor;
    }

    public void setValueTypeface(Typeface tf) {
        mValueTypeface = tf;
    }

    public Typeface getValueTypeface() {
        return mValueTypeface;
    }

    public void setValueTextSize(float size) {
        mValueTextSize = Utils.convertDpToPixel(size);
    }

    public float getValueTextSize() {
        return mValueTextSize;
    }

    public boolean contains(Entry e) {
        for (Entry entry : mYVals) {
            if (entry.equals(e))
                return true;
        }
        return false;
    }

    public void clear() {
        mYVals.clear();
        mLastStart = 0;
        mLastEnd = 0;
        notifyDataSetChanged();
    }

    public enum Rounding {

        UP, DOWN, CLOSEST
    }
}