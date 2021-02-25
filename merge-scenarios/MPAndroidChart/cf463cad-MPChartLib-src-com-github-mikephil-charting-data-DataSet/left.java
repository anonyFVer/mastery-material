package com.github.mikephil.charting.data;

import java.util.ArrayList;
import java.util.List;

public abstract class DataSet<T extends Entry> extends BaseDataSet<T> {

    protected List<T> mValues = null;

    protected float mYMax = 0.0f;

    protected float mYMin = 0.0f;

    protected float mXMax = 0.0f;

    protected float mXMin = 0.0f;

    public DataSet(List<T> yVals, String label) {
        super(label);
        this.mValues = yVals;
        if (mValues == null)
            mValues = new ArrayList<T>();
        calcMinMax(0, mValues.size());
    }

    @Override
    public void calcMinMax(int start, int end) {
        if (mValues == null)
            return;
        final int yValCount = mValues.size();
        if (yValCount == 0)
            return;
        int endValue;
        if (end == 0 || end >= yValCount)
            endValue = yValCount - 1;
        else
            endValue = end;
        mYMin = Float.MAX_VALUE;
        mYMax = -Float.MAX_VALUE;
        mXMin = Float.MAX_VALUE;
        mXMax = -Float.MAX_VALUE;
        for (int i = start; i <= endValue; i++) {
            T e = mValues.get(i);
            if (e != null && !Float.isNaN(e.getVal())) {
                if (e.getVal() < mYMin)
                    mYMin = e.getVal();
                if (e.getVal() > mYMax)
                    mYMax = e.getVal();
                if (e.getXIndex() < mXMin)
                    mXMin = e.getXIndex();
                if (e.getXIndex() > mXMax)
                    mXMax = e.getXIndex();
            }
        }
        if (mYMin == Float.MAX_VALUE) {
            mYMin = 0.f;
            mYMax = 0.f;
        }
    }

    @Override
    public int getEntryCount() {
        return mValues.size();
    }

    public List<T> getYVals() {
        return mValues;
    }

    public abstract DataSet<T> copy();

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append(toSimpleString());
        for (int i = 0; i < mValues.size(); i++) {
            buffer.append(mValues.get(i).toString() + " ");
        }
        return buffer.toString();
    }

    public String toSimpleString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("DataSet, label: " + (getLabel() == null ? "" : getLabel()) + ", entries: " + mValues.size() + "\n");
        return buffer.toString();
    }

    @Override
    public float getYMin() {
        return mYMin;
    }

    @Override
    public float getYMax() {
        return mYMax;
    }

    @Override
    public float getXMin() {
        return mXMin;
    }

    @Override
    public float getXMax() {
        return mXMax;
    }

    @Override
    public void addEntryOrdered(T e) {
        if (e == null)
            return;
        float val = e.getVal();
        if (mValues == null) {
            mValues = new ArrayList<T>();
        }
        if (mValues.size() == 0) {
            mYMax = val;
            mYMin = val;
        } else {
            if (mYMax < val)
                mYMax = val;
            if (mYMin > val)
                mYMin = val;
        }
        if (mValues.size() > 0 && mValues.get(mValues.size() - 1).getXIndex() > e.getXIndex()) {
            int closestIndex = getEntryIndex(e.getXIndex(), Rounding.UP);
            mValues.add(closestIndex, e);
            return;
        }
        mValues.add(e);
    }

    @Override
    public void clear() {
        mValues.clear();
        notifyDataSetChanged();
    }

    @Override
    public boolean addEntry(T e) {
        if (e == null)
            return false;
        float val = e.getVal();
        List<T> yVals = getYVals();
        if (yVals == null) {
            yVals = new ArrayList<T>();
        }
        if (yVals.size() == 0) {
            mYMax = val;
            mYMin = val;
        } else {
            if (mYMax < val)
                mYMax = val;
            if (mYMin > val)
                mYMin = val;
        }
        yVals.add(e);
        return true;
    }

    @Override
    public boolean removeEntry(T e) {
        if (e == null)
            return false;
        if (mValues == null)
            return false;
        boolean removed = mValues.remove(e);
        if (removed) {
            calcMinMax(0, mValues.size());
        }
        return removed;
    }

    @Override
    public int getEntryIndex(Entry e) {
        return mValues.indexOf(e);
    }

    @Override
    public T getEntryForXIndex(int xIndex, Rounding rounding) {
        int index = getEntryIndex(xIndex, rounding);
        if (index > -1)
            return mValues.get(index);
        return null;
    }

    @Override
    public T getEntryForXIndex(int xIndex) {
        return getEntryForXIndex(xIndex, Rounding.CLOSEST);
    }

    @Override
    public T getEntryForIndex(int index) {
        return mValues.get(index);
    }

    @Override
    public int getEntryIndex(int xIndex, Rounding rounding) {
        int low = 0;
        int high = mValues.size() - 1;
        int closest = -1;
        while (low <= high) {
            int m = (high + low) / 2;
            if (xIndex == mValues.get(m).getXIndex()) {
                while (m > 0 && mValues.get(m - 1).getXIndex() == xIndex) m--;
                return m;
            }
            if (xIndex > mValues.get(m).getXIndex())
                low = m + 1;
            else
                high = m - 1;
            closest = m;
        }
        if (closest != -1) {
            int closestXIndex = mValues.get(closest).getXIndex();
            if (rounding == Rounding.UP) {
                if (closestXIndex < xIndex && closest < mValues.size() - 1) {
                    ++closest;
                }
            } else if (rounding == Rounding.DOWN) {
                if (closestXIndex > xIndex && closest > 0) {
                    --closest;
                }
            }
        }
        return closest;
    }

    @Override
    public float getYValForXIndex(int xIndex) {
        Entry e = getEntryForXIndex(xIndex);
        if (e != null && e.getXIndex() == xIndex)
            return e.getVal();
        else
            return Float.NaN;
    }

    public List<T> getEntriesForXIndex(int xIndex) {
        List<T> entries = new ArrayList<T>();
        int low = 0;
        int high = mValues.size() - 1;
        while (low <= high) {
            int m = (high + low) / 2;
            T entry = mValues.get(m);
            if (xIndex == entry.getXIndex()) {
                while (m > 0 && mValues.get(m - 1).getXIndex() == xIndex) m--;
                high = mValues.size();
                for (; m < high; m++) {
                    entry = mValues.get(m);
                    if (entry.getXIndex() == xIndex) {
                        entries.add(entry);
                    } else {
                        break;
                    }
                }
            }
            if (xIndex > entry.getXIndex())
                low = m + 1;
            else
                high = m - 1;
        }
        return entries;
    }

    public enum Rounding {

        UP, DOWN, CLOSEST
    }
}