package com.github.mikephil.charting.data;

import java.util.ArrayList;
import java.util.List;

public abstract class DataSet<T extends Entry> extends BaseDataSet<T> {

    protected List<T> mYVals = null;

    protected float mYMax = 0.0f;

    protected float mYMin = 0.0f;

    public DataSet(List<T> yVals, String label) {
        super(label);
        this.mYVals = yVals;
        if (mYVals == null)
            mYVals = new ArrayList<T>();
        calcMinMax(0, mYVals.size());
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
            T e = mYVals.get(i);
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

    @Override
    public int getEntryCount() {
        return mYVals.size();
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
        buffer.append("DataSet, label: " + (getLabel() == null ? "" : getLabel()) + ", entries: " + mYVals.size() + "\n");
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
        if (mYVals.size() > 0 && mYVals.get(mYVals.size() - 1).getXIndex() > e.getXIndex()) {
            int closestIndex = getEntryIndex(e.getXIndex(), Rounding.UP);
            mYVals.add(closestIndex, (T) e);
            return;
        }
        mYVals.add((T) e);
    }

    public boolean removeFirst() {
        T entry = mYVals.remove(0);
        boolean removed = entry != null;
        if (removed) {
            calcMinMax(0, mYVals.size());
        }
        return removed;
    }

    public boolean removeLast() {
        if (mYVals.size() <= 0)
            return false;
        T entry = mYVals.remove(mYVals.size() - 1);
        boolean removed = entry != null;
        if (removed) {
            calcMinMax(0, mYVals.size());
        }
        return removed;
    }

    public void clear() {
        mYVals.clear();
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
        if (mYVals == null)
            return false;
        boolean removed = mYVals.remove(e);
        if (removed) {
            calcMinMax(0, mYVals.size());
        }
        return removed;
    }

    public boolean removeEntry(int xIndex) {
        T e = getEntryForXIndex(xIndex);
        return removeEntry(e);
    }

    public boolean contains(Entry e) {
        List<T> values = getYVals();
        for (Entry entry : values) {
            if (entry.equals(e))
                return true;
        }
        return false;
    }

    @Override
    public int getEntryIndex(Entry e) {
        return mYVals.indexOf(e);
    }

    @Override
    public T getEntryForXIndex(int xIndex, Rounding rounding) {
        int index = getEntryIndex(xIndex, rounding);
        if (index > -1)
            return mYVals.get(index);
        return null;
    }

    @Override
    public T getEntryForXIndex(int xIndex) {
        return getEntryForXIndex(xIndex, Rounding.CLOSEST);
    }

    @Override
    public T getEntryForIndex(int index) {
        return mYVals.get(index);
    }

    @Override
    public int getEntryIndex(int xIndex, Rounding rounding) {
        int low = 0;
        int high = mYVals.size() - 1;
        int closest = -1;
        while (low <= high) {
            int m = (high + low) / 2;
            if (xIndex == mYVals.get(m).getXIndex()) {
                while (m > 0 && mYVals.get(m - 1).getXIndex() == xIndex) m--;
                return m;
            }
            if (xIndex > mYVals.get(m).getXIndex())
                low = m + 1;
            else
                high = m - 1;
            closest = m;
        }
        if (closest != -1) {
            int closestXIndex = mYVals.get(closest).getXIndex();
            if (rounding == Rounding.UP) {
                if (closestXIndex < xIndex && closest < mYVals.size() - 1) {
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

    public enum Rounding {

        UP, DOWN, CLOSEST
    }
}