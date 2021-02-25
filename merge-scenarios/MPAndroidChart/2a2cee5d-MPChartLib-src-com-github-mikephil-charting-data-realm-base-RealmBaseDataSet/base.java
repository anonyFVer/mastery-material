package com.github.mikephil.charting.data.realm.base;

import com.github.mikephil.charting.data.BaseDataSet;
import com.github.mikephil.charting.data.DataSet;
import com.github.mikephil.charting.data.Entry;
import java.util.ArrayList;
import java.util.List;
import io.realm.RealmObject;
import io.realm.RealmResults;
import io.realm.Sort;

public abstract class RealmBaseDataSet<T extends RealmObject, S extends Entry> extends BaseDataSet<S> {

    protected RealmResults<T> results;

    protected List<S> mValues;

    protected float mYMax = 0.0f;

    protected float mYMin = 0.0f;

    protected String mValuesField;

    protected String mIndexField;

    public RealmBaseDataSet(RealmResults<T> results, String yValuesField) {
        this.results = results;
        this.mValuesField = yValuesField;
        this.mValues = new ArrayList<S>();
        if (mIndexField != null)
            this.results.sort(mIndexField, Sort.ASCENDING);
    }

    public RealmBaseDataSet(RealmResults<T> results, String yValuesField, String xIndexField) {
        this.results = results;
        this.mValuesField = yValuesField;
        this.mIndexField = xIndexField;
        this.mValues = new ArrayList<S>();
        if (mIndexField != null)
            this.results.sort(mIndexField, Sort.ASCENDING);
    }

    public abstract void build(RealmResults<T> results);

    @Override
    public float getYMin() {
        return mYMin;
    }

    @Override
    public float getYMax() {
        return mYMax;
    }

    @Override
    public int getEntryCount() {
        return mValues.size();
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
        for (int i = start; i <= endValue; i++) {
            S e = mValues.get(i);
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
    public S getEntryForXIndex(int xIndex) {
        return getEntryForXIndex(xIndex, DataSet.Rounding.CLOSEST);
    }

    @Override
    public S getEntryForXIndex(int xIndex, DataSet.Rounding rounding) {
        int index = getEntryIndex(xIndex, rounding);
        if (index > -1)
            return mValues.get(index);
        return null;
    }

    @Override
    public S getEntryForIndex(int index) {
        return mValues.get(index);
    }

    @Override
    public int getEntryIndex(int x, DataSet.Rounding rounding) {
        int low = 0;
        int high = mValues.size() - 1;
        int closest = -1;
        while (low <= high) {
            int m = (high + low) / 2;
            if (x == mValues.get(m).getXIndex()) {
                while (m > 0 && mValues.get(m - 1).getXIndex() == x) m--;
                return m;
            }
            if (x > mValues.get(m).getXIndex())
                low = m + 1;
            else
                high = m - 1;
            closest = m;
        }
        if (closest != -1) {
            int closestXIndex = mValues.get(closest).getXIndex();
            if (rounding == DataSet.Rounding.UP) {
                if (closestXIndex < x && closest < mValues.size() - 1) {
                    ++closest;
                }
            } else if (rounding == DataSet.Rounding.DOWN) {
                if (closestXIndex > x && closest > 0) {
                    --closest;
                }
            }
        }
        return closest;
    }

    @Override
    public int getEntryIndex(S e) {
        return mValues.indexOf(e);
    }

    @Override
    public float getYValForXIndex(int xIndex) {
        Entry e = getEntryForXIndex(xIndex);
        if (e != null && e.getXIndex() == xIndex)
            return e.getVal();
        else
            return Float.NaN;
    }

    @Override
    public boolean addEntry(S e) {
        if (e == null)
            return false;
        float val = e.getVal();
        if (mValues == null) {
            mValues = new ArrayList<S>();
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
        mValues.add(e);
        return true;
    }

    @Override
    public boolean removeEntry(S e) {
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
    public void addEntryOrdered(S e) {
        if (e == null)
            return;
        float val = e.getVal();
        if (mValues == null) {
            mValues = new ArrayList<S>();
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
            int closestIndex = getEntryIndex(e.getXIndex(), DataSet.Rounding.UP);
            mValues.add(closestIndex, e);
            return;
        }
        mValues.add(e);
    }

    public List<S> getValues() {
        return mValues;
    }

    @Override
    public void clear() {
        mValues.clear();
        notifyDataSetChanged();
    }

    public RealmResults<T> getResults() {
        return results;
    }

    public String getValuesField() {
        return mValuesField;
    }

    public void setValuesField(String yValuesField) {
        this.mValuesField = yValuesField;
    }

    public String getIndexField() {
        return mIndexField;
    }

    public void setIndexField(String xIndexField) {
        this.mIndexField = xIndexField;
    }
}