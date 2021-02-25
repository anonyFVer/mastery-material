package com.github.mikephil.charting.data.realm.implementation;

import android.graphics.Color;
import com.github.mikephil.charting.data.BarEntry;
import com.github.mikephil.charting.data.realm.base.RealmBarLineScatterCandleBubbleDataSet;
import com.github.mikephil.charting.interfaces.datasets.IBarDataSet;
import io.realm.DynamicRealmObject;
import io.realm.RealmList;
import io.realm.RealmObject;
import io.realm.RealmResults;

public class RealmBarDataSet<T extends RealmObject> extends RealmBarLineScatterCandleBubbleDataSet<T, BarEntry> implements IBarDataSet {

    private String mStackValueFieldName;

    private float mBarSpace = 0.15f;

    private int mStackSize = 1;

    private int mBarShadowColor = Color.rgb(215, 215, 215);

    private float mBarBorderWidth = 0.0f;

    private int mBarBorderColor = Color.BLACK;

    private int mHighLightAlpha = 120;

    private String[] mStackLabels = new String[] { "Stack" };

    public RealmBarDataSet(RealmResults<T> results, String yValuesField, String xIndexField) {
        super(results, yValuesField, xIndexField);
        mHighLightColor = Color.rgb(0, 0, 0);
        build(this.results);
        calcMinMax(0, results.size());
    }

    public RealmBarDataSet(RealmResults<T> results, String yValuesField, String xIndexField, String stackValueFieldName) {
        super(results, yValuesField, xIndexField);
        this.mStackValueFieldName = stackValueFieldName;
        mHighLightColor = Color.rgb(0, 0, 0);
        build(this.results);
        calcMinMax(0, results.size());
    }

    @Override
    public void build(RealmResults<T> results) {
        for (T realmObject : results) {
            DynamicRealmObject dynamicObject = new DynamicRealmObject(realmObject);
            try {
                float value = dynamicObject.getFloat(mValuesField);
                mValues.add(new BarEntry(value, dynamicObject.getInt(mIndexField)));
            } catch (IllegalArgumentException e) {
                RealmList<DynamicRealmObject> list = dynamicObject.getList(mValuesField);
                float[] values = new float[list.size()];
                int i = 0;
                for (DynamicRealmObject o : list) {
                    values[i] = o.getFloat(mStackValueFieldName);
                    i++;
                }
                mValues.add(new BarEntry(values, dynamicObject.getInt(mIndexField)));
            }
        }
        calcStackSize();
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
            BarEntry e = mValues.get(i);
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

    private void calcStackSize() {
        for (int i = 0; i < mValues.size(); i++) {
            float[] vals = mValues.get(i).getVals();
            if (vals != null && vals.length > mStackSize)
                mStackSize = vals.length;
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

    public void setBarBorderWidth(float width) {
        mBarBorderWidth = width;
    }

    @Override
    public float getBarBorderWidth() {
        return mBarBorderWidth;
    }

    public void setBarBorderColor(int color) {
        mBarBorderColor = color;
    }

    @Override
    public int getBarBorderColor() {
        return mBarBorderColor;
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