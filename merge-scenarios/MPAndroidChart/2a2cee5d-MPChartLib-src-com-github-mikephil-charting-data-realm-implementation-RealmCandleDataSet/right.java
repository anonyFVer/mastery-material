package com.github.mikephil.charting.data.realm.implementation;

import android.graphics.Paint;
import com.github.mikephil.charting.data.CandleEntry;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.realm.base.RealmLineScatterCandleRadarDataSet;
import com.github.mikephil.charting.interfaces.datasets.ICandleDataSet;
import com.github.mikephil.charting.utils.ColorTemplate;
import com.github.mikephil.charting.utils.Utils;
import io.realm.DynamicRealmObject;
import io.realm.RealmObject;
import io.realm.RealmResults;

public class RealmCandleDataSet<T extends RealmObject> extends RealmLineScatterCandleRadarDataSet<T, CandleEntry> implements ICandleDataSet {

    private String mHighField;

    private String mLowField;

    private String mOpenField;

    private String mCloseField;

    private float mShadowWidth = 3f;

    private boolean mShowCandleBar = true;

    private float mBarSpace = 0.1f;

    private boolean mShadowColorSameAsCandle = false;

    protected Paint.Style mIncreasingPaintStyle = Paint.Style.STROKE;

    protected Paint.Style mDecreasingPaintStyle = Paint.Style.FILL;

    protected int mNeutralColor = ColorTemplate.COLOR_NONE;

    protected int mIncreasingColor = ColorTemplate.COLOR_NONE;

    protected int mDecreasingColor = ColorTemplate.COLOR_NONE;

    protected int mShadowColor = ColorTemplate.COLOR_NONE;

    public RealmCandleDataSet(RealmResults<T> result, String highField, String lowField, String openField, String closeField) {
        super(result, null);
        this.mHighField = highField;
        this.mLowField = lowField;
        this.mOpenField = openField;
        this.mCloseField = closeField;
        build(this.results);
        calcMinMax(0, this.results.size());
    }

    public RealmCandleDataSet(RealmResults<T> result, String highField, String lowField, String openField, String closeField, String xIndexField) {
        super(result, null, xIndexField);
        this.mHighField = highField;
        this.mLowField = lowField;
        this.mOpenField = openField;
        this.mCloseField = closeField;
        build(this.results);
        calcMinMax(0, this.results.size());
    }

    public CandleEntry buildEntryFromResultObject(T realmObject, int xIndex) {
        DynamicRealmObject dynamicObject = new DynamicRealmObject(realmObject);
        return new CandleEntry(mIndexField == null ? xIndex : dynamicObject.getInt(mIndexField), dynamicObject.getFloat(mHighField), dynamicObject.getFloat(mLowField), dynamicObject.getFloat(mOpenField), dynamicObject.getFloat(mCloseField));
    }

    @Override
    public void calcMinMax(int start, int end) {
        if (mValues == null)
            return;
        if (mValues.size() == 0)
            return;
        int endValue;
        if (end == 0 || end >= mValues.size())
            endValue = mValues.size() - 1;
        else
            endValue = end;
        mYMin = Float.MAX_VALUE;
        mYMax = -Float.MAX_VALUE;
        for (int i = start; i <= endValue; i++) {
            CandleEntry e = mValues.get(i);
            if (e.getLow() < mYMin)
                mYMin = e.getLow();
            if (e.getHigh() > mYMax)
                mYMax = e.getHigh();
        }
    }

    public void setBarSpace(float space) {
        if (space < 0f)
            space = 0f;
        if (space > 0.45f)
            space = 0.45f;
        mBarSpace = space;
    }

    @Override
    public float getBarSpace() {
        return mBarSpace;
    }

    public void setShadowWidth(float width) {
        mShadowWidth = Utils.convertDpToPixel(width);
    }

    @Override
    public float getShadowWidth() {
        return mShadowWidth;
    }

    public void setShowCandleBar(boolean showCandleBar) {
        mShowCandleBar = showCandleBar;
    }

    @Override
    public boolean getShowCandleBar() {
        return mShowCandleBar;
    }

    public void setNeutralColor(int color) {
        mNeutralColor = color;
    }

    @Override
    public int getNeutralColor() {
        return mNeutralColor;
    }

    public void setIncreasingColor(int color) {
        mIncreasingColor = color;
    }

    @Override
    public int getIncreasingColor() {
        return mIncreasingColor;
    }

    public void setDecreasingColor(int color) {
        mDecreasingColor = color;
    }

    @Override
    public int getDecreasingColor() {
        return mDecreasingColor;
    }

    @Override
    public Paint.Style getIncreasingPaintStyle() {
        return mIncreasingPaintStyle;
    }

    public void setIncreasingPaintStyle(Paint.Style paintStyle) {
        this.mIncreasingPaintStyle = paintStyle;
    }

    @Override
    public Paint.Style getDecreasingPaintStyle() {
        return mDecreasingPaintStyle;
    }

    public void setDecreasingPaintStyle(Paint.Style decreasingPaintStyle) {
        this.mDecreasingPaintStyle = decreasingPaintStyle;
    }

    @Override
    public int getShadowColor() {
        return mShadowColor;
    }

    public void setShadowColor(int shadowColor) {
        this.mShadowColor = shadowColor;
    }

    @Override
    public boolean getShadowColorSameAsCandle() {
        return mShadowColorSameAsCandle;
    }

    public void setShadowColorSameAsCandle(boolean shadowColorSameAsCandle) {
        this.mShadowColorSameAsCandle = shadowColorSameAsCandle;
    }
}