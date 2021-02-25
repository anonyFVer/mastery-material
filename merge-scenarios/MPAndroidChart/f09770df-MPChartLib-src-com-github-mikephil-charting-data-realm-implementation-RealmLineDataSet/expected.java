package com.github.mikephil.charting.data.realm.implementation;

import android.content.Context;
import android.graphics.Color;
import android.graphics.DashPathEffect;
import android.graphics.drawable.Drawable;
import com.github.mikephil.charting.data.realm.base.RealmLineRadarDataSet;
import com.github.mikephil.charting.formatter.DefaultFillFormatter;
import com.github.mikephil.charting.formatter.FillFormatter;
import com.github.mikephil.charting.interfaces.datasets.ILineDataSet;
import com.github.mikephil.charting.utils.ColorTemplate;
import com.github.mikephil.charting.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import io.realm.RealmObject;
import io.realm.RealmResults;

public class RealmLineDataSet<T extends RealmObject> extends RealmLineRadarDataSet<T> implements ILineDataSet {

    private List<Integer> mCircleColors = null;

    private int mCircleColorHole = Color.WHITE;

    private float mCircleSize = 8f;

    private float mCubicIntensity = 0.2f;

    private DashPathEffect mDashPathEffect = null;

    private FillFormatter mFillFormatter = new DefaultFillFormatter();

    private boolean mDrawCircles = true;

    private boolean mDrawCubic = false;

    private boolean mDrawCircleHole = true;

    public RealmLineDataSet(RealmResults<T> result, String yValuesField) {
        super(result, yValuesField);
        mCircleColors = new ArrayList<Integer>();
        mCircleColors.add(Color.rgb(140, 234, 255));
        build(this.results);
        calcMinMax(0, results.size());
    }

    public RealmLineDataSet(RealmResults<T> result, String yValuesField, String xIndexField) {
        super(result, yValuesField, xIndexField);
        mCircleColors = new ArrayList<Integer>();
        mCircleColors.add(Color.rgb(140, 234, 255));
        build(this.results);
        calcMinMax(0, results.size());
    }

    @Override
    public void build(RealmResults<T> results) {
        super.build(results);
    }

    public void setCubicIntensity(float intensity) {
        if (intensity > 1f)
            intensity = 1f;
        if (intensity < 0.05f)
            intensity = 0.05f;
        mCubicIntensity = intensity;
    }

    @Override
    public float getCubicIntensity() {
        return mCubicIntensity;
    }

    public void setCircleSize(float size) {
        mCircleSize = Utils.convertDpToPixel(size);
    }

    @Override
    public float getCircleRadius() {
        return mCircleSize;
    }

    public void enableDashedLine(float lineLength, float spaceLength, float phase) {
        mDashPathEffect = new DashPathEffect(new float[] { lineLength, spaceLength }, phase);
    }

    public void disableDashedLine() {
        mDashPathEffect = null;
    }

    @Override
    public boolean isDashedLineEnabled() {
        return mDashPathEffect == null ? false : true;
    }

    @Override
    public DashPathEffect getDashPathEffect() {
        return mDashPathEffect;
    }

    public void setDrawCircles(boolean enabled) {
        this.mDrawCircles = enabled;
    }

    @Override
    public boolean isDrawCirclesEnabled() {
        return mDrawCircles;
    }

    public void setDrawCubic(boolean enabled) {
        mDrawCubic = enabled;
    }

    @Override
    public boolean isDrawCubicEnabled() {
        return mDrawCubic;
    }

    public List<Integer> getCircleColors() {
        return mCircleColors;
    }

    @Override
    public int getCircleColor(int index) {
        return mCircleColors.get(index % mCircleColors.size());
    }

    public void setCircleColors(List<Integer> colors) {
        mCircleColors = colors;
    }

    public void setCircleColors(int[] colors) {
        this.mCircleColors = ColorTemplate.createColors(colors);
    }

    public void setCircleColors(int[] colors, Context c) {
        List<Integer> clrs = new ArrayList<Integer>();
        for (int color : colors) {
            clrs.add(c.getResources().getColor(color));
        }
        mCircleColors = clrs;
    }

    public void setCircleColor(int color) {
        resetCircleColors();
        mCircleColors.add(color);
    }

    public void resetCircleColors() {
        mCircleColors = new ArrayList<Integer>();
    }

    public void setCircleColorHole(int color) {
        mCircleColorHole = color;
    }

    @Override
    public int getCircleHoleColor() {
        return mCircleColorHole;
    }

    public void setDrawCircleHole(boolean enabled) {
        mDrawCircleHole = enabled;
    }

    @Override
    public boolean isDrawCircleHoleEnabled() {
        return mDrawCircleHole;
    }

    public void setFillFormatter(FillFormatter formatter) {
        if (formatter == null)
            mFillFormatter = new DefaultFillFormatter();
        else
            mFillFormatter = formatter;
    }

    @Override
    public FillFormatter getFillFormatter() {
        return mFillFormatter;
    }
}