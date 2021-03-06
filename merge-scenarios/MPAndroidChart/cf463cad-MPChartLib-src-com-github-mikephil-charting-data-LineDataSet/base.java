package com.github.mikephil.charting.data;

import android.content.Context;
import android.graphics.Color;
import android.graphics.DashPathEffect;
import android.graphics.drawable.Drawable;
import com.github.mikephil.charting.interfaces.datasets.ILineDataSet;
import com.github.mikephil.charting.utils.ColorTemplate;
import com.github.mikephil.charting.formatter.DefaultFillFormatter;
import com.github.mikephil.charting.formatter.FillFormatter;
import com.github.mikephil.charting.utils.Utils;
import java.util.ArrayList;
import java.util.List;

public class LineDataSet extends LineRadarDataSet<Entry> implements ILineDataSet {

    private List<Integer> mCircleColors = null;

    private int mCircleColorHole = Color.WHITE;

    private float mCircleRadius = 8f;

    private float mCubicIntensity = 0.2f;

    private DashPathEffect mDashPathEffect = null;

    private FillFormatter mFillFormatter = new DefaultFillFormatter();

    private boolean mDrawCircles = true;

    private boolean mDrawCubic = false;

    private boolean mDrawStepped = false;

    private boolean mDrawCircleHole = true;

    public LineDataSet(List<Entry> yVals, String label) {
        super(yVals, label);
        mCircleColors = new ArrayList<Integer>();
        mCircleColors.add(Color.rgb(140, 234, 255));
    }

    @Override
    public DataSet<Entry> copy() {
        List<Entry> yVals = new ArrayList<Entry>();
        for (int i = 0; i < mYVals.size(); i++) {
            yVals.add(mYVals.get(i).copy());
        }
        LineDataSet copied = new LineDataSet(yVals, getLabel());
        copied.mColors = mColors;
        copied.mCircleRadius = mCircleRadius;
        copied.mCircleColors = mCircleColors;
        copied.mDashPathEffect = mDashPathEffect;
        copied.mDrawCircles = mDrawCircles;
        copied.mDrawCubic = mDrawCubic;
        copied.mHighLightColor = mHighLightColor;
        return copied;
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

    public void setCircleRadius(float radius) {
        mCircleRadius = Utils.convertDpToPixel(radius);
    }

    @Override
    public float getCircleRadius() {
        return mCircleRadius;
    }

    @Deprecated
    public void setCircleSize(float size) {
        setCircleRadius(size);
    }

    @Deprecated
    public float getCircleSize() {
        return getCircleRadius();
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

    public void setDrawStepped(boolean enabled) {
        mDrawStepped = enabled;
    }

    @Override
    public boolean isDrawSteppedEnabled() {
        return mDrawStepped;
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