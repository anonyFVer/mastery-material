package com.github.mikephil.charting.charts;

import android.content.Context;
import android.graphics.RectF;
import android.util.AttributeSet;
import android.util.Log;
import com.github.mikephil.charting.components.YAxis.AxisDependency;
import com.github.mikephil.charting.data.BarData;
import com.github.mikephil.charting.data.BarDataSet;
import com.github.mikephil.charting.data.BarEntry;
import com.github.mikephil.charting.data.DataSet;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.highlight.BarHighlighter;
import com.github.mikephil.charting.highlight.Highlight;
import com.github.mikephil.charting.interfaces.BarDataProvider;
import com.github.mikephil.charting.renderer.BarChartRenderer;
import com.github.mikephil.charting.renderer.XAxisRendererBarChart;

public class BarChart extends BarLineChartBase<BarData> implements BarDataProvider {

    private boolean mDrawHighlightArrow = false;

    private boolean mDrawValueAboveBar = true;

    private boolean mDrawBarShadow = false;

    public BarChart(Context context) {
        super(context);
    }

    public BarChart(Context context, AttributeSet attrs) {
        super(context, attrs);
    }

    public BarChart(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
    }

    @Override
    protected void init() {
        super.init();
        mRenderer = new BarChartRenderer(this, mAnimator, mViewPortHandler);
        mXAxisRenderer = new XAxisRendererBarChart(mViewPortHandler, mXAxis, mLeftAxisTransformer, this);
        mHighlighter = new BarHighlighter(this);
        mXChartMin = -0.5f;
    }

    @Override
    protected void calcMinMax() {
        super.calcMinMax();
        mDeltaX += 0.5f;
        mDeltaX *= mData.getDataSetCount();
        float groupSpace = mData.getGroupSpace();
        mDeltaX += mData.getXValCount() * groupSpace;
        mXChartMax = mDeltaX - mXChartMin;
    }

    @Override
    public Highlight getHighlightByTouchPoint(float x, float y) {
        if (mData == null) {
            Log.e(LOG_TAG, "Can't select by touch. No data set.");
            return null;
        } else
            return mHighlighter.getHighlight(x, y);
    }

    public RectF getBarBounds(BarEntry e) {
        BarDataSet set = mData.getDataSetForEntry(e);
        if (set == null)
            return null;
        float barspace = set.getBarSpace();
        float y = e.getVal();
        float x = e.getXIndex();
        float barWidth = 0.5f;
        float spaceHalf = barspace / 2f;
        float left = x - barWidth + spaceHalf;
        float right = x + barWidth - spaceHalf;
        float top = y >= 0 ? y : 0;
        float bottom = y <= 0 ? y : 0;
        RectF bounds = new RectF(left, top, right, bottom);
        getTransformer(set.getAxisDependency()).rectValueToPixel(bounds);
        return bounds;
    }

    public void setDrawHighlightArrow(boolean enabled) {
        mDrawHighlightArrow = enabled;
    }

    public boolean isDrawHighlightArrowEnabled() {
        return mDrawHighlightArrow;
    }

    public void setDrawValueAboveBar(boolean enabled) {
        mDrawValueAboveBar = enabled;
    }

    public boolean isDrawValueAboveBarEnabled() {
        return mDrawValueAboveBar;
    }

    public void setDrawBarShadow(boolean enabled) {
        mDrawBarShadow = enabled;
    }

    public boolean isDrawBarShadowEnabled() {
        return mDrawBarShadow;
    }

    @Override
    public BarData getBarData() {
        return mData;
    }

    @Override
    public int getLowestVisibleXIndex() {
        float step = mData.getDataSetCount();
        float div = (step <= 1) ? 1 : step + mData.getGroupSpace();
        float[] pts = new float[] { mViewPortHandler.contentLeft(), mViewPortHandler.contentBottom() };
        getTransformer(AxisDependency.LEFT).pixelsToValue(pts);
        return (int) ((pts[0] <= getXChartMin()) ? 0 : (pts[0] / div) + 1);
    }

    @Override
    public int getHighestVisibleXIndex() {
        float step = mData.getDataSetCount();
        float div = (step <= 1) ? 1 : step + mData.getGroupSpace();
        float[] pts = new float[] { mViewPortHandler.contentRight(), mViewPortHandler.contentBottom() };
        getTransformer(AxisDependency.LEFT).pixelsToValue(pts);
        return (int) ((pts[0] >= getXChartMax()) ? getXChartMax() / div : (pts[0] / div));
    }
}