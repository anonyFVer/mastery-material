package com.github.mikephil.charting.charts;

import android.content.Context;
import android.util.AttributeSet;
import com.github.mikephil.charting.data.BarData;
import com.github.mikephil.charting.data.BubbleData;
import com.github.mikephil.charting.data.BubbleDataSet;
import com.github.mikephil.charting.data.CandleData;
import com.github.mikephil.charting.data.CombinedData;
import com.github.mikephil.charting.data.LineData;
import com.github.mikephil.charting.data.ScatterData;
import com.github.mikephil.charting.highlight.CombinedHighlighter;
import com.github.mikephil.charting.interfaces.BarDataProvider;
import com.github.mikephil.charting.interfaces.BubbleDataProvider;
import com.github.mikephil.charting.interfaces.CandleDataProvider;
import com.github.mikephil.charting.interfaces.LineDataProvider;
import com.github.mikephil.charting.interfaces.ScatterDataProvider;
import com.github.mikephil.charting.renderer.CombinedChartRenderer;

public class CombinedChart extends BarLineChartBase<CombinedData> implements LineDataProvider, BarDataProvider, ScatterDataProvider, CandleDataProvider, BubbleDataProvider {

    private boolean mDrawHighlightArrow = false;

    private boolean mDrawValueAboveBar = true;

    private boolean mDrawBarShadow = false;

    protected DrawOrder[] mDrawOrder = new DrawOrder[] { DrawOrder.BAR, DrawOrder.BUBBLE, DrawOrder.LINE, DrawOrder.CANDLE, DrawOrder.SCATTER };

    public enum DrawOrder {

        BAR, BUBBLE, LINE, CANDLE, SCATTER
    }

    public CombinedChart(Context context) {
        super(context);
    }

    public CombinedChart(Context context, AttributeSet attrs) {
        super(context, attrs);
    }

    public CombinedChart(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
    }

    @Override
    protected void init() {
        super.init();
        mHighlighter = new CombinedHighlighter(this);
    }

    @Override
    protected void calcMinMax() {
        super.calcMinMax();
        if (getBarData() != null || getCandleData() != null || getBubbleData() != null) {
            mXChartMin = -0.5f;
            mXChartMax = mData.getXVals().size() - 0.5f;
            if (getBubbleData() != null) {
                for (BubbleDataSet set : getBubbleData().getDataSets()) {
                    final float xmin = set.getXMin();
                    final float xmax = set.getXMax();
                    if (xmin < mXChartMin)
                        mXChartMin = xmin;
                    if (xmax > mXChartMax)
                        mXChartMax = xmax;
                }
            }
        }
        mDeltaX = Math.abs(mXChartMax - mXChartMin);
        if (mDeltaX == 0.f && getLineData() != null && getLineData().getYValCount() > 0) {
            mDeltaX = 1.f;
        }
    }

    @Override
    public void setData(CombinedData data) {
        mData = null;
        mRenderer = null;
        super.setData(data);
        mRenderer = new CombinedChartRenderer(this, mAnimator, mViewPortHandler);
        mRenderer.initBuffers();
    }

    @Override
    public LineData getLineData() {
        if (mData == null)
            return null;
        return mData.getLineData();
    }

    @Override
    public BarData getBarData() {
        if (mData == null)
            return null;
        return mData.getBarData();
    }

    @Override
    public ScatterData getScatterData() {
        if (mData == null)
            return null;
        return mData.getScatterData();
    }

    @Override
    public CandleData getCandleData() {
        if (mData == null)
            return null;
        return mData.getCandleData();
    }

    @Override
    public BubbleData getBubbleData() {
        if (mData == null)
            return null;
        return mData.getBubbleData();
    }

    @Override
    public boolean isDrawBarShadowEnabled() {
        return mDrawBarShadow;
    }

    @Override
    public boolean isDrawValueAboveBarEnabled() {
        return mDrawValueAboveBar;
    }

    @Override
    public boolean isDrawHighlightArrowEnabled() {
        return mDrawHighlightArrow;
    }

    public void setDrawHighlightArrow(boolean enabled) {
        mDrawHighlightArrow = enabled;
    }

    public void setDrawValueAboveBar(boolean enabled) {
        mDrawValueAboveBar = enabled;
    }

    public void setDrawBarShadow(boolean enabled) {
        mDrawBarShadow = enabled;
    }

    public DrawOrder[] getDrawOrder() {
        return mDrawOrder;
    }

    public void setDrawOrder(DrawOrder[] order) {
        if (order == null || order.length <= 0)
            return;
        mDrawOrder = order;
    }
}