package com.github.mikephil.charting.charts;

import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.PointF;
import android.graphics.RectF;
import android.util.AttributeSet;
import com.github.mikephil.charting.components.XAxis;
import com.github.mikephil.charting.components.YAxis;
import com.github.mikephil.charting.components.YAxis.AxisDependency;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.RadarData;
import com.github.mikephil.charting.highlight.Highlight;
import com.github.mikephil.charting.renderer.RadarChartRenderer;
import com.github.mikephil.charting.renderer.XAxisRendererRadarChart;
import com.github.mikephil.charting.renderer.YAxisRendererRadarChart;
import com.github.mikephil.charting.utils.Utils;

public class RadarChart extends PieRadarChartBase<RadarData> {

    private float mWebLineWidth = 2.5f;

    private float mInnerWebLineWidth = 1.5f;

    private int mWebColor = Color.rgb(122, 122, 122);

    private int mWebColorInner = Color.rgb(122, 122, 122);

    private int mWebAlpha = 150;

    private boolean mDrawWeb = true;

    private YAxis mYAxis;

    private XAxis mXAxis;

    protected YAxisRendererRadarChart mYAxisRenderer;

    protected XAxisRendererRadarChart mXAxisRenderer;

    public RadarChart(Context context) {
        super(context);
    }

    public RadarChart(Context context, AttributeSet attrs) {
        super(context, attrs);
    }

    public RadarChart(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
    }

    @Override
    protected void init() {
        super.init();
        mYAxis = new YAxis(AxisDependency.LEFT);
        mXAxis = new XAxis();
        mXAxis.setSpaceBetweenLabels(0);
        mWebLineWidth = Utils.convertDpToPixel(1.5f);
        mInnerWebLineWidth = Utils.convertDpToPixel(0.75f);
        mRenderer = new RadarChartRenderer(this, mAnimator, mViewPortHandler);
        mYAxisRenderer = new YAxisRendererRadarChart(mViewPortHandler, mYAxis, this);
        mXAxisRenderer = new XAxisRendererRadarChart(mViewPortHandler, mXAxis, this);
    }

    @Override
    protected void calcMinMax() {
        super.calcMinMax();
        float minLeft = mData.getYMin(AxisDependency.LEFT);
        float maxLeft = mData.getYMax(AxisDependency.LEFT);
        mXChartMax = mData.getXVals().size() - 1;
        mDeltaX = Math.abs(mXChartMax - mXChartMin);
        float leftRange = Math.abs(maxLeft - (mYAxis.isStartAtZeroEnabled() ? 0 : minLeft));
        float topSpaceLeft = leftRange / 100f * mYAxis.getSpaceTop();
        float bottomSpaceLeft = leftRange / 100f * mYAxis.getSpaceBottom();
        mXChartMax = mData.getXVals().size() - 1;
        mDeltaX = Math.abs(mXChartMax - mXChartMin);
        if (mYAxis.isStartAtZeroEnabled()) {
            if (minLeft < 0.f && maxLeft < 0.f) {
                mYAxis.mAxisMinimum = Math.min(0.f, !Float.isNaN(mYAxis.getAxisMinValue()) ? mYAxis.getAxisMinValue() : (minLeft - bottomSpaceLeft));
                mYAxis.mAxisMaximum = 0.f;
            } else if (minLeft >= 0.0) {
                mYAxis.mAxisMinimum = 0.f;
                mYAxis.mAxisMaximum = Math.max(0.f, !Float.isNaN(mYAxis.getAxisMaxValue()) ? mYAxis.getAxisMaxValue() : (maxLeft + topSpaceLeft));
            } else {
                mYAxis.mAxisMinimum = Math.min(0.f, !Float.isNaN(mYAxis.getAxisMinValue()) ? mYAxis.getAxisMinValue() : (minLeft - bottomSpaceLeft));
                mYAxis.mAxisMaximum = Math.max(0.f, !Float.isNaN(mYAxis.getAxisMaxValue()) ? mYAxis.getAxisMaxValue() : (maxLeft + topSpaceLeft));
            }
        } else {
            mYAxis.mAxisMinimum = !Float.isNaN(mYAxis.getAxisMinValue()) ? mYAxis.getAxisMinValue() : (minLeft - bottomSpaceLeft);
            mYAxis.mAxisMaximum = !Float.isNaN(mYAxis.getAxisMaxValue()) ? mYAxis.getAxisMaxValue() : (maxLeft + topSpaceLeft);
        }
        mYAxis.mAxisRange = Math.abs(mYAxis.mAxisMaximum - mYAxis.mAxisMinimum);
    }

    @Override
    protected float[] getMarkerPosition(Entry e, Highlight highlight) {
        float angle = getSliceAngle() * e.getXIndex() + getRotationAngle();
        float val = e.getVal() * getFactor();
        PointF c = getCenterOffsets();
        PointF p = new PointF((float) (c.x + val * Math.cos(Math.toRadians(angle))), (float) (c.y + val * Math.sin(Math.toRadians(angle))));
        return new float[] { p.x, p.y };
    }

    @Override
    public void notifyDataSetChanged() {
        if (mDataNotSet)
            return;
        calcMinMax();
        if (mYAxis.needsDefaultFormatter()) {
            mYAxis.setValueFormatter(mDefaultFormatter);
        }
        mYAxisRenderer.computeAxis(mYAxis.mAxisMinimum, mYAxis.mAxisMaximum);
        mXAxisRenderer.computeAxis(mData.getXValAverageLength(), mData.getXVals());
        if (mLegend != null && !mLegend.isLegendCustom())
            mLegendRenderer.computeLegend(mData);
        calculateOffsets();
    }

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        if (mDataNotSet)
            return;
        mXAxisRenderer.renderAxisLabels(canvas);
        if (mDrawWeb)
            mRenderer.drawExtras(canvas);
        mYAxisRenderer.renderLimitLines(canvas);
        mRenderer.drawData(canvas);
        if (valuesToHighlight())
            mRenderer.drawHighlighted(canvas, mIndicesToHightlight);
        mYAxisRenderer.renderAxisLabels(canvas);
        mRenderer.drawValues(canvas);
        mLegendRenderer.renderLegend(canvas);
        drawDescription(canvas);
        drawMarkers(canvas);
    }

    public float getFactor() {
        RectF content = mViewPortHandler.getContentRect();
        return (float) Math.min(content.width() / 2f, content.height() / 2f) / mYAxis.mAxisRange;
    }

    public float getSliceAngle() {
        return 360f / (float) mData.getXValCount();
    }

    @Override
    public int getIndexForAngle(float angle) {
        float a = Utils.getNormalizedAngle(angle - getRotationAngle());
        float sliceangle = getSliceAngle();
        for (int i = 0; i < mData.getXValCount(); i++) {
            if (sliceangle * (i + 1) - sliceangle / 2f > a)
                return i;
        }
        return 0;
    }

    public YAxis getYAxis() {
        return mYAxis;
    }

    public XAxis getXAxis() {
        return mXAxis;
    }

    public void setWebLineWidth(float width) {
        mWebLineWidth = Utils.convertDpToPixel(width);
    }

    public float getWebLineWidth() {
        return mWebLineWidth;
    }

    public void setWebLineWidthInner(float width) {
        mInnerWebLineWidth = Utils.convertDpToPixel(width);
    }

    public float getWebLineWidthInner() {
        return mInnerWebLineWidth;
    }

    public void setWebAlpha(int alpha) {
        mWebAlpha = alpha;
    }

    public int getWebAlpha() {
        return mWebAlpha;
    }

    public void setWebColor(int color) {
        mWebColor = color;
    }

    public int getWebColor() {
        return mWebColor;
    }

    public void setWebColorInner(int color) {
        mWebColorInner = color;
    }

    public int getWebColorInner() {
        return mWebColorInner;
    }

    public void setDrawWeb(boolean enabled) {
        mDrawWeb = enabled;
    }

    @Override
    protected float getRequiredBottomOffset() {
        return mLegendRenderer.getLabelPaint().getTextSize() * 4.f;
    }

    @Override
    protected float getRequiredBaseOffset() {
        return mXAxis.isEnabled() && mXAxis.isDrawLabelsEnabled() ? mXAxis.mLabelWidth : Utils.convertDpToPixel(10f);
    }

    @Override
    public float getRadius() {
        RectF content = mViewPortHandler.getContentRect();
        return Math.min(content.width() / 2f, content.height() / 2f);
    }

    public float getYChartMax() {
        return mYAxis.mAxisMaximum;
    }

    public float getYChartMin() {
        return mYAxis.mAxisMinimum;
    }

    public float getYRange() {
        return mYAxis.mAxisRange;
    }
}