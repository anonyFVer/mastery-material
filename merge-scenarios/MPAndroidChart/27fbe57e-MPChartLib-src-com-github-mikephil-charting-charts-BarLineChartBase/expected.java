package com.github.mikephil.charting.charts;

import android.annotation.SuppressLint;
import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Matrix;
import android.graphics.Paint;
import android.graphics.Paint.Style;
import android.graphics.PointF;
import android.util.AttributeSet;
import android.util.Log;
import android.view.MotionEvent;
import com.github.mikephil.charting.components.Legend.LegendPosition;
import com.github.mikephil.charting.components.XAxis;
import com.github.mikephil.charting.components.XAxis.XAxisPosition;
import com.github.mikephil.charting.components.YAxis;
import com.github.mikephil.charting.components.YAxis.AxisDependency;
import com.github.mikephil.charting.data.BarData;
import com.github.mikephil.charting.data.BarEntry;
import com.github.mikephil.charting.data.BarLineScatterCandleBubbleData;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.highlight.ChartHighlighter;
import com.github.mikephil.charting.highlight.Highlight;
import com.github.mikephil.charting.interfaces.dataprovider.BarLineScatterCandleBubbleDataProvider;
import com.github.mikephil.charting.interfaces.datasets.IBarLineScatterCandleBubbleDataSet;
import com.github.mikephil.charting.jobs.AnimatedMoveViewJob;
import com.github.mikephil.charting.jobs.AnimatedZoomJob;
import com.github.mikephil.charting.jobs.MoveViewJob;
import com.github.mikephil.charting.listener.BarLineChartTouchListener;
import com.github.mikephil.charting.listener.OnDrawListener;
import com.github.mikephil.charting.renderer.XAxisRenderer;
import com.github.mikephil.charting.renderer.YAxisRenderer;
import com.github.mikephil.charting.utils.PointD;
import com.github.mikephil.charting.utils.Transformer;
import com.github.mikephil.charting.utils.Utils;

@SuppressLint("RtlHardcoded")
public abstract class BarLineChartBase<T extends BarLineScatterCandleBubbleData<? extends IBarLineScatterCandleBubbleDataSet<? extends Entry>>> extends Chart<T> implements BarLineScatterCandleBubbleDataProvider {

    protected int mMaxVisibleCount = 100;

    private boolean mAutoScaleMinMaxEnabled = false;

    private Integer mAutoScaleLastLowestVisibleXIndex = null;

    private Integer mAutoScaleLastHighestVisibleXIndex = null;

    protected boolean mPinchZoomEnabled = false;

    protected boolean mDoubleTapToZoomEnabled = true;

    protected boolean mHighlightPerDragEnabled = true;

    private boolean mDragEnabled = true;

    private boolean mScaleXEnabled = true;

    private boolean mScaleYEnabled = true;

    protected Paint mGridBackgroundPaint;

    protected Paint mBorderPaint;

    protected boolean mDrawGridBackground = false;

    protected boolean mDrawBorders = false;

    protected float mMinOffset = 15.f;

    protected OnDrawListener mDrawListener;

    protected YAxis mAxisLeft;

    protected YAxis mAxisRight;

    protected XAxis mXAxis;

    protected YAxisRenderer mAxisRendererLeft;

    protected YAxisRenderer mAxisRendererRight;

    protected Transformer mLeftAxisTransformer;

    protected Transformer mRightAxisTransformer;

    protected XAxisRenderer mXAxisRenderer;

    public BarLineChartBase(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
    }

    public BarLineChartBase(Context context, AttributeSet attrs) {
        super(context, attrs);
    }

    public BarLineChartBase(Context context) {
        super(context);
    }

    @Override
    protected void init() {
        super.init();
        mAxisLeft = new YAxis(AxisDependency.LEFT);
        mAxisRight = new YAxis(AxisDependency.RIGHT);
        mXAxis = new XAxis();
        mLeftAxisTransformer = new Transformer(mViewPortHandler);
        mRightAxisTransformer = new Transformer(mViewPortHandler);
        mAxisRendererLeft = new YAxisRenderer(mViewPortHandler, mAxisLeft, mLeftAxisTransformer);
        mAxisRendererRight = new YAxisRenderer(mViewPortHandler, mAxisRight, mRightAxisTransformer);
        mXAxisRenderer = new XAxisRenderer(mViewPortHandler, mXAxis, mLeftAxisTransformer);
        setHighlighter(new ChartHighlighter(this));
        mChartTouchListener = new BarLineChartTouchListener(this, mViewPortHandler.getMatrixTouch());
        mGridBackgroundPaint = new Paint();
        mGridBackgroundPaint.setStyle(Style.FILL);
        mGridBackgroundPaint.setColor(Color.rgb(240, 240, 240));
        mBorderPaint = new Paint();
        mBorderPaint.setStyle(Style.STROKE);
        mBorderPaint.setColor(Color.BLACK);
        mBorderPaint.setStrokeWidth(Utils.convertDpToPixel(1f));
    }

    private long totalTime = 0;

    private long drawCycles = 0;

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        if (mData == null)
            return;
        long starttime = System.currentTimeMillis();
        calcModulus();
        mXAxisRenderer.calcXBounds(this, mXAxis.mAxisLabelModulus);
        mRenderer.calcXBounds(this, mXAxis.mAxisLabelModulus);
        drawGridBackground(canvas);
        if (mAxisLeft.isEnabled())
            mAxisRendererLeft.computeAxis(mAxisLeft.mAxisMinimum, mAxisLeft.mAxisMaximum);
        if (mAxisRight.isEnabled())
            mAxisRendererRight.computeAxis(mAxisRight.mAxisMinimum, mAxisRight.mAxisMaximum);
        mXAxisRenderer.renderAxisLine(canvas);
        mAxisRendererLeft.renderAxisLine(canvas);
        mAxisRendererRight.renderAxisLine(canvas);
        if (mAutoScaleMinMaxEnabled) {
            final int lowestVisibleXIndex = getLowestVisibleXIndex();
            final int highestVisibleXIndex = getHighestVisibleXIndex();
            if (mAutoScaleLastLowestVisibleXIndex == null || mAutoScaleLastLowestVisibleXIndex != lowestVisibleXIndex || mAutoScaleLastHighestVisibleXIndex == null || mAutoScaleLastHighestVisibleXIndex != highestVisibleXIndex) {
                calcMinMax();
                calculateOffsets();
                mAutoScaleLastLowestVisibleXIndex = lowestVisibleXIndex;
                mAutoScaleLastHighestVisibleXIndex = highestVisibleXIndex;
            }
        }
        int clipRestoreCount = canvas.save();
        canvas.clipRect(mViewPortHandler.getContentRect());
        mXAxisRenderer.renderGridLines(canvas);
        mAxisRendererLeft.renderGridLines(canvas);
        mAxisRendererRight.renderGridLines(canvas);
        if (mXAxis.isDrawLimitLinesBehindDataEnabled())
            mXAxisRenderer.renderLimitLines(canvas);
        if (mAxisLeft.isDrawLimitLinesBehindDataEnabled())
            mAxisRendererLeft.renderLimitLines(canvas);
        if (mAxisRight.isDrawLimitLinesBehindDataEnabled())
            mAxisRendererRight.renderLimitLines(canvas);
        mRenderer.drawData(canvas);
        if (!mXAxis.isDrawLimitLinesBehindDataEnabled())
            mXAxisRenderer.renderLimitLines(canvas);
        if (!mAxisLeft.isDrawLimitLinesBehindDataEnabled())
            mAxisRendererLeft.renderLimitLines(canvas);
        if (!mAxisRight.isDrawLimitLinesBehindDataEnabled())
            mAxisRendererRight.renderLimitLines(canvas);
        if (valuesToHighlight())
            mRenderer.drawHighlighted(canvas, mIndicesToHighlight);
        canvas.restoreToCount(clipRestoreCount);
        mRenderer.drawExtras(canvas);
        mXAxisRenderer.renderAxisLabels(canvas);
        mAxisRendererLeft.renderAxisLabels(canvas);
        mAxisRendererRight.renderAxisLabels(canvas);
        mRenderer.drawValues(canvas);
        mLegendRenderer.renderLegend(canvas);
        drawMarkers(canvas);
        drawDescription(canvas);
        if (mLogEnabled) {
            long drawtime = (System.currentTimeMillis() - starttime);
            totalTime += drawtime;
            drawCycles += 1;
            long average = totalTime / drawCycles;
            Log.i(LOG_TAG, "Drawtime: " + drawtime + " ms, average: " + average + " ms, cycles: " + drawCycles);
        }
    }

    public void resetTracking() {
        totalTime = 0;
        drawCycles = 0;
    }

    protected void prepareValuePxMatrix() {
        if (mLogEnabled)
            Log.i(LOG_TAG, "Preparing Value-Px Matrix, xmin: " + mXChartMin + ", xmax: " + mXChartMax + ", xdelta: " + mDeltaX);
        mRightAxisTransformer.prepareMatrixValuePx(mXChartMin, mDeltaX, mAxisRight.mAxisRange, mAxisRight.mAxisMinimum);
        mLeftAxisTransformer.prepareMatrixValuePx(mXChartMin, mDeltaX, mAxisLeft.mAxisRange, mAxisLeft.mAxisMinimum);
    }

    protected void prepareOffsetMatrix() {
        mRightAxisTransformer.prepareMatrixOffset(mAxisRight.isInverted());
        mLeftAxisTransformer.prepareMatrixOffset(mAxisLeft.isInverted());
    }

    @Override
    public void notifyDataSetChanged() {
        if (mData == null) {
            if (mLogEnabled)
                Log.i(LOG_TAG, "Preparing... DATA NOT SET.");
            return;
        } else {
            if (mLogEnabled)
                Log.i(LOG_TAG, "Preparing...");
        }
        if (mRenderer != null)
            mRenderer.initBuffers();
        calcMinMax();
        mAxisRendererLeft.computeAxis(mAxisLeft.mAxisMinimum, mAxisLeft.mAxisMaximum);
        mAxisRendererRight.computeAxis(mAxisRight.mAxisMinimum, mAxisRight.mAxisMaximum);
        mXAxisRenderer.computeAxis(mData.getXValMaximumLength(), mData.getXVals());
        if (mLegend != null)
            mLegendRenderer.computeLegend(mData);
        calculateOffsets();
    }

    @Override
    protected void calcMinMax() {
        if (mAutoScaleMinMaxEnabled)
            mData.calcMinMax(getLowestVisibleXIndex(), getHighestVisibleXIndex());
        float minLeft = !Float.isNaN(mAxisLeft.getAxisMinValue()) ? mAxisLeft.getAxisMinValue() : mData.getYMin(AxisDependency.LEFT);
        float maxLeft = !Float.isNaN(mAxisLeft.getAxisMaxValue()) ? mAxisLeft.getAxisMaxValue() : mData.getYMax(AxisDependency.LEFT);
        float minRight = !Float.isNaN(mAxisRight.getAxisMinValue()) ? mAxisRight.getAxisMinValue() : mData.getYMin(AxisDependency.RIGHT);
        float maxRight = !Float.isNaN(mAxisRight.getAxisMaxValue()) ? mAxisRight.getAxisMaxValue() : mData.getYMax(AxisDependency.RIGHT);
        float leftRange = Math.abs(maxLeft - minLeft);
        float rightRange = Math.abs(maxRight - minRight);
        if (leftRange == 0f) {
            maxLeft = maxLeft + 1f;
            minLeft = minLeft - 1f;
        }
        if (rightRange == 0f) {
            maxRight = maxRight + 1f;
            minRight = minRight - 1f;
        }
        float topSpaceLeft = leftRange / 100f * mAxisLeft.getSpaceTop();
        float topSpaceRight = rightRange / 100f * mAxisRight.getSpaceTop();
        float bottomSpaceLeft = leftRange / 100f * mAxisLeft.getSpaceBottom();
        float bottomSpaceRight = rightRange / 100f * mAxisRight.getSpaceBottom();
        mXChartMax = mData.getXVals().size() - 1;
        mDeltaX = Math.abs(mXChartMax - mXChartMin);
        mAxisLeft.mAxisMinimum = !Float.isNaN(mAxisLeft.getAxisMinValue()) ? mAxisLeft.getAxisMinValue() : (minLeft - bottomSpaceLeft);
        mAxisLeft.mAxisMaximum = !Float.isNaN(mAxisLeft.getAxisMaxValue()) ? mAxisLeft.getAxisMaxValue() : (maxLeft + topSpaceLeft);
        mAxisRight.mAxisMinimum = !Float.isNaN(mAxisRight.getAxisMinValue()) ? mAxisRight.getAxisMinValue() : (minRight - bottomSpaceRight);
        mAxisRight.mAxisMaximum = !Float.isNaN(mAxisRight.getAxisMaxValue()) ? mAxisRight.getAxisMaxValue() : (maxRight + topSpaceRight);
        mAxisLeft.mAxisRange = Math.abs(mAxisLeft.mAxisMaximum - mAxisLeft.mAxisMinimum);
        mAxisRight.mAxisRange = Math.abs(mAxisRight.mAxisMaximum - mAxisRight.mAxisMinimum);
    }

    @Override
    public void calculateOffsets() {
        if (!mCustomViewPortEnabled) {
            float offsetLeft = 0f, offsetRight = 0f, offsetTop = 0f, offsetBottom = 0f;
            if (mLegend != null && mLegend.isEnabled()) {
                if (mLegend.getPosition() == LegendPosition.RIGHT_OF_CHART || mLegend.getPosition() == LegendPosition.RIGHT_OF_CHART_CENTER) {
                    offsetRight += Math.min(mLegend.mNeededWidth, mViewPortHandler.getChartWidth() * mLegend.getMaxSizePercent()) + mLegend.getXOffset() * 2f;
                } else if (mLegend.getPosition() == LegendPosition.LEFT_OF_CHART || mLegend.getPosition() == LegendPosition.LEFT_OF_CHART_CENTER) {
                    offsetLeft += Math.min(mLegend.mNeededWidth, mViewPortHandler.getChartWidth() * mLegend.getMaxSizePercent()) + mLegend.getXOffset() * 2f;
                } else if (mLegend.getPosition() == LegendPosition.BELOW_CHART_LEFT || mLegend.getPosition() == LegendPosition.BELOW_CHART_RIGHT || mLegend.getPosition() == LegendPosition.BELOW_CHART_CENTER) {
                    float yOffset = mLegend.mTextHeightMax;
                    offsetBottom += Math.min(mLegend.mNeededHeight + yOffset, mViewPortHandler.getChartHeight() * mLegend.getMaxSizePercent());
                } else if (mLegend.getPosition() == LegendPosition.ABOVE_CHART_LEFT || mLegend.getPosition() == LegendPosition.ABOVE_CHART_RIGHT || mLegend.getPosition() == LegendPosition.ABOVE_CHART_CENTER) {
                    float yOffset = mLegend.mTextHeightMax;
                    offsetTop += Math.min(mLegend.mNeededHeight + yOffset, mViewPortHandler.getChartHeight() * mLegend.getMaxSizePercent());
                }
            }
            if (mAxisLeft.needsOffset()) {
                offsetLeft += mAxisLeft.getRequiredWidthSpace(mAxisRendererLeft.getPaintAxisLabels());
            }
            if (mAxisRight.needsOffset()) {
                offsetRight += mAxisRight.getRequiredWidthSpace(mAxisRendererRight.getPaintAxisLabels());
            }
            if (mXAxis.isEnabled() && mXAxis.isDrawLabelsEnabled()) {
                float xlabelheight = mXAxis.mLabelRotatedHeight + mXAxis.getYOffset();
                if (mXAxis.getPosition() == XAxisPosition.BOTTOM) {
                    offsetBottom += xlabelheight;
                } else if (mXAxis.getPosition() == XAxisPosition.TOP) {
                    offsetTop += xlabelheight;
                } else if (mXAxis.getPosition() == XAxisPosition.BOTH_SIDED) {
                    offsetBottom += xlabelheight;
                    offsetTop += xlabelheight;
                }
            }
            offsetTop += getExtraTopOffset();
            offsetRight += getExtraRightOffset();
            offsetBottom += getExtraBottomOffset();
            offsetLeft += getExtraLeftOffset();
            float minOffset = Utils.convertDpToPixel(mMinOffset);
            mViewPortHandler.restrainViewPort(Math.max(minOffset, offsetLeft), Math.max(minOffset, offsetTop), Math.max(minOffset, offsetRight), Math.max(minOffset, offsetBottom));
            if (mLogEnabled) {
                Log.i(LOG_TAG, "offsetLeft: " + offsetLeft + ", offsetTop: " + offsetTop + ", offsetRight: " + offsetRight + ", offsetBottom: " + offsetBottom);
                Log.i(LOG_TAG, "Content: " + mViewPortHandler.getContentRect().toString());
            }
        }
        prepareOffsetMatrix();
        prepareValuePxMatrix();
    }

    protected void calcModulus() {
        if (mXAxis == null || !mXAxis.isEnabled())
            return;
        if (!mXAxis.isAxisModulusCustom()) {
            float[] values = new float[9];
            mViewPortHandler.getMatrixTouch().getValues(values);
            mXAxis.mAxisLabelModulus = (int) Math.ceil((mData.getXValCount() * mXAxis.mLabelRotatedWidth) / (mViewPortHandler.contentWidth() * values[Matrix.MSCALE_X]));
        }
        if (mLogEnabled)
            Log.i(LOG_TAG, "X-Axis modulus: " + mXAxis.mAxisLabelModulus + ", x-axis label width: " + mXAxis.mLabelWidth + ", x-axis label rotated width: " + mXAxis.mLabelRotatedWidth + ", content width: " + mViewPortHandler.contentWidth());
        if (mXAxis.mAxisLabelModulus < 1)
            mXAxis.mAxisLabelModulus = 1;
    }

    @Override
    protected float[] getMarkerPosition(Entry e, Highlight highlight) {
        int dataSetIndex = highlight.getDataSetIndex();
        float xPos = e.getXIndex();
        float yPos = e.getVal();
        if (this instanceof BarChart) {
            BarData bd = (BarData) mData;
            float space = bd.getGroupSpace();
            int setCount = mData.getDataSetCount();
            int i = e.getXIndex();
            if (this instanceof HorizontalBarChart) {
                float y = i + i * (setCount - 1) + dataSetIndex + space * i + space / 2f;
                yPos = y;
                BarEntry entry = (BarEntry) e;
                if (entry.getVals() != null) {
                    xPos = highlight.getRange().to;
                } else {
                    xPos = e.getVal();
                }
                xPos *= mAnimator.getPhaseY();
            } else {
                float x = i + i * (setCount - 1) + dataSetIndex + space * i + space / 2f;
                xPos = x;
                BarEntry entry = (BarEntry) e;
                if (entry.getVals() != null) {
                    yPos = highlight.getRange().to;
                } else {
                    yPos = e.getVal();
                }
                yPos *= mAnimator.getPhaseY();
            }
        } else {
            yPos *= mAnimator.getPhaseY();
        }
        float[] pts = new float[] { xPos, yPos };
        getTransformer(mData.getDataSetByIndex(dataSetIndex).getAxisDependency()).pointValuesToPixel(pts);
        return pts;
    }

    protected void drawGridBackground(Canvas c) {
        if (mDrawGridBackground) {
            c.drawRect(mViewPortHandler.getContentRect(), mGridBackgroundPaint);
        }
        if (mDrawBorders) {
            c.drawRect(mViewPortHandler.getContentRect(), mBorderPaint);
        }
    }

    public Transformer getTransformer(AxisDependency which) {
        if (which == AxisDependency.LEFT)
            return mLeftAxisTransformer;
        else
            return mRightAxisTransformer;
    }

    @Override
    public boolean onTouchEvent(MotionEvent event) {
        super.onTouchEvent(event);
        if (mChartTouchListener == null || mData == null)
            return false;
        if (!mTouchEnabled)
            return false;
        else
            return mChartTouchListener.onTouch(this, event);
    }

    @Override
    public void computeScroll() {
        if (mChartTouchListener instanceof BarLineChartTouchListener)
            ((BarLineChartTouchListener) mChartTouchListener).computeScroll();
    }

    public void zoomIn() {
        Matrix save = mViewPortHandler.zoomIn(getWidth() / 2f, -(getHeight() / 2f));
        mViewPortHandler.refresh(save, this, true);
        calculateOffsets();
        postInvalidate();
    }

    public void zoomOut() {
        Matrix save = mViewPortHandler.zoomOut(getWidth() / 2f, -(getHeight() / 2f));
        mViewPortHandler.refresh(save, this, true);
        calculateOffsets();
        postInvalidate();
    }

    public void zoom(float scaleX, float scaleY, float x, float y) {
        Matrix save = mViewPortHandler.zoom(scaleX, scaleY, x, -y);
        mViewPortHandler.refresh(save, this, true);
        calculateOffsets();
        postInvalidate();
    }

    private void zoomAndCenterAnimated(float scaleX, float scaleY, float xValue, float yValue, AxisDependency axis, long duration) {
        PointD bounds = getValuesByTouchPoint(mViewPortHandler.contentLeft(), mViewPortHandler.contentTop(), axis);
        float valsInView = getDeltaY(axis) / mViewPortHandler.getScaleY();
        float xsInView = getXAxis().getValues().size() / mViewPortHandler.getScaleX();
        Runnable job = new AnimatedZoomJob(mViewPortHandler, this, getTransformer(axis), scaleX, scaleY, mViewPortHandler.getScaleX(), mViewPortHandler.getScaleY(), xValue - xsInView / 2f, yValue + valsInView / 2f, (float) bounds.x, (float) bounds.y, duration);
        postJob(job);
    }

    public void fitScreen() {
        Matrix save = mViewPortHandler.fitScreen();
        mViewPortHandler.refresh(save, this, true);
        calculateOffsets();
        postInvalidate();
    }

    public void setScaleMinima(float scaleX, float scaleY) {
        mViewPortHandler.setMinimumScaleX(scaleX);
        mViewPortHandler.setMinimumScaleY(scaleY);
    }

    public void setVisibleXRangeMaximum(float maxXRange) {
        float xScale = mDeltaX / (maxXRange);
        mViewPortHandler.setMinimumScaleX(xScale);
    }

    public void setVisibleXRangeMinimum(float minXRange) {
        float xScale = mDeltaX / (minXRange);
        mViewPortHandler.setMaximumScaleX(xScale);
    }

    public void setVisibleXRange(float minXRange, float maxXRange) {
        float maxScale = mDeltaX / minXRange;
        float minScale = mDeltaX / maxXRange;
        mViewPortHandler.setMinMaxScaleX(minScale, maxScale);
    }

    public void setVisibleYRangeMaximum(float maxYRange, AxisDependency axis) {
        float yScale = getDeltaY(axis) / maxYRange;
        mViewPortHandler.setMinimumScaleY(yScale);
    }

    public void moveViewToX(float xIndex) {
        Runnable job = new MoveViewJob(mViewPortHandler, xIndex, 0f, getTransformer(AxisDependency.LEFT), this);
        postJob(job);
    }

    public void moveViewToY(float yValue, AxisDependency axis) {
        float valsInView = getDeltaY(axis) / mViewPortHandler.getScaleY();
        Runnable job = new MoveViewJob(mViewPortHandler, 0f, yValue + valsInView / 2f, getTransformer(axis), this);
        postJob(job);
    }

    public void moveViewTo(float xIndex, float yValue, AxisDependency axis) {
        float valsInView = getDeltaY(axis) / mViewPortHandler.getScaleY();
        Runnable job = new MoveViewJob(mViewPortHandler, xIndex, yValue + valsInView / 2f, getTransformer(axis), this);
        postJob(job);
    }

    public void moveViewToAnimated(float xIndex, float yValue, AxisDependency axis, long duration) {
        PointD bounds = getValuesByTouchPoint(mViewPortHandler.contentLeft(), mViewPortHandler.contentTop(), axis);
        float valsInView = getDeltaY(axis) / mViewPortHandler.getScaleY();
        Runnable job = new AnimatedMoveViewJob(mViewPortHandler, xIndex, yValue + valsInView / 2f, getTransformer(axis), this, (float) bounds.x, (float) bounds.y, duration);
        postJob(job);
    }

    public void centerViewTo(float xIndex, float yValue, AxisDependency axis) {
        float valsInView = getDeltaY(axis) / mViewPortHandler.getScaleY();
        float xsInView = getXAxis().getValues().size() / mViewPortHandler.getScaleX();
        Runnable job = new MoveViewJob(mViewPortHandler, xIndex - xsInView / 2f, yValue + valsInView / 2f, getTransformer(axis), this);
        postJob(job);
    }

    public void centerViewToAnimated(float xIndex, float yValue, AxisDependency axis, long duration) {
        PointD bounds = getValuesByTouchPoint(mViewPortHandler.contentLeft(), mViewPortHandler.contentTop(), axis);
        float valsInView = getDeltaY(axis) / mViewPortHandler.getScaleY();
        float xsInView = getXAxis().getValues().size() / mViewPortHandler.getScaleX();
        Runnable job = new AnimatedMoveViewJob(mViewPortHandler, xIndex - xsInView / 2f, yValue + valsInView / 2f, getTransformer(axis), this, (float) bounds.x, (float) bounds.y, duration);
        postJob(job);
    }

    private boolean mCustomViewPortEnabled = false;

    public void setViewPortOffsets(final float left, final float top, final float right, final float bottom) {
        mCustomViewPortEnabled = true;
        post(new Runnable() {

            @Override
            public void run() {
                mViewPortHandler.restrainViewPort(left, top, right, bottom);
                prepareOffsetMatrix();
                prepareValuePxMatrix();
            }
        });
    }

    public void resetViewPortOffsets() {
        mCustomViewPortEnabled = false;
        calculateOffsets();
    }

    public float getDeltaY(AxisDependency axis) {
        if (axis == AxisDependency.LEFT)
            return mAxisLeft.mAxisRange;
        else
            return mAxisRight.mAxisRange;
    }

    public void setOnDrawListener(OnDrawListener drawListener) {
        this.mDrawListener = drawListener;
    }

    public OnDrawListener getDrawListener() {
        return mDrawListener;
    }

    public PointF getPosition(Entry e, AxisDependency axis) {
        if (e == null)
            return null;
        float[] vals = new float[] { e.getXIndex(), e.getVal() };
        getTransformer(axis).pointValuesToPixel(vals);
        return new PointF(vals[0], vals[1]);
    }

    public void setMaxVisibleValueCount(int count) {
        this.mMaxVisibleCount = count;
    }

    public int getMaxVisibleCount() {
        return mMaxVisibleCount;
    }

    public void setHighlightPerDragEnabled(boolean enabled) {
        mHighlightPerDragEnabled = enabled;
    }

    public boolean isHighlightPerDragEnabled() {
        return mHighlightPerDragEnabled;
    }

    public void setGridBackgroundColor(int color) {
        mGridBackgroundPaint.setColor(color);
    }

    public void setDragEnabled(boolean enabled) {
        this.mDragEnabled = enabled;
    }

    public boolean isDragEnabled() {
        return mDragEnabled;
    }

    public void setScaleEnabled(boolean enabled) {
        this.mScaleXEnabled = enabled;
        this.mScaleYEnabled = enabled;
    }

    public void setScaleXEnabled(boolean enabled) {
        mScaleXEnabled = enabled;
    }

    public void setScaleYEnabled(boolean enabled) {
        mScaleYEnabled = enabled;
    }

    public boolean isScaleXEnabled() {
        return mScaleXEnabled;
    }

    public boolean isScaleYEnabled() {
        return mScaleYEnabled;
    }

    public void setDoubleTapToZoomEnabled(boolean enabled) {
        mDoubleTapToZoomEnabled = enabled;
    }

    public boolean isDoubleTapToZoomEnabled() {
        return mDoubleTapToZoomEnabled;
    }

    public void setDrawGridBackground(boolean enabled) {
        mDrawGridBackground = enabled;
    }

    public void setDrawBorders(boolean enabled) {
        mDrawBorders = enabled;
    }

    public void setBorderWidth(float width) {
        mBorderPaint.setStrokeWidth(Utils.convertDpToPixel(width));
    }

    public void setBorderColor(int color) {
        mBorderPaint.setColor(color);
    }

    public float getMinOffset() {
        return mMinOffset;
    }

    public void setMinOffset(float minOffset) {
        mMinOffset = minOffset;
    }

    public Highlight getHighlightByTouchPoint(float x, float y) {
        if (mData == null) {
            Log.e(LOG_TAG, "Can't select by touch. No data set.");
            return null;
        } else
            return getHighlighter().getHighlight(x, y);
    }

    public PointD getValuesByTouchPoint(float x, float y, AxisDependency axis) {
        float[] pts = new float[2];
        pts[0] = x;
        pts[1] = y;
        getTransformer(axis).pixelsToValue(pts);
        double xTouchVal = pts[0];
        double yTouchVal = pts[1];
        return new PointD(xTouchVal, yTouchVal);
    }

    public PointD getPixelsForValues(float x, float y, AxisDependency axis) {
        float[] pts = new float[] { x, y };
        getTransformer(axis).pointValuesToPixel(pts);
        return new PointD(pts[0], pts[1]);
    }

    public float getYValueByTouchPoint(float x, float y, AxisDependency axis) {
        return (float) getValuesByTouchPoint(x, y, axis).y;
    }

    public Entry getEntryByTouchPoint(float x, float y) {
        Highlight h = getHighlightByTouchPoint(x, y);
        if (h != null) {
            return mData.getEntryForHighlight(h);
        }
        return null;
    }

    public IBarLineScatterCandleBubbleDataSet getDataSetByTouchPoint(float x, float y) {
        Highlight h = getHighlightByTouchPoint(x, y);
        if (h != null) {
            return mData.getDataSetByIndex(h.getDataSetIndex());
        }
        return null;
    }

    @Override
    public int getLowestVisibleXIndex() {
        float[] pts = new float[] { mViewPortHandler.contentLeft(), mViewPortHandler.contentBottom() };
        getTransformer(AxisDependency.LEFT).pixelsToValue(pts);
        return (pts[0] <= 0) ? 0 : (int) (pts[0] + 1.0f);
    }

    @Override
    public int getHighestVisibleXIndex() {
        float[] pts = new float[] { mViewPortHandler.contentRight(), mViewPortHandler.contentBottom() };
        getTransformer(AxisDependency.LEFT).pixelsToValue(pts);
        return (pts[0] >= mData.getXValCount()) ? mData.getXValCount() - 1 : (int) pts[0];
    }

    public float getScaleX() {
        if (mViewPortHandler == null)
            return 1f;
        else
            return mViewPortHandler.getScaleX();
    }

    public float getScaleY() {
        if (mViewPortHandler == null)
            return 1f;
        else
            return mViewPortHandler.getScaleY();
    }

    public boolean isFullyZoomedOut() {
        return mViewPortHandler.isFullyZoomedOut();
    }

    public YAxis getAxisLeft() {
        return mAxisLeft;
    }

    public YAxis getAxisRight() {
        return mAxisRight;
    }

    public YAxis getAxis(AxisDependency axis) {
        if (axis == AxisDependency.LEFT)
            return mAxisLeft;
        else
            return mAxisRight;
    }

    @Override
    public boolean isInverted(AxisDependency axis) {
        return getAxis(axis).isInverted();
    }

    public XAxis getXAxis() {
        return mXAxis;
    }

    public void setPinchZoom(boolean enabled) {
        mPinchZoomEnabled = enabled;
    }

    public boolean isPinchZoomEnabled() {
        return mPinchZoomEnabled;
    }

    public void setDragOffsetX(float offset) {
        mViewPortHandler.setDragOffsetX(offset);
    }

    public void setDragOffsetY(float offset) {
        mViewPortHandler.setDragOffsetY(offset);
    }

    public boolean hasNoDragOffset() {
        return mViewPortHandler.hasNoDragOffset();
    }

    public XAxisRenderer getRendererXAxis() {
        return mXAxisRenderer;
    }

    public void setXAxisRenderer(XAxisRenderer xAxisRenderer) {
        mXAxisRenderer = xAxisRenderer;
    }

    public YAxisRenderer getRendererLeftYAxis() {
        return mAxisRendererLeft;
    }

    public void setRendererLeftYAxis(YAxisRenderer rendererLeftYAxis) {
        mAxisRendererLeft = rendererLeftYAxis;
    }

    public YAxisRenderer getRendererRightYAxis() {
        return mAxisRendererRight;
    }

    public void setRendererRightYAxis(YAxisRenderer rendererRightYAxis) {
        mAxisRendererRight = rendererRightYAxis;
    }

    @Override
    public float getYChartMax() {
        return Math.max(mAxisLeft.mAxisMaximum, mAxisRight.mAxisMaximum);
    }

    @Override
    public float getYChartMin() {
        return Math.min(mAxisLeft.mAxisMinimum, mAxisRight.mAxisMinimum);
    }

    public boolean isAnyAxisInverted() {
        if (mAxisLeft.isInverted())
            return true;
        if (mAxisRight.isInverted())
            return true;
        return false;
    }

    public void setAutoScaleMinMaxEnabled(boolean enabled) {
        mAutoScaleMinMaxEnabled = enabled;
    }

    public boolean isAutoScaleMinMaxEnabled() {
        return mAutoScaleMinMaxEnabled;
    }

    @Override
    public void setPaint(Paint p, int which) {
        super.setPaint(p, which);
        switch(which) {
            case PAINT_GRID_BACKGROUND:
                mGridBackgroundPaint = p;
                break;
        }
    }

    @Override
    public Paint getPaint(int which) {
        Paint p = super.getPaint(which);
        if (p != null)
            return p;
        switch(which) {
            case PAINT_GRID_BACKGROUND:
                return mGridBackgroundPaint;
        }
        return null;
    }
}