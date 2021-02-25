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
import com.github.mikephil.charting.data.BarLineScatterCandleData;
import com.github.mikephil.charting.data.BarLineScatterCandleDataSet;
import com.github.mikephil.charting.data.DataSet;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.LineData;
import com.github.mikephil.charting.data.LineDataSet;
import com.github.mikephil.charting.data.filter.Approximator;
import com.github.mikephil.charting.interfaces.BarLineScatterCandleDataProvider;
import com.github.mikephil.charting.jobs.MoveViewJob;
import com.github.mikephil.charting.listener.BarLineChartTouchListener;
import com.github.mikephil.charting.listener.OnDrawListener;
import com.github.mikephil.charting.renderer.XAxisRenderer;
import com.github.mikephil.charting.renderer.YAxisRenderer;
import com.github.mikephil.charting.utils.FillFormatter;
import com.github.mikephil.charting.utils.Highlight;
import com.github.mikephil.charting.utils.PointD;
import com.github.mikephil.charting.utils.SelInfo;
import com.github.mikephil.charting.utils.Transformer;
import com.github.mikephil.charting.utils.Utils;
import java.util.ArrayList;
import java.util.List;

@SuppressLint("RtlHardcoded")
public abstract class BarLineChartBase<T extends BarLineScatterCandleData<? extends BarLineScatterCandleDataSet<? extends Entry>>> extends Chart<T> implements BarLineScatterCandleDataProvider {

    protected int mMaxVisibleCount = 100;

    private boolean mAutoScaleMinMaxEnabled = false;

    private Integer mAutoScaleLastLowestVisibleXIndex = null;

    private Integer mAutoScaleLastHighestVisibleXIndex = null;

    protected boolean mPinchZoomEnabled = false;

    protected boolean mDoubleTapToZoomEnabled = true;

    protected boolean mHighlightPerDragEnabled = true;

    protected boolean mHighLightIndicatorEnabled = true;

    private boolean mDragEnabled = true;

    private boolean mScaleXEnabled = true;

    private boolean mScaleYEnabled = true;

    protected boolean mFilterData = false;

    protected Paint mGridBackgroundPaint;

    protected Paint mBorderPaint;

    protected boolean mDrawGridBackground = true;

    protected boolean mDrawBorders = false;

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
        mListener = new BarLineChartTouchListener<BarLineChartBase<? extends BarLineScatterCandleData<? extends BarLineScatterCandleDataSet<? extends Entry>>>>(this, mViewPortHandler.getMatrixTouch());
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
        if (mDataNotSet)
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
        if (mHighlightEnabled && mHighLightIndicatorEnabled && valuesToHighlight())
            mRenderer.drawHighlighted(canvas, mIndicesToHightlight);
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
        if (mDataNotSet) {
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
        if (mAxisLeft.needsDefaultFormatter())
            mAxisLeft.setValueFormatter(mDefaultFormatter);
        if (mAxisRight.needsDefaultFormatter())
            mAxisRight.setValueFormatter(mDefaultFormatter);
        mAxisRendererLeft.computeAxis(mAxisLeft.mAxisMinimum, mAxisLeft.mAxisMaximum);
        mAxisRendererRight.computeAxis(mAxisRight.mAxisMinimum, mAxisRight.mAxisMaximum);
        mXAxisRenderer.computeAxis(mData.getXValAverageLength(), mData.getXVals());
        if (mLegend != null)
            mLegendRenderer.computeLegend(mData);
        calculateOffsets();
    }

    @Override
    protected void calcMinMax() {
        if (mAutoScaleMinMaxEnabled)
            mData.calcMinMax(getLowestVisibleXIndex(), getHighestVisibleXIndex());
        float minLeft = mData.getYMin(AxisDependency.LEFT);
        float maxLeft = mData.getYMax(AxisDependency.LEFT);
        float minRight = mData.getYMin(AxisDependency.RIGHT);
        float maxRight = mData.getYMax(AxisDependency.RIGHT);
        float leftRange = Math.abs(maxLeft - (mAxisLeft.isStartAtZeroEnabled() ? 0 : minLeft));
        float rightRange = Math.abs(maxRight - (mAxisRight.isStartAtZeroEnabled() ? 0 : minRight));
        if (leftRange == 0f) {
            maxLeft = maxLeft + 1f;
            if (!mAxisLeft.isStartAtZeroEnabled())
                minLeft = minLeft - 1f;
        }
        if (rightRange == 0f) {
            maxRight = maxRight + 1f;
            if (!mAxisRight.isStartAtZeroEnabled())
                minRight = minRight - 1f;
        }
        float topSpaceLeft = leftRange / 100f * mAxisLeft.getSpaceTop();
        float topSpaceRight = rightRange / 100f * mAxisRight.getSpaceTop();
        float bottomSpaceLeft = leftRange / 100f * mAxisLeft.getSpaceBottom();
        float bottomSpaceRight = rightRange / 100f * mAxisRight.getSpaceBottom();
        mXChartMax = mData.getXVals().size() - 1;
        mDeltaX = Math.abs(mXChartMax - mXChartMin);
        mAxisLeft.mAxisMaximum = !Float.isNaN(mAxisLeft.getAxisMaxValue()) ? mAxisLeft.getAxisMaxValue() : maxLeft + topSpaceLeft;
        mAxisRight.mAxisMaximum = !Float.isNaN(mAxisRight.getAxisMaxValue()) ? mAxisRight.getAxisMaxValue() : maxRight + topSpaceRight;
        mAxisLeft.mAxisMinimum = !Float.isNaN(mAxisLeft.getAxisMinValue()) ? mAxisLeft.getAxisMinValue() : minLeft - bottomSpaceLeft;
        mAxisRight.mAxisMinimum = !Float.isNaN(mAxisRight.getAxisMinValue()) ? mAxisRight.getAxisMinValue() : minRight - bottomSpaceRight;
        if (mAxisLeft.isStartAtZeroEnabled())
            mAxisLeft.mAxisMinimum = 0f;
        if (mAxisRight.isStartAtZeroEnabled())
            mAxisRight.mAxisMinimum = 0f;
        mAxisLeft.mAxisRange = Math.abs(mAxisLeft.mAxisMaximum - mAxisLeft.mAxisMinimum);
        mAxisRight.mAxisRange = Math.abs(mAxisRight.mAxisMaximum - mAxisRight.mAxisMinimum);
    }

    @Override
    protected void calculateOffsets() {
        if (!mCustomViewPortEnabled) {
            float offsetLeft = 0f, offsetRight = 0f, offsetTop = 0f, offsetBottom = 0f;
            if (mLegend != null && mLegend.isEnabled()) {
                if (mLegend.getPosition() == LegendPosition.RIGHT_OF_CHART || mLegend.getPosition() == LegendPosition.RIGHT_OF_CHART_CENTER) {
                    offsetRight += mLegend.mTextWidthMax + mLegend.getXOffset() * 2f;
                } else if (mLegend.getPosition() == LegendPosition.LEFT_OF_CHART || mLegend.getPosition() == LegendPosition.LEFT_OF_CHART_CENTER) {
                    offsetLeft += mLegend.mTextWidthMax + mLegend.getXOffset() * 2f;
                } else if (mLegend.getPosition() == LegendPosition.BELOW_CHART_LEFT || mLegend.getPosition() == LegendPosition.BELOW_CHART_RIGHT || mLegend.getPosition() == LegendPosition.BELOW_CHART_CENTER) {
                    offsetBottom += mLegend.mTextHeightMax * 3f;
                }
            }
            if (mAxisLeft.needsOffset()) {
                offsetLeft += mAxisLeft.getRequiredWidthSpace(mAxisRendererLeft.getPaintAxisLabels());
            }
            if (mAxisRight.needsOffset()) {
                offsetRight += mAxisRight.getRequiredWidthSpace(mAxisRendererRight.getPaintAxisLabels());
            }
            float xlabelheight = mXAxis.mLabelHeight * 2f;
            if (mXAxis.isEnabled()) {
                if (mXAxis.getPosition() == XAxisPosition.BOTTOM) {
                    offsetBottom += xlabelheight;
                } else if (mXAxis.getPosition() == XAxisPosition.TOP) {
                    offsetTop += xlabelheight;
                } else if (mXAxis.getPosition() == XAxisPosition.BOTH_SIDED) {
                    offsetBottom += xlabelheight;
                    offsetTop += xlabelheight;
                }
            }
            float min = Utils.convertDpToPixel(10f);
            mViewPortHandler.restrainViewPort(Math.max(min, offsetLeft), Math.max(min, offsetTop), Math.max(min, offsetRight), Math.max(min, offsetBottom));
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
            mXAxis.mAxisLabelModulus = (int) Math.ceil((mData.getXValCount() * mXAxis.mLabelWidth) / (mViewPortHandler.contentWidth() * values[Matrix.MSCALE_X]));
        }
        if (mLogEnabled)
            Log.i(LOG_TAG, "X-Axis modulus: " + mXAxis.mAxisLabelModulus + ", x-axis label width: " + mXAxis.mLabelWidth + ", content width: " + mViewPortHandler.contentWidth());
        if (mXAxis.mAxisLabelModulus < 1)
            mXAxis.mAxisLabelModulus = 1;
    }

    @Override
    protected float[] getMarkerPosition(Entry e, int dataSetIndex) {
        float xPos = e.getXIndex();
        if (this instanceof BarChart) {
            BarData bd = (BarData) mData;
            float space = bd.getGroupSpace();
            float j = mData.getDataSetByIndex(dataSetIndex).getEntryPosition(e);
            float x = (j * (mData.getDataSetCount() - 1)) + dataSetIndex + space * j + space / 2f;
            xPos += x;
        }
        float[] pts = new float[] { xPos, e.getVal() * mAnimator.getPhaseY() };
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

    protected OnTouchListener mListener;

    @Override
    public boolean onTouchEvent(MotionEvent event) {
        super.onTouchEvent(event);
        if (mListener == null || mDataNotSet)
            return false;
        if (!mTouchEnabled)
            return false;
        else
            return mListener.onTouch(this, event);
    }

    @Override
    public void computeScroll() {
        if (mListener instanceof BarLineChartTouchListener)
            ((BarLineChartTouchListener<?>) mListener).computeScroll();
    }

    public void zoomIn() {
        Matrix save = mViewPortHandler.zoomIn(getWidth() / 2f, -(getHeight() / 2f));
        mViewPortHandler.refresh(save, this, true);
    }

    public void zoomOut() {
        Matrix save = mViewPortHandler.zoomOut(getWidth() / 2f, -(getHeight() / 2f));
        mViewPortHandler.refresh(save, this, true);
    }

    public void zoom(float scaleX, float scaleY, float x, float y) {
        Matrix save = mViewPortHandler.zoom(scaleX, scaleY, x, -y);
        mViewPortHandler.refresh(save, this, true);
    }

    public void fitScreen() {
        Matrix save = mViewPortHandler.fitScreen();
        mViewPortHandler.refresh(save, this, true);
    }

    public void setScaleMinima(float scaleX, float scaleY) {
        mViewPortHandler.setMinimumScaleX(scaleX);
        mViewPortHandler.setMinimumScaleY(scaleY);
    }

    public void setVisibleXRange(float xRange) {
        float xScale = mDeltaX / (xRange);
        mViewPortHandler.setMinimumScaleX(xScale);
    }

    public void setVisibleYRange(float yRange, AxisDependency axis) {
        float yScale = getDeltaY(axis) / yRange;
        mViewPortHandler.setMinimumScaleY(yScale);
    }

    public void moveViewToX(float xIndex) {
        Runnable job = new MoveViewJob(mViewPortHandler, xIndex, 0f, getTransformer(AxisDependency.LEFT), this);
        if (mViewPortHandler.hasChartDimens()) {
            post(job);
        } else {
            mJobs.add(job);
        }
    }

    public void moveViewToY(float yValue, AxisDependency axis) {
        float valsInView = getDeltaY(axis) / mViewPortHandler.getScaleY();
        Runnable job = new MoveViewJob(mViewPortHandler, 0f, yValue + valsInView / 2f, getTransformer(axis), this);
        if (mViewPortHandler.hasChartDimens()) {
            post(job);
        } else {
            mJobs.add(job);
        }
    }

    public void moveViewTo(float xIndex, float yValue, AxisDependency axis) {
        float valsInView = getDeltaY(axis) / mViewPortHandler.getScaleY();
        Runnable job = new MoveViewJob(mViewPortHandler, xIndex, yValue + valsInView / 2f, getTransformer(axis), this);
        if (mViewPortHandler.hasChartDimens()) {
            post(job);
        } else {
            mJobs.add(job);
        }
    }

    public void centerViewTo(int xIndex, float yValue, AxisDependency axis) {
        float valsInView = getDeltaY(axis) / mViewPortHandler.getScaleY();
        float xsInView = getXAxis().getValues().size() / mViewPortHandler.getScaleX();
        Runnable job = new MoveViewJob(mViewPortHandler, xIndex - xsInView / 2f, yValue + valsInView / 2f, getTransformer(axis), this);
        if (mViewPortHandler.hasChartDimens()) {
            post(job);
        } else {
            mJobs.add(job);
        }
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

    public void setOnTouchListener(OnTouchListener l) {
        this.mListener = l;
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

    public void setHighlightIndicatorEnabled(boolean enabled) {
        mHighLightIndicatorEnabled = enabled;
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

    public Highlight getHighlightByTouchPoint(float x, float y) {
        if (mDataNotSet || mData == null) {
            Log.e(LOG_TAG, "Can't select by touch. No data set.");
            return null;
        }
        float[] pts = new float[2];
        pts[0] = x;
        mLeftAxisTransformer.pixelsToValue(pts);
        double xTouchVal = pts[0];
        double base = Math.floor(xTouchVal);
        double touchOffset = mDeltaX * 0.025;
        if (xTouchVal < -touchOffset || xTouchVal > mDeltaX + touchOffset)
            return null;
        if (base < 0)
            base = 0;
        if (base >= mDeltaX)
            base = mDeltaX - 1;
        int xIndex = (int) base;
        if (xTouchVal - base > 0.5) {
            xIndex = (int) base + 1;
        }
        List<SelInfo> valsAtIndex = getYValsAtIndex(xIndex);
        float leftdist = Utils.getMinimumDistance(valsAtIndex, y, AxisDependency.LEFT);
        float rightdist = Utils.getMinimumDistance(valsAtIndex, y, AxisDependency.RIGHT);
        if (mData.getFirstRight() == null)
            rightdist = Float.MAX_VALUE;
        if (mData.getFirstLeft() == null)
            leftdist = Float.MAX_VALUE;
        AxisDependency axis = leftdist < rightdist ? AxisDependency.LEFT : AxisDependency.RIGHT;
        int dataSetIndex = Utils.getClosestDataSetIndex(valsAtIndex, y, axis);
        if (dataSetIndex == -1)
            return null;
        return new Highlight(xIndex, dataSetIndex);
    }

    public List<SelInfo> getYValsAtIndex(int xIndex) {
        List<SelInfo> vals = new ArrayList<SelInfo>();
        float[] pts = new float[2];
        for (int i = 0; i < mData.getDataSetCount(); i++) {
            DataSet<?> dataSet = mData.getDataSetByIndex(i);
            float yVal = dataSet.getYValForXIndex(xIndex);
            pts[1] = yVal;
            getTransformer(dataSet.getAxisDependency()).pointValuesToPixel(pts);
            if (!Float.isNaN(pts[1])) {
                vals.add(new SelInfo(pts[1], i, dataSet));
            }
        }
        return vals;
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

    public BarLineScatterCandleDataSet<? extends Entry> getDataSetByTouchPoint(float x, float y) {
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
        return (pts[0] <= 0) ? 0 : (int) (pts[0] + 1);
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

    public void enableFiltering(Approximator a) {
        mFilterData = true;
    }

    public void disableFiltering() {
        mFilterData = false;
    }

    public boolean isFilteringEnabled() {
        return mFilterData;
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

    public YAxisRenderer getRendererLeftYAxis() {
        return mAxisRendererLeft;
    }

    public YAxisRenderer getRendererRightYAxis() {
        return mAxisRendererRight;
    }

    public float getYChartMax() {
        return Math.max(mAxisLeft.mAxisMaximum, mAxisRight.mAxisMaximum);
    }

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

    protected class DefaultFillFormatter implements FillFormatter {

        @Override
        public float getFillLinePosition(LineDataSet dataSet, LineData data, float chartMaxY, float chartMinY) {
            float fillMin = 0f;
            if (dataSet.getYMax() > 0 && dataSet.getYMin() < 0) {
                fillMin = 0f;
            } else {
                if (!getAxis(dataSet.getAxisDependency()).isStartAtZeroEnabled()) {
                    float max, min;
                    if (data.getYMax() > 0)
                        max = 0f;
                    else
                        max = chartMaxY;
                    if (data.getYMin() < 0)
                        min = 0f;
                    else
                        min = chartMinY;
                    fillMin = dataSet.getYMin() >= 0 ? min : max;
                } else {
                    fillMin = 0f;
                }
            }
            return fillMin;
        }
    }
}