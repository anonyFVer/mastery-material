package com.github.mikephil.charting.charts;

import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Matrix;
import android.graphics.Paint;
import android.graphics.Paint.Align;
import android.graphics.Paint.Style;
import android.graphics.Path;
import android.graphics.Rect;
import android.graphics.Typeface;
import android.util.AttributeSet;
import android.util.Log;
import android.view.ViewParent;
import com.github.mikephil.charting.data.ChartData;
import com.github.mikephil.charting.data.DataSet;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.filter.Approximator;
import com.github.mikephil.charting.interfaces.OnDrawListener;
import com.github.mikephil.charting.listener.BarLineChartTouchListener;
import com.github.mikephil.charting.utils.Highlight;
import com.github.mikephil.charting.utils.Legend.LegendPosition;
import com.github.mikephil.charting.utils.YLabels.YLabelPosition;
import com.github.mikephil.charting.utils.PointD;
import com.github.mikephil.charting.utils.SelInfo;
import com.github.mikephil.charting.utils.Utils;
import com.github.mikephil.charting.utils.YLabels;
import java.text.DecimalFormat;
import java.util.ArrayList;

public abstract class BarLineChartBase extends Chart {

    protected String mUnit = "";

    protected int mMaxVisibleCount = 100;

    private float mMinScaleY = 1f;

    private float mMinScaleX = 1f;

    protected float mScaleX = 1f;

    protected float mScaleY = 1f;

    protected float mMaxScaleY = 7f;

    protected int mXLabelWidth = 1;

    protected int mXAxisLabelModulus = 1;

    protected int mYLabelCount = 9;

    protected float mGridWidth = 1f;

    protected boolean mDrawUnitInChart = false;

    private boolean mDrawTopYLabelEntry = true;

    protected boolean mPinchZoomEnabled = false;

    protected boolean mDrawUnitsInLabels = true;

    protected boolean mCenterXAxisLabels = false;

    protected boolean mAdjustXAxisLabels = true;

    protected boolean mDragEnabled = true;

    protected boolean mFixedYValues = false;

    protected boolean mStartAtZero = true;

    protected boolean mFilterData = false;

    protected Paint mGridPaint;

    protected Paint mGridBackgroundPaint;

    protected Paint mBorderPaint;

    protected Paint mXLabelPaint;

    protected Paint mYLabelPaint;

    protected Paint mHighlightPaint;

    protected boolean mHighLightIndicatorEnabled = true;

    protected boolean mAutoFinishDrawing;

    protected boolean mDrawVerticalGrid = true;

    protected boolean mDrawHorizontalGrid = true;

    protected boolean mDrawYLabels = true;

    protected boolean mDrawXLabels = true;

    protected boolean mDrawBorder = true;

    protected boolean mDrawGridBackground = true;

    protected OnDrawListener mDrawListener;

    protected YLabels mYLabels = new YLabels();

    private Approximator mApproximator;

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
        mListener = new BarLineChartTouchListener(this, mMatrixTouch);
        mXLabelPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mXLabelPaint.setColor(Color.BLACK);
        mXLabelPaint.setTextAlign(Align.CENTER);
        mXLabelPaint.setTextSize(Utils.convertDpToPixel(10f));
        mYLabelPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mYLabelPaint.setColor(Color.BLACK);
        mYLabelPaint.setTextSize(Utils.convertDpToPixel(10f));
        mGridPaint = new Paint();
        mGridPaint.setColor(Color.GRAY);
        mGridPaint.setStrokeWidth(mGridWidth);
        mGridPaint.setStyle(Style.STROKE);
        mGridPaint.setAlpha(90);
        mBorderPaint = new Paint();
        mBorderPaint.setColor(Color.BLACK);
        mBorderPaint.setStrokeWidth(mGridWidth * 2f);
        mBorderPaint.setStyle(Style.STROKE);
        mGridBackgroundPaint = new Paint();
        mGridBackgroundPaint.setStyle(Style.FILL);
        mGridBackgroundPaint.setColor(Color.rgb(240, 240, 240));
        mHighlightPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mHighlightPaint.setStyle(Paint.Style.STROKE);
        mHighlightPaint.setStrokeWidth(2f);
        mHighlightPaint.setColor(Color.rgb(255, 187, 115));
    }

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        if (mDataNotSet)
            return;
        long starttime = System.currentTimeMillis();
        if (mFilterData) {
            mCurrentData = getFilteredData();
            Log.i(LOG_TAG, "FilterTime: " + (System.currentTimeMillis() - starttime) + " ms");
            starttime = System.currentTimeMillis();
        } else {
            mCurrentData = getDataOriginal();
        }
        if (mAdjustXAxisLabels)
            calcModulus();
        drawGridBackground();
        drawBorder();
        prepareYLabels();
        int clipRestoreCount = mDrawCanvas.save();
        mDrawCanvas.clipRect(mContentRect);
        drawHorizontalGrid();
        drawVerticalGrid();
        drawData();
        drawHighlights();
        mDrawCanvas.restoreToCount(clipRestoreCount);
        drawAdditional();
        drawXLabels();
        drawYLabels();
        drawValues();
        drawLegend();
        drawMarkers();
        drawDescription();
        canvas.drawBitmap(mDrawBitmap, 0, 0, mDrawPaint);
        Log.i(LOG_TAG, "DrawTime: " + (System.currentTimeMillis() - starttime) + " ms");
    }

    @Override
    public void prepare() {
        if (mDataNotSet)
            return;
        calcMinMax(mFixedYValues);
        prepareXLabels();
        calcFormats();
        prepareLegend();
    }

    @Override
    public void notifyDataSetChanged() {
        if (!mFixedYValues) {
            prepare();
        } else {
            calcMinMax(mFixedYValues);
        }
    }

    @Override
    public void calculateOffsets() {
        if (mLegend.getPosition() == LegendPosition.RIGHT_OF_CHART) {
            mOffsetRight = mLegend.getMaximumEntryLength(mLegendLabelPaint);
            mLegendLabelPaint.setTextAlign(Align.LEFT);
        } else if (mLegend.getPosition() == LegendPosition.BELOW_CHART_LEFT || mLegend.getPosition() == LegendPosition.BELOW_CHART_RIGHT) {
            mOffsetBottom = (int) (mLegendLabelPaint.getTextSize() * 3.5f);
        }
        if (mYLabels.getPosition() == YLabelPosition.LEFT) {
            mOffsetLeft = Utils.calcTextWidth(mYLabelPaint, (int) mDeltaY + ".0000" + mUnit);
            mYLabelPaint.setTextAlign(Align.RIGHT);
        } else if (mYLabels.getPosition() == YLabelPosition.RIGHT) {
            mOffsetRight = Utils.calcTextWidth(mYLabelPaint, (int) mDeltaY + ".0000" + mUnit);
            mYLabelPaint.setTextAlign(Align.LEFT);
        } else if (mYLabels.getPosition() == YLabelPosition.BOTH_SIDED) {
            mOffsetRight = Utils.calcTextWidth(mYLabelPaint, (int) mDeltaY + ".0000" + mUnit);
            mOffsetLeft = Utils.calcTextWidth(mYLabelPaint, (int) mDeltaY + ".0000" + mUnit);
        }
        prepareContentRect();
        float scaleX = (float) ((getWidth() - mOffsetLeft - mOffsetRight) / mDeltaX);
        float scaleY = (float) ((getHeight() - mOffsetBottom - mOffsetTop) / mDeltaY);
        Matrix val = new Matrix();
        val.postTranslate(0, -mYChartMin);
        val.postScale(scaleX, -scaleY);
        mMatrixValueToPx.set(val);
        Matrix offset = new Matrix();
        offset.postTranslate(mOffsetLeft, getHeight() - mOffsetBottom);
        mMatrixOffset.set(offset);
    }

    protected void calcModulus() {
        float[] values = new float[9];
        mMatrixTouch.getValues(values);
        mXAxisLabelModulus = (int) Math.ceil((mCurrentData.getXValCount() * mXLabelWidth) / (mContentRect.width() * values[Matrix.MSCALE_X]));
    }

    protected DecimalFormat mFormatValue = null;

    protected int mYLabelFormatDigits = -1;

    protected void calcFormats() {
        if (mValueDigitsToUse == -1)
            mValueFormatDigits = Utils.getFormatDigits(mDeltaY);
        else
            mValueFormatDigits = mValueDigitsToUse;
        StringBuffer b = new StringBuffer();
        for (int i = 0; i < mValueFormatDigits; i++) {
            if (i == 0)
                b.append(".");
            b.append("0");
        }
        mFormatValue = new DecimalFormat("###,###,###,##0" + b.toString());
    }

    @Override
    protected void calcMinMax(boolean fixedValues) {
        super.calcMinMax(fixedValues);
        float space = mDeltaY / 100f * 10f;
        if (mStartAtZero) {
            mYChartMin = 0;
        } else {
            mYChartMin = mYChartMin - space;
        }
        mYChartMax = mYChartMax + space;
        mDeltaY = Math.abs(mYChartMax - mYChartMin);
    }

    protected void prepareXLabels() {
        StringBuffer a = new StringBuffer();
        int length = (int) (((float) (mCurrentData.getXVals().get(0).length() + mCurrentData.getXVals().get(mCurrentData.getXValCount() - 1).length())));
        if (mCurrentData.getXVals().get(0).length() <= 3)
            length *= 2;
        for (int i = 0; i < length; i++) {
            a.append("h");
        }
        mXLabelWidth = Utils.calcTextWidth(mXLabelPaint, a.toString());
    }

    private void prepareYLabels() {
        PointD p1 = getValuesByTouchPoint(mContentRect.left, mContentRect.top);
        PointD p2 = getValuesByTouchPoint(mContentRect.left, mContentRect.bottom);
        mYChartMin = (float) p2.y;
        mYChartMax = (float) p1.y;
        float yMin = mYChartMin;
        float yMax = mYChartMax;
        double range = yMax - yMin;
        if (mYLabelCount == 0 || range <= 0) {
            mYLabels.mEntries = new float[] {};
            mYLabels.mEntryCount = 0;
            return;
        }
        double rawInterval = range / mYLabelCount;
        double interval = Utils.roundToNextSignificant(rawInterval);
        double intervalMagnitude = Math.pow(10, (int) Math.log10(interval));
        int intervalSigDigit = (int) (interval / intervalMagnitude);
        if (intervalSigDigit > 5) {
            interval = Math.floor(10 * intervalMagnitude);
        }
        double first = Math.ceil(yMin / interval) * interval;
        double last = Math.nextUp(Math.floor(yMax / interval) * interval);
        double f;
        int i;
        int n = 0;
        for (f = first; f <= last; f += interval) {
            ++n;
        }
        mYLabels.mEntryCount = n;
        if (mYLabels.mEntries.length < n) {
            mYLabels.mEntries = new float[n];
        }
        for (f = first, i = 0; i < n; f += interval, ++i) {
            mYLabels.mEntries[i] = (float) f;
        }
        if (interval < 1) {
            mYLabels.mDecimals = (int) Math.ceil(-Math.log10(interval));
        } else {
            mYLabels.mDecimals = 0;
        }
    }

    protected void drawXLabels() {
        if (!mDrawXLabels)
            return;
        float[] position = new float[] { 0f, 0f };
        for (int i = 0; i < mCurrentData.getXValCount(); i++) {
            if (i % mXAxisLabelModulus == 0) {
                position[0] = i;
                if (mCenterXAxisLabels)
                    position[0] += 0.5f;
                transformPointArray(position);
                if (position[0] >= mOffsetLeft && position[0] <= getWidth() - mOffsetRight + 10) {
                    mDrawCanvas.drawText(mCurrentData.getXVals().get(i), position[0], mOffsetTop - 5, mXLabelPaint);
                }
            }
        }
    }

    protected void drawYLabels() {
        if (!mDrawYLabels)
            return;
        float[] positions = new float[mYLabels.mEntryCount * 2];
        for (int i = 0; i < positions.length; i += 2) {
            positions[i + 1] = mYLabels.mEntries[i / 2];
        }
        transformPointArray(positions);
        if (mYLabels.getPosition() == YLabelPosition.LEFT) {
            mYLabelPaint.setTextAlign(Align.RIGHT);
            drawYLabels(mOffsetLeft - 13, positions);
        } else if (mYLabels.getPosition() == YLabelPosition.RIGHT) {
            mYLabelPaint.setTextAlign(Align.LEFT);
            drawYLabels(getWidth() - mOffsetRight + 13, positions);
        } else {
            mYLabelPaint.setTextAlign(Align.RIGHT);
            drawYLabels(mOffsetLeft - 13, positions);
            mYLabelPaint.setTextAlign(Align.LEFT);
            drawYLabels(getWidth() - mOffsetRight + 13, positions);
        }
    }

    private void drawYLabels(float xPos, float[] positions) {
        for (int i = 0; i < mYLabels.mEntryCount; i++) {
            String text = Utils.formatNumber(mYLabels.mEntries[i], mYLabels.mDecimals, mSeparateTousands);
            if (!mDrawTopYLabelEntry && i >= mYLabels.mEntryCount - 1)
                return;
            if (mDrawUnitsInLabels) {
                mDrawCanvas.drawText(text + mUnit, xPos, positions[i * 2 + 1], mYLabelPaint);
            } else {
                mDrawCanvas.drawText(text, xPos, positions[i * 2 + 1], mYLabelPaint);
            }
        }
    }

    public enum BorderStyle {

        LEFT, RIGHT, TOP, BOTTOM
    }

    private BorderStyle[] mBorderStyles = new BorderStyle[] { BorderStyle.BOTTOM };

    protected void drawBorder() {
        if (!mDrawBorder || mBorderStyles == null)
            return;
        for (int i = 0; i < mBorderStyles.length; i++) {
            switch(mBorderStyles[i]) {
                case LEFT:
                    mDrawCanvas.drawLine(mOffsetLeft, mOffsetTop, mOffsetLeft, getHeight() - mOffsetBottom, mBorderPaint);
                    break;
                case RIGHT:
                    mDrawCanvas.drawLine(getWidth() - mOffsetRight, mOffsetTop, getWidth() - mOffsetRight, getHeight() - mOffsetBottom, mBorderPaint);
                    break;
                case TOP:
                    mDrawCanvas.drawLine(mOffsetLeft, mOffsetTop, getWidth() - mOffsetRight, mOffsetTop, mBorderPaint);
                    break;
                case BOTTOM:
                    mDrawCanvas.drawLine(mOffsetLeft, getHeight() - mOffsetBottom, getWidth() - mOffsetRight, getHeight() - mOffsetBottom, mBorderPaint);
                    break;
            }
        }
    }

    protected void drawGridBackground() {
        if (!mDrawGridBackground)
            return;
        Rect gridBackground = new Rect(mOffsetLeft + 1, mOffsetTop + 1, getWidth() - mOffsetRight, getHeight() - mOffsetBottom);
        mDrawCanvas.drawRect(gridBackground, mGridBackgroundPaint);
    }

    protected void drawHorizontalGrid() {
        if (!mDrawHorizontalGrid)
            return;
        Path p = new Path();
        for (int i = 0; i < mYLabels.mEntryCount; i++) {
            p.reset();
            p.moveTo(0, mYLabels.mEntries[i]);
            p.lineTo(mDeltaX, mYLabels.mEntries[i]);
            transformPath(p);
            mDrawCanvas.drawPath(p, mGridPaint);
        }
    }

    protected void drawVerticalGrid() {
        if (!mDrawVerticalGrid)
            return;
        float[] position = new float[] { 0f, 0f };
        for (int i = 0; i < mCurrentData.getXValCount(); i++) {
            if (i % mXAxisLabelModulus == 0) {
                position[0] = i;
                transformPointArray(position);
                if (position[0] >= mOffsetLeft && position[0] <= getWidth()) {
                    mDrawCanvas.drawLine(position[0], mOffsetTop, position[0], getHeight() - mOffsetBottom, mGridPaint);
                }
            }
        }
    }

    protected boolean isOffContentRight(float p) {
        if (p > mContentRect.right)
            return true;
        else
            return false;
    }

    protected boolean isOffContentLeft(float p) {
        if (p < mContentRect.left)
            return true;
        else
            return false;
    }

    protected boolean isOffContentTop(float p) {
        if (p < mContentRect.top)
            return true;
        else
            return false;
    }

    protected boolean isOffContentBottom(float p) {
        if (p > mContentRect.bottom)
            return true;
        else
            return false;
    }

    public void disableScroll() {
        ViewParent parent = getParent();
        parent.requestDisallowInterceptTouchEvent(true);
    }

    public void enableScroll() {
        ViewParent parent = getParent();
        parent.requestDisallowInterceptTouchEvent(false);
    }

    public void zoomIn(float x, float y) {
        Matrix save = new Matrix();
        save.set(mMatrixTouch);
        save.postScale(1.4f, 1.4f, x, y);
        refreshTouch(save);
    }

    public void zoomOut(float x, float y) {
        Matrix save = new Matrix();
        save.set(mMatrixTouch);
        save.postScale(0.7f, 0.7f, x, y);
        refreshTouch(save);
    }

    public void zoom(float scaleX, float scaleY, float x, float y) {
        Matrix save = new Matrix();
        save.set(mMatrixTouch);
        save.postScale(scaleX, scaleY, x, -y);
        refreshTouch(save);
    }

    public synchronized void centerViewPort(final int xIndex, final float yVal) {
        post(new Runnable() {

            @Override
            public void run() {
                float indicesInView = mDeltaX / mScaleX;
                float valsInView = mDeltaY / mScaleY;
                float[] pts = new float[] { xIndex - indicesInView / 2f, yVal + valsInView / 2f };
                Matrix save = new Matrix();
                save.set(mMatrixTouch);
                transformPointArray(pts);
                final float x = -pts[0] + getOffsetLeft();
                final float y = -pts[1] + getOffsetBottom();
                save.postTranslate(x, y);
                refreshTouchNoInvalidate(save);
            }
        });
    }

    public Matrix refreshTouch(Matrix newTouchMatrix) {
        mMatrixTouch.set(newTouchMatrix);
        limitTransAndScale(mMatrixTouch);
        invalidate();
        newTouchMatrix.set(mMatrixTouch);
        return newTouchMatrix;
    }

    public Matrix refreshTouchNoInvalidate(Matrix newTouchMatrix) {
        mMatrixTouch.set(newTouchMatrix);
        limitTransAndScale(mMatrixTouch);
        newTouchMatrix.set(mMatrixTouch);
        return newTouchMatrix;
    }

    protected void limitTransAndScale(Matrix matrix) {
        float[] vals = new float[9];
        matrix.getValues(vals);
        float curTransX = vals[Matrix.MTRANS_X];
        float curScaleX = vals[Matrix.MSCALE_X];
        float curTransY = vals[Matrix.MTRANS_Y];
        float curScaleY = vals[Matrix.MSCALE_Y];
        mScaleX = Math.max(mMinScaleX, Math.min(getMaxScaleX(), curScaleX));
        mScaleY = Math.max(mMinScaleY, Math.min(getMaxScaleY(), curScaleY));
        if (mContentRect == null)
            return;
        float maxTransX = -(float) mContentRect.width() * (mScaleX - 1f);
        float newTransX = Math.min(Math.max(curTransX, maxTransX), 0f);
        float maxTransY = (float) mContentRect.height() * (mScaleY - 1f);
        float newTransY = Math.max(Math.min(curTransY, maxTransY), 0f);
        vals[Matrix.MTRANS_X] = newTransX;
        vals[Matrix.MSCALE_X] = mScaleX;
        vals[Matrix.MTRANS_Y] = newTransY;
        vals[Matrix.MSCALE_Y] = mScaleY;
        matrix.setValues(vals);
    }

    public void setOnDrawListener(OnDrawListener drawListener) {
        this.mDrawListener = drawListener;
    }

    public void setDrawingEnabled(boolean drawingEnabled) {
        if (mListener instanceof BarLineChartTouchListener) {
            ((BarLineChartTouchListener) mListener).setDrawingEnabled(drawingEnabled);
        }
    }

    public void setAutoFinish(boolean enabled) {
        this.mAutoFinishDrawing = enabled;
    }

    public boolean isAutoFinishEnabled() {
        return mAutoFinishDrawing;
    }

    public OnDrawListener getDrawListener() {
        return mDrawListener;
    }

    public void setScaleMinima(float scaleXmin, float scaleYmin) {
        mMinScaleX = scaleXmin;
        mMinScaleY = scaleYmin;
        zoom(mMinScaleX, mMinScaleY, 0f, 0f);
    }

    public void setDrawTopYLabelEntry(boolean enabled) {
        mDrawTopYLabelEntry = enabled;
    }

    public void setYRange(float minY, float maxY, boolean invalidate) {
        if (Float.isNaN(minY) || Float.isNaN(maxY)) {
            resetYRange(invalidate);
            return;
        }
        mFixedYValues = true;
        mYChartMin = minY;
        mYChartMax = maxY;
        if (minY < 0) {
            mStartAtZero = false;
        }
        mDeltaY = mYChartMax - mYChartMin;
        calcFormats();
        prepareMatrix();
        if (invalidate)
            invalidate();
    }

    public void resetYRange(boolean invalidate) {
        mFixedYValues = false;
        calcMinMax(mFixedYValues);
        prepareMatrix();
        if (invalidate)
            invalidate();
    }

    public void setYLabelCount(int yCount) {
        if (yCount > 15)
            yCount = 15;
        if (yCount < 3)
            yCount = 3;
        mYLabelCount = yCount;
    }

    public void setAdjustXLabels(boolean enabled) {
        mAdjustXAxisLabels = enabled;
    }

    public boolean isAdjustXLabelsEnabled() {
        return mAdjustXAxisLabels;
    }

    public boolean hasFixedYValues() {
        return mFixedYValues;
    }

    public void setGridColor(int color) {
        mGridPaint.setColor(color);
    }

    public void setMaxVisibleValueCount(int count) {
        this.mMaxVisibleCount = count;
    }

    public void setYLabelTextSize(float size) {
        if (size > 14f)
            size = 14f;
        if (size < 7f)
            size = 7f;
        mYLabelPaint.setTextSize(Utils.convertDpToPixel(size));
    }

    public void setXLabelTextSize(float size) {
        if (size > 14f)
            size = 14f;
        if (size < 7f)
            size = 7f;
        mXLabelPaint.setTextSize(Utils.convertDpToPixel(size));
    }

    public void setHighlightIndicatorEnabled(boolean enabled) {
        mHighLightIndicatorEnabled = enabled;
    }

    public void setStartAtZero(boolean enabled) {
        this.mStartAtZero = enabled;
        prepare();
        prepareMatrix();
    }

    public void setUnit(String unit) {
        mUnit = unit;
    }

    public boolean isStartAtZeroEnabled() {
        return mStartAtZero;
    }

    public void setDrawUnitsInChart(boolean enabled) {
        mDrawUnitInChart = enabled;
    }

    public void setDrawUnitsInYLabel(boolean enabled) {
        mDrawUnitsInLabels = enabled;
    }

    public void setCenterXLabelText(boolean enabled) {
        mCenterXAxisLabels = enabled;
    }

    public boolean isXLabelTextCentered() {
        return mCenterXAxisLabels;
    }

    public void setGridWidth(float width) {
        if (width < 0.1f)
            width = 0.1f;
        if (width > 3.0f)
            width = 3.0f;
        mGridWidth = width;
    }

    public void setDragEnabled(boolean enabled) {
        this.mDragEnabled = enabled;
    }

    public boolean isDragEnabled() {
        return mDragEnabled;
    }

    public void setDrawVerticalGrid(boolean enabled) {
        mDrawVerticalGrid = enabled;
    }

    public void setDrawHorizontalGrid(boolean enabled) {
        mDrawHorizontalGrid = enabled;
    }

    public boolean isDrawVerticalGridEnabled() {
        return mDrawVerticalGrid;
    }

    public boolean isDrawHorizontalGridEnabled() {
        return mDrawHorizontalGrid;
    }

    public void setDrawBorder(boolean enabled) {
        mDrawBorder = enabled;
    }

    public void setDrawGridBackground(boolean enabled) {
        mDrawGridBackground = enabled;
    }

    public void setDrawXLabels(boolean enabled) {
        mDrawXLabels = enabled;
    }

    public void setDrawYLabels(boolean enabled) {
        mDrawYLabels = enabled;
    }

    public void setBorderStyles(BorderStyle[] styles) {
        mBorderStyles = styles;
    }

    public Highlight getHighlightByTouchPoint(float x, float y) {
        float[] pts = new float[2];
        pts[0] = x;
        pts[1] = y;
        Matrix tmp = new Matrix();
        mMatrixOffset.invert(tmp);
        tmp.mapPoints(pts);
        mMatrixTouch.invert(tmp);
        tmp.mapPoints(pts);
        mMatrixValueToPx.invert(tmp);
        tmp.mapPoints(pts);
        double xTouchVal = pts[0];
        double yTouchVal = pts[1];
        double base = Math.floor(xTouchVal);
        Log.i(LOG_TAG, "touchindex x: " + xTouchVal + ", touchindex y: " + yTouchVal);
        if ((this instanceof LineChart || this instanceof ScatterChart) && (xTouchVal < 0 || xTouchVal > mDeltaX))
            return null;
        if (this instanceof BarChart && (xTouchVal < 0 || xTouchVal > mDeltaX + 1))
            return null;
        int xIndex = (int) base;
        int dataSetIndex = 0;
        if (this instanceof LineChart || this instanceof ScatterChart) {
            if (xTouchVal - base > 0.5) {
                xIndex = (int) base + 1;
            }
        }
        ArrayList<SelInfo> valsAtIndex = getYValsAtIndex(xIndex);
        dataSetIndex = getClosestDataSetIndex(valsAtIndex, (float) yTouchVal);
        if (dataSetIndex == -1)
            return null;
        return new Highlight(xIndex, dataSetIndex);
    }

    private int getClosestDataSetIndex(ArrayList<SelInfo> valsAtIndex, float val) {
        int index = -1;
        float distance = Float.MAX_VALUE;
        for (int i = 0; i < valsAtIndex.size(); i++) {
            float cdistance = Math.abs((float) valsAtIndex.get(i).val - val);
            if (cdistance < distance) {
                index = valsAtIndex.get(i).dataSetIndex;
                distance = cdistance;
            }
        }
        Log.i(LOG_TAG, "Closest DataSet index: " + index);
        return index;
    }

    public PointD getValuesByTouchPoint(float x, float y) {
        float[] pts = new float[2];
        pts[0] = x;
        pts[1] = y;
        Matrix tmp = new Matrix();
        mMatrixOffset.invert(tmp);
        tmp.mapPoints(pts);
        mMatrixTouch.invert(tmp);
        tmp.mapPoints(pts);
        mMatrixValueToPx.invert(tmp);
        tmp.mapPoints(pts);
        double xTouchVal = pts[0];
        double yTouchVal = pts[1];
        return new PointD(xTouchVal, yTouchVal);
    }

    public PointD getPixelsForValues(float x, float y) {
        float[] pts = new float[] { x, y };
        transformPointArray(pts);
        return new PointD(pts[0], pts[1]);
    }

    public float getYValueByTouchPoint(float x, float y) {
        return (float) getValuesByTouchPoint(x, y).y;
    }

    public Entry getEntryByTouchPoint(float x, float y) {
        Highlight h = getHighlightByTouchPoint(x, y);
        if (h != null) {
            return mCurrentData.getEntryForHighlight(h);
        }
        return null;
    }

    public float getScaleX() {
        return mScaleX;
    }

    public float getScaleY() {
        return mScaleY;
    }

    public float getMaxScaleX() {
        return mDeltaX / 2f;
    }

    public float getMaxScaleY() {
        return mMaxScaleY;
    }

    public void setMaxScaleY(float factor) {
        if (factor < 1f)
            factor = 1f;
        if (factor > 20f)
            factor = 20f;
        mMaxScaleY = factor;
    }

    public void setXLabelTypeface(Typeface t) {
        mXLabelPaint.setTypeface(t);
    }

    public void setYLabelTypeface(Typeface t) {
        mYLabelPaint.setTypeface(t);
    }

    public void setLabelTypeface(Typeface t) {
        setXLabelTypeface(t);
        setYLabelTypeface(t);
    }

    public YLabels getYLabels() {
        return mYLabels;
    }

    public void enableFiltering(Approximator a) {
        mFilterData = true;
        mApproximator = a;
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

    private ChartData getFilteredData() {
        float deltaRatio = mDeltaY / mDeltaX;
        float scaleRatio = mScaleY / mScaleX;
        mApproximator.setRatios(deltaRatio, scaleRatio);
        ArrayList<DataSet> dataSets = new ArrayList<DataSet>();
        for (int j = 0; j < mOriginalData.getDataSetCount(); j++) {
            DataSet old = mOriginalData.getDataSetByIndex(j);
            ArrayList<Entry> approximated = mApproximator.filter(old.getYVals());
            DataSet set = new DataSet(approximated, old.getLabel());
            dataSets.add(set);
        }
        ChartData d = new ChartData(mOriginalData.getXVals(), dataSets);
        return d;
    }

    @Override
    public void setPaint(Paint p, int which) {
        super.setPaint(p, which);
        switch(which) {
            case PAINT_GRID:
                mGridPaint = p;
                break;
            case PAINT_GRID_BACKGROUND:
                mGridBackgroundPaint = p;
                break;
            case PAINT_BORDER:
                mBorderPaint = p;
                break;
            case PAINT_XLABEL:
                mXLabelPaint = p;
                break;
            case PAINT_YLABEL:
                mYLabelPaint = p;
                break;
        }
    }

    @Override
    public Paint getPaint(int which) {
        super.getPaint(which);
        switch(which) {
            case PAINT_GRID:
                return mGridPaint;
            case PAINT_GRID_BACKGROUND:
                return mGridBackgroundPaint;
            case PAINT_BORDER:
                return mBorderPaint;
            case PAINT_XLABEL:
                return mXLabelPaint;
            case PAINT_YLABEL:
                return mYLabelPaint;
        }
        return null;
    }
}