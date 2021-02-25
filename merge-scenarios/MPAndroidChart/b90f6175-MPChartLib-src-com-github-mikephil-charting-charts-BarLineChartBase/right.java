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
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.filter.Approximator;
import com.github.mikephil.charting.data.filter.ZoomHandler;
import com.github.mikephil.charting.interfaces.OnDrawListener;
import com.github.mikephil.charting.listener.BarLineChartTouchListener;
import com.github.mikephil.charting.utils.Highlight;
import com.github.mikephil.charting.utils.PointD;
import com.github.mikephil.charting.utils.SelInfo;
import com.github.mikephil.charting.utils.Utils;
import com.github.mikephil.charting.utils.YLegend;
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

    protected int mXLegendWidth = 1;

    protected int mXLegendGridModulus = 1;

    protected int mYLegendCount = 9;

    protected float mGridWidth = 1f;

    protected boolean mDrawUnitInChart = false;

    private boolean mDrawTopYLegendEntry = true;

    protected boolean mPinchZoomEnabled = false;

    protected boolean mDrawUnitInLegend = true;

    protected boolean mCenterXLegendText = false;

    protected boolean mAdjustXLegend = true;

    protected boolean mDrawGrid = true;

    protected boolean mDragEnabled = true;

    protected boolean mFixedYValues = false;

    protected boolean mStartAtZero = true;

    protected boolean mFilterData = true;

    protected Paint mGridPaint;

    protected Paint mGridBackgroundPaint;

    protected Paint mOutLinePaint;

    protected Paint mXLegendPaint;

    protected Paint mYLegendPaint;

    protected Paint mHighlightPaint;

    protected boolean mHighLightIndicatorEnabled = true;

    protected boolean mAutoFinishDrawing;

    protected OnDrawListener mDrawListener;

    protected YLegend mYLegend = new YLegend();

    protected ZoomHandler mZoomHandler = new ZoomHandler();

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
        mXLegendPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mXLegendPaint.setColor(Color.BLACK);
        mXLegendPaint.setTextAlign(Align.CENTER);
        mXLegendPaint.setTextSize(Utils.convertDpToPixel(10f));
        mYLegendPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mYLegendPaint.setColor(Color.BLACK);
        mYLegendPaint.setTextAlign(Align.RIGHT);
        mYLegendPaint.setTextSize(Utils.convertDpToPixel(10f));
        mGridPaint = new Paint();
        mGridPaint.setColor(Color.GRAY);
        mGridPaint.setStrokeWidth(mGridWidth);
        mGridPaint.setStyle(Style.STROKE);
        mGridPaint.setAlpha(90);
        mOutLinePaint = new Paint();
        mOutLinePaint.setColor(Color.BLACK);
        mOutLinePaint.setStrokeWidth(mGridWidth * 2f);
        mOutLinePaint.setStyle(Style.STROKE);
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
        if (mAdjustXLegend)
            calcModulus();
        drawOutline();
        drawGridBackground();
        prepareYLegend();
        int clipRestoreCount = mDrawCanvas.save();
        mDrawCanvas.clipRect(mContentRect);
        drawHorizontalGrid();
        drawVerticalGrid();
        drawData();
        drawHighlights();
        mDrawCanvas.restoreToCount(clipRestoreCount);
        drawAdditional();
        drawMarkers();
        drawValues();
        drawXLegend();
        drawYLegend();
        drawDescription();
        canvas.drawBitmap(mDrawBitmap, 0, 0, mDrawPaint);
        Log.i(LOG_TAG, "DrawTime: " + (System.currentTimeMillis() - starttime) + " ms");
    }

    @Override
    public void prepare() {
        if (mDataNotSet)
            return;
        calcMinMax(mFixedYValues);
        prepareXLegend();
        calcFormats();
        if (!mFixedYValues)
            prepareMatrix();
    }

    @Override
    public void notifyDataSetChanged() {
        if (!mFixedYValues) {
            prepare();
        } else {
            calcMinMax(mFixedYValues);
        }
    }

    protected void calcModulus() {
        float[] values = new float[9];
        mMatrixTouch.getValues(values);
        mXLegendGridModulus = (int) Math.ceil((mData.getXValCount() * mXLegendWidth) / (mContentRect.width() * values[Matrix.MSCALE_X]));
    }

    protected DecimalFormat mFormatValue = null;

    protected int mYLegendFormatDigits = -1;

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

    protected void prepareXLegend() {
        StringBuffer a = new StringBuffer();
        int length = (int) (((float) (mData.getXVals().get(0).length() + mData.getXVals().get(mData.getXValCount() - 1).length())));
        if (mData.getXVals().get(0).length() <= 3)
            length *= 2;
        for (int i = 0; i < length; i++) {
            a.append("h");
        }
        mXLegendWidth = calcTextWidth(mXLegendPaint, a.toString());
    }

    private void prepareYLegend() {
        PointD p1 = getValuesByTouchPoint(mContentRect.left, mContentRect.top);
        PointD p2 = getValuesByTouchPoint(mContentRect.left, mContentRect.bottom);
        mYChartMin = (float) p2.y;
        mYChartMax = (float) p1.y;
        float yMin = mYChartMin;
        float yMax = mYChartMax;
        double range = yMax - yMin;
        if (mYLegendCount == 0 || range <= 0) {
            mYLegend.mEntries = new float[] {};
            mYLegend.mEntryCount = 0;
            return;
        }
        double rawInterval = range / mYLegendCount;
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
        mYLegend.mEntryCount = n;
        if (mYLegend.mEntries.length < n) {
            mYLegend.mEntries = new float[n];
        }
        for (f = first, i = 0; i < n; f += interval, ++i) {
            mYLegend.mEntries[i] = (float) f;
        }
        if (interval < 1) {
            mYLegend.mDecimals = (int) Math.ceil(-Math.log10(interval));
        } else {
            mYLegend.mDecimals = 0;
        }
    }

    protected void drawXLegend() {
        float[] position = new float[] { 0f, 0f };
        for (int i = 0; i < mData.getXValCount(); i++) {
            if (i % mXLegendGridModulus == 0) {
                position[0] = i;
                if (mCenterXLegendText)
                    position[0] += 0.5f;
                transformPointArray(position);
                if (position[0] >= mOffsetLeft && position[0] <= getWidth() - mOffsetRight + 10) {
                    mDrawCanvas.drawText(mData.getXVals().get(i), position[0], mOffsetTop - 5, mXLegendPaint);
                }
            }
        }
    }

    protected void drawYLegend() {
        float[] positions = new float[mYLegend.mEntryCount * 2];
        for (int i = 0; i < positions.length; i += 2) {
            positions[i + 1] = mYLegend.mEntries[i / 2];
        }
        transformPointArray(positions);
        for (int i = 0; i < mYLegend.mEntryCount; i++) {
            String text = Utils.formatNumber(mYLegend.mEntries[i], mYLegend.mDecimals, mSeparateTousands);
            if (!mDrawTopYLegendEntry && i >= mYLegend.mEntryCount - 1)
                return;
            if (mDrawUnitInLegend) {
                mDrawCanvas.drawText(text + mUnit, mOffsetLeft - 10, positions[i * 2 + 1], mYLegendPaint);
            } else {
                mDrawCanvas.drawText(text, mOffsetLeft - 10, positions[i * 2 + 1], mYLegendPaint);
            }
        }
    }

    protected void drawOutline() {
        mDrawCanvas.drawRect(mOffsetLeft, mOffsetTop, getWidth() - mOffsetRight, getHeight() - mOffsetBottom, mOutLinePaint);
    }

    protected void drawGridBackground() {
        Rect gridBackground = new Rect(mOffsetLeft, mOffsetTop, getWidth() - mOffsetRight, getHeight() - mOffsetBottom);
        mDrawCanvas.drawRect(gridBackground, mGridBackgroundPaint);
    }

    protected void drawHorizontalGrid() {
        if (!mDrawGrid)
            return;
        Path p = new Path();
        for (int i = 0; i < mYLegend.mEntryCount; i++) {
            p.reset();
            p.moveTo(0, mYLegend.mEntries[i]);
            p.lineTo(mDeltaX, mYLegend.mEntries[i]);
            transformPath(p);
            mDrawCanvas.drawPath(p, mGridPaint);
        }
    }

    protected void drawVerticalGrid() {
        if (!mDrawGrid)
            return;
        float[] position = new float[] { 0f, 0f };
        for (int i = 0; i < mData.getXValCount(); i++) {
            if (i % mXLegendGridModulus == 0) {
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
        save.postScale(scaleX, scaleY, x, y);
        refreshTouch(save);
    }

    public Matrix refreshTouch(Matrix newTouchMatrix) {
        mMatrixTouch.set(newTouchMatrix);
        limitTransAndScale(mMatrixTouch);
        invalidate();
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
    }

    public void setDrawTopYLegendEntry(boolean enabled) {
        mDrawTopYLegendEntry = enabled;
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

    public void setYLegendCount(int yCount) {
        if (yCount > 15)
            yCount = 15;
        if (yCount < 3)
            yCount = 3;
        mYLegendCount = yCount;
    }

    public void setAdjustXLegend(boolean enabled) {
        mAdjustXLegend = enabled;
    }

    public boolean isAdjustXLegendEnabled() {
        return mAdjustXLegend;
    }

    public void setGridColor(int color) {
        mGridPaint.setColor(color);
    }

    public void setMaxVisibleValueCount(int count) {
        this.mMaxVisibleCount = count;
    }

    public void setYLegendTextSize(float size) {
        if (size > 14f)
            size = 14f;
        if (size < 7f)
            size = 7f;
        mYLegendPaint.setTextSize(Utils.convertDpToPixel(size));
    }

    public void setXLegendTextSize(float size) {
        if (size > 14f)
            size = 14f;
        if (size < 7f)
            size = 7f;
        mXLegendPaint.setTextSize(Utils.convertDpToPixel(size));
    }

    public boolean isDrawGridEnabled() {
        return mDrawGrid;
    }

    public void setDrawGrid(boolean enabled) {
        this.mDrawGrid = enabled;
    }

    public void setHighlightIndicatorEnabled(boolean enabled) {
        mHighLightIndicatorEnabled = enabled;
    }

    public void setStartAtZero(boolean enabled) {
        this.mStartAtZero = enabled;
        prepare();
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

    public void setDrawUnitsInLegend(boolean enabled) {
        mDrawUnitInLegend = enabled;
    }

    public void setCenterXLegend(boolean enabled) {
        mCenterXLegendText = enabled;
    }

    public boolean isXLegendCentered() {
        return mCenterXLegendText;
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
            return mData.getEntryForHighlight(h);
        }
        return null;
    }

    public ZoomHandler getZoomHandler() {
        return mZoomHandler;
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

    public void setXLegendTypeface(Typeface t) {
        mXLegendPaint.setTypeface(t);
    }

    public void setYLegendTypeface(Typeface t) {
        mYLegendPaint.setTypeface(t);
    }

    public void setLegendTypeface(Typeface t) {
        setXLegendTypeface(t);
        setYLegendTypeface(t);
    }

    public void enableFiltering() {
        mFilterData = true;
        mZoomHandler.setCustomApproximator(null);
    }

    public void enableFiltering(Approximator a) {
        mFilterData = true;
        mZoomHandler.setCustomApproximator(a);
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
            case PAINT_OUTLINE:
                mOutLinePaint = p;
                break;
            case PAINT_XLEGEND:
                mXLegendPaint = p;
                break;
            case PAINT_YLEGEND:
                mYLegendPaint = p;
                break;
        }
    }
}