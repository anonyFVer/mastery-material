package com.github.mikephil.charting.charts;

import android.annotation.SuppressLint;
import android.content.ContentValues;
import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.Bitmap.CompressFormat;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Matrix;
import android.graphics.Paint;
import android.graphics.Paint.Align;
import android.graphics.Paint.Style;
import android.graphics.Path;
import android.graphics.PointF;
import android.graphics.Rect;
import android.graphics.RectF;
import android.graphics.Typeface;
import android.os.Environment;
import android.provider.MediaStore.Images;
import android.text.TextUtils;
import android.util.AttributeSet;
import android.util.Log;
import android.view.MotionEvent;
import android.view.View;
import com.github.mikephil.charting.data.BarDataSet;
import com.github.mikephil.charting.data.ChartData;
import com.github.mikephil.charting.data.DataSet;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.PieDataSet;
import com.github.mikephil.charting.interfaces.OnChartValueSelectedListener;
import com.github.mikephil.charting.utils.Highlight;
import com.github.mikephil.charting.utils.Legend;
import com.github.mikephil.charting.utils.MarkerView;
import com.github.mikephil.charting.utils.SelInfo;
import com.github.mikephil.charting.utils.Utils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

public abstract class Chart extends View {

    public static final String LOG_TAG = "MPChart";

    protected int mColorDarkBlue = Color.rgb(41, 128, 186);

    protected int mColorDarkRed = Color.rgb(232, 76, 59);

    protected String mUnit = "";

    private int mBackgroundColor = Color.WHITE;

    protected int mValueDigitsToUse = -1;

    protected int mValueFormatDigits = -1;

    protected float mOffsetLeft = 12;

    protected float mOffsetTop = 12;

    protected float mOffsetRight = 12;

    protected float mOffsetBottom = 12;

    protected ChartData mCurrentData = null;

    protected ChartData mOriginalData = null;

    protected Bitmap mDrawBitmap;

    protected Canvas mDrawCanvas;

    protected float mYChartMin = 0.0f;

    protected float mYChartMax = 0.0f;

    protected Paint mDrawPaint;

    protected Paint mDescPaint;

    protected Paint mInfoPaint;

    protected Paint mValuePaint;

    protected Paint mRenderPaint;

    protected Paint mLegendLabelPaint;

    protected Paint mLegendFormPaint;

    protected String mDescription = "Description.";

    protected boolean mDataNotSet = true;

    protected boolean mDrawUnitInChart = false;

    protected float mDeltaY = 1f;

    protected float mDeltaX = 1f;

    protected Matrix mMatrixValueToPx = new Matrix();

    protected Matrix mMatrixOffset = new Matrix();

    protected final Matrix mMatrixTouch = new Matrix();

    protected boolean mTouchEnabled = true;

    protected boolean mDrawYValues = true;

    protected boolean mHighlightEnabled = true;

    protected boolean mSeparateTousands = true;

    protected boolean mDrawLegend = true;

    protected Rect mContentRect = new Rect();

    protected Legend mLegend;

    protected OnChartValueSelectedListener mSelectionListener;

    private String mNoDataText = "No chart data available.";

    private String mNoDataTextDescription;

    public Chart(Context context) {
        super(context);
        init();
    }

    public Chart(Context context, AttributeSet attrs) {
        super(context, attrs);
        init();
    }

    public Chart(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
        init();
    }

    protected void init() {
        Utils.init(getContext().getResources());
        mOffsetBottom = (int) Utils.convertDpToPixel(mOffsetBottom);
        mOffsetLeft = (int) Utils.convertDpToPixel(mOffsetLeft);
        mOffsetRight = (int) Utils.convertDpToPixel(mOffsetRight);
        mOffsetTop = (int) Utils.convertDpToPixel(mOffsetTop);
        mRenderPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mRenderPaint.setStyle(Style.FILL);
        mDrawPaint = new Paint();
        mDescPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mDescPaint.setColor(Color.BLACK);
        mDescPaint.setTextAlign(Align.RIGHT);
        mDescPaint.setTextSize(Utils.convertDpToPixel(9f));
        mInfoPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mInfoPaint.setColor(Color.rgb(247, 189, 51));
        mInfoPaint.setTextAlign(Align.CENTER);
        mInfoPaint.setTextSize(Utils.convertDpToPixel(12f));
        mValuePaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mValuePaint.setColor(Color.rgb(63, 63, 63));
        mValuePaint.setTextAlign(Align.CENTER);
        mValuePaint.setTextSize(Utils.convertDpToPixel(9f));
        mLegendFormPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mLegendFormPaint.setStyle(Paint.Style.FILL);
        mLegendFormPaint.setStrokeWidth(3f);
        mLegendLabelPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mLegendLabelPaint.setTextSize(Utils.convertDpToPixel(9f));
    }

    protected boolean mOffsetsCalculated = false;

    public void setData(ChartData data) {
        if (data == null || !data.isValid()) {
            Log.e(LOG_TAG, "Cannot set data for chart. Provided chart values are null or contain less than 2 entries.");
            mDataNotSet = true;
            return;
        }
        mDataNotSet = false;
        mOffsetsCalculated = false;
        mCurrentData = data;
        mOriginalData = data;
        prepare();
        Log.i(LOG_TAG, "Data is set.");
    }

    public abstract void prepare();

    public abstract void notifyDataSetChanged();

    protected abstract void calculateOffsets();

    protected void calcMinMax(boolean fixedValues) {
        if (!fixedValues) {
            mYChartMin = mCurrentData.getYMin();
            mYChartMax = mCurrentData.getYMax();
        }
        mDeltaY = Math.abs(mYChartMax - mYChartMin);
        mDeltaX = mCurrentData.getXVals().size() - 1;
    }

    @SuppressLint("NewApi")
    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        if (!mOffsetsCalculated) {
            calculateOffsets();
            mOffsetsCalculated = true;
        }
        if (mDataNotSet) {
            canvas.drawText(mNoDataText, getWidth() / 2, getHeight() / 2, mInfoPaint);
            if (!TextUtils.isEmpty(mNoDataTextDescription)) {
                float textOffset = -mInfoPaint.ascent() + mInfoPaint.descent();
                canvas.drawText(mNoDataTextDescription, getWidth() / 2, (getHeight() / 2) + textOffset, mInfoPaint);
            }
            return;
        }
        if (mDrawBitmap == null || mDrawCanvas == null) {
            mDrawBitmap = Bitmap.createBitmap(getWidth(), getHeight(), Bitmap.Config.RGB_565);
            mDrawCanvas = new Canvas(mDrawBitmap);
        }
        mDrawCanvas.drawColor(mBackgroundColor);
    }

    protected void prepareMatrix() {
        float scaleX = (float) ((getWidth() - mOffsetLeft - mOffsetRight) / mDeltaX);
        float scaleY = (float) ((getHeight() - mOffsetBottom - mOffsetTop) / mDeltaY);
        mMatrixValueToPx.reset();
        mMatrixValueToPx.postTranslate(0, -mYChartMin);
        mMatrixValueToPx.postScale(scaleX, -scaleY);
        mMatrixOffset.reset();
        mMatrixOffset.postTranslate(mOffsetLeft, getHeight() - mOffsetBottom);
        Log.i(LOG_TAG, "Matrices prepared.");
    }

    protected void prepareContentRect() {
        mContentRect.set((int) mOffsetLeft, (int) mOffsetTop, getMeasuredWidth() - (int) mOffsetRight, getMeasuredHeight() - (int) mOffsetBottom);
    }

    public void prepareLegend() {
        ArrayList<String> labels = new ArrayList<String>();
        ArrayList<Integer> colors = new ArrayList<Integer>();
        for (int i = 0; i < mOriginalData.getDataSetCount(); i++) {
            DataSet dataSet = mOriginalData.getDataSetByIndex(i);
            ArrayList<Integer> clrs = dataSet.getColors();
            int entryCount = dataSet.getEntryCount();
            if (dataSet instanceof BarDataSet && ((BarDataSet) dataSet).getStackSize() > 1) {
                BarDataSet bds = (BarDataSet) dataSet;
                String[] sLabels = bds.getStackLabels();
                for (int j = 0; j < clrs.size() && j < entryCount && j < bds.getStackSize(); j++) {
                    labels.add(sLabels[j % sLabels.length]);
                    colors.add(clrs.get(j));
                }
                colors.add(-1);
                labels.add(bds.getLabel());
            } else if (dataSet instanceof PieDataSet) {
                ArrayList<String> xVals = mOriginalData.getXVals();
                PieDataSet pds = (PieDataSet) dataSet;
                for (int j = 0; j < clrs.size() && j < entryCount && j < xVals.size(); j++) {
                    labels.add(xVals.get(j));
                    colors.add(clrs.get(j));
                }
                colors.add(-1);
                labels.add(pds.getLabel());
            } else {
                for (int j = 0; j < clrs.size() && j < entryCount; j++) {
                    if (j < clrs.size() - 1 && j < entryCount - 1) {
                        labels.add(null);
                    } else {
                        String label = mOriginalData.getDataSetByIndex(i).getLabel();
                        labels.add(label);
                    }
                    colors.add(clrs.get(j));
                }
            }
        }
        Legend l = new Legend(colors, labels);
        if (mLegend != null) {
            l.apply(mLegend);
        }
        mLegend = l;
    }

    protected float[] generateTransformedValues(ArrayList<Entry> entries, float xOffset) {
        float[] valuePoints = new float[entries.size() * 2];
        for (int j = 0; j < valuePoints.length; j += 2) {
            valuePoints[j] = entries.get(j / 2).getXIndex() + xOffset;
            valuePoints[j + 1] = entries.get(j / 2).getSum();
        }
        transformPointArray(valuePoints);
        return valuePoints;
    }

    protected float[] generateTransformedValuesForStacks(ArrayList<Entry> entries, int entryCount, float xOffset) {
        float[] valuePoints = new float[entryCount * 2];
        int cnt = 0;
        for (int i = 0; i < entries.size(); i++) {
            Entry e = entries.get(i);
            float[] vals = e.getVals();
            if (vals == null) {
                valuePoints[cnt] = e.getXIndex() + xOffset;
                valuePoints[cnt + 1] = e.getVal();
                cnt += 2;
            } else {
                float all = e.getSum();
                for (int j = 0; j < vals.length; j++) {
                    all -= vals[j];
                    valuePoints[cnt] = e.getXIndex() + xOffset;
                    valuePoints[cnt + 1] = vals[j] + all;
                    cnt += 2;
                }
            }
        }
        transformPointArray(valuePoints);
        return valuePoints;
    }

    protected void transformPath(Path path) {
        path.transform(mMatrixValueToPx);
        path.transform(mMatrixTouch);
        path.transform(mMatrixOffset);
    }

    protected void transformPaths(ArrayList<Path> paths) {
        for (int i = 0; i < paths.size(); i++) {
            transformPath(paths.get(i));
        }
    }

    protected void transformPointArray(float[] pts) {
        mMatrixValueToPx.mapPoints(pts);
        mMatrixTouch.mapPoints(pts);
        mMatrixOffset.mapPoints(pts);
    }

    protected void transformRect(RectF r) {
        mMatrixValueToPx.mapRect(r);
        mMatrixTouch.mapRect(r);
        mMatrixOffset.mapRect(r);
    }

    protected void transformRects(ArrayList<RectF> rects) {
        for (int i = 0; i < rects.size(); i++) transformRect(rects.get(i));
    }

    protected void transformRectsTouch(ArrayList<RectF> rects) {
        for (int i = 0; i < rects.size(); i++) {
            mMatrixTouch.mapRect(rects.get(i));
        }
    }

    protected void transformPathsTouch(ArrayList<Path> paths) {
        for (int i = 0; i < paths.size(); i++) {
            paths.get(i).transform(mMatrixTouch);
        }
    }

    protected void drawLegend() {
        if (!mDrawLegend || mLegend == null)
            return;
        String[] labels = mLegend.getLegendLabels();
        Typeface tf = mLegend.getTypeface();
        if (tf != null)
            mLegendLabelPaint.setTypeface(tf);
        mLegendLabelPaint.setTextSize(mLegend.getTextSize());
        float formSize = mLegend.getFormSize();
        float formTextSpaceAndForm = mLegend.getFormToTextSpace() + formSize;
        float stackSpace = mLegend.getStackSpace();
        float textSize = mLegend.getTextSize();
        float textDrop = (Utils.calcTextHeight(mLegendLabelPaint, "AQJ") + formSize) / 2f;
        float posX, posY;
        switch(mLegend.getPosition()) {
            case BELOW_CHART_LEFT:
                posX = mLegend.getOffsetLeft();
                posY = getHeight() - mLegend.getOffsetBottom() / 2f - formSize / 2f;
                for (int i = 0; i < labels.length; i++) {
                    mLegend.drawForm(mDrawCanvas, posX, posY, mLegendFormPaint, i);
                    if (labels[i] != null) {
                        if (mLegend.getColors()[i] != -1)
                            posX += formTextSpaceAndForm;
                        mLegend.drawLabel(mDrawCanvas, posX, posY + textDrop, mLegendLabelPaint, i);
                        posX += Utils.calcTextWidth(mLegendLabelPaint, labels[i]) + mLegend.getXEntrySpace();
                    } else {
                        posX += formSize + stackSpace;
                    }
                }
                break;
            case BELOW_CHART_RIGHT:
                posX = getWidth() - getOffsetRight();
                posY = getHeight() - mLegend.getOffsetBottom() / 2f - formSize / 2f;
                for (int i = labels.length - 1; i >= 0; i--) {
                    if (labels[i] != null) {
                        posX -= Utils.calcTextWidth(mLegendLabelPaint, labels[i]) + mLegend.getXEntrySpace();
                        mLegend.drawLabel(mDrawCanvas, posX, posY + textDrop, mLegendLabelPaint, i);
                        if (mLegend.getColors()[i] != -1)
                            posX -= formTextSpaceAndForm;
                    } else {
                        posX -= stackSpace + formSize;
                    }
                    mLegend.drawForm(mDrawCanvas, posX, posY, mLegendFormPaint, i);
                }
                break;
            case RIGHT_OF_CHART:
                posX = getWidth() - mLegend.getMaximumEntryLength(mLegendLabelPaint) - formTextSpaceAndForm;
                posY = mLegend.getOffsetTop();
                float stack = 0f;
                boolean wasStacked = false;
                for (int i = 0; i < labels.length; i++) {
                    mLegend.drawForm(mDrawCanvas, posX + stack, posY, mLegendFormPaint, i);
                    if (labels[i] != null) {
                        if (!wasStacked) {
                            float x = posX;
                            if (mLegend.getColors()[i] != -1)
                                x += formTextSpaceAndForm;
                            posY += textDrop;
                            mLegend.drawLabel(mDrawCanvas, x, posY, mLegendLabelPaint, i);
                        } else {
                            posY += textSize * 1.2f + formSize;
                            mLegend.drawLabel(mDrawCanvas, posX, posY, mLegendLabelPaint, i);
                        }
                        posY += mLegend.getYEntrySpace();
                        stack = 0f;
                    } else {
                        stack += formSize + stackSpace;
                        wasStacked = true;
                    }
                }
                break;
            case BELOW_CHART_CENTER:
                float fullSize = mLegend.getFullWidth(mLegendLabelPaint);
                posX = getWidth() / 2f - fullSize / 2f;
                posY = getHeight() - mLegend.getOffsetBottom() / 2f - formSize / 2f;
                for (int i = 0; i < labels.length; i++) {
                    mLegend.drawForm(mDrawCanvas, posX, posY, mLegendFormPaint, i);
                    if (labels[i] != null) {
                        if (mLegend.getColors()[i] != -1)
                            posX += formTextSpaceAndForm;
                        mLegend.drawLabel(mDrawCanvas, posX, posY + textDrop, mLegendLabelPaint, i);
                        posX += Utils.calcTextWidth(mLegendLabelPaint, labels[i]) + mLegend.getXEntrySpace();
                    } else {
                        posX += formSize + stackSpace;
                    }
                }
                break;
        }
    }

    protected void drawDescription() {
        mDrawCanvas.drawText(mDescription, getWidth() - mOffsetRight - 10, getHeight() - mOffsetBottom - 10, mDescPaint);
    }

    protected abstract void drawValues();

    protected abstract void drawData();

    protected abstract void drawAdditional();

    protected abstract void drawHighlights();

    protected OnTouchListener mListener;

    @Override
    public boolean onTouchEvent(MotionEvent event) {
        if (mListener == null || mDataNotSet)
            return false;
        if (!mTouchEnabled)
            return false;
        else
            return mListener.onTouch(this, event);
    }

    protected Highlight[] mIndicesToHightlight = new Highlight[0];

    public boolean needsHighlight(int xIndex, int dataSetIndex) {
        if (!valuesToHighlight())
            return false;
        for (int i = 0; i < mIndicesToHightlight.length; i++) if (mIndicesToHightlight[i].getXIndex() == xIndex && mIndicesToHightlight[i].getDataSetIndex() == dataSetIndex && xIndex <= mDeltaX)
            return true;
        return false;
    }

    public boolean valuesToHighlight() {
        return mIndicesToHightlight == null || mIndicesToHightlight.length <= 0 || mIndicesToHightlight[0] == null ? false : true;
    }

    public void highlightValues(Highlight[] highs) {
        mIndicesToHightlight = highs;
        invalidate();
        if (mSelectionListener != null) {
            if (!valuesToHighlight())
                mSelectionListener.onNothingSelected();
            else {
                Entry[] values = new Entry[highs.length];
                for (int i = 0; i < values.length; i++) values[i] = getEntryByDataSetIndex(highs[i].getXIndex(), highs[i].getDataSetIndex());
                mSelectionListener.onValuesSelected(values, highs);
            }
        }
    }

    protected boolean mDrawMarkerViews = true;

    protected MarkerView mMarkerView;

    protected void drawMarkers() {
        if (mMarkerView == null || !mDrawMarkerViews || !valuesToHighlight())
            return;
        for (int i = 0; i < mIndicesToHightlight.length; i++) {
            int xIndex = mIndicesToHightlight[i].getXIndex();
            drawMarkerView(xIndex, mIndicesToHightlight[i].getDataSetIndex());
        }
    }

    private void drawMarkerView(int xIndex, int dataSetIndex) {
        float value = getYValueByDataSetIndex(xIndex, dataSetIndex);
        float xPos = (float) xIndex;
        if (this instanceof BarChart)
            xPos += 0.5f;
        float[] pts = new float[] { xPos, value };
        transformPointArray(pts);
        float posX = pts[0];
        float posY = pts[1];
        mMarkerView.refreshContent(xIndex, value, dataSetIndex);
        mMarkerView.draw(mDrawCanvas, posX, posY);
    }

    public void setOnTouchListener(OnTouchListener l) {
        this.mListener = l;
    }

    public void setOnChartValueSelectedListener(OnChartValueSelectedListener l) {
        this.mSelectionListener = l;
    }

    public void setHighlightEnabled(boolean enabled) {
        mHighlightEnabled = enabled;
    }

    public boolean isHighlightEnabled() {
        return mHighlightEnabled;
    }

    public float getYValueSum() {
        return mCurrentData.getYValueSum();
    }

    public float getYMax() {
        return mCurrentData.getYMax();
    }

    public float getYChartMin() {
        return mYChartMin;
    }

    public float getYChartMax() {
        return mYChartMax;
    }

    public float getYMin() {
        return mCurrentData.getYMin();
    }

    public float getDeltaX() {
        return mDeltaX;
    }

    public float getAverage() {
        return getYValueSum() / mCurrentData.getYValCount();
    }

    public float getAverage(String dataSetLabel) {
        DataSet ds = mCurrentData.getDataSetByLabel(dataSetLabel, true);
        return ds.getYValueSum() / ds.getEntryCount();
    }

    public int getValueCount() {
        return mCurrentData.getYValCount();
    }

    public PointF getCenter() {
        return new PointF(getWidth() / 2, getHeight() / 2);
    }

    public PointF getCenterOffsets() {
        return new PointF(mContentRect.left + mContentRect.width() / 2, mContentRect.top + mContentRect.height() / 2);
    }

    public void setDescriptionTextSize(float size) {
        if (size > 14f)
            size = 14f;
        if (size < 7f)
            size = 7f;
        mInfoPaint.setTextSize(Utils.convertDpToPixel(size));
    }

    public void setDescription(String desc) {
        this.mDescription = desc;
    }

    public void setNoDataText(String text) {
        mNoDataText = text;
    }

    public void setNoDataTextDescription(String text) {
        mNoDataTextDescription = text;
    }

    public void setOffsets(float left, float top, float right, float bottom) {
        mOffsetBottom = Utils.convertDpToPixel(bottom);
        mOffsetLeft = Utils.convertDpToPixel(left);
        mOffsetRight = Utils.convertDpToPixel(right);
        mOffsetTop = Utils.convertDpToPixel(top);
    }

    public float getOffsetLeft() {
        return mOffsetLeft;
    }

    public float getOffsetBottom() {
        return mOffsetBottom;
    }

    public float getOffsetRight() {
        return mOffsetRight;
    }

    public float getOffsetTop() {
        return mOffsetTop;
    }

    public void setTouchEnabled(boolean enabled) {
        this.mTouchEnabled = enabled;
    }

    public void setDrawYValues(boolean enabled) {
        this.mDrawYValues = enabled;
    }

    public void setMarkerView(MarkerView v) {
        mMarkerView = v;
    }

    public MarkerView getMarkerView() {
        return mMarkerView;
    }

    public void setDrawUnitsInChart(boolean enabled) {
        mDrawUnitInChart = enabled;
    }

    public void setUnit(String unit) {
        mUnit = unit;
    }

    public String getUnit() {
        return mUnit;
    }

    public void setDrawLegend(boolean enabled) {
        mDrawLegend = enabled;
    }

    public boolean isDrawLegendEnabled() {
        return mDrawLegend;
    }

    public Legend getLegend() {
        return mLegend;
    }

    public static final int PAINT_GRID = 3;

    public static final int PAINT_GRID_BACKGROUND = 4;

    public static final int PAINT_YLABEL = 5;

    public static final int PAINT_XLABEL = 6;

    public static final int PAINT_INFO = 7;

    public static final int PAINT_VALUES = 8;

    public static final int PAINT_CIRCLES_INNER = 10;

    public static final int PAINT_DESCRIPTION = 11;

    public static final int PAINT_BORDER = 12;

    public static final int PAINT_HOLE = 13;

    public static final int PAINT_CENTER_TEXT = 14;

    public static final int PAINT_HIGHLIGHT_LINE = 15;

    public static final int PAINT_HIGHLIGHT_BAR = 16;

    public static final int PAINT_RENDER = 17;

    public static final int PAINT_LEGEND_LABEL = 18;

    public void setPaint(Paint p, int which) {
        switch(which) {
            case PAINT_INFO:
                mInfoPaint = p;
                break;
            case PAINT_DESCRIPTION:
                mDescPaint = p;
                break;
            case PAINT_VALUES:
                mValuePaint = p;
                break;
            case PAINT_RENDER:
                mRenderPaint = p;
                break;
            case PAINT_LEGEND_LABEL:
                mLegendLabelPaint = p;
                break;
        }
    }

    public Paint getPaint(int which) {
        switch(which) {
            case PAINT_INFO:
                return mInfoPaint;
            case PAINT_DESCRIPTION:
                return mDescPaint;
            case PAINT_VALUES:
                return mValuePaint;
            case PAINT_RENDER:
                return mRenderPaint;
            case PAINT_LEGEND_LABEL:
                return mLegendLabelPaint;
        }
        return null;
    }

    public boolean isDrawMarkerViewEnabled() {
        return mDrawMarkerViews;
    }

    public void setDrawMarkerViews(boolean enabled) {
        mDrawMarkerViews = enabled;
    }

    public void setValueTextColor(int color) {
        mValuePaint.setColor(color);
    }

    public void setValueTextSize(float size) {
        mValuePaint.setTextSize(Utils.convertDpToPixel(size));
    }

    public void setSeparateThousands(boolean enabled) {
        mSeparateTousands = enabled;
    }

    public boolean isDrawYValuesEnabled() {
        return mDrawYValues;
    }

    public String getXValue(int index) {
        if (mCurrentData == null || mCurrentData.getXValCount() <= index)
            return null;
        else
            return mCurrentData.getXVals().get(index);
    }

    public float getYValue(int index) {
        return mCurrentData.getDataSetByIndex(0).getYVals().get(index).getVal();
    }

    public float getYValue(int index, String dataSetLabel) {
        DataSet set = mCurrentData.getDataSetByLabel(dataSetLabel, true);
        return set.getYVals().get(index).getVal();
    }

    public float getYValueByDataSetIndex(int xIndex, int dataSet) {
        DataSet set = mCurrentData.getDataSetByIndex(dataSet);
        return set.getYValForXIndex(xIndex);
    }

    public DataSet getDataSetByIndex(int index) {
        return mCurrentData.getDataSetByIndex(index);
    }

    public DataSet getDataSetByLabel(String dataSetLabel) {
        return mCurrentData.getDataSetByLabel(dataSetLabel, true);
    }

    public Entry getEntry(int index) {
        return mCurrentData.getDataSetByIndex(0).getYVals().get(index);
    }

    public Entry getEntry(int index, String dataSetLabel) {
        return mCurrentData.getDataSetByLabel(dataSetLabel, true).getYVals().get(index);
    }

    public Entry getEntryByDataSetIndex(int xIndex, int dataSetIndex) {
        return mCurrentData.getDataSetByIndex(dataSetIndex).getEntryForXIndex(xIndex);
    }

    protected ArrayList<SelInfo> getYValsAtIndex(int xIndex) {
        ArrayList<SelInfo> vals = new ArrayList<SelInfo>();
        for (int i = 0; i < mCurrentData.getDataSetCount(); i++) {
            float yVal = mCurrentData.getDataSetByIndex(i).getYValForXIndex(xIndex);
            if (!Float.isNaN(yVal)) {
                vals.add(new SelInfo(yVal, i));
            }
        }
        return vals;
    }

    public ArrayList<Entry> getEntriesAtIndex(int xIndex) {
        ArrayList<Entry> vals = new ArrayList<Entry>();
        for (int i = 0; i < mCurrentData.getDataSetCount(); i++) {
            DataSet set = mCurrentData.getDataSetByIndex(i);
            Entry e = set.getEntryForXIndex(xIndex);
            if (e != null) {
                vals.add(e);
            }
        }
        return vals;
    }

    public ChartData getDataCurrent() {
        return mCurrentData;
    }

    public ChartData getDataOriginal() {
        return mOriginalData;
    }

    public float getPercentOfTotal(float val) {
        return val / mCurrentData.getYValueSum() * 100f;
    }

    public void setValueTypeface(Typeface t) {
        mValuePaint.setTypeface(t);
    }

    public void setDescriptionTypeface(Typeface t) {
        mDescPaint.setTypeface(t);
    }

    public void setValueDigits(int digits) {
        mValueDigitsToUse = digits;
    }

    public int getValueDigits() {
        return mValueDigitsToUse;
    }

    @Override
    public void setBackgroundColor(int color) {
        super.setBackgroundColor(color);
        mBackgroundColor = color;
    }

    public boolean saveToPath(String title, String pathOnSD) {
        OutputStream stream = null;
        try {
            stream = new FileOutputStream(Environment.getExternalStorageDirectory().getPath() + pathOnSD + "/" + title + ".png");
            mDrawBitmap.compress(CompressFormat.PNG, 40, stream);
            stream.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean saveToGallery(String fileName, int quality) {
        if (quality < 0 || quality > 100)
            quality = 50;
        long currentTime = System.currentTimeMillis();
        File extBaseDir = Environment.getExternalStorageDirectory();
        File file = new File(extBaseDir.getAbsolutePath() + "/DCIM");
        if (!file.exists()) {
            if (!file.mkdirs()) {
                return false;
            }
        }
        String filePath = file.getAbsolutePath() + "/" + fileName;
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(filePath);
            mDrawBitmap.compress(Bitmap.CompressFormat.JPEG, quality, out);
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        long size = new File(filePath).length();
        ContentValues values = new ContentValues(8);
        values.put(Images.Media.TITLE, fileName);
        values.put(Images.Media.DISPLAY_NAME, fileName);
        values.put(Images.Media.DATE_ADDED, currentTime);
        values.put(Images.Media.MIME_TYPE, "image/jpeg");
        values.put(Images.Media.DESCRIPTION, "MPAndroidChart-Library Save");
        values.put(Images.Media.ORIENTATION, 0);
        values.put(Images.Media.DATA, filePath);
        values.put(Images.Media.SIZE, size);
        return getContext().getContentResolver().insert(Images.Media.EXTERNAL_CONTENT_URI, values) == null ? false : true;
    }

    @Override
    protected void onMeasure(int widthMeasureSpec, int heightMeasureSpec) {
        super.onMeasure(widthMeasureSpec, heightMeasureSpec);
    }

    private boolean mMatrixOnLayoutPrepared = false;

    @Override
    protected void onLayout(boolean changed, int left, int top, int right, int bottom) {
        super.onLayout(changed, left, top, right, bottom);
        prepareContentRect();
        Log.i(LOG_TAG, "onLayout(), width: " + mContentRect.width() + ", height: " + mContentRect.height());
        if (this instanceof BarLineChartBase) {
            BarLineChartBase b = (BarLineChartBase) this;
            if (!b.hasFixedYValues() && !mMatrixOnLayoutPrepared) {
                prepareMatrix();
                mMatrixOnLayoutPrepared = true;
            }
        } else {
            prepareMatrix();
        }
    }

    @Override
    protected void onSizeChanged(int w, int h, int oldw, int oldh) {
        super.onSizeChanged(w, h, oldw, oldh);
    }
}