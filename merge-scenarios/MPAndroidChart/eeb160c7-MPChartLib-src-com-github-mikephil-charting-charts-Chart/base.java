package com.github.mikephil.charting.charts;

import android.content.ContentValues;
import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.Bitmap.CompressFormat;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Paint.Align;
import android.graphics.Paint.Style;
import android.graphics.PointF;
import android.graphics.RectF;
import android.graphics.Typeface;
import android.graphics.drawable.Drawable;
import android.os.Environment;
import android.provider.MediaStore.Images;
import android.text.TextUtils;
import android.util.AttributeSet;
import android.util.Log;
import android.view.View;
import android.view.ViewGroup;
import android.view.ViewParent;
import com.github.mikephil.charting.data.BarData;
import com.github.mikephil.charting.data.BarDataSet;
import com.github.mikephil.charting.data.ChartData;
import com.github.mikephil.charting.data.DataSet;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.PieDataSet;
import com.github.mikephil.charting.interfaces.ChartInterface;
import com.github.mikephil.charting.interfaces.OnChartGestureListener;
import com.github.mikephil.charting.interfaces.OnChartValueSelectedListener;
import com.github.mikephil.charting.renderer.Transformer;
import com.github.mikephil.charting.utils.Highlight;
import com.github.mikephil.charting.utils.Legend;
import com.github.mikephil.charting.utils.Legend.LegendPosition;
import com.github.mikephil.charting.utils.MarkerView;
import com.github.mikephil.charting.utils.SelInfo;
import com.github.mikephil.charting.utils.Utils;
import com.github.mikephil.charting.utils.ValueFormatter;
import com.nineoldandroids.animation.ObjectAnimator;
import com.nineoldandroids.animation.ValueAnimator;
import com.nineoldandroids.animation.ValueAnimator.AnimatorUpdateListener;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;

public abstract class Chart<T extends ChartData<? extends DataSet<? extends Entry>>> extends ViewGroup implements AnimatorUpdateListener, ChartInterface {

    public static final String LOG_TAG = "MPChart";

    protected boolean mLogEnabled = false;

    protected String mUnit = "";

    protected ValueFormatter mValueFormatter = null;

    private boolean mUseDefaultFormatter = true;

    protected float mOffsetLeft = 12;

    protected float mOffsetTop = 12;

    protected float mOffsetRight = 12;

    protected float mOffsetBottom = 12;

    protected T mData = null;

    protected Canvas mDrawCanvas;

    protected float mYChartMin = 0.0f;

    protected float mYChartMax = 0.0f;

    protected Paint mXLabelPaint;

    protected Paint mYLabelPaint;

    protected Paint mHighlightPaint;

    protected Paint mDescPaint;

    protected Paint mInfoPaint;

    protected Paint mValuePaint;

    protected Paint mRenderPaint;

    protected Paint mLegendLabelPaint;

    protected Paint mLegendFormPaint;

    protected Paint mLimitLinePaint;

    protected String mDescription = "Description.";

    protected boolean mDataNotSet = true;

    protected boolean mDrawUnitInChart = false;

    protected float mDeltaY = 1f;

    protected float mDeltaX = 1f;

    protected boolean mTouchEnabled = true;

    protected boolean mDrawYValues = true;

    protected boolean mHighlightEnabled = true;

    protected boolean mDrawLegend = true;

    protected RectF mContentRect = new RectF();

    protected Legend mLegend;

    protected Transformer mTrans;

    protected OnChartValueSelectedListener mSelectionListener;

    private String mNoDataText = "No chart data available.";

    private OnChartGestureListener mGestureListener;

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
        mTrans = new Transformer();
        Utils.init(getContext().getResources());
        mOffsetBottom = (int) Utils.convertDpToPixel(mOffsetBottom);
        mOffsetLeft = (int) Utils.convertDpToPixel(mOffsetLeft);
        mOffsetRight = (int) Utils.convertDpToPixel(mOffsetRight);
        mOffsetTop = (int) Utils.convertDpToPixel(mOffsetTop);
        mRenderPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mRenderPaint.setStyle(Style.FILL);
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
        mHighlightPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mHighlightPaint.setStyle(Paint.Style.STROKE);
        mHighlightPaint.setStrokeWidth(2f);
        mHighlightPaint.setColor(Color.rgb(255, 187, 115));
        mXLabelPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mXLabelPaint.setColor(Color.BLACK);
        mXLabelPaint.setTextAlign(Align.CENTER);
        mXLabelPaint.setTextSize(Utils.convertDpToPixel(10f));
        mYLabelPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mYLabelPaint.setColor(Color.BLACK);
        mYLabelPaint.setTextSize(Utils.convertDpToPixel(10f));
        mLimitLinePaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mLimitLinePaint.setStyle(Paint.Style.STROKE);
        mDrawPaint = new Paint(Paint.DITHER_FLAG);
    }

    public void setData(T data) {
        if (data == null) {
            Log.e(LOG_TAG, "Cannot set data for chart. Provided data object is null.");
            return;
        }
        mDataNotSet = false;
        mOffsetsCalculated = false;
        mData = data;
        mData = data;
        prepare();
        calcFormats();
        Log.i(LOG_TAG, "Data is set.");
    }

    public void clear() {
        mData = null;
        mData = null;
        mDataNotSet = true;
        invalidate();
    }

    public boolean isEmpty() {
        if (mData == null)
            return true;
        else {
            if (mData.getYValCount() <= 0)
                return true;
            else
                return false;
        }
    }

    public abstract void prepare();

    public abstract void notifyDataSetChanged();

    protected abstract void calculateOffsets();

    protected void calcMinMax(boolean fixedValues) {
        if (!fixedValues) {
            mYChartMin = mData.getYMin();
            mYChartMax = mData.getYMax();
        }
        mDeltaY = Math.abs(mYChartMax - mYChartMin);
        mDeltaX = mData.getXVals().size() - 1;
    }

    protected void calcFormats() {
        if (mUseDefaultFormatter) {
            float reference = 0f;
            if (mData == null || mData.getXValCount() < 2) {
                reference = Math.max(Math.abs(mYChartMin), Math.abs(mYChartMax));
            } else {
                reference = mDeltaY;
            }
            int digits = Utils.getDecimals(reference);
            StringBuffer b = new StringBuffer();
            for (int i = 0; i < digits; i++) {
                if (i == 0)
                    b.append(".");
                b.append("0");
            }
            DecimalFormat formatter = new DecimalFormat("###,###,###,##0" + b.toString());
            mValueFormatter = new DefaultValueFormatter(formatter);
        }
    }

    private boolean mOffsetsCalculated = false;

    protected Bitmap mDrawBitmap;

    protected Paint mDrawPaint;

    @Override
    protected void onDraw(Canvas canvas) {
        if (mDataNotSet) {
            canvas.drawText(mNoDataText, getWidth() / 2, getHeight() / 2, mInfoPaint);
            if (!TextUtils.isEmpty(mNoDataTextDescription)) {
                float textOffset = -mInfoPaint.ascent() + mInfoPaint.descent();
                canvas.drawText(mNoDataTextDescription, getWidth() / 2, (getHeight() / 2) + textOffset, mInfoPaint);
            }
            return;
        }
        if (!mOffsetsCalculated) {
            calculateOffsets();
            mOffsetsCalculated = true;
        }
        if (mDrawBitmap == null || mDrawCanvas == null) {
            mDrawBitmap = Bitmap.createBitmap(getWidth(), getHeight(), Bitmap.Config.ARGB_4444);
            mDrawCanvas = new Canvas(mDrawBitmap);
        }
        mDrawBitmap.eraseColor(Color.TRANSPARENT);
    }

    protected void prepareContentRect() {
        mContentRect.set(mOffsetLeft, mOffsetTop, getWidth() - mOffsetRight, getHeight() - mOffsetBottom);
    }

    public void prepareLegend() {
        ArrayList<String> labels = new ArrayList<String>();
        ArrayList<Integer> colors = new ArrayList<Integer>();
        for (int i = 0; i < mData.getDataSetCount(); i++) {
            DataSet<? extends Entry> dataSet = mData.getDataSetByIndex(i);
            ArrayList<Integer> clrs = dataSet.getColors();
            int entryCount = dataSet.getEntryCount();
            if (dataSet instanceof BarDataSet && ((BarDataSet) dataSet).getStackSize() > 1) {
                BarDataSet bds = (BarDataSet) dataSet;
                String[] sLabels = bds.getStackLabels();
                for (int j = 0; j < clrs.size() && j < entryCount && j < bds.getStackSize(); j++) {
                    labels.add(sLabels[j % sLabels.length]);
                    colors.add(clrs.get(j));
                }
                colors.add(-2);
                labels.add(bds.getLabel());
            } else if (dataSet instanceof PieDataSet) {
                ArrayList<String> xVals = mData.getXVals();
                PieDataSet pds = (PieDataSet) dataSet;
                for (int j = 0; j < clrs.size() && j < entryCount && j < xVals.size(); j++) {
                    labels.add(xVals.get(j));
                    colors.add(clrs.get(j));
                }
                colors.add(-2);
                labels.add(pds.getLabel());
            } else {
                for (int j = 0; j < clrs.size() && j < entryCount; j++) {
                    if (j < clrs.size() - 1 && j < entryCount - 1) {
                        labels.add(null);
                    } else {
                        String label = mData.getDataSetByIndex(i).getLabel();
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

    protected void drawLegend() {
        if (!mDrawLegend || mLegend == null || mLegend.getPosition() == LegendPosition.NONE)
            return;
        String[] labels = mLegend.getLegendLabels();
        Typeface tf = mLegend.getTypeface();
        if (tf != null)
            mLegendLabelPaint.setTypeface(tf);
        mLegendLabelPaint.setTextSize(mLegend.getTextSize());
        mLegendLabelPaint.setColor(mLegend.getTextColor());
        float formSize = mLegend.getFormSize();
        float formTextSpaceAndForm = mLegend.getFormToTextSpace() + formSize;
        float stackSpace = mLegend.getStackSpace();
        float textSize = mLegend.getTextSize();
        float textDrop = (Utils.calcTextHeight(mLegendLabelPaint, "AQJ") + formSize) / 2f;
        float posX, posY;
        float stack = 0f;
        boolean wasStacked = false;
        switch(mLegend.getPosition()) {
            case BELOW_CHART_LEFT:
                posX = mLegend.getOffsetLeft();
                posY = getHeight() - mLegend.getOffsetBottom() / 2f - formSize / 2f;
                for (int i = 0; i < labels.length; i++) {
                    mLegend.drawForm(mDrawCanvas, posX, posY, mLegendFormPaint, i);
                    if (labels[i] != null) {
                        if (mLegend.getColors()[i] != -2)
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
                        if (mLegend.getColors()[i] != -2)
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
                for (int i = 0; i < labels.length; i++) {
                    mLegend.drawForm(mDrawCanvas, posX + stack, posY, mLegendFormPaint, i);
                    if (labels[i] != null) {
                        if (!wasStacked) {
                            float x = posX;
                            if (mLegend.getColors()[i] != -2)
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
            case RIGHT_OF_CHART_CENTER:
                posX = getWidth() - mLegend.getMaximumEntryLength(mLegendLabelPaint) - formTextSpaceAndForm;
                posY = getHeight() / 2f - mLegend.getFullHeight(mLegendLabelPaint) / 2f;
                for (int i = 0; i < labels.length; i++) {
                    mLegend.drawForm(mDrawCanvas, posX + stack, posY, mLegendFormPaint, i);
                    if (labels[i] != null) {
                        if (!wasStacked) {
                            float x = posX;
                            if (mLegend.getColors()[i] != -2)
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
                        if (mLegend.getColors()[i] != -2)
                            posX += formTextSpaceAndForm;
                        mLegend.drawLabel(mDrawCanvas, posX, posY + textDrop, mLegendLabelPaint, i);
                        posX += Utils.calcTextWidth(mLegendLabelPaint, labels[i]) + mLegend.getXEntrySpace();
                    } else {
                        posX += formSize + stackSpace;
                    }
                }
                Log.i(LOG_TAG, "content bottom: " + mContentRect.bottom + ", height: " + getHeight() + ", posY: " + posY + ", formSize: " + formSize);
                break;
            case PIECHART_CENTER:
                posX = getWidth() / 2f - (mLegend.getMaximumEntryLength(mLegendLabelPaint) + mLegend.getXEntrySpace()) / 2f;
                posY = getHeight() / 2f - mLegend.getFullHeight(mLegendLabelPaint) / 2f;
                for (int i = 0; i < labels.length; i++) {
                    mLegend.drawForm(mDrawCanvas, posX + stack, posY, mLegendFormPaint, i);
                    if (labels[i] != null) {
                        if (!wasStacked) {
                            float x = posX;
                            if (mLegend.getColors()[i] != -2)
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
            case RIGHT_OF_CHART_INSIDE:
                posX = getWidth() - mLegend.getMaximumEntryLength(mLegendLabelPaint) - formTextSpaceAndForm;
                posY = mLegend.getOffsetTop();
                for (int i = 0; i < labels.length; i++) {
                    mLegend.drawForm(mDrawCanvas, posX + stack, posY, mLegendFormPaint, i);
                    if (labels[i] != null) {
                        if (!wasStacked) {
                            float x = posX;
                            if (mLegend.getColors()[i] != -2)
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
            case NONE:
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

    protected Highlight[] mIndicesToHightlight = new Highlight[0];

    public boolean valuesToHighlight() {
        return mIndicesToHightlight == null || mIndicesToHightlight.length <= 0 || mIndicesToHightlight[0] == null ? false : true;
    }

    public void highlightValues(Highlight[] highs) {
        mIndicesToHightlight = highs;
        invalidate();
    }

    public void highlightValue(int xIndex, int dataSetIndex) {
        if (xIndex < 0 || dataSetIndex < 0 || xIndex >= mData.getXValCount() || dataSetIndex >= mData.getDataSetCount()) {
            highlightValues(null);
        } else {
            highlightValues(new Highlight[] { new Highlight(xIndex, dataSetIndex) });
        }
    }

    public void highlightTouch(Highlight high) {
        if (high == null)
            mIndicesToHightlight = null;
        else {
            mIndicesToHightlight = new Highlight[] { high };
        }
        invalidate();
        if (mSelectionListener != null) {
            if (!valuesToHighlight())
                mSelectionListener.onNothingSelected();
            else {
                Entry e = getEntryByDataSetIndex(high.getXIndex(), high.getDataSetIndex());
                mSelectionListener.onValueSelected(e, high.getDataSetIndex());
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
            int dataSetIndex = mIndicesToHightlight[i].getDataSetIndex();
            if (xIndex <= mDeltaX && xIndex <= mDeltaX * mPhaseX) {
                Entry e = getEntryByDataSetIndex(xIndex, dataSetIndex);
                if (e == null)
                    continue;
                float[] pos = getMarkerPosition(e, dataSetIndex);
                if (pos[0] < mOffsetLeft || pos[0] > getWidth() - mOffsetRight || pos[1] < mOffsetTop || pos[1] > getHeight() - mOffsetBottom)
                    continue;
                mMarkerView.refreshContent(e, dataSetIndex);
                mMarkerView.measure(MeasureSpec.makeMeasureSpec(0, MeasureSpec.UNSPECIFIED), MeasureSpec.makeMeasureSpec(0, MeasureSpec.UNSPECIFIED));
                mMarkerView.layout(0, 0, mMarkerView.getMeasuredWidth(), mMarkerView.getMeasuredHeight());
                mMarkerView.draw(mDrawCanvas, pos[0], pos[1]);
            }
        }
    }

    private float[] getMarkerPosition(Entry e, int dataSetIndex) {
        float xPos = (float) e.getXIndex();
        if (this instanceof CandleStickChart)
            xPos += 0.5f;
        else if (this instanceof BarChart) {
            BarData bd = (BarData) mData;
            float space = bd.getGroupSpace();
            float j = mData.getDataSetByIndex(dataSetIndex).getEntryPosition(e);
            float x = (j * (mData.getDataSetCount() - 1)) + dataSetIndex + space * j + space / 2f + 0.5f;
            xPos += x;
        } else if (this instanceof RadarChart) {
            RadarChart rc = (RadarChart) this;
            float angle = rc.getSliceAngle() * e.getXIndex() + rc.getRotationAngle();
            float val = e.getVal() * rc.getFactor();
            PointF c = getCenterOffsets();
            PointF p = new PointF((float) (c.x + val * Math.cos(Math.toRadians(angle))), (float) (c.y + val * Math.sin(Math.toRadians(angle))));
            return new float[] { p.x, p.y };
        }
        float[] pts = new float[] { xPos, e.getVal() * mPhaseY };
        mTrans.pointValuesToPixel(pts);
        return pts;
    }

    protected float mPhaseY = 1f;

    protected float mPhaseX = 1f;

    private ObjectAnimator mAnimatorY;

    private ObjectAnimator mAnimatorX;

    public void animateXY(int durationMillisX, int durationMillisY) {
        mAnimatorY = ObjectAnimator.ofFloat(this, "phaseY", 0f, 1f);
        mAnimatorY.setDuration(durationMillisY);
        mAnimatorX = ObjectAnimator.ofFloat(this, "phaseX", 0f, 1f);
        mAnimatorX.setDuration(durationMillisX);
        if (durationMillisX > durationMillisY) {
            mAnimatorX.addUpdateListener(this);
        } else {
            mAnimatorY.addUpdateListener(this);
        }
        mAnimatorX.start();
        mAnimatorY.start();
    }

    public void animateX(int durationMillis) {
        mAnimatorX = ObjectAnimator.ofFloat(this, "phaseX", 0f, 1f);
        mAnimatorX.setDuration(durationMillis);
        mAnimatorX.addUpdateListener(this);
        mAnimatorX.start();
    }

    public void animateY(int durationMillis) {
        mAnimatorY = ObjectAnimator.ofFloat(this, "phaseY", 0f, 1f);
        mAnimatorY.setDuration(durationMillis);
        mAnimatorY.addUpdateListener(this);
        mAnimatorY.start();
    }

    @Override
    public void onAnimationUpdate(ValueAnimator va) {
        invalidate();
    }

    public float getPhaseY() {
        return mPhaseY;
    }

    public void setPhaseY(float phase) {
        mPhaseY = phase;
    }

    public float getPhaseX() {
        return mPhaseX;
    }

    public void setPhaseX(float phase) {
        mPhaseX = phase;
    }

    public Canvas getCanvas() {
        return mDrawCanvas;
    }

    public void setOnChartValueSelectedListener(OnChartValueSelectedListener l) {
        this.mSelectionListener = l;
    }

    public void setOnChartGestureListener(OnChartGestureListener l) {
        this.mGestureListener = l;
    }

    public OnChartGestureListener getOnChartGestureListener() {
        return mGestureListener;
    }

    public void setHighlightEnabled(boolean enabled) {
        mHighlightEnabled = enabled;
    }

    public boolean isHighlightEnabled() {
        return mHighlightEnabled;
    }

    public float getYValueSum() {
        return mData.getYValueSum();
    }

    public float getYMax() {
        return mData.getYMax();
    }

    public float getYChartMin() {
        return mYChartMin;
    }

    public float getYChartMax() {
        return mYChartMax;
    }

    public float getYMin() {
        return mData.getYMin();
    }

    public float getDeltaX() {
        return mDeltaX;
    }

    public float getDeltaY() {
        return mDeltaY;
    }

    public float getAverage() {
        return getYValueSum() / mData.getYValCount();
    }

    public float getAverage(String dataSetLabel) {
        DataSet<? extends Entry> ds = mData.getDataSetByLabel(dataSetLabel, true);
        return ds.getYValueSum() / ds.getEntryCount();
    }

    public int getValueCount() {
        return mData.getYValCount();
    }

    public PointF getCenter() {
        return new PointF(getWidth() / 2f, getHeight() / 2f);
    }

    public PointF getCenterOffsets() {
        return new PointF(mContentRect.centerX(), mContentRect.centerY());
    }

    public void setDescriptionTextSize(float size) {
        if (size > 14f)
            size = 14f;
        if (size < 7f)
            size = 7f;
        mInfoPaint.setTextSize(Utils.convertDpToPixel(size));
    }

    public void setLogEnabled(boolean enabled) {
        mLogEnabled = enabled;
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

    public RectF getContentRect() {
        return mContentRect;
    }

    public Transformer getTransformer() {
        return mTrans;
    }

    public void disableScroll() {
        ViewParent parent = getParent();
        parent.requestDisallowInterceptTouchEvent(true);
    }

    public void enableScroll() {
        ViewParent parent = getParent();
        parent.requestDisallowInterceptTouchEvent(false);
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

    public static final int PAINT_HIGHLIGHT = 15;

    public static final int PAINT_RADAR_WEB = 16;

    public static final int PAINT_RENDER = 17;

    public static final int PAINT_LEGEND_LABEL = 18;

    public static final int PAINT_LIMIT_LINE = 19;

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
            case PAINT_XLABEL:
                mXLabelPaint = p;
                break;
            case PAINT_YLABEL:
                mYLabelPaint = p;
                break;
            case PAINT_HIGHLIGHT:
                mHighlightPaint = p;
                break;
            case PAINT_LIMIT_LINE:
                mLimitLinePaint = p;
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
            case PAINT_XLABEL:
                return mXLabelPaint;
            case PAINT_YLABEL:
                return mYLabelPaint;
            case PAINT_HIGHLIGHT:
                return mHighlightPaint;
            case PAINT_LIMIT_LINE:
                return mLimitLinePaint;
        }
        return null;
    }

    public boolean isDrawMarkerViewEnabled() {
        return mDrawMarkerViews;
    }

    public void setDrawMarkerViews(boolean enabled) {
        mDrawMarkerViews = enabled;
    }

    public void setValueFormatter(ValueFormatter f) {
        mValueFormatter = f;
        if (f == null)
            mUseDefaultFormatter = true;
        else
            mUseDefaultFormatter = false;
    }

    public ValueFormatter getValueFormatter() {
        return mValueFormatter;
    }

    public void setValueTextColor(int color) {
        mValuePaint.setColor(color);
    }

    public void setValueTextSize(float size) {
        mValuePaint.setTextSize(Utils.convertDpToPixel(size));
    }

    public boolean isDrawYValuesEnabled() {
        return mDrawYValues;
    }

    public String getXValue(int index) {
        if (mData == null || mData.getXValCount() <= index)
            return null;
        else
            return mData.getXVals().get(index);
    }

    public float getYValue(int index, String dataSetLabel) {
        DataSet<? extends Entry> set = mData.getDataSetByLabel(dataSetLabel, true);
        return set.getYVals().get(index).getVal();
    }

    public float getYValue(int xIndex, int dataSetIndex) {
        DataSet<? extends Entry> set = mData.getDataSetByIndex(dataSetIndex);
        return set.getYValForXIndex(xIndex);
    }

    public DataSet<? extends Entry> getDataSetByIndex(int index) {
        return mData.getDataSetByIndex(index);
    }

    public DataSet<? extends Entry> getDataSetByLabel(String dataSetLabel) {
        return mData.getDataSetByLabel(dataSetLabel, true);
    }

    public Entry getEntry(int index) {
        return mData.getDataSetByIndex(0).getYVals().get(index);
    }

    public Entry getEntry(int index, String dataSetLabel) {
        return mData.getDataSetByLabel(dataSetLabel, true).getYVals().get(index);
    }

    public Entry getEntryByDataSetIndex(int xIndex, int dataSetIndex) {
        return mData.getDataSetByIndex(dataSetIndex).getEntryForXIndex(xIndex);
    }

    public ArrayList<SelInfo> getYValsAtIndex(int xIndex) {
        ArrayList<SelInfo> vals = new ArrayList<SelInfo>();
        for (int i = 0; i < mData.getDataSetCount(); i++) {
            float yVal = mData.getDataSetByIndex(i).getYValForXIndex(xIndex);
            if (!Float.isNaN(yVal)) {
                vals.add(new SelInfo(yVal, i));
            }
        }
        return vals;
    }

    public ArrayList<Entry> getEntriesAtIndex(int xIndex) {
        ArrayList<Entry> vals = new ArrayList<Entry>();
        for (int i = 0; i < mData.getDataSetCount(); i++) {
            DataSet<? extends Entry> set = mData.getDataSetByIndex(i);
            Entry e = set.getEntryForXIndex(xIndex);
            if (e != null) {
                vals.add(e);
            }
        }
        return vals;
    }

    public T getData() {
        return mData;
    }

    public float getPercentOfTotal(float val) {
        return val / mData.getYValueSum() * 100f;
    }

    public void setValueTypeface(Typeface t) {
        mValuePaint.setTypeface(t);
    }

    public void setDescriptionTypeface(Typeface t) {
        mDescPaint.setTypeface(t);
    }

    public Bitmap getChartBitmap() {
        Bitmap returnedBitmap = Bitmap.createBitmap(getWidth(), getHeight(), Bitmap.Config.RGB_565);
        Canvas canvas = new Canvas(returnedBitmap);
        Drawable bgDrawable = getBackground();
        if (bgDrawable != null)
            bgDrawable.draw(canvas);
        else
            canvas.drawColor(Color.WHITE);
        draw(canvas);
        return returnedBitmap;
    }

    public boolean saveToPath(String title, String pathOnSD) {
        Bitmap b = getChartBitmap();
        OutputStream stream = null;
        try {
            stream = new FileOutputStream(Environment.getExternalStorageDirectory().getPath() + pathOnSD + "/" + title + ".png");
            b.compress(CompressFormat.PNG, 40, stream);
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
            Bitmap b = getChartBitmap();
            b.compress(Bitmap.CompressFormat.JPEG, quality, out);
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

    @Override
    protected void onLayout(boolean changed, int left, int top, int right, int bottom) {
        prepareContentRect();
        for (int i = 0; i < getChildCount(); i++) {
            getChildAt(i).layout(left, top, right, bottom);
        }
    }

    @Override
    protected void onSizeChanged(int w, int h, int oldw, int oldh) {
        mDrawBitmap = Bitmap.createBitmap(w, h, Bitmap.Config.ARGB_4444);
        mDrawCanvas = new Canvas(mDrawBitmap);
        prepareContentRect();
        prepare();
        super.onSizeChanged(w, h, oldw, oldh);
    }

    private class DefaultValueFormatter implements ValueFormatter {

        private DecimalFormat mFormat;

        public DefaultValueFormatter(DecimalFormat f) {
            mFormat = f;
        }

        @Override
        public String getFormattedValue(float value) {
            return mFormat.format(value);
        }
    }

    @Override
    public View getChartView() {
        return this;
    }
}