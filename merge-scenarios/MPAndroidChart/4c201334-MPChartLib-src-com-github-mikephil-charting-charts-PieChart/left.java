package com.github.mikephil.charting.charts;

import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Matrix;
import android.graphics.Paint;
import android.graphics.Paint.Align;
import android.graphics.PointF;
import android.graphics.RectF;
import android.graphics.Typeface;
import android.os.Handler;
import android.util.AttributeSet;
import android.util.Log;
import android.view.MotionEvent;
import java.text.DecimalFormat;
import java.util.ArrayList;
import com.github.mikephil.charting.data.DataSet;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.listener.PieChartTouchListener;
import com.github.mikephil.charting.utils.Utils;
import com.github.mikephil.charting.utils.Legend.LegendPosition;

public class PieChart extends Chart {

    private RectF mCircleBox = new RectF();

    private float mChartAngle = 0f;

    private float[] mDrawAngles;

    private float[] mAbsoluteAngles;

    private boolean mDrawHole = true;

    private String mCenterText = null;

    private float mShift = 20f;

    private float mHoleRadiusPercent = 50f;

    private float mTransparentCircleRadius = 55f;

    private boolean mDrawCenterText = true;

    private boolean mDrawXVals = true;

    private boolean mUsePercentValues = false;

    private Paint mHolePaint;

    private Paint mCenterTextPaint;

    public PieChart(Context context) {
        super(context);
    }

    public PieChart(Context context, AttributeSet attrs) {
        super(context, attrs);
    }

    public PieChart(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
    }

    @Override
    protected void init() {
        super.init();
        mShift = Utils.convertDpToPixel(mShift);
        mHolePaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mHolePaint.setColor(Color.WHITE);
        mCenterTextPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mCenterTextPaint.setColor(mColorDarkBlue);
        mCenterTextPaint.setTextSize(Utils.convertDpToPixel(12f));
        mCenterTextPaint.setTextAlign(Align.CENTER);
        mValuePaint.setTextSize(Utils.convertDpToPixel(13f));
        mValuePaint.setColor(Color.WHITE);
        mValuePaint.setTextAlign(Align.CENTER);
        mListener = new PieChartTouchListener(this);
        mDrawYValues = true;
    }

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        if (mDataNotSet)
            return;
        long starttime = System.currentTimeMillis();
        drawHighlights();
        drawData();
        drawAdditional();
        drawValues();
        drawLegend();
        drawDescription();
        drawCenterText();
        canvas.drawBitmap(mDrawBitmap, 0, 0, mDrawPaint);
        Log.i(LOG_TAG, "PieChart DrawTime: " + (System.currentTimeMillis() - starttime) + " ms");
    }

    @Override
    public void prepare() {
        if (mDataNotSet)
            return;
        calcMinMax(false);
        if (mCenterText == null)
            mCenterText = "Total Value\n" + (int) getYValueSum();
        calcFormats();
        prepareLegend();
    }

    @Override
    public void notifyDataSetChanged() {
    }

    @Override
    public void calculateOffsets() {
        if (mLegend.getPosition() == LegendPosition.RIGHT_OF_CHART) {
            mLegendLabelPaint.setTextAlign(Align.LEFT);
            mOffsetTop = (int) (mLegendLabelPaint.getTextSize() * 3.5f);
        } else if (mLegend.getPosition() == LegendPosition.BELOW_CHART_LEFT || mLegend.getPosition() == LegendPosition.BELOW_CHART_RIGHT) {
            mOffsetBottom = (int) (mLegendLabelPaint.getTextSize() * 3.5f);
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

    protected DecimalFormat mFormatValue = null;

    protected void calcFormats() {
        if (mValueDigitsToUse == -1)
            mValueFormatDigits = Utils.getPieFormatDigits(mDeltaY);
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
    public boolean onTouchEvent(MotionEvent event) {
        return mListener.onTouch(this, event);
    }

    private float mStartAngle = 0f;

    public void setStartAngle(float x, float y) {
        mStartAngle = getAngleForPoint(x, y);
        mStartAngle -= mChartAngle;
    }

    public void updateRotation(float x, float y) {
        mChartAngle = getAngleForPoint(x, y);
        mChartAngle -= mStartAngle;
        mChartAngle = (mChartAngle + 360f) % 360f;
    }

    @Override
    protected void prepareContentRect() {
        super.prepareContentRect();
        int width = mContentRect.width() + mOffsetLeft + mOffsetRight;
        int height = mContentRect.height() + mOffsetTop + mOffsetBottom;
        float diameter = getDiameter();
        mCircleBox.set(width / 2 - diameter / 2 + mShift, height / 2 - diameter / 2 + mShift, width / 2 + diameter / 2 - mShift, height / 2 + diameter / 2 - mShift);
    }

    @Override
    protected void calcMinMax(boolean fixedValues) {
        super.calcMinMax(fixedValues);
        calcAngles();
    }

    private void calcAngles() {
        mDrawAngles = new float[mCurrentData.getYValCount()];
        mAbsoluteAngles = new float[mCurrentData.getYValCount()];
        ArrayList<DataSet> dataSets = mCurrentData.getDataSets();
        int cnt = 0;
        for (int i = 0; i < mCurrentData.getDataSetCount(); i++) {
            DataSet set = dataSets.get(i);
            ArrayList<Entry> entries = set.getYVals();
            for (int j = 0; j < entries.size(); j++) {
                mDrawAngles[cnt] = calcAngle(entries.get(j).getVal());
                if (cnt == 0) {
                    mAbsoluteAngles[cnt] = mDrawAngles[cnt];
                } else {
                    mAbsoluteAngles[cnt] = mAbsoluteAngles[cnt - 1] + mDrawAngles[cnt];
                }
                cnt++;
            }
        }
    }

    @Override
    protected void drawHighlights() {
        if (mHighlightEnabled && valuesToHighlight()) {
            float angle = 0f;
            for (int i = 0; i < mIndicesToHightlight.length; i++) {
                int xIndex = mIndicesToHightlight[i].getXIndex();
                if (xIndex >= mDrawAngles.length || xIndex > mDeltaX)
                    continue;
                if (xIndex == 0)
                    angle = mChartAngle;
                else
                    angle = mChartAngle + mAbsoluteAngles[xIndex - 1];
                float sliceDegrees = mDrawAngles[xIndex];
                float shiftangle = (float) Math.toRadians(angle + sliceDegrees / 2f);
                float xShift = mShift * (float) Math.cos(shiftangle);
                float yShift = mShift * (float) Math.sin(shiftangle);
                RectF highlighted = new RectF(mCircleBox.left + xShift, mCircleBox.top + yShift, mCircleBox.right + xShift, mCircleBox.bottom + yShift);
                DataSet set = mCurrentData.getDataSetByIndex(mIndicesToHightlight[i].getDataSetIndex());
                int color = mCt.getDataSetColor(mIndicesToHightlight[i].getDataSetIndex(), set.getIndexInEntries(xIndex));
                mRenderPaint.setColor(color);
                mDrawCanvas.drawArc(highlighted, angle, sliceDegrees, true, mRenderPaint);
            }
        }
    }

    @Override
    protected void drawData() {
        float angle = mChartAngle;
        ArrayList<DataSet> dataSets = mCurrentData.getDataSets();
        int cnt = 0;
        for (int i = 0; i < mCurrentData.getDataSetCount(); i++) {
            DataSet dataSet = dataSets.get(i);
            ArrayList<Entry> entries = dataSet.getYVals();
            ArrayList<Integer> colors = mCt.getDataSetColors(i % mCt.getColors().size());
            for (int j = 0; j < entries.size(); j++) {
                float newangle = mDrawAngles[cnt];
                if (!needsHighlight(entries.get(j).getXIndex(), i)) {
                    mRenderPaint.setColor(colors.get(j % colors.size()));
                    mDrawCanvas.drawArc(mCircleBox, angle, newangle, true, mRenderPaint);
                }
                angle += newangle;
                cnt++;
            }
        }
    }

    private void drawHole() {
        if (mDrawHole) {
            float radius = getRadius();
            PointF c = getCenterCircleBox();
            int color = mHolePaint.getColor();
            mDrawCanvas.drawCircle(c.x, c.y, radius / 100 * mHoleRadiusPercent, mHolePaint);
            mHolePaint.setColor(color & 0x60FFFFFF);
            mDrawCanvas.drawCircle(c.x, c.y, radius / 100 * mTransparentCircleRadius, mHolePaint);
            mHolePaint.setColor(color);
        }
    }

    private void drawCenterText() {
        if (mDrawCenterText) {
            PointF c = getCenterCircleBox();
            String[] lines = mCenterText.split("\n");
            float lineHeight = Utils.calcTextHeight(mCenterTextPaint, lines[0]);
            float linespacing = lineHeight * 0.2f;
            float totalheight = lineHeight * lines.length - linespacing * (lines.length - 1);
            int cnt = lines.length;
            float y = c.y;
            for (int i = 0; i < lines.length; i++) {
                String line = lines[lines.length - i - 1];
                mDrawCanvas.drawText(line, c.x, y + lineHeight * cnt - totalheight / 2f, mCenterTextPaint);
                cnt--;
                y -= linespacing;
            }
        }
    }

    @Override
    protected void drawValues() {
        if (!mDrawXVals && !mDrawYValues)
            return;
        PointF center = getCenterCircleBox();
        float r = getRadius();
        float off = r / 2f;
        if (mDrawHole) {
            off = (r - (r / 100f * mHoleRadiusPercent)) / 2f;
        }
        r -= off;
        ArrayList<DataSet> dataSets = mCurrentData.getDataSets();
        int cnt = 0;
        for (int i = 0; i < mCurrentData.getDataSetCount(); i++) {
            DataSet dataSet = dataSets.get(i);
            ArrayList<Entry> entries = dataSet.getYVals();
            for (int j = 0; j < entries.size(); j++) {
                float offset = mDrawAngles[cnt] / 2;
                float x = (float) (r * Math.cos(Math.toRadians(mChartAngle + mAbsoluteAngles[cnt] - offset)) + center.x);
                float y = (float) (r * Math.sin(Math.toRadians(mChartAngle + mAbsoluteAngles[cnt] - offset)) + center.y);
                String val = "";
                float value = entries.get(j).getVal();
                if (mUsePercentValues)
                    val = mFormatValue.format(getPercentOfTotal(value)) + " %";
                else
                    val = mFormatValue.format(value);
                if (mDrawXVals && mDrawYValues) {
                    float lineHeight = (mValuePaint.ascent() + mValuePaint.descent()) * 1.6f;
                    y -= lineHeight / 2;
                    mDrawCanvas.drawText(val, x, y, mValuePaint);
                    mDrawCanvas.drawText(mCurrentData.getXVals().get(j), x, y + lineHeight, mValuePaint);
                } else if (mDrawXVals && !mDrawYValues) {
                    mDrawCanvas.drawText(mCurrentData.getXVals().get(j), x, y, mValuePaint);
                } else if (!mDrawXVals && mDrawYValues) {
                    mDrawCanvas.drawText(val, x, y, mValuePaint);
                }
                cnt++;
            }
        }
    }

    @Override
    protected void drawAdditional() {
        drawHole();
    }

    private float calcAngle(float value) {
        return value / mCurrentData.getYValueSum() * 360f;
    }

    public int getIndexForAngle(float angle) {
        float a = (angle - mChartAngle + 360) % 360f;
        for (int i = 0; i < mAbsoluteAngles.length; i++) {
            if (mAbsoluteAngles[i] > a)
                return i;
        }
        return -1;
    }

    public int getDataSetIndexForIndex(int xIndex) {
        ArrayList<DataSet> sets = mCurrentData.getDataSets();
        for (int i = 0; i < sets.size(); i++) {
            if (sets.get(i).getEntryForXIndex(xIndex) != null)
                return i;
        }
        return -1;
    }

    public float[] getDrawAngles() {
        return mDrawAngles;
    }

    public float[] getAbsoluteAngles() {
        return mAbsoluteAngles;
    }

    public void setStartAngle(float angle) {
        mChartAngle = angle;
    }

    public float getCurrentRotation() {
        return mChartAngle;
    }

    public void setShift(float shift) {
        mShift = Utils.convertDpToPixel(shift);
    }

    public void setDrawHoleEnabled(boolean enabled) {
        this.mDrawHole = enabled;
    }

    public boolean isDrawHoleEnabled() {
        return mDrawHole;
    }

    public void setCenterText(String text) {
        mCenterText = text;
    }

    public String getCenterText() {
        return mCenterText;
    }

    public void setDrawCenterText(boolean enabled) {
        this.mDrawCenterText = enabled;
    }

    public boolean isDrawCenterTextEnabled() {
        return mDrawCenterText;
    }

    public void setUsePercentValues(boolean enabled) {
        mUsePercentValues = enabled;
    }

    public boolean isUsePercentValuesEnabled() {
        return mUsePercentValues;
    }

    public void setDrawXValues(boolean enabled) {
        mDrawXVals = enabled;
    }

    public boolean isDrawXValuesEnabled() {
        return mDrawXVals;
    }

    public float getRadius() {
        if (mCircleBox == null)
            return 0;
        else
            return Math.min(mCircleBox.width() / 2f, mCircleBox.height() / 2f);
    }

    public float getDiameter() {
        if (mContentRect == null)
            return 0;
        else
            return Math.min(mContentRect.width(), mContentRect.height());
    }

    public RectF getCircleBox() {
        return mCircleBox;
    }

    public PointF getCenterCircleBox() {
        return new PointF(mCircleBox.centerX(), mCircleBox.centerY());
    }

    public float getAngleForPoint(float x, float y) {
        PointF c = getCenterCircleBox();
        double tx = x - c.x, ty = y - c.y;
        double length = Math.sqrt(tx * tx + ty * ty);
        double r = Math.acos(ty / length);
        float angle = (float) Math.toDegrees(r);
        if (x > c.x)
            angle = 360f - angle;
        angle = angle + 90f;
        if (angle > 360f)
            angle = angle - 360f;
        return angle;
    }

    public float distanceToCenter(float x, float y) {
        PointF c = getCenterCircleBox();
        float dist = 0f;
        float xDist = 0f;
        float yDist = 0f;
        if (x > c.x) {
            xDist = x - c.x;
        } else {
            xDist = c.x - x;
        }
        if (y > c.y) {
            yDist = y - c.y;
        } else {
            yDist = c.y - y;
        }
        dist = (float) Math.sqrt(Math.pow(xDist, 2.0) + Math.pow(yDist, 2.0));
        return dist;
    }

    public void setCenterTextTypeface(Typeface t) {
        mCenterTextPaint.setTypeface(t);
    }

    public void setCenterTextSize(float size) {
        mCenterTextPaint.setTextSize(Utils.convertDpToPixel(size));
    }

    public void setHoleRadius(final float percent) {
        Handler h = new Handler();
        h.post(new Runnable() {

            @Override
            public void run() {
                mHoleRadiusPercent = percent;
            }
        });
    }

    public void setTransparentCircleRadius(final float percent) {
        Handler h = new Handler();
        h.post(new Runnable() {

            @Override
            public void run() {
                mTransparentCircleRadius = percent;
            }
        });
    }

    @Override
    public void setPaint(Paint p, int which) {
        super.setPaint(p, which);
        switch(which) {
            case PAINT_HOLE:
                mHolePaint = p;
                break;
            case PAINT_CENTER_TEXT:
                mCenterTextPaint = p;
                break;
        }
    }

    @Override
    public Paint getPaint(int which) {
        super.getPaint(which);
        switch(which) {
            case PAINT_HOLE:
                return mHolePaint;
            case PAINT_CENTER_TEXT:
                return mCenterTextPaint;
        }
        return null;
    }
}