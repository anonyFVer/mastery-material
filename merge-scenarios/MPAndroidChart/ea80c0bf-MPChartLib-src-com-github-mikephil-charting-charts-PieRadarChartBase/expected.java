package com.github.mikephil.charting.charts;

import android.animation.ObjectAnimator;
import android.animation.ValueAnimator;
import android.animation.ValueAnimator.AnimatorUpdateListener;
import android.annotation.SuppressLint;
import android.content.Context;
import android.graphics.PointF;
import android.graphics.RectF;
import android.util.AttributeSet;
import android.util.Log;
import android.view.MotionEvent;
import com.github.mikephil.charting.animation.Easing;
import com.github.mikephil.charting.components.Legend.LegendPosition;
import com.github.mikephil.charting.components.XAxis;
import com.github.mikephil.charting.data.ChartData;
import com.github.mikephil.charting.data.DataSet;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.listener.PieRadarChartTouchListener;
import com.github.mikephil.charting.utils.SelInfo;
import com.github.mikephil.charting.utils.Utils;
import java.util.ArrayList;
import java.util.List;

public abstract class PieRadarChartBase<T extends ChartData<? extends DataSet<? extends Entry>>> extends Chart<T> {

    private float mRotationAngle = 270f;

    private float mRawRotationAngle = 270f;

    protected boolean mRotateEnabled = true;

    protected OnTouchListener mListener;

    public PieRadarChartBase(Context context) {
        super(context);
    }

    public PieRadarChartBase(Context context, AttributeSet attrs) {
        super(context, attrs);
    }

    public PieRadarChartBase(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
    }

    @Override
    protected void init() {
        super.init();
        mListener = new PieRadarChartTouchListener(this);
    }

    @Override
    protected void calcMinMax() {
        mDeltaX = mData.getXVals().size() - 1;
    }

    @Override
    public boolean onTouchEvent(MotionEvent event) {
        if (mTouchEnabled && mListener != null)
            return mListener.onTouch(this, event);
        else
            return super.onTouchEvent(event);
    }

    @Override
    public void computeScroll() {
        if (mListener instanceof PieRadarChartTouchListener)
            ((PieRadarChartTouchListener) mListener).computeScroll();
    }

    @Override
    public void notifyDataSetChanged() {
        if (mDataNotSet)
            return;
        calcMinMax();
        if (mLegend != null)
            mLegendRenderer.computeLegend(mData);
        calculateOffsets();
    }

    @Override
    protected void calculateOffsets() {
        float legendLeft = 0f, legendRight = 0f, legendBottom = 0f, legendTop = 0f;
        if (mLegend != null && mLegend.isEnabled()) {
            float fullLegendWidth = Math.min(mLegend.mNeededWidth, mViewPortHandler.getChartWidth() * mLegend.getMaxSizePercent()) + mLegend.getFormSize() + mLegend.getFormToTextSpace();
            if (mLegend.getPosition() == LegendPosition.RIGHT_OF_CHART_CENTER) {
                float spacing = Utils.convertDpToPixel(13f);
                legendRight = fullLegendWidth + spacing;
            } else if (mLegend.getPosition() == LegendPosition.RIGHT_OF_CHART) {
                float spacing = Utils.convertDpToPixel(8f);
                float legendWidth = fullLegendWidth + spacing;
                float legendHeight = mLegend.mNeededHeight + mLegend.mTextHeightMax;
                PointF c = getCenter();
                PointF bottomRight = new PointF(getWidth() - legendWidth + 15, legendHeight + 15);
                float distLegend = distanceToCenter(bottomRight.x, bottomRight.y);
                PointF reference = getPosition(c, getRadius(), getAngleForPoint(bottomRight.x, bottomRight.y));
                float distReference = distanceToCenter(reference.x, reference.y);
                float min = Utils.convertDpToPixel(5f);
                if (distLegend < distReference) {
                    float diff = distReference - distLegend;
                    legendRight = min + diff;
                }
                if (bottomRight.y >= c.y && getHeight() - legendWidth > getWidth()) {
                    legendRight = legendWidth;
                }
            } else if (mLegend.getPosition() == LegendPosition.LEFT_OF_CHART_CENTER) {
                float spacing = Utils.convertDpToPixel(13f);
                legendLeft = fullLegendWidth + spacing;
            } else if (mLegend.getPosition() == LegendPosition.LEFT_OF_CHART) {
                float spacing = Utils.convertDpToPixel(8f);
                float legendWidth = fullLegendWidth + spacing;
                float legendHeight = mLegend.mNeededHeight + mLegend.mTextHeightMax;
                PointF c = getCenter();
                PointF bottomLeft = new PointF(legendWidth - 15, legendHeight + 15);
                float distLegend = distanceToCenter(bottomLeft.x, bottomLeft.y);
                PointF reference = getPosition(c, getRadius(), getAngleForPoint(bottomLeft.x, bottomLeft.y));
                float distReference = distanceToCenter(reference.x, reference.y);
                float min = Utils.convertDpToPixel(5f);
                if (distLegend < distReference) {
                    float diff = distReference - distLegend;
                    legendLeft = min + diff;
                }
                if (bottomLeft.y >= c.y && getHeight() - legendWidth > getWidth()) {
                    legendLeft = legendWidth;
                }
            } else if (mLegend.getPosition() == LegendPosition.BELOW_CHART_LEFT || mLegend.getPosition() == LegendPosition.BELOW_CHART_RIGHT || mLegend.getPosition() == LegendPosition.BELOW_CHART_CENTER) {
                float yOffset = getRequiredBottomOffset();
                legendBottom = Math.min(mLegend.mNeededHeight + yOffset, mViewPortHandler.getChartHeight() * mLegend.getMaxSizePercent());
            }
            legendLeft += getRequiredBaseOffset();
            legendRight += getRequiredBaseOffset();
            legendTop += getRequiredBaseOffset();
        }
        float min = Utils.convertDpToPixel(10f);
        if (this instanceof RadarChart) {
            XAxis x = ((RadarChart) this).getXAxis();
            if (x.isEnabled()) {
                min = Math.max(Utils.convertDpToPixel(10f), x.mLabelWidth);
            }
        }
        legendTop += getExtraTopOffset();
        legendRight += getExtraRightOffset();
        legendBottom += getExtraBottomOffset();
        legendLeft += getExtraLeftOffset();
        float offsetLeft = Math.max(min, legendLeft);
        float offsetTop = Math.max(min, legendTop);
        float offsetRight = Math.max(min, legendRight);
        float offsetBottom = Math.max(min, Math.max(getRequiredBaseOffset(), legendBottom));
        mViewPortHandler.restrainViewPort(offsetLeft, offsetTop, offsetRight, offsetBottom);
        if (mLogEnabled)
            Log.i(LOG_TAG, "offsetLeft: " + offsetLeft + ", offsetTop: " + offsetTop + ", offsetRight: " + offsetRight + ", offsetBottom: " + offsetBottom);
    }

    public float getAngleForPoint(float x, float y) {
        PointF c = getCenterOffsets();
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

    protected PointF getPosition(PointF center, float dist, float angle) {
        PointF p = new PointF((float) (center.x + dist * Math.cos(Math.toRadians(angle))), (float) (center.y + dist * Math.sin(Math.toRadians(angle))));
        return p;
    }

    public float distanceToCenter(float x, float y) {
        PointF c = getCenterOffsets();
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

    public abstract int getIndexForAngle(float angle);

    public void setRotationAngle(float angle) {
        mRawRotationAngle = angle;
        mRotationAngle = Utils.getNormalizedAngle(mRawRotationAngle);
    }

    public float getRawRotationAngle() {
        return mRawRotationAngle;
    }

    public float getRotationAngle() {
        return mRotationAngle;
    }

    public void setRotationEnabled(boolean enabled) {
        mRotateEnabled = enabled;
    }

    public boolean isRotationEnabled() {
        return mRotateEnabled;
    }

    public float getDiameter() {
        RectF content = mViewPortHandler.getContentRect();
        return Math.min(content.width(), content.height());
    }

    public abstract float getRadius();

    protected abstract float getRequiredBottomOffset();

    protected abstract float getRequiredBaseOffset();

    public void setOnTouchListener(OnTouchListener l) {
        this.mListener = l;
    }

    @Override
    public float getYChartMax() {
        return 0;
    }

    @Override
    public float getYChartMin() {
        return 0;
    }

    public List<SelInfo> getYValsAtIndex(int xIndex) {
        List<SelInfo> vals = new ArrayList<SelInfo>();
        for (int i = 0; i < mData.getDataSetCount(); i++) {
            DataSet<?> dataSet = mData.getDataSetByIndex(i);
            float yVal = dataSet.getYValForXIndex(xIndex);
            if (!Float.isNaN(yVal)) {
                vals.add(new SelInfo(yVal, i, dataSet));
            }
        }
        return vals;
    }

    @SuppressLint("NewApi")
    public void spin(int durationmillis, float fromangle, float toangle, Easing.EasingOption easing) {
        if (android.os.Build.VERSION.SDK_INT < 11)
            return;
        setRotationAngle(fromangle);
        ObjectAnimator spinAnimator = ObjectAnimator.ofFloat(this, "rotationAngle", fromangle, toangle);
        spinAnimator.setDuration(durationmillis);
        spinAnimator.setInterpolator(Easing.getEasingFunctionFromOption(easing));
        spinAnimator.addUpdateListener(new AnimatorUpdateListener() {

            @Override
            public void onAnimationUpdate(ValueAnimator animation) {
                postInvalidate();
            }
        });
        spinAnimator.start();
    }
}