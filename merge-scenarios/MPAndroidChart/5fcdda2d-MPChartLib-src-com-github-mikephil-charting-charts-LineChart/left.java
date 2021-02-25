package com.github.mikephil.charting.charts;

import android.content.Context;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Path;
import android.util.AttributeSet;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.LineData;
import com.github.mikephil.charting.data.LineDataSet;
import java.util.ArrayList;

public class LineChart extends BarLineChartBase<LineData> {

    protected float mHighlightWidth = 3f;

    protected Paint mCirclePaintInner;

    public LineChart(Context context) {
        super(context);
    }

    public LineChart(Context context, AttributeSet attrs) {
        super(context, attrs);
    }

    public LineChart(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
    }

    @Override
    protected void init() {
        super.init();
        mCirclePaintInner = new Paint(Paint.ANTI_ALIAS_FLAG);
        mCirclePaintInner.setStyle(Paint.Style.FILL);
        mCirclePaintInner.setColor(Color.WHITE);
        mHighlightPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mHighlightPaint.setStyle(Paint.Style.STROKE);
        mHighlightPaint.setStrokeWidth(2f);
        mHighlightPaint.setColor(Color.rgb(255, 187, 115));
    }

    @Override
    protected void calcMinMax(boolean fixedValues) {
        super.calcMinMax(fixedValues);
        if (mDeltaX == 0 && mOriginalData.getYValCount() > 0)
            mDeltaX = 1;
    }

    @Override
    protected void drawHighlights() {
        for (int i = 0; i < mIndicesToHightlight.length; i++) {
            LineDataSet set = mOriginalData.getDataSetByIndex(mIndicesToHightlight[i].getDataSetIndex());
            if (set == null)
                continue;
            mHighlightPaint.setColor(set.getHighLightColor());
            int xIndex = mIndicesToHightlight[i].getXIndex();
            if (xIndex > mDeltaX * mPhaseX)
                continue;
            float y = set.getYValForXIndex(xIndex) * mPhaseY;
            float[] pts = new float[] { xIndex, mYChartMax, xIndex, mYChartMin, 0, y, mDeltaX, y };
            transformPointArray(pts);
            mDrawCanvas.drawLines(pts, mHighlightPaint);
        }
    }

    private class CPoint {

        public float x = 0f;

        public float y = 0f;

        public float dx = 0f;

        public float dy = 0f;

        public CPoint(float x, float y) {
            this.x = x;
            this.y = y;
        }
    }

    @Override
    protected void drawData() {
        ArrayList<LineDataSet> dataSets = mCurrentData.getDataSets();
        for (int i = 0; i < mCurrentData.getDataSetCount(); i++) {
            LineDataSet dataSet = dataSets.get(i);
            ArrayList<Entry> entries = dataSet.getYVals();
            mRenderPaint.setStrokeWidth(dataSet.getLineWidth());
            mRenderPaint.setPathEffect(dataSet.getDashPathEffect());
            if (dataSet.isDrawCubicEnabled()) {
                mRenderPaint.setColor(dataSet.getColor());
                float intensity = dataSet.getCubicIntensity();
                Path spline = new Path();
                ArrayList<CPoint> points = new ArrayList<CPoint>();
                for (Entry e : entries) points.add(new CPoint(e.getXIndex(), e.getVal()));
                if (points.size() > 1) {
                    for (int j = 0; j < points.size() * mPhaseX; j++) {
                        CPoint point = points.get(j);
                        if (j == 0) {
                            CPoint next = points.get(j + 1);
                            point.dx = ((next.x - point.x) * intensity);
                            point.dy = ((next.y - point.y) * intensity);
                        } else if (j == points.size() - 1) {
                            CPoint prev = points.get(j - 1);
                            point.dx = ((point.x - prev.x) * intensity);
                            point.dy = ((point.y - prev.y) * intensity);
                        } else {
                            CPoint next = points.get(j + 1);
                            CPoint prev = points.get(j - 1);
                            point.dx = ((next.x - prev.x) * intensity);
                            point.dy = ((next.y - prev.y) * intensity);
                        }
                        if (j == 0) {
                            spline.moveTo(point.x, point.y * mPhaseY);
                        } else {
                            CPoint prev = points.get(j - 1);
                            spline.cubicTo(prev.x + prev.dx, (prev.y + prev.dy) * mPhaseY, point.x - point.dx, (point.y - point.dy) * mPhaseY, point.x, point.y * mPhaseY);
                        }
                    }
                }
                if (dataSet.isDrawFilledEnabled()) {
                    float fillMin = dataSet.getYMin() >= 0 ? mYChartMin : 0;
                    spline.lineTo((entries.size() - 1) * mPhaseX, fillMin);
                    spline.lineTo(0, fillMin);
                    spline.close();
                    mRenderPaint.setStyle(Paint.Style.FILL);
                } else {
                    mRenderPaint.setStyle(Paint.Style.STROKE);
                }
                transformPath(spline);
                mDrawCanvas.drawPath(spline, mRenderPaint);
            } else {
                mRenderPaint.setStyle(Paint.Style.STROKE);
                if (dataSet.getColors() == null || dataSet.getColors().size() > 1) {
                    float[] valuePoints = generateTransformedValuesLineScatter(entries);
                    for (int j = 0; j < (valuePoints.length - 2) * mPhaseX; j += 2) {
                        if (isOffContentRight(valuePoints[j]))
                            break;
                        if (j != 0 && isOffContentLeft(valuePoints[j - 1]) && isOffContentTop(valuePoints[j + 1]) && isOffContentBottom(valuePoints[j + 1]))
                            continue;
                        mRenderPaint.setColor(dataSet.getColor(j / 2));
                        mDrawCanvas.drawLine(valuePoints[j], valuePoints[j + 1], valuePoints[j + 2], valuePoints[j + 3], mRenderPaint);
                    }
                } else {
                    mRenderPaint.setColor(dataSet.getColor());
                    float[] valuePoints = generateTransformedValuesLineScatter(entries);
                    for (int j = 0; j < (valuePoints.length - 2) * mPhaseX; j += 2) {
                        if (isOffContentRight(valuePoints[j]))
                            break;
                        if (j != 0 && isOffContentLeft(valuePoints[j - 1]) && isOffContentTop(valuePoints[j + 1]) && isOffContentBottom(valuePoints[j + 1]))
                            continue;
                        mDrawCanvas.drawLine(valuePoints[j], valuePoints[j + 1], valuePoints[j + 2], valuePoints[j + 3], mRenderPaint);
                    }
                }
                mRenderPaint.setPathEffect(null);
                if (dataSet.isDrawFilledEnabled() && entries.size() > 0) {
                    mRenderPaint.setStyle(Paint.Style.FILL);
                    mRenderPaint.setColor(dataSet.getFillColor());
                    mRenderPaint.setAlpha(dataSet.getFillAlpha());
                    float fillMin = dataSet.getYMin() >= 0 ? mYChartMin : 0;
                    Path filled = generateFilledPath(entries, fillMin);
                    transformPath(filled);
                    mDrawCanvas.drawPath(filled, mRenderPaint);
                    mRenderPaint.setAlpha(255);
                }
            }
            mRenderPaint.setPathEffect(null);
        }
    }

    private Path generateFilledPath(ArrayList<Entry> entries, float fillMin) {
        Path filled = new Path();
        filled.moveTo(entries.get(0).getXIndex(), entries.get(0).getVal() * mPhaseY);
        for (int x = 1; x < entries.size() * mPhaseX; x++) {
            Entry e = entries.get(x);
            filled.lineTo(e.getXIndex(), e.getVal() * mPhaseY);
        }
        filled.lineTo(entries.get((int) ((entries.size() - 1) * mPhaseX)).getXIndex(), fillMin);
        filled.lineTo(entries.get(0).getXIndex(), fillMin);
        filled.close();
        return filled;
    }

    @Override
    protected void drawValues() {
        if (mDrawYValues && mCurrentData.getYValCount() < mMaxVisibleCount * mScaleX) {
            ArrayList<LineDataSet> dataSets = mCurrentData.getDataSets();
            for (int i = 0; i < mCurrentData.getDataSetCount(); i++) {
                LineDataSet dataSet = dataSets.get(i);
                int valOffset = (int) (dataSet.getCircleSize() * 1.75f);
                if (!dataSet.isDrawCirclesEnabled())
                    valOffset = valOffset / 2;
                ArrayList<Entry> entries = dataSet.getYVals();
                float[] positions = generateTransformedValuesLineScatter(entries);
                for (int j = 0; j < positions.length * mPhaseX; j += 2) {
                    if (isOffContentRight(positions[j]))
                        break;
                    if (isOffContentLeft(positions[j]) || isOffContentTop(positions[j + 1]) || isOffContentBottom(positions[j + 1]))
                        continue;
                    float val = entries.get(j / 2).getVal();
                    if (mDrawUnitInChart) {
                        mDrawCanvas.drawText(mValueFormatter.getFormattedValue(val) + mUnit, positions[j], positions[j + 1] - valOffset, mValuePaint);
                    } else {
                        mDrawCanvas.drawText(mValueFormatter.getFormattedValue(val), positions[j], positions[j + 1] - valOffset, mValuePaint);
                    }
                }
            }
        }
    }

    @Override
    protected void drawAdditional() {
        mRenderPaint.setStyle(Paint.Style.FILL);
        ArrayList<LineDataSet> dataSets = mCurrentData.getDataSets();
        for (int i = 0; i < mCurrentData.getDataSetCount(); i++) {
            LineDataSet dataSet = dataSets.get(i);
            if (dataSet.isDrawCirclesEnabled()) {
                ArrayList<Entry> entries = dataSet.getYVals();
                float[] positions = generateTransformedValuesLineScatter(entries);
                for (int j = 0; j < positions.length * mPhaseX; j += 2) {
                    mRenderPaint.setColor(dataSet.getCircleColor(j / 2));
                    if (isOffContentRight(positions[j]))
                        break;
                    if (isOffContentLeft(positions[j]) || isOffContentTop(positions[j + 1]) || isOffContentBottom(positions[j + 1]))
                        continue;
                    mDrawCanvas.drawCircle(positions[j], positions[j + 1], dataSet.getCircleSize(), mRenderPaint);
                    mDrawCanvas.drawCircle(positions[j], positions[j + 1], dataSet.getCircleSize() / 2f, mCirclePaintInner);
                }
            }
        }
    }

    public void setHighlightLineWidth(float width) {
        mHighlightWidth = width;
    }

    public float getHighlightLineWidth() {
        return mHighlightWidth;
    }

    @Override
    public void setPaint(Paint p, int which) {
        super.setPaint(p, which);
        switch(which) {
            case PAINT_CIRCLES_INNER:
                mCirclePaintInner = p;
                break;
        }
    }

    @Override
    public Paint getPaint(int which) {
        Paint p = super.getPaint(which);
        if (p != null)
            return p;
        switch(which) {
            case PAINT_CIRCLES_INNER:
                return mCirclePaintInner;
        }
        return null;
    }
}