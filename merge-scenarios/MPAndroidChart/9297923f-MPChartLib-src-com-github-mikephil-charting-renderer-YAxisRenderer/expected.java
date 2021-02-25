package com.github.mikephil.charting.renderer;

import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Paint.Align;
import android.graphics.Path;
import com.github.mikephil.charting.components.LimitLine;
import com.github.mikephil.charting.components.YAxis;
import com.github.mikephil.charting.components.YAxis.AxisDependency;
import com.github.mikephil.charting.components.YAxis.YAxisLabelPosition;
import com.github.mikephil.charting.utils.PointD;
import com.github.mikephil.charting.utils.Transformer;
import com.github.mikephil.charting.utils.Utils;
import com.github.mikephil.charting.utils.ViewPortHandler;
import java.util.List;

public class YAxisRenderer extends AxisRenderer {

    protected YAxis mYAxis;

    protected Paint mZeroLinePaint;

    public YAxisRenderer(ViewPortHandler viewPortHandler, YAxis yAxis, Transformer trans) {
        super(viewPortHandler, trans);
        this.mYAxis = yAxis;
        mAxisLabelPaint.setColor(Color.BLACK);
        mAxisLabelPaint.setTextSize(Utils.convertDpToPixel(10f));
        mZeroLinePaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mZeroLinePaint.setColor(Color.GRAY);
        mZeroLinePaint.setStrokeWidth(1f);
        mZeroLinePaint.setStyle(Paint.Style.STROKE);
    }

    public void computeAxis(float yMin, float yMax) {
        if (mViewPortHandler.contentWidth() > 10 && !mViewPortHandler.isFullyZoomedOutY()) {
            PointD p1 = mTrans.getValuesByTouchPoint(mViewPortHandler.contentLeft(), mViewPortHandler.contentTop());
            PointD p2 = mTrans.getValuesByTouchPoint(mViewPortHandler.contentLeft(), mViewPortHandler.contentBottom());
            if (!mYAxis.isInverted()) {
                yMin = (float) p2.y;
                yMax = (float) p1.y;
            } else {
                yMin = (float) p1.y;
                yMax = (float) p2.y;
            }
        }
        computeAxisValues(yMin, yMax);
    }

    protected void computeAxisValues(float min, float max) {
        float yMin = min;
        float yMax = max;
        int labelCount = mYAxis.getLabelCount();
        double range = Math.abs(yMax - yMin);
        if (labelCount == 0 || range <= 0) {
            mYAxis.mEntries = new float[] {};
            mYAxis.mEntryCount = 0;
            return;
        }
        double rawInterval = range / labelCount;
        double interval = Utils.roundToNextSignificant(rawInterval);
        double intervalMagnitude = Math.pow(10, (int) Math.log10(interval));
        int intervalSigDigit = (int) (interval / intervalMagnitude);
        if (intervalSigDigit > 5) {
            interval = Math.floor(10 * intervalMagnitude);
        }
        if (mYAxis.isForceLabelsEnabled()) {
            float step = (float) range / (float) (labelCount - 1);
            mYAxis.mEntryCount = labelCount;
            if (mYAxis.mEntries.length < labelCount) {
                mYAxis.mEntries = new float[labelCount];
            }
            float v = min;
            for (int i = 0; i < labelCount; i++) {
                mYAxis.mEntries[i] = v;
                v += step;
            }
        } else {
            if (mYAxis.isShowOnlyMinMaxEnabled()) {
                mYAxis.mEntryCount = 2;
                mYAxis.mEntries = new float[2];
                mYAxis.mEntries[0] = yMin;
                mYAxis.mEntries[1] = yMax;
            } else {
                double first = Math.ceil(yMin / interval) * interval;
                double last = Utils.nextUp(Math.floor(yMax / interval) * interval);
                double f;
                int i;
                int n = 0;
                for (f = first; f <= last; f += interval) {
                    ++n;
                }
                mYAxis.mEntryCount = n;
                if (mYAxis.mEntries.length < n) {
                    mYAxis.mEntries = new float[n];
                }
                f = first;
                Double d = new Double(f);
                mYAxis.mEntries[0] = (float) (d.equals(-0.0) ? Math.abs(f) : f);
                f += interval;
                for (i = 1; i < n; f += interval, ++i) {
                    mYAxis.mEntries[i] = (float) f;
                }
            }
        }
        if (interval < 1) {
            mYAxis.mDecimals = (int) Math.ceil(-Math.log10(interval));
        } else {
            mYAxis.mDecimals = 0;
        }
    }

    @Override
    public void renderAxisLabels(Canvas c) {
        if (!mYAxis.isEnabled() || !mYAxis.isDrawLabelsEnabled())
            return;
        float[] positions = new float[mYAxis.mEntryCount * 2];
        for (int i = 0; i < positions.length; i += 2) {
            positions[i + 1] = mYAxis.mEntries[i / 2];
        }
        mTrans.pointValuesToPixel(positions);
        mAxisLabelPaint.setTypeface(mYAxis.getTypeface());
        mAxisLabelPaint.setTextSize(mYAxis.getTextSize());
        mAxisLabelPaint.setColor(mYAxis.getTextColor());
        float xoffset = mYAxis.getXOffset();
        float yoffset = Utils.calcTextHeight(mAxisLabelPaint, "A") / 2.5f + mYAxis.getYOffset();
        AxisDependency dependency = mYAxis.getAxisDependency();
        YAxisLabelPosition labelPosition = mYAxis.getLabelPosition();
        float xPos = 0f;
        if (dependency == AxisDependency.LEFT) {
            if (labelPosition == YAxisLabelPosition.OUTSIDE_CHART) {
                mAxisLabelPaint.setTextAlign(Align.RIGHT);
                xPos = mViewPortHandler.offsetLeft() - xoffset;
            } else {
                mAxisLabelPaint.setTextAlign(Align.LEFT);
                xPos = mViewPortHandler.offsetLeft() + xoffset;
            }
        } else {
            if (labelPosition == YAxisLabelPosition.OUTSIDE_CHART) {
                mAxisLabelPaint.setTextAlign(Align.LEFT);
                xPos = mViewPortHandler.contentRight() + xoffset;
            } else {
                mAxisLabelPaint.setTextAlign(Align.RIGHT);
                xPos = mViewPortHandler.contentRight() - xoffset;
            }
        }
        drawYLabels(c, xPos, positions, yoffset);
    }

    @Override
    public void renderAxisLine(Canvas c) {
        if (!mYAxis.isEnabled() || !mYAxis.isDrawAxisLineEnabled())
            return;
        mAxisLinePaint.setColor(mYAxis.getAxisLineColor());
        mAxisLinePaint.setStrokeWidth(mYAxis.getAxisLineWidth());
        if (mYAxis.getAxisDependency() == AxisDependency.LEFT) {
            c.drawLine(mViewPortHandler.contentLeft(), mViewPortHandler.contentTop(), mViewPortHandler.contentLeft(), mViewPortHandler.contentBottom(), mAxisLinePaint);
        } else {
            c.drawLine(mViewPortHandler.contentRight(), mViewPortHandler.contentTop(), mViewPortHandler.contentRight(), mViewPortHandler.contentBottom(), mAxisLinePaint);
        }
    }

    protected void drawYLabels(Canvas c, float fixedPosition, float[] positions, float offset) {
        for (int i = 0; i < mYAxis.mEntryCount; i++) {
            String text = mYAxis.getFormattedLabel(i);
            if (!mYAxis.isDrawTopYLabelEntryEnabled() && i >= mYAxis.mEntryCount - 1)
                return;
            c.drawText(text, fixedPosition, positions[i * 2 + 1] + offset, mAxisLabelPaint);
        }
    }

    @Override
    public void renderGridLines(Canvas c) {
        if (!mYAxis.isEnabled())
            return;
        float[] position = new float[2];
        if (mYAxis.isDrawGridLinesEnabled()) {
            mGridPaint.setColor(mYAxis.getGridColor());
            mGridPaint.setStrokeWidth(mYAxis.getGridLineWidth());
            mGridPaint.setPathEffect(mYAxis.getGridDashPathEffect());
            Path gridLinePath = new Path();
            for (int i = 0; i < mYAxis.mEntryCount; i++) {
                position[1] = mYAxis.mEntries[i];
                mTrans.pointValuesToPixel(position);
                gridLinePath.moveTo(mViewPortHandler.offsetLeft(), position[1]);
                gridLinePath.lineTo(mViewPortHandler.contentRight(), position[1]);
                c.drawPath(gridLinePath, mGridPaint);
                gridLinePath.reset();
            }
        }
        if (mYAxis.isDrawZeroLineEnabled()) {
            position[1] = 0f;
            mTrans.pointValuesToPixel(position);
            drawZeroLine(c, mViewPortHandler.offsetLeft(), mViewPortHandler.contentRight(), position[1] - 1, position[1] - 1);
        }
    }

    protected void drawZeroLine(Canvas c, float x1, float x2, float y1, float y2) {
        mZeroLinePaint.setColor(mYAxis.getZeroLineColor());
        mZeroLinePaint.setStrokeWidth(mYAxis.getZeroLineWidth());
        Path zeroLinePath = new Path();
        zeroLinePath.moveTo(x1, y1);
        zeroLinePath.lineTo(x2, y2);
        c.drawPath(zeroLinePath, mZeroLinePaint);
    }

    @Override
    public void renderLimitLines(Canvas c) {
        List<LimitLine> limitLines = mYAxis.getLimitLines();
        if (limitLines == null || limitLines.size() <= 0)
            return;
        float[] pts = new float[2];
        Path limitLinePath = new Path();
        for (int i = 0; i < limitLines.size(); i++) {
            LimitLine l = limitLines.get(i);
            if (!l.isEnabled())
                continue;
            mLimitLinePaint.setStyle(Paint.Style.STROKE);
            mLimitLinePaint.setColor(l.getLineColor());
            mLimitLinePaint.setStrokeWidth(l.getLineWidth());
            mLimitLinePaint.setPathEffect(l.getDashPathEffect());
            pts[1] = l.getLimit();
            mTrans.pointValuesToPixel(pts);
            limitLinePath.moveTo(mViewPortHandler.contentLeft(), pts[1]);
            limitLinePath.lineTo(mViewPortHandler.contentRight(), pts[1]);
            c.drawPath(limitLinePath, mLimitLinePaint);
            limitLinePath.reset();
            String label = l.getLabel();
            if (label != null && !label.equals("")) {
                mLimitLinePaint.setStyle(l.getTextStyle());
                mLimitLinePaint.setPathEffect(null);
                mLimitLinePaint.setColor(l.getTextColor());
                mLimitLinePaint.setTypeface(l.getTypeface());
                mLimitLinePaint.setStrokeWidth(0.5f);
                mLimitLinePaint.setTextSize(l.getTextSize());
                final float labelLineHeight = Utils.calcTextHeight(mLimitLinePaint, label);
                float xOffset = Utils.convertDpToPixel(4f) + l.getXOffset();
                float yOffset = l.getLineWidth() + labelLineHeight + l.getYOffset();
                final LimitLine.LimitLabelPosition position = l.getLabelPosition();
                if (position == LimitLine.LimitLabelPosition.RIGHT_TOP) {
                    mLimitLinePaint.setTextAlign(Align.RIGHT);
                    c.drawText(label, mViewPortHandler.contentRight() - xOffset, pts[1] - yOffset + labelLineHeight, mLimitLinePaint);
                } else if (position == LimitLine.LimitLabelPosition.RIGHT_BOTTOM) {
                    mLimitLinePaint.setTextAlign(Align.RIGHT);
                    c.drawText(label, mViewPortHandler.contentRight() - xOffset, pts[1] + yOffset, mLimitLinePaint);
                } else if (position == LimitLine.LimitLabelPosition.LEFT_TOP) {
                    mLimitLinePaint.setTextAlign(Align.LEFT);
                    c.drawText(label, mViewPortHandler.contentLeft() + xOffset, pts[1] - yOffset + labelLineHeight, mLimitLinePaint);
                } else {
                    mLimitLinePaint.setTextAlign(Align.LEFT);
                    c.drawText(label, mViewPortHandler.offsetLeft() + xOffset, pts[1] + yOffset, mLimitLinePaint);
                }
            }
        }
    }
}