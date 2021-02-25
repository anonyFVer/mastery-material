package com.github.mikephil.charting.renderer;

import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Path;
import com.github.mikephil.charting.animation.ChartAnimator;
import com.github.mikephil.charting.buffer.CircleBuffer;
import com.github.mikephil.charting.buffer.LineBuffer;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.LineData;
import com.github.mikephil.charting.data.LineDataSet;
import com.github.mikephil.charting.interfaces.LineDataProvider;
import com.github.mikephil.charting.utils.Highlight;
import com.github.mikephil.charting.utils.Transformer;
import com.github.mikephil.charting.utils.ViewPortHandler;
import java.util.List;

public class LineChartRenderer extends DataRenderer {

    protected LineDataProvider mChart;

    protected Paint mCirclePaintInner;

    protected Bitmap mPathBitmap;

    protected Canvas mBitmapCanvas;

    protected Path cubicPath = new Path();

    protected Path cubicFillPath = new Path();

    protected LineBuffer[] mLineBuffers;

    protected CircleBuffer[] mCircleBuffers;

    public LineChartRenderer(LineDataProvider chart, ChartAnimator animator, ViewPortHandler viewPortHandler) {
        super(animator, viewPortHandler);
        mChart = chart;
        mCirclePaintInner = new Paint(Paint.ANTI_ALIAS_FLAG);
        mCirclePaintInner.setStyle(Paint.Style.FILL);
        mCirclePaintInner.setColor(Color.WHITE);
    }

    @Override
    public void initBuffers() {
        LineData lineData = mChart.getLineData();
        mLineBuffers = new LineBuffer[lineData.getDataSetCount()];
        mCircleBuffers = new CircleBuffer[lineData.getDataSetCount()];
        for (int i = 0; i < mLineBuffers.length; i++) {
            LineDataSet set = lineData.getDataSetByIndex(i);
            mLineBuffers[i] = new LineBuffer(set.getEntryCount() * 4 - 4);
            mCircleBuffers[i] = new CircleBuffer(set.getEntryCount() * 2);
        }
    }

    @Override
    public void drawData(Canvas c) {
        if (mPathBitmap == null || (mPathBitmap.getWidth() != (int) mViewPortHandler.getChartWidth()) || (mPathBitmap.getHeight() != (int) mViewPortHandler.getChartHeight())) {
            mPathBitmap = Bitmap.createBitmap((int) mViewPortHandler.getChartWidth(), (int) mViewPortHandler.getChartHeight(), Bitmap.Config.ARGB_4444);
            mBitmapCanvas = new Canvas(mPathBitmap);
        }
        mPathBitmap.eraseColor(Color.TRANSPARENT);
        LineData lineData = mChart.getLineData();
        for (LineDataSet set : lineData.getDataSets()) {
            if (set.isVisible())
                drawDataSet(c, set);
        }
        c.drawBitmap(mPathBitmap, 0, 0, mRenderPaint);
    }

    protected void drawDataSet(Canvas c, LineDataSet dataSet) {
        List<Entry> entries = dataSet.getYVals();
        if (entries.size() < 1)
            return;
        calcXBounds(mChart.getTransformer(dataSet.getAxisDependency()));
        mRenderPaint.setStrokeWidth(dataSet.getLineWidth());
        mRenderPaint.setPathEffect(dataSet.getDashPathEffect());
        if (dataSet.isDrawCubicEnabled()) {
            drawCubic(c, dataSet, entries);
        } else {
            drawLinear(c, dataSet, entries);
        }
        mRenderPaint.setPathEffect(null);
    }

    protected void drawCubic(Canvas c, LineDataSet dataSet, List<Entry> entries) {
        Transformer trans = mChart.getTransformer(dataSet.getAxisDependency());
        Entry entryFrom = dataSet.getEntryForXIndex(mMinX);
        Entry entryTo = dataSet.getEntryForXIndex(mMaxX);
        int minx = dataSet.getEntryPosition(entryFrom);
        int maxx = Math.min(dataSet.getEntryPosition(entryTo) + 1, entries.size());
        float phaseX = mAnimator.getPhaseX();
        float phaseY = mAnimator.getPhaseY();
        float intensity = dataSet.getCubicIntensity();
        cubicPath.reset();
        int size = (int) Math.ceil((maxx - minx) * phaseX + minx);
        minx = Math.max(minx - 2, 0);
        size = Math.min(size + 2, entries.size());
        if (size - minx >= 2) {
            float prevDx = 0f;
            float prevDy = 0f;
            float curDx = 0f;
            float curDy = 0f;
            Entry cur = entries.get(minx);
            Entry next = entries.get(minx + 1);
            Entry prev = entries.get(minx);
            Entry prevPrev = entries.get(minx);
            cubicPath.moveTo(cur.getXIndex(), cur.getVal() * phaseY);
            prevDx = (next.getXIndex() - cur.getXIndex()) * intensity;
            prevDy = (next.getVal() - cur.getVal()) * intensity;
            cur = entries.get(1);
            next = entries.get((entries.size() > 2) ? 2 : 1);
            curDx = (next.getXIndex() - prev.getXIndex()) * intensity;
            curDy = (next.getVal() - prev.getVal()) * intensity;
            cubicPath.cubicTo(prev.getXIndex() + prevDx, (prev.getVal() + prevDy) * phaseY, cur.getXIndex() - curDx, (cur.getVal() - curDy) * phaseY, cur.getXIndex(), cur.getVal() * phaseY);
            for (int j = minx + 2; j < size - 1; j++) {
                prevPrev = entries.get(j - 2);
                prev = entries.get(j - 1);
                cur = entries.get(j);
                next = entries.get(j + 1);
                prevDx = (cur.getXIndex() - prevPrev.getXIndex()) * intensity;
                prevDy = (cur.getVal() - prevPrev.getVal()) * intensity;
                curDx = (next.getXIndex() - prev.getXIndex()) * intensity;
                curDy = (next.getVal() - prev.getVal()) * intensity;
                cubicPath.cubicTo(prev.getXIndex() + prevDx, (prev.getVal() + prevDy) * phaseY, cur.getXIndex() - curDx, (cur.getVal() - curDy) * phaseY, cur.getXIndex(), cur.getVal() * phaseY);
            }
            if (size > entries.size() - 1) {
                cur = entries.get(entries.size() - 1);
                prev = entries.get(entries.size() - 2);
                prevPrev = entries.get((entries.size() >= 3) ? entries.size() - 3 : entries.size() - 2);
                next = cur;
                prevDx = (cur.getXIndex() - prevPrev.getXIndex()) * intensity;
                prevDy = (cur.getVal() - prevPrev.getVal()) * intensity;
                curDx = (next.getXIndex() - prev.getXIndex()) * intensity;
                curDy = (next.getVal() - prev.getVal()) * intensity;
                cubicPath.cubicTo(prev.getXIndex() + prevDx, (prev.getVal() + prevDy) * phaseY, cur.getXIndex() - curDx, (cur.getVal() - curDy) * phaseY, cur.getXIndex(), cur.getVal() * phaseY);
            }
        }
        if (dataSet.isDrawFilledEnabled()) {
            cubicFillPath.reset();
            cubicFillPath.addPath(cubicPath);
            drawCubicFill(mBitmapCanvas, dataSet, cubicFillPath, trans, minx, size);
        }
        mRenderPaint.setColor(dataSet.getColor());
        mRenderPaint.setStyle(Paint.Style.STROKE);
        trans.pathValueToPixel(cubicPath);
        mBitmapCanvas.drawPath(cubicPath, mRenderPaint);
        mRenderPaint.setPathEffect(null);
    }

    protected void drawCubicFill(Canvas c, LineDataSet dataSet, Path spline, Transformer trans, int from, int to) {
        float fillMin = mChart.getFillFormatter().getFillLinePosition(dataSet, mChart.getLineData(), mChart.getYChartMax(), mChart.getYChartMin());
        spline.lineTo(to - 1, fillMin);
        spline.lineTo(from, fillMin);
        spline.close();
        mRenderPaint.setStyle(Paint.Style.FILL);
        mRenderPaint.setColor(dataSet.getFillColor());
        mRenderPaint.setAlpha(dataSet.getFillAlpha());
        trans.pathValueToPixel(spline);
        mBitmapCanvas.drawPath(spline, mRenderPaint);
        mRenderPaint.setAlpha(255);
    }

    protected void drawLinear(Canvas c, LineDataSet dataSet, List<Entry> entries) {
        int dataSetIndex = mChart.getLineData().getIndexOfDataSet(dataSet);
        Transformer trans = mChart.getTransformer(dataSet.getAxisDependency());
        float phaseX = mAnimator.getPhaseX();
        float phaseY = mAnimator.getPhaseY();
        mRenderPaint.setStyle(Paint.Style.STROKE);
        Canvas canvas = null;
        if (dataSet.isDashedLineEnabled()) {
            canvas = mBitmapCanvas;
        } else {
            canvas = c;
        }
        Entry entryFrom = dataSet.getEntryForXIndex(mMinX);
        Entry entryTo = dataSet.getEntryForXIndex(mMaxX);
        int minx = dataSet.getEntryPosition(entryFrom);
        int maxx = Math.min(dataSet.getEntryPosition(entryTo) + 1, entries.size());
        int range = (maxx - minx) * 4 - 4;
        LineBuffer buffer = mLineBuffers[dataSetIndex];
        buffer.setPhases(phaseX, phaseY);
        buffer.limitFrom(minx);
        buffer.limitTo(maxx);
        buffer.feed(entries);
        trans.pointValuesToPixel(buffer.buffer);
        if (dataSet.getColors().size() > 1) {
            for (int j = 0; j < range; j += 4) {
                if (!mViewPortHandler.isInBoundsRight(buffer.buffer[j]))
                    break;
                if (!mViewPortHandler.isInBoundsLeft(buffer.buffer[j + 2]) || (!mViewPortHandler.isInBoundsTop(buffer.buffer[j + 1]) && !mViewPortHandler.isInBoundsBottom(buffer.buffer[j + 3])) || (!mViewPortHandler.isInBoundsTop(buffer.buffer[j + 1]) && !mViewPortHandler.isInBoundsBottom(buffer.buffer[j + 3])))
                    continue;
                mRenderPaint.setColor(dataSet.getColor(j / 4 + minx));
                canvas.drawLine(buffer.buffer[j], buffer.buffer[j + 1], buffer.buffer[j + 2], buffer.buffer[j + 3], mRenderPaint);
            }
        } else {
            mRenderPaint.setColor(dataSet.getColor());
            canvas.drawLines(buffer.buffer, 0, range, mRenderPaint);
        }
        mRenderPaint.setPathEffect(null);
        if (dataSet.isDrawFilledEnabled() && entries.size() > 0) {
            drawLinearFill(c, dataSet, entries, minx, maxx, trans);
        }
    }

    protected void drawLinearFill(Canvas c, LineDataSet dataSet, List<Entry> entries, int minx, int maxx, Transformer trans) {
        mRenderPaint.setStyle(Paint.Style.FILL);
        mRenderPaint.setColor(dataSet.getFillColor());
        mRenderPaint.setAlpha(dataSet.getFillAlpha());
        Path filled = generateFilledPath(entries, mChart.getFillFormatter().getFillLinePosition(dataSet, mChart.getLineData(), mChart.getYChartMax(), mChart.getYChartMin()), minx, maxx);
        trans.pathValueToPixel(filled);
        c.drawPath(filled, mRenderPaint);
        mRenderPaint.setAlpha(255);
    }

    private Path generateFilledPath(List<Entry> entries, float fillMin, int from, int to) {
        float phaseX = mAnimator.getPhaseX();
        float phaseY = mAnimator.getPhaseY();
        Path filled = new Path();
        filled.moveTo(entries.get(from).getXIndex(), fillMin);
        filled.lineTo(entries.get(from).getXIndex(), entries.get(from).getVal() * phaseY);
        for (int x = from + 1, count = (int) Math.ceil((to - from) * phaseX + from); x < count; x++) {
            Entry e = entries.get(x);
            filled.lineTo(e.getXIndex(), e.getVal() * phaseY);
        }
        filled.lineTo(entries.get(Math.max(Math.min((int) Math.ceil((to - from) * phaseX + from) - 1, entries.size() - 1), 0)).getXIndex(), fillMin);
        filled.close();
        return filled;
    }

    @Override
    public void drawValues(Canvas c) {
        if (mChart.getLineData().getYValCount() < mChart.getMaxVisibleCount() * mViewPortHandler.getScaleX()) {
            List<LineDataSet> dataSets = mChart.getLineData().getDataSets();
            for (int i = 0; i < dataSets.size(); i++) {
                LineDataSet dataSet = dataSets.get(i);
                if (!dataSet.isDrawValuesEnabled())
                    continue;
                applyValueTextStyle(dataSet);
                Transformer trans = mChart.getTransformer(dataSet.getAxisDependency());
                int valOffset = (int) (dataSet.getCircleSize() * 1.75f);
                if (!dataSet.isDrawCirclesEnabled())
                    valOffset = valOffset / 2;
                List<Entry> entries = dataSet.getYVals();
                Entry entryFrom = dataSet.getEntryForXIndex(mMinX);
                Entry entryTo = dataSet.getEntryForXIndex(mMaxX);
                int minx = dataSet.getEntryPosition(entryFrom);
                if (minx < 0)
                    minx = 0;
                int maxx = Math.min(dataSet.getEntryPosition(entryTo) + 1, entries.size());
                float[] positions = trans.generateTransformedValuesLine(entries, mAnimator.getPhaseX(), mAnimator.getPhaseY(), minx, maxx);
                for (int j = 0; j < positions.length; j += 2) {
                    float x = positions[j];
                    float y = positions[j + 1];
                    if (!mViewPortHandler.isInBoundsRight(x))
                        break;
                    if (!mViewPortHandler.isInBoundsLeft(x) || !mViewPortHandler.isInBoundsY(y))
                        continue;
                    float val = entries.get(j / 2 + minx).getVal();
                    c.drawText(dataSet.getValueFormatter().getFormattedValue(val), x, y - valOffset, mValuePaint);
                }
            }
        }
    }

    @Override
    public void drawExtras(Canvas c) {
        drawCircles(c);
    }

    protected void drawCircles(Canvas c) {
        mRenderPaint.setStyle(Paint.Style.FILL);
        float phaseX = mAnimator.getPhaseX();
        float phaseY = mAnimator.getPhaseY();
        List<LineDataSet> dataSets = mChart.getLineData().getDataSets();
        for (int i = 0; i < dataSets.size(); i++) {
            LineDataSet dataSet = dataSets.get(i);
            if (!dataSet.isVisible() || !dataSet.isDrawCirclesEnabled())
                continue;
            mCirclePaintInner.setColor(dataSet.getCircleHoleColor());
            Transformer trans = mChart.getTransformer(dataSet.getAxisDependency());
            List<Entry> entries = dataSet.getYVals();
            Entry entryFrom = dataSet.getEntryForXIndex((mMinX < 0) ? 0 : mMinX);
            Entry entryTo = dataSet.getEntryForXIndex(mMaxX);
            int minx = dataSet.getEntryPosition(entryFrom);
            int maxx = Math.min(dataSet.getEntryPosition(entryTo) + 1, entries.size());
            CircleBuffer buffer = mCircleBuffers[i];
            buffer.setPhases(phaseX, phaseY);
            buffer.limitFrom(minx);
            buffer.limitTo(maxx);
            buffer.feed(entries);
            trans.pointValuesToPixel(buffer.buffer);
            float halfsize = dataSet.getCircleSize() / 2f;
            for (int j = 0, count = (int) Math.ceil((maxx - minx) * phaseX + minx) * 2; j < count; j += 2) {
                float x = buffer.buffer[j];
                float y = buffer.buffer[j + 1];
                if (!mViewPortHandler.isInBoundsRight(x))
                    break;
                if (!mViewPortHandler.isInBoundsLeft(x) || !mViewPortHandler.isInBoundsY(y))
                    continue;
                int circleColor = dataSet.getCircleColor(j / 2 + minx);
                mRenderPaint.setColor(circleColor);
                c.drawCircle(x, y, dataSet.getCircleSize(), mRenderPaint);
                if (dataSet.isDrawCircleHoleEnabled() && circleColor != mCirclePaintInner.getColor())
                    c.drawCircle(x, y, halfsize, mCirclePaintInner);
            }
        }
    }

    @Override
    public void drawHighlighted(Canvas c, Highlight[] indices) {
        for (int i = 0; i < indices.length; i++) {
            LineDataSet set = mChart.getLineData().getDataSetByIndex(indices[i].getDataSetIndex());
            if (set == null)
                continue;
            mHighlightPaint.setColor(set.getHighLightColor());
            int xIndex = indices[i].getXIndex();
            if (xIndex > mChart.getXChartMax() * mAnimator.getPhaseX())
                continue;
            float y = set.getYValForXIndex(xIndex) * mAnimator.getPhaseY();
            float[] pts = new float[] { xIndex, mChart.getYChartMax(), xIndex, mChart.getYChartMin(), 0, y, mChart.getXChartMax(), y };
            mChart.getTransformer(set.getAxisDependency()).pointValuesToPixel(pts);
            c.drawLines(pts, mHighlightPaint);
        }
    }
}