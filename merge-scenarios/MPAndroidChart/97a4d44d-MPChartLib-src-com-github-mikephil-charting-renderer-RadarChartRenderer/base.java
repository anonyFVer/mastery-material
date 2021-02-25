package com.github.mikephil.charting.renderer;

import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Path;
import android.graphics.PointF;
import com.github.mikephil.charting.animation.ChartAnimator;
import com.github.mikephil.charting.charts.RadarChart;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.RadarData;
import com.github.mikephil.charting.data.RadarDataSet;
import com.github.mikephil.charting.utils.Highlight;
import com.github.mikephil.charting.utils.Utils;
import com.github.mikephil.charting.utils.ViewPortHandler;
import java.util.List;

public class RadarChartRenderer extends DataRenderer {

    protected RadarChart mChart;

    protected Paint mWebPaint;

    public RadarChartRenderer(RadarChart chart, ChartAnimator animator, ViewPortHandler viewPortHandler) {
        super(animator, viewPortHandler);
        mChart = chart;
        mHighlightPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mHighlightPaint.setStyle(Paint.Style.STROKE);
        mHighlightPaint.setStrokeWidth(2f);
        mHighlightPaint.setColor(Color.rgb(255, 187, 115));
        mWebPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mWebPaint.setStyle(Paint.Style.STROKE);
    }

    public Paint getWebPaint() {
        return mWebPaint;
    }

    @Override
    public void initBuffers() {
    }

    @Override
    public void drawData(Canvas c) {
        RadarData radarData = mChart.getData();
        for (RadarDataSet set : radarData.getDataSets()) {
            if (set.isVisible())
                drawDataSet(c, set);
        }
    }

    protected void drawDataSet(Canvas c, RadarDataSet dataSet) {
        float sliceangle = mChart.getSliceAngle();
        float factor = mChart.getFactor();
        PointF center = mChart.getCenterOffsets();
        List<Entry> entries = dataSet.getYVals();
        Path surface = new Path();
        for (int j = 0; j < entries.size(); j++) {
            mRenderPaint.setColor(dataSet.getColor(j));
            Entry e = entries.get(j);
            PointF p = Utils.getPosition(center, (e.getVal() - mChart.getYChartMin()) * factor, sliceangle * j + mChart.getRotationAngle());
            if (j == 0)
                surface.moveTo(p.x, p.y);
            else
                surface.lineTo(p.x, p.y);
        }
        surface.close();
        if (dataSet.isDrawFilledEnabled()) {
            mRenderPaint.setStyle(Paint.Style.FILL);
            mRenderPaint.setAlpha(dataSet.getFillAlpha());
            c.drawPath(surface, mRenderPaint);
            mRenderPaint.setAlpha(255);
        }
        mRenderPaint.setStrokeWidth(dataSet.getLineWidth());
        mRenderPaint.setStyle(Paint.Style.STROKE);
        if (!dataSet.isDrawFilledEnabled() || dataSet.getFillAlpha() < 255)
            c.drawPath(surface, mRenderPaint);
    }

    @Override
    public void drawValues(Canvas c) {
        float sliceangle = mChart.getSliceAngle();
        float factor = mChart.getFactor();
        PointF center = mChart.getCenterOffsets();
        float yoffset = Utils.convertDpToPixel(5f);
        for (int i = 0; i < mChart.getData().getDataSetCount(); i++) {
            RadarDataSet dataSet = mChart.getData().getDataSetByIndex(i);
            if (!dataSet.isDrawValuesEnabled())
                continue;
            applyValueTextStyle(dataSet);
            List<Entry> entries = dataSet.getYVals();
            for (int j = 0; j < entries.size(); j++) {
                Entry e = entries.get(j);
                PointF p = Utils.getPosition(center, (e.getVal() - mChart.getYChartMin()) * factor, sliceangle * j + mChart.getRotationAngle());
                c.drawText(dataSet.getValueFormatter().getFormattedValue(e.getVal()), p.x, p.y - yoffset, mValuePaint);
            }
        }
    }

    @Override
    public void drawExtras(Canvas c) {
        drawWeb(c);
    }

    protected void drawWeb(Canvas c) {
        float sliceangle = mChart.getSliceAngle();
        float factor = mChart.getFactor();
        float rotationangle = mChart.getRotationAngle();
        PointF center = mChart.getCenterOffsets();
        mWebPaint.setStrokeWidth(mChart.getWebLineWidth());
        mWebPaint.setColor(mChart.getWebColor());
        mWebPaint.setAlpha(mChart.getWebAlpha());
        for (int i = 0; i < mChart.getData().getXValCount(); i++) {
            PointF p = Utils.getPosition(center, mChart.getYRange() * factor, sliceangle * i + rotationangle);
            c.drawLine(center.x, center.y, p.x, p.y, mWebPaint);
        }
        mWebPaint.setStrokeWidth(mChart.getWebLineWidthInner());
        mWebPaint.setColor(mChart.getWebColorInner());
        mWebPaint.setAlpha(mChart.getWebAlpha());
        int labelCount = mChart.getYAxis().mEntryCount;
        for (int j = 0; j < labelCount; j++) {
            for (int i = 0; i < mChart.getData().getXValCount(); i++) {
                float r = (mChart.getYAxis().mEntries[j] - mChart.getYChartMin()) * factor;
                PointF p1 = Utils.getPosition(center, r, sliceangle * i + rotationangle);
                PointF p2 = Utils.getPosition(center, r, sliceangle * (i + 1) + rotationangle);
                c.drawLine(p1.x, p1.y, p2.x, p2.y, mWebPaint);
            }
        }
    }

    @Override
    public void drawHighlighted(Canvas c, Highlight[] indices) {
        float sliceangle = mChart.getSliceAngle();
        float factor = mChart.getFactor();
        PointF center = mChart.getCenterOffsets();
        for (int i = 0; i < indices.length; i++) {
            RadarDataSet set = mChart.getData().getDataSetByIndex(indices[i].getDataSetIndex());
            if (set == null || !set.isHighlightEnabled())
                continue;
            mHighlightPaint.setColor(set.getHighLightColor());
            int xIndex = indices[i].getXIndex();
            Entry e = set.getEntryForXIndex(xIndex);
            if (e == null || e.getXIndex() != xIndex)
                continue;
            int j = set.getEntryPosition(e);
            float y = (e.getVal() - mChart.getYChartMin());
            PointF p = Utils.getPosition(center, y * factor, sliceangle * j + mChart.getRotationAngle());
            float[] pts = new float[] { p.x, 0, p.x, mViewPortHandler.getChartHeight(), 0, p.y, mViewPortHandler.getChartWidth(), p.y };
            c.drawLines(pts, mHighlightPaint);
        }
    }
}