package com.github.mikephil.charting.renderer;

import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Paint.Align;
import android.graphics.PointF;
import android.graphics.RectF;
import android.util.Log;
import com.github.mikephil.charting.animation.ChartAnimator;
import com.github.mikephil.charting.charts.PieChart;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.PieData;
import com.github.mikephil.charting.data.PieDataSet;
import com.github.mikephil.charting.utils.Highlight;
import com.github.mikephil.charting.utils.Utils;
import com.github.mikephil.charting.utils.ViewPortHandler;
import java.util.List;

public class PieChartRenderer extends DataRenderer {

    protected PieChart mChart;

    private Paint mHolePaint;

    private Paint mCenterTextPaint;

    protected Bitmap mDrawBitmap;

    protected Canvas mBitmapCanvas;

    public PieChartRenderer(PieChart chart, ChartAnimator animator, ViewPortHandler viewPortHandler) {
        super(animator, viewPortHandler);
        mChart = chart;
        mHolePaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mHolePaint.setColor(Color.WHITE);
        mCenterTextPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mCenterTextPaint.setColor(Color.BLACK);
        mCenterTextPaint.setTextSize(Utils.convertDpToPixel(12f));
        mCenterTextPaint.setTextAlign(Align.CENTER);
        mValuePaint.setTextSize(Utils.convertDpToPixel(13f));
        mValuePaint.setColor(Color.WHITE);
        mValuePaint.setTextAlign(Align.CENTER);
    }

    public Paint getPaintHole() {
        return mHolePaint;
    }

    public Paint getPaintCenterText() {
        return mCenterTextPaint;
    }

    @Override
    public void initBuffers() {
    }

    @Override
    public void drawData(Canvas c) {
        if (mDrawBitmap == null || mDrawBitmap.getWidth() != mViewPortHandler.getChartWidth() || mDrawBitmap.getHeight() != mViewPortHandler.getChartHeight()) {
            mDrawBitmap = Bitmap.createBitmap((int) mViewPortHandler.getChartWidth(), (int) mViewPortHandler.getChartHeight(), Bitmap.Config.ARGB_4444);
            mBitmapCanvas = new Canvas(mDrawBitmap);
        }
        mDrawBitmap.eraseColor(Color.TRANSPARENT);
        PieData pieData = mChart.getData();
        for (PieDataSet set : pieData.getDataSets()) {
            if (set.isVisible())
                drawDataSet(c, set);
        }
        c.drawBitmap(mDrawBitmap, 0, 0, mRenderPaint);
    }

    protected void drawDataSet(Canvas c, PieDataSet dataSet) {
        float angle = mChart.getRotationAngle();
        int cnt = 0;
        List<Entry> entries = dataSet.getYVals();
        float[] drawAngles = mChart.getDrawAngles();
        for (int j = 0; j < entries.size(); j++) {
            float newangle = drawAngles[cnt];
            float sliceSpace = dataSet.getSliceSpace();
            Entry e = entries.get(j);
            if ((Math.abs(e.getVal()) > 0.000001)) {
                if (!mChart.needsHighlight(e.getXIndex(), mChart.getData().getIndexOfDataSet(dataSet))) {
                    mRenderPaint.setColor(dataSet.getColor(j));
                    mBitmapCanvas.drawArc(mChart.getCircleBox(), angle + sliceSpace / 2f, newangle * mAnimator.getPhaseY() - sliceSpace / 2f, true, mRenderPaint);
                }
            }
            angle += newangle * mAnimator.getPhaseX();
            cnt++;
        }
    }

    @Override
    public void drawValues(Canvas c) {
        PointF center = mChart.getCenterCircleBox();
        float r = mChart.getRadius();
        float rotationAngle = mChart.getRotationAngle();
        float[] drawAngles = mChart.getDrawAngles();
        float[] absoluteAngles = mChart.getAbsoluteAngles();
        float off = r / 3f;
        if (mChart.isDrawHoleEnabled()) {
            off = (r - (r / 100f * mChart.getHoleRadius())) / 2f;
        }
        r -= off;
        PieData data = mChart.getData();
        List<PieDataSet> dataSets = data.getDataSets();
        boolean drawXVals = mChart.isDrawSliceTextEnabled();
        int cnt = 0;
        for (int i = 0; i < dataSets.size(); i++) {
            PieDataSet dataSet = dataSets.get(i);
            if (!dataSet.isDrawValuesEnabled() && !drawXVals)
                continue;
            applyValueTextStyle(dataSet);
            List<Entry> entries = dataSet.getYVals();
            for (int j = 0, maxEntry = Math.min((int) Math.ceil(entries.size() * mAnimator.getPhaseX()), entries.size()); j < maxEntry; j++) {
                float offset = drawAngles[cnt] / 2;
                float x = (float) (r * Math.cos(Math.toRadians((rotationAngle + absoluteAngles[cnt] - offset) * mAnimator.getPhaseY())) + center.x);
                float y = (float) (r * Math.sin(Math.toRadians((rotationAngle + absoluteAngles[cnt] - offset) * mAnimator.getPhaseY())) + center.y);
                float value = mChart.isUsePercentValuesEnabled() ? entries.get(j).getVal() / mChart.getYValueSum() * 100f : entries.get(j).getVal();
                String val = dataSet.getValueFormatter().getFormattedValue(value);
                boolean drawYVals = dataSet.isDrawValuesEnabled();
                if (drawXVals && drawYVals) {
                    float lineHeight = (mValuePaint.ascent() + mValuePaint.descent()) * 1.6f;
                    y -= lineHeight / 2;
                    c.drawText(val, x, y, mValuePaint);
                    if (j < data.getXValCount())
                        c.drawText(data.getXVals().get(j), x, y + lineHeight, mValuePaint);
                } else if (drawXVals && !drawYVals) {
                    if (j < data.getXValCount())
                        c.drawText(data.getXVals().get(j), x, y, mValuePaint);
                } else if (!drawXVals && drawYVals) {
                    c.drawText(val, x, y, mValuePaint);
                }
                cnt++;
            }
        }
    }

    @Override
    public void drawExtras(Canvas c) {
        drawHole(c);
        drawCenterText(c);
    }

    protected void drawHole(Canvas c) {
        if (mChart.isDrawHoleEnabled()) {
            float transparentCircleRadius = mChart.getTransparentCircleRadius();
            float holeRadius = mChart.getHoleRadius();
            float radius = mChart.getRadius();
            PointF center = mChart.getCenterCircleBox();
            int color = mHolePaint.getColor();
            mBitmapCanvas.drawCircle(center.x, center.y, radius / 100 * holeRadius, mHolePaint);
            if (transparentCircleRadius > holeRadius) {
                mHolePaint.setColor(color & 0x60FFFFFF);
                mBitmapCanvas.drawCircle(center.x, center.y, radius / 100 * transparentCircleRadius, mHolePaint);
                mHolePaint.setColor(color);
            }
        }
    }

    protected void drawCenterText(Canvas c) {
        String centerText = mChart.getCenterText();
        if (mChart.isDrawCenterTextEnabled() && centerText != null) {
            PointF center = mChart.getCenterCircleBox();
            String[] lines = centerText.split("\n");
            float maxlineheight = 0f;
            for (String line : lines) {
                float curHeight = Utils.calcTextHeight(mCenterTextPaint, line);
                if (curHeight > maxlineheight)
                    maxlineheight = curHeight;
            }
            float linespacing = maxlineheight * 0.25f;
            float totalheight = maxlineheight * lines.length - linespacing * (lines.length - 1);
            int cnt = lines.length;
            float y = center.y;
            for (int i = 0; i < lines.length; i++) {
                String line = lines[lines.length - i - 1];
                c.drawText(line, center.x, y + maxlineheight * cnt - totalheight / 2f, mCenterTextPaint);
                cnt--;
                y -= linespacing;
            }
        }
    }

    @Override
    public void drawHighlighted(Canvas c, Highlight[] indices) {
        float rotationAngle = mChart.getRotationAngle();
        float angle = 0f;
        float[] drawAngles = mChart.getDrawAngles();
        float[] absoluteAngles = mChart.getAbsoluteAngles();
        for (int i = 0; i < indices.length; i++) {
            int xIndex = indices[i].getXIndex();
            if (xIndex >= drawAngles.length)
                continue;
            if (xIndex == 0)
                angle = rotationAngle;
            else
                angle = rotationAngle + absoluteAngles[xIndex - 1];
            angle *= mAnimator.getPhaseY();
            float sliceDegrees = drawAngles[xIndex];
            PieDataSet set = mChart.getData().getDataSetByIndex(indices[i].getDataSetIndex());
            if (set == null)
                continue;
            float shift = set.getSelectionShift();
            RectF circleBox = mChart.getCircleBox();
            RectF highlighted = new RectF(circleBox.left - shift, circleBox.top - shift, circleBox.right + shift, circleBox.bottom + shift);
            mRenderPaint.setColor(set.getColor(xIndex));
            mBitmapCanvas.drawArc(highlighted, angle + set.getSliceSpace() / 2f, sliceDegrees * mAnimator.getPhaseY() - set.getSliceSpace() / 2f, true, mRenderPaint);
        }
    }
}