package com.github.mikephil.charting.charts;

import android.content.Context;
import android.graphics.Canvas;
import android.graphics.Paint;
import android.graphics.PointF;
import android.graphics.PorterDuff;
import android.graphics.PorterDuffXfermode;
import android.graphics.RectF;
import android.graphics.Typeface;
import android.text.SpannableString;
import android.util.AttributeSet;
import com.github.mikephil.charting.data.DataSet;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.PieData;
import com.github.mikephil.charting.data.PieDataSet;
import com.github.mikephil.charting.highlight.Highlight;
import com.github.mikephil.charting.renderer.PieChartRenderer;
import com.github.mikephil.charting.utils.Utils;
import java.util.List;

public class PieChart extends PieRadarChartBase<PieData> {

    private RectF mCircleBox = new RectF();

    private boolean mDrawXLabels = true;

    private float[] mDrawAngles;

    private float[] mAbsoluteAngles;

    private boolean mDrawHole = true;

    private boolean mUsePercentValues = false;

    private boolean mDrawRoundedSlices = false;

    private SpannableString mCenterText = new SpannableString("");

    private float mHoleRadiusPercent = 50f;

    protected float mTransparentCircleRadiusPercent = 55f;

    private boolean mDrawCenterText = true;

    private float mCenterTextRadiusPercent = 1.f;

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
        mRenderer = new PieChartRenderer(this, mAnimator, mViewPortHandler);
    }

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        if (mDataNotSet)
            return;
        mRenderer.drawData(canvas);
        if (valuesToHighlight())
            mRenderer.drawHighlighted(canvas, mIndicesToHighlight);
        mRenderer.drawExtras(canvas);
        mRenderer.drawValues(canvas);
        mLegendRenderer.renderLegend(canvas);
        drawDescription(canvas);
        drawMarkers(canvas);
    }

    @Override
    public void calculateOffsets() {
        super.calculateOffsets();
        if (mDataNotSet)
            return;
        float diameter = getDiameter();
        float radius = diameter / 2f;
        PointF c = getCenterOffsets();
        float shift = mData.getDataSet().getSelectionShift();
        mCircleBox.set(c.x - radius + shift, c.y - radius + shift, c.x + radius - shift, c.y + radius - shift);
    }

    @Override
    protected void calcMinMax() {
        super.calcMinMax();
        calcAngles();
    }

    @Override
    protected float[] getMarkerPosition(Entry e, Highlight highlight) {
        PointF center = getCenterCircleBox();
        float r = getRadius();
        float off = r / 10f * 3.6f;
        if (isDrawHoleEnabled()) {
            off = (r - (r / 100f * getHoleRadius())) / 2f;
        }
        r -= off;
        float rotationAngle = getRotationAngle();
        int i = e.getXIndex();
        float offset = mDrawAngles[i] / 2;
        float x = (float) (r * Math.cos(Math.toRadians((rotationAngle + mAbsoluteAngles[i] - offset) * mAnimator.getPhaseY())) + center.x);
        float y = (float) (r * Math.sin(Math.toRadians((rotationAngle + mAbsoluteAngles[i] - offset) * mAnimator.getPhaseY())) + center.y);
        return new float[] { x, y };
    }

    private void calcAngles() {
        mDrawAngles = new float[mData.getYValCount()];
        mAbsoluteAngles = new float[mData.getYValCount()];
        List<PieDataSet> dataSets = mData.getDataSets();
        int cnt = 0;
        for (int i = 0; i < mData.getDataSetCount(); i++) {
            PieDataSet set = dataSets.get(i);
            List<Entry> entries = set.getYVals();
            for (int j = 0; j < entries.size(); j++) {
                mDrawAngles[cnt] = calcAngle(Math.abs(entries.get(j).getVal()));
                if (cnt == 0) {
                    mAbsoluteAngles[cnt] = mDrawAngles[cnt];
                } else {
                    mAbsoluteAngles[cnt] = mAbsoluteAngles[cnt - 1] + mDrawAngles[cnt];
                }
                cnt++;
            }
        }
    }

    public boolean needsHighlight(int xIndex, int dataSetIndex) {
        if (!valuesToHighlight() || dataSetIndex < 0)
            return false;
        for (int i = 0; i < mIndicesToHighlight.length; i++) if (mIndicesToHighlight[i].getXIndex() == xIndex && mIndicesToHighlight[i].getDataSetIndex() == dataSetIndex)
            return true;
        return false;
    }

    private float calcAngle(float value) {
        return value / mData.getYValueSum() * 360f;
    }

    @Override
    public int getIndexForAngle(float angle) {
        float a = Utils.getNormalizedAngle(angle - getRotationAngle());
        for (int i = 0; i < mAbsoluteAngles.length; i++) {
            if (mAbsoluteAngles[i] > a)
                return i;
        }
        return -1;
    }

    public int getDataSetIndexForIndex(int xIndex) {
        List<? extends DataSet<? extends Entry>> dataSets = mData.getDataSets();
        for (int i = 0; i < dataSets.size(); i++) {
            if (dataSets.get(i).getEntryForXIndex(xIndex) != null)
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

    public void setHoleColor(int color) {
        ((PieChartRenderer) mRenderer).getPaintHole().setXfermode(null);
        ((PieChartRenderer) mRenderer).getPaintHole().setColor(color);
    }

    public void setHoleColorTransparent(boolean enable) {
        if (enable) {
            ((PieChartRenderer) mRenderer).getPaintHole().setColor(0xFFFFFFFF);
            ((PieChartRenderer) mRenderer).getPaintHole().setXfermode(new PorterDuffXfermode(PorterDuff.Mode.CLEAR));
        } else {
            ((PieChartRenderer) mRenderer).getPaintHole().setXfermode(null);
        }
    }

    public boolean isHoleTransparent() {
        return ((PieChartRenderer) mRenderer).getPaintHole().getXfermode() != null;
    }

    public void setDrawHoleEnabled(boolean enabled) {
        this.mDrawHole = enabled;
    }

    public boolean isDrawHoleEnabled() {
        return mDrawHole;
    }

    public void setCenterText(SpannableString text) {
        if (text == null)
            mCenterText = new SpannableString("");
        else
            mCenterText = text;
    }

    public void setCenterText(String text) {
        setCenterText(new SpannableString(text));
    }

    public SpannableString getCenterText() {
        return mCenterText;
    }

    public void setDrawCenterText(boolean enabled) {
        this.mDrawCenterText = enabled;
    }

    public boolean isDrawCenterTextEnabled() {
        return mDrawCenterText;
    }

    @Override
    protected float getRequiredLegendOffset() {
        return mLegendRenderer.getLabelPaint().getTextSize() * 2.f;
    }

    @Override
    protected float getRequiredBaseOffset() {
        return 0;
    }

    @Override
    public float getRadius() {
        if (mCircleBox == null)
            return 0;
        else
            return Math.min(mCircleBox.width() / 2f, mCircleBox.height() / 2f);
    }

    public RectF getCircleBox() {
        return mCircleBox;
    }

    public PointF getCenterCircleBox() {
        return new PointF(mCircleBox.centerX(), mCircleBox.centerY());
    }

    public void setCenterTextTypeface(Typeface t) {
        ((PieChartRenderer) mRenderer).getPaintCenterText().setTypeface(t);
    }

    public void setCenterTextSize(float sizeDp) {
        ((PieChartRenderer) mRenderer).getPaintCenterText().setTextSize(Utils.convertDpToPixel(sizeDp));
    }

    public void setCenterTextSizePixels(float sizePixels) {
        ((PieChartRenderer) mRenderer).getPaintCenterText().setTextSize(sizePixels);
    }

    public void setCenterTextColor(int color) {
        ((PieChartRenderer) mRenderer).getPaintCenterText().setColor(color);
    }

    public void setHoleRadius(final float percent) {
        mHoleRadiusPercent = percent;
    }

    public float getHoleRadius() {
        return mHoleRadiusPercent;
    }

    public void setTransparentCircleColor(int color) {
        Paint p = ((PieChartRenderer) mRenderer).getPaintTransparentCircle();
        int alpha = p.getAlpha();
        p.setColor(color);
        p.setAlpha(alpha);
    }

    public void setTransparentCircleRadius(final float percent) {
        mTransparentCircleRadiusPercent = percent;
    }

    public float getTransparentCircleRadius() {
        return mTransparentCircleRadiusPercent;
    }

    public void setTransparentCircleAlpha(int alpha) {
        ((PieChartRenderer) mRenderer).getPaintTransparentCircle().setAlpha(alpha);
    }

    public void setDrawSliceText(boolean enabled) {
        mDrawXLabels = enabled;
    }

    public boolean isDrawSliceTextEnabled() {
        return mDrawXLabels;
    }

    public boolean isDrawRoundedSlicesEnabled() {
        return mDrawRoundedSlices;
    }

    public void setUsePercentValues(boolean enabled) {
        mUsePercentValues = enabled;
    }

    public boolean isUsePercentValuesEnabled() {
        return mUsePercentValues;
    }

    public void setCenterTextRadiusPercent(float percent) {
        mCenterTextRadiusPercent = percent;
    }

    public float getCenterTextRadiusPercent() {
        return mCenterTextRadiusPercent;
    }

    @Override
    protected void onDetachedFromWindow() {
        if (mRenderer != null && mRenderer instanceof PieChartRenderer) {
            ((PieChartRenderer) mRenderer).releaseBitmap();
        }
        super.onDetachedFromWindow();
    }
}