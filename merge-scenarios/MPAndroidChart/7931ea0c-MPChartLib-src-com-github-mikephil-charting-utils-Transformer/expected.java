package com.github.mikephil.charting.utils;

import android.graphics.Matrix;
import android.graphics.Path;
import android.graphics.RectF;
import com.github.mikephil.charting.data.BarData;
import com.github.mikephil.charting.data.CandleEntry;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.interfaces.datasets.IBarDataSet;
import com.github.mikephil.charting.interfaces.datasets.IBubbleDataSet;
import com.github.mikephil.charting.interfaces.datasets.ICandleDataSet;
import com.github.mikephil.charting.interfaces.datasets.ILineDataSet;
import com.github.mikephil.charting.interfaces.datasets.IScatterDataSet;
import java.util.List;

public class Transformer {

    protected Matrix mMatrixValueToPx = new Matrix();

    protected Matrix mMatrixOffset = new Matrix();

    protected ViewPortHandler mViewPortHandler;

    public Transformer(ViewPortHandler viewPortHandler) {
        this.mViewPortHandler = viewPortHandler;
    }

    public void prepareMatrixValuePx(float xChartMin, float deltaX, float deltaY, float yChartMin) {
        float scaleX = (float) ((mViewPortHandler.contentWidth()) / deltaX);
        float scaleY = (float) ((mViewPortHandler.contentHeight()) / deltaY);
        if (Float.isInfinite(scaleX)) {
            scaleX = 0;
        }
        if (Float.isInfinite(scaleY)) {
            scaleY = 0;
        }
        mMatrixValueToPx.reset();
        mMatrixValueToPx.postTranslate(-xChartMin, -yChartMin);
        mMatrixValueToPx.postScale(scaleX, -scaleY);
    }

    public void prepareMatrixOffset(boolean inverted) {
        mMatrixOffset.reset();
        if (!inverted)
            mMatrixOffset.postTranslate(mViewPortHandler.offsetLeft(), mViewPortHandler.getChartHeight() - mViewPortHandler.offsetBottom());
        else {
            mMatrixOffset.setTranslate(mViewPortHandler.offsetLeft(), -mViewPortHandler.offsetTop());
            mMatrixOffset.postScale(1.0f, -1.0f);
        }
    }

    public float[] generateTransformedValuesScatter(IScatterDataSet data, float phaseY) {
        float[] valuePoints = new float[data.getEntryCount() * 2];
        for (int j = 0; j < valuePoints.length; j += 2) {
            Entry e = data.getEntryForIndex(j / 2);
            if (e != null) {
                valuePoints[j] = e.getXIndex();
                valuePoints[j + 1] = e.getVal() * phaseY;
            }
        }
        getValueToPixelMatrix().mapPoints(valuePoints);
        return valuePoints;
    }

    public float[] generateTransformedValuesBubble(IBubbleDataSet data, float phaseX, float phaseY, int from, int to) {
        final int count = (int) Math.ceil(to - from) * 2;
        float[] valuePoints = new float[count];
        for (int j = 0; j < count; j += 2) {
            Entry e = data.getEntryForIndex(j / 2 + from);
            if (e != null) {
                valuePoints[j] = (float) (e.getXIndex() - from) * phaseX + from;
                valuePoints[j + 1] = e.getVal() * phaseY;
            }
        }
        getValueToPixelMatrix().mapPoints(valuePoints);
        return valuePoints;
    }

    public float[] generateTransformedValuesLine(ILineDataSet data, float phaseX, float phaseY, int from, int to) {
        final int count = (int) Math.ceil((to - from) * phaseX) * 2;
        float[] valuePoints = new float[count];
        for (int j = 0; j < count; j += 2) {
            Entry e = data.getEntryForIndex(j / 2 + from);
            if (e != null) {
                valuePoints[j] = e.getXIndex();
                valuePoints[j + 1] = e.getVal() * phaseY;
            }
        }
        getValueToPixelMatrix().mapPoints(valuePoints);
        return valuePoints;
    }

    public float[] generateTransformedValuesCandle(ICandleDataSet data, float phaseX, float phaseY, int from, int to) {
        final int count = (int) Math.ceil((to - from) * phaseX) * 2;
        float[] valuePoints = new float[count];
        for (int j = 0; j < count; j += 2) {
            CandleEntry e = data.getEntryForIndex(j / 2 + from);
            if (e != null) {
                valuePoints[j] = e.getXIndex();
                valuePoints[j + 1] = e.getHigh() * phaseY;
            }
        }
        getValueToPixelMatrix().mapPoints(valuePoints);
        return valuePoints;
    }

    public float[] generateTransformedValuesBarChart(IBarDataSet data, int dataSetIndex, BarData bd, float phaseY) {
        float[] valuePoints = new float[data.getEntryCount() * 2];
        int setCount = bd.getDataSetCount();
        float space = bd.getGroupSpace();
        for (int j = 0; j < valuePoints.length; j += 2) {
            Entry e = data.getEntryForIndex(j / 2);
            int i = e.getXIndex();
            float x = e.getXIndex() + i * (setCount - 1) + dataSetIndex + space * i + space / 2f;
            float y = e.getVal();
            valuePoints[j] = x;
            valuePoints[j + 1] = y * phaseY;
        }
        getValueToPixelMatrix().mapPoints(valuePoints);
        return valuePoints;
    }

    public float[] generateTransformedValuesHorizontalBarChart(IBarDataSet data, int dataSet, BarData bd, float phaseY) {
        float[] valuePoints = new float[data.getEntryCount() * 2];
        int setCount = bd.getDataSetCount();
        float space = bd.getGroupSpace();
        for (int j = 0; j < valuePoints.length; j += 2) {
            Entry e = data.getEntryForIndex(j / 2);
            int i = e.getXIndex();
            float x = i + i * (setCount - 1) + dataSet + space * i + space / 2f;
            float y = e.getVal();
            valuePoints[j] = y * phaseY;
            valuePoints[j + 1] = x;
        }
        getValueToPixelMatrix().mapPoints(valuePoints);
        return valuePoints;
    }

    public void pathValueToPixel(Path path) {
        path.transform(mMatrixValueToPx);
        path.transform(mViewPortHandler.getMatrixTouch());
        path.transform(mMatrixOffset);
    }

    public void pathValuesToPixel(List<Path> paths) {
        for (int i = 0; i < paths.size(); i++) {
            pathValueToPixel(paths.get(i));
        }
    }

    public void pointValuesToPixel(float[] pts) {
        mMatrixValueToPx.mapPoints(pts);
        mViewPortHandler.getMatrixTouch().mapPoints(pts);
        mMatrixOffset.mapPoints(pts);
    }

    public void rectValueToPixel(RectF r) {
        mMatrixValueToPx.mapRect(r);
        mViewPortHandler.getMatrixTouch().mapRect(r);
        mMatrixOffset.mapRect(r);
    }

    public void rectValueToPixel(RectF r, float phaseY) {
        r.top *= phaseY;
        r.bottom *= phaseY;
        mMatrixValueToPx.mapRect(r);
        mViewPortHandler.getMatrixTouch().mapRect(r);
        mMatrixOffset.mapRect(r);
    }

    public void rectValueToPixelHorizontal(RectF r) {
        mMatrixValueToPx.mapRect(r);
        mViewPortHandler.getMatrixTouch().mapRect(r);
        mMatrixOffset.mapRect(r);
    }

    public void rectValueToPixelHorizontal(RectF r, float phaseY) {
        r.left *= phaseY;
        r.right *= phaseY;
        mMatrixValueToPx.mapRect(r);
        mViewPortHandler.getMatrixTouch().mapRect(r);
        mMatrixOffset.mapRect(r);
    }

    public void rectValuesToPixel(List<RectF> rects) {
        Matrix m = getValueToPixelMatrix();
        for (int i = 0; i < rects.size(); i++) m.mapRect(rects.get(i));
    }

    public void pixelsToValue(float[] pixels) {
        Matrix tmp = new Matrix();
        mMatrixOffset.invert(tmp);
        tmp.mapPoints(pixels);
        mViewPortHandler.getMatrixTouch().invert(tmp);
        tmp.mapPoints(pixels);
        mMatrixValueToPx.invert(tmp);
        tmp.mapPoints(pixels);
    }

    public PointD getValuesByTouchPoint(float x, float y) {
        float[] pts = new float[2];
        pts[0] = x;
        pts[1] = y;
        pixelsToValue(pts);
        double xTouchVal = pts[0];
        double yTouchVal = pts[1];
        return new PointD(xTouchVal, yTouchVal);
    }

    public Matrix getValueMatrix() {
        return mMatrixValueToPx;
    }

    public Matrix getOffsetMatrix() {
        return mMatrixOffset;
    }

    private Matrix mMBuffer1 = new Matrix();

    public Matrix getValueToPixelMatrix() {
        mMBuffer1.set(mMatrixValueToPx);
        mMBuffer1.postConcat(mViewPortHandler.mMatrixTouch);
        mMBuffer1.postConcat(mMatrixOffset);
        return mMBuffer1;
    }

    private Matrix mMBuffer2 = new Matrix();

    public Matrix getPixelToValueMatrix() {
        getValueToPixelMatrix().invert(mMBuffer2);
        return mMBuffer2;
    }
}