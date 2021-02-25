package com.github.mikephil.charting.utils;

import android.graphics.Matrix;
import android.graphics.PointF;
import android.graphics.RectF;
import android.util.Log;
import android.view.View;

public class ViewPortHandler {

    protected final Matrix mMatrixTouch = new Matrix();

    protected RectF mContentRect = new RectF();

    protected float mChartWidth = 0f;

    protected float mChartHeight = 0f;

    private float mMinScaleY = 1f;

    private float mMinScaleX = 1f;

    private float mScaleX = 1f;

    private float mScaleY = 1f;

    private float mTransOffsetX = 0f;

    private float mTransOffsetY = 0f;

    public ViewPortHandler() {
    }

    public void setChartDimens(float width, float height) {
        mChartHeight = height;
        mChartWidth = width;
        if (mContentRect.width() <= 0 || mContentRect.height() <= 0)
            mContentRect.set(0, 0, width, height);
    }

    public boolean hasChartDimens() {
        if (mChartHeight > 0 && mChartWidth > 0)
            return true;
        else
            return false;
    }

    public void restrainViewPort(float offsetLeft, float offsetTop, float offsetRight, float offsetBottom) {
        mContentRect.set(offsetLeft, offsetTop, mChartWidth - offsetRight, mChartHeight - offsetBottom);
    }

    public float offsetLeft() {
        return mContentRect.left;
    }

    public float offsetRight() {
        return mChartWidth - mContentRect.right;
    }

    public float offsetTop() {
        return mContentRect.top;
    }

    public float offsetBottom() {
        return mChartHeight - mContentRect.bottom;
    }

    public float contentTop() {
        return mContentRect.top;
    }

    public float contentLeft() {
        return mContentRect.left;
    }

    public float contentRight() {
        return mContentRect.right;
    }

    public float contentBottom() {
        return mContentRect.bottom;
    }

    public float contentWidth() {
        return mContentRect.width();
    }

    public float contentHeight() {
        return mContentRect.height();
    }

    public RectF getContentRect() {
        return mContentRect;
    }

    public PointF getContentCenter() {
        return new PointF(mContentRect.centerX(), mContentRect.centerY());
    }

    public float getChartHeight() {
        return mChartHeight;
    }

    public float getChartWidth() {
        return mChartWidth;
    }

    public Matrix zoomIn(float x, float y) {
        Matrix save = new Matrix();
        save.set(mMatrixTouch);
        save.postScale(1.4f, 1.4f, x, y);
        return save;
    }

    public Matrix zoomOut(float x, float y) {
        Matrix save = new Matrix();
        save.set(mMatrixTouch);
        save.postScale(0.7f, 0.7f, x, y);
        return save;
    }

    public Matrix zoom(float scaleX, float scaleY, float x, float y) {
        Matrix save = new Matrix();
        save.set(mMatrixTouch);
        save.postScale(scaleX, scaleY, x, y);
        return save;
    }

    public Matrix fitScreen() {
        Matrix save = new Matrix();
        save.set(mMatrixTouch);
        float[] vals = new float[9];
        save.getValues(vals);
        vals[Matrix.MTRANS_X] = 0f;
        vals[Matrix.MTRANS_Y] = 0f;
        vals[Matrix.MSCALE_X] = 1f;
        vals[Matrix.MSCALE_Y] = 1f;
        save.setValues(vals);
        return save;
    }

    public synchronized void centerViewPort(final float[] transformedPts, final View view) {
        Matrix save = new Matrix();
        save.set(mMatrixTouch);
        final float x = transformedPts[0] - offsetLeft();
        final float y = transformedPts[1] - offsetTop();
        save.postTranslate(-x, -y);
        refresh(save, view, false);
    }

    public Matrix refresh(Matrix newMatrix, View chart, boolean invalidate) {
        mMatrixTouch.set(newMatrix);
        limitTransAndScale(mMatrixTouch, mContentRect);
        chart.invalidate();
        newMatrix.set(mMatrixTouch);
        return newMatrix;
    }

    public void limitTransAndScale(Matrix matrix, RectF content) {
        float[] vals = new float[9];
        matrix.getValues(vals);
        float curTransX = vals[Matrix.MTRANS_X];
        float curScaleX = vals[Matrix.MSCALE_X];
        float curTransY = vals[Matrix.MTRANS_Y];
        float curScaleY = vals[Matrix.MSCALE_Y];
        mScaleX = Math.max(mMinScaleX, curScaleX);
        mScaleY = Math.max(mMinScaleY, curScaleY);
        float width = 0f;
        float height = 0f;
        if (content != null) {
            width = content.width();
            height = content.height();
        }
        float maxTransX = -width * (mScaleX - 1f);
        float newTransX = Math.min(Math.max(curTransX, maxTransX - mTransOffsetX), mTransOffsetX);
        float maxTransY = height * (mScaleY - 1f);
        float newTransY = Math.max(Math.min(curTransY, maxTransY + mTransOffsetY), -mTransOffsetY);
        vals[Matrix.MTRANS_X] = newTransX;
        vals[Matrix.MSCALE_X] = mScaleX;
        vals[Matrix.MTRANS_Y] = newTransY;
        vals[Matrix.MSCALE_Y] = mScaleY;
        matrix.setValues(vals);
    }

    public void setMinimumScaleX(float xScale) {
        if (xScale < 1f)
            xScale = 1f;
        mMinScaleX = xScale;
        limitTransAndScale(mMatrixTouch, mContentRect);
    }

    public void setMinimumScaleY(float yScale) {
        if (yScale < 1f)
            yScale = 1f;
        mMinScaleY = yScale;
        limitTransAndScale(mMatrixTouch, mContentRect);
    }

    public Matrix getMatrixTouch() {
        return mMatrixTouch;
    }

    public boolean isInBoundsX(float x) {
        if (isInBoundsLeft(x) && isInBoundsRight(x))
            return true;
        else
            return false;
    }

    public boolean isInBoundsY(float y) {
        if (isInBoundsTop(y) && isInBoundsBottom(y))
            return true;
        else
            return false;
    }

    public boolean isInBounds(float x, float y) {
        if (isInBoundsX(x) && isInBoundsY(y))
            return true;
        else
            return false;
    }

    public boolean isInBoundsLeft(float x) {
        return mContentRect.left <= x ? true : false;
    }

    public boolean isInBoundsRight(float x) {
        x = (float) ((int) (x * 100.f)) / 100.f;
        return mContentRect.right >= x ? true : false;
    }

    public boolean isInBoundsTop(float y) {
        return mContentRect.top <= y ? true : false;
    }

    public boolean isInBoundsBottom(float y) {
        y = (float) ((int) (y * 100.f)) / 100.f;
        return mContentRect.bottom >= y ? true : false;
    }

    public float getScaleX() {
        return mScaleX;
    }

    public float getScaleY() {
        return mScaleY;
    }

    public boolean isFullyZoomedOut() {
        if (isFullyZoomedOutX() && isFullyZoomedOutY())
            return true;
        else
            return false;
    }

    public boolean isFullyZoomedOutY() {
        if (mScaleY > mMinScaleY || mMinScaleY > 1f)
            return false;
        else
            return true;
    }

    public boolean isFullyZoomedOutX() {
        if (mScaleX > mMinScaleX || mMinScaleX > 1f)
            return false;
        else
            return true;
    }

    public void setDragOffsetX(float offset) {
        mTransOffsetX = Utils.convertDpToPixel(offset);
    }

    public void setDragOffsetY(float offset) {
        mTransOffsetY = Utils.convertDpToPixel(offset);
    }

    public boolean hasNoDragOffset() {
        return mTransOffsetX <= 0 && mTransOffsetY <= 0 ? true : false;
    }
}