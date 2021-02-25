package com.github.mikephil.charting.charts;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Locale;
import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.Bitmap.CompressFormat;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Matrix;
import android.graphics.Paint;
import android.graphics.Paint.Align;
import android.graphics.Paint.Style;
import android.graphics.Path;
import android.graphics.PointF;
import android.graphics.Rect;
import android.graphics.RectF;
import android.graphics.Typeface;
import android.os.Environment;
import android.provider.MediaStore;
import android.util.AttributeSet;
import android.util.Log;
import android.view.MotionEvent;
import android.view.View;
import android.widget.RelativeLayout;
import com.github.mikephil.charting.data.ChartData;
import com.github.mikephil.charting.data.DataSet;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.interfaces.OnChartValueSelectedListener;
import com.github.mikephil.charting.utils.ColorTemplate;
import com.github.mikephil.charting.utils.Highlight;
import com.github.mikephil.charting.utils.SelInfo;
import com.github.mikephil.charting.utils.Utils;

public abstract class Chart extends View {

    public static final String LOG_TAG = "MPChart";

    protected int mColorDarkBlue = Color.rgb(41, 128, 186);

    protected int mColorDarkRed = Color.rgb(232, 76, 59);

    protected int mValueDigitsToUse = -1;

    protected int mValueFormatDigits = -1;

    protected int mOffsetLeft = 35;

    protected int mOffsetTop = 25;

    protected int mOffsetRight = 20;

    protected int mOffsetBottom = 15;

    protected ChartData mData = null;

    protected Bitmap mDrawBitmap;

    protected Canvas mDrawCanvas;

    protected float mYChartMin = 0.0f;

    protected float mYChartMax = 0.0f;

    protected Paint mDrawPaint;

    protected Paint mDescPaint;

    protected Paint mInfoPaint;

    protected Paint mValuePaint;

    protected Paint mRenderPaint;

    protected ColorTemplate mCt;

    protected String mDescription = "Description.";

    protected boolean mDataNotSet = true;

    protected float mDeltaY = 1f;

    protected float mDeltaX = 1f;

    protected Matrix mMatrixValueToPx;

    protected Matrix mMatrixOffset;

    protected Matrix mMatrixTouch = new Matrix();

    protected int mDrawColor = Color.rgb(56, 199, 240);

    protected boolean mTouchEnabled = true;

    protected boolean mDrawYValues = true;

    protected boolean mHighlightEnabled = true;

    protected boolean mSeparateTousands = true;

    protected Rect mContentRect;

    protected OnChartValueSelectedListener mSelectionListener;

    public Chart(Context context) {
        super(context);
        init();
    }

    public Chart(Context context, AttributeSet attrs) {
        super(context, attrs);
        init();
    }

    public Chart(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
        init();
    }

    protected void init() {
        Utils.init(getContext().getResources());
        setBackgroundColor(Color.WHITE);
        mOffsetBottom = (int) Utils.convertDpToPixel(mOffsetBottom);
        mOffsetLeft = (int) Utils.convertDpToPixel(mOffsetLeft);
        mOffsetRight = (int) Utils.convertDpToPixel(mOffsetRight);
        mOffsetTop = (int) Utils.convertDpToPixel(mOffsetTop);
        mRenderPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mRenderPaint.setStyle(Style.FILL);
        mDrawPaint = new Paint();
        mDescPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mDescPaint.setColor(Color.BLACK);
        mDescPaint.setTextAlign(Align.RIGHT);
        mDescPaint.setTextSize(Utils.convertDpToPixel(9f));
        mInfoPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mInfoPaint.setColor(Color.rgb(247, 189, 51));
        mInfoPaint.setTextAlign(Align.CENTER);
        mInfoPaint.setTextSize(Utils.convertDpToPixel(12f));
        mValuePaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mValuePaint.setColor(Color.rgb(63, 63, 63));
        mValuePaint.setTextAlign(Align.CENTER);
        mValuePaint.setTextSize(Utils.convertDpToPixel(9f));
        mCt = new ColorTemplate();
        mCt.addDataSetColors(ColorTemplate.LIBERTY_COLORS, getContext());
    }

    public void initWithDummyData() {
        ColorTemplate template = new ColorTemplate();
        template.addColorsForDataSets(ColorTemplate.COLORFUL_COLORS, getContext());
        setColorTemplate(template);
        setDrawYValues(false);
        ArrayList<String> xVals = new ArrayList<String>();
        Calendar calendar = Calendar.getInstance();
        for (int i = 0; i < 12; i++) {
            xVals.add(calendar.getDisplayName(Calendar.MONTH, Calendar.SHORT, Locale.getDefault()));
        }
        ArrayList<DataSet> dataSets = new ArrayList<DataSet>();
        for (int i = 0; i < 3; i++) {
            ArrayList<Entry> yVals = new ArrayList<Entry>();
            for (int j = 0; j < 12; j++) {
                float val = (float) (Math.random() * 100);
                yVals.add(new Entry(val, j));
            }
            DataSet set = new DataSet(yVals, i);
            dataSets.add(set);
        }
        ChartData data = new ChartData(xVals, dataSets);
        setData(data);
        invalidate();
    }

    public void setData(ChartData data) {
        if (data == null || !data.isValid()) {
            Log.e(LOG_TAG, "Cannot set data for chart. Provided chart values are null or contain less than 2 entries.");
            mDataNotSet = true;
            return;
        }
        mDataNotSet = false;
        mData = data;
        prepare();
    }

    public void setData(ArrayList<String> xVals, ArrayList<Float> yVals) {
        ArrayList<Entry> series = new ArrayList<Entry>();
        for (int i = 0; i < yVals.size(); i++) {
            series.add(new Entry(yVals.get(i), i));
        }
        DataSet set = new DataSet(series, 0);
        ArrayList<DataSet> dataSets = new ArrayList<DataSet>();
        dataSets.add(set);
        ChartData data = new ChartData(xVals, dataSets);
        setData(data);
    }

    public abstract void prepare();

    public abstract void notifyDataSetChanged();

    protected void calcMinMax(boolean fixedValues) {
        if (!fixedValues) {
            mYChartMin = mData.getYMin();
            mYChartMax = mData.getYMax();
        }
        mDeltaY = Math.abs(mData.getYMax() - mData.getYMin());
        mDeltaX = mData.getXVals().size() - 1;
    }

    private boolean mFirstDraw = true;

    private boolean mContentRectSetup = false;

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        if (!mContentRectSetup) {
            mContentRectSetup = true;
            prepareContentRect();
        }
        if (mDataNotSet) {
            canvas.drawText("No chart data available.", getWidth() / 2, getHeight() / 2, mInfoPaint);
            return;
        }
        if (mFirstDraw) {
            mFirstDraw = false;
            prepareMatrix();
        }
        if (mDrawBitmap == null || mDrawCanvas == null) {
            mDrawBitmap = Bitmap.createBitmap(getWidth(), getHeight(), Bitmap.Config.RGB_565);
            mDrawCanvas = new Canvas(mDrawBitmap);
        }
        mDrawCanvas.drawColor(Color.WHITE);
    }

    protected void prepareMatrix() {
        float scaleX = (float) ((getWidth() - mOffsetLeft - mOffsetRight) / mDeltaX);
        float scaleY = (float) ((getHeight() - mOffsetBottom - mOffsetTop) / mDeltaY);
        mMatrixValueToPx = new Matrix();
        mMatrixValueToPx.reset();
        mMatrixValueToPx.postTranslate(0, -mYChartMin);
        mMatrixValueToPx.postScale(scaleX, -scaleY);
        mMatrixOffset = new Matrix();
        mMatrixOffset.postTranslate(mOffsetLeft, getHeight() - mOffsetBottom);
    }

    protected void prepareContentRect() {
        mContentRect = new Rect(mOffsetLeft, mOffsetTop, getWidth() - mOffsetRight, getHeight() - mOffsetBottom);
    }

    protected float[] generateTransformedValues(ArrayList<Entry> entries, float xOffset) {
        float[] valuePoints = new float[entries.size() * 2];
        for (int j = 0; j < valuePoints.length; j += 2) {
            valuePoints[j] = entries.get(j / 2).getXIndex() + xOffset;
            valuePoints[j + 1] = entries.get(j / 2).getVal();
        }
        transformPointArray(valuePoints);
        return valuePoints;
    }

    protected void transformPath(Path path) {
        path.transform(mMatrixValueToPx);
        path.transform(mMatrixTouch);
        path.transform(mMatrixOffset);
    }

    protected void transformPaths(ArrayList<Path> paths) {
        for (int i = 0; i < paths.size(); i++) {
            transformPath(paths.get(i));
        }
    }

    protected void transformPointArray(float[] pts) {
        mMatrixValueToPx.mapPoints(pts);
        mMatrixTouch.mapPoints(pts);
        mMatrixOffset.mapPoints(pts);
    }

    protected void transformRect(RectF r) {
        mMatrixValueToPx.mapRect(r);
        mMatrixTouch.mapRect(r);
        mMatrixOffset.mapRect(r);
    }

    protected void transformRects(ArrayList<RectF> rects) {
        for (int i = 0; i < rects.size(); i++) transformRect(rects.get(i));
    }

    protected void transformRectsTouch(ArrayList<RectF> rects) {
        for (int i = 0; i < rects.size(); i++) {
            mMatrixTouch.mapRect(rects.get(i));
        }
    }

    protected void transformPathsTouch(ArrayList<Path> paths) {
        for (int i = 0; i < paths.size(); i++) {
            paths.get(i).transform(mMatrixTouch);
        }
    }

    protected void drawDescription() {
        mDrawCanvas.drawText(mDescription, getWidth() - mOffsetRight - 10, getHeight() - mOffsetBottom - 10, mDescPaint);
    }

    protected int calcTextWidth(Paint paint, String demoText) {
        Rect r = new Rect();
        paint.getTextBounds(demoText, 0, demoText.length(), r);
        return r.width();
    }

    protected abstract void drawValues();

    protected abstract void drawData();

    protected abstract void drawAdditional();

    protected abstract void drawHighlights();

    protected OnTouchListener mListener;

    @Override
    public boolean onTouchEvent(MotionEvent event) {
        if (mListener == null)
            return false;
        if (!mTouchEnabled)
            return false;
        else
            return mListener.onTouch(this, event);
    }

    protected Highlight[] mIndicesToHightlight = new Highlight[0];

    public boolean needsHighlight(int xIndex, int dataSetIndex) {
        if (!valuesToHighlight())
            return false;
        for (int i = 0; i < mIndicesToHightlight.length; i++) if (mIndicesToHightlight[i].getXIndex() == xIndex && mIndicesToHightlight[i].getDataSetIndex() == dataSetIndex && xIndex <= mDeltaX)
            return true;
        return false;
    }

    public boolean valuesToHighlight() {
        return mIndicesToHightlight == null || mIndicesToHightlight.length <= 0 || mIndicesToHightlight[0] == null ? false : true;
    }

    public void highlightValues(Highlight[] highs) {
        mIndicesToHightlight = highs;
        invalidate();
        if (mSelectionListener != null) {
            if (!valuesToHighlight())
                mSelectionListener.onNothingSelected();
            else {
                Entry[] values = new Entry[highs.length];
                for (int i = 0; i < values.length; i++) values[i] = getEntryByDataSetIndex(highs[i].getXIndex(), highs[i].getDataSetIndex());
                mSelectionListener.onValuesSelected(values, highs);
            }
        }
    }

    protected float mMarkerPosX = 0f;

    protected float mMarkerPosY = 0f;

    protected boolean mDrawMarkerView = true;

    protected RelativeLayout mMarkerView;

    protected void drawMarkerView() {
        if (mMarkerView == null || !mDrawMarkerView || !valuesToHighlight())
            return;
    }

    public void setOnTouchListener(OnTouchListener l) {
        this.mListener = l;
    }

    public void setOnChartValueSelectedListener(OnChartValueSelectedListener l) {
        this.mSelectionListener = l;
    }

    public void setHighlightEnabled(boolean enabled) {
        mHighlightEnabled = enabled;
    }

    public boolean isHighlightEnabled() {
        return mHighlightEnabled;
    }

    public float getYValueSum() {
        return mData.getYValueSum();
    }

    public float getYMax() {
        return mData.getYMax();
    }

    public float getYChartMin() {
        return mYChartMin;
    }

    public float getYChartMax() {
        return mYChartMax;
    }

    public float getYMin() {
        return mData.getYMin();
    }

    public float getDeltaX() {
        return mDeltaX;
    }

    public float getAverage() {
        return getYValueSum() / mData.getYValCount();
    }

    public float getAverage(int dataSetType) {
        return mData.getDataSetByType(dataSetType).getYValueSum() / mData.getDataSetByType(dataSetType).getEntryCount();
    }

    public int getValueCount() {
        return mData.getYValCount();
    }

    public PointF getCenter() {
        return new PointF(getWidth() / 2, getHeight() / 2);
    }

    public float getOffsetLeft() {
        return mOffsetLeft;
    }

    public void setDescriptionTextSize(float size) {
        if (size > 14f)
            size = 14f;
        if (size < 7f)
            size = 7f;
        mInfoPaint.setTextSize(Utils.convertDpToPixel(size));
    }

    public void setDrawColor(int color) {
        mDrawColor = color;
    }

    public void setDescription(String desc) {
        this.mDescription = desc;
    }

    public void setOffsets(int left, int top, int right, int bottom) {
        mOffsetBottom = (int) Utils.convertDpToPixel(bottom);
        mOffsetLeft = (int) Utils.convertDpToPixel(left);
        mOffsetRight = (int) Utils.convertDpToPixel(right);
        mOffsetTop = (int) Utils.convertDpToPixel(top);
    }

    public void setTouchEnabled(boolean enabled) {
        this.mTouchEnabled = enabled;
    }

    public void setDrawYValues(boolean enabled) {
        this.mDrawYValues = enabled;
    }

    public void setYStartEnd(float start, float end) {
        mYChartMin = start;
        mDeltaY = end - start;
    }

    public void setColorTemplate(ColorTemplate ct) {
        this.mCt = ct;
    }

    public ColorTemplate getColorTemplate() {
        return mCt;
    }

    public void setMarkerView(View v) {
        mMarkerView = new RelativeLayout(getContext());
        mMarkerView.setLayoutParams(new RelativeLayout.LayoutParams(RelativeLayout.LayoutParams.WRAP_CONTENT, RelativeLayout.LayoutParams.WRAP_CONTENT));
        mMarkerView.addView(v);
        mMarkerView.measure(getWidth(), getHeight());
        mMarkerView.layout(0, 0, getWidth(), getHeight());
    }

    public View getMarkerView() {
        return mMarkerView;
    }

    public float getMarkerPosX() {
        return mMarkerPosX;
    }

    public float getMarkerPosY() {
        return mMarkerPosY;
    }

    public static final int PAINT_GRID = 3;

    public static final int PAINT_GRID_BACKGROUND = 4;

    public static final int PAINT_YLEGEND = 5;

    public static final int PAINT_XLEGEND = 6;

    public static final int PAINT_INFO = 7;

    public static final int PAINT_VALUES = 8;

    public static final int PAINT_CIRCLES_INNER = 10;

    public static final int PAINT_DESCRIPTION = 11;

    public static final int PAINT_OUTLINE = 12;

    public static final int PAINT_HOLE = 13;

    public static final int PAINT_CENTER_TEXT = 14;

    public static final int PAINT_HIGHLIGHT_LINE = 15;

    public static final int PAINT_HIGHLIGHT_BAR = 16;

    public void setPaint(Paint p, int which) {
        switch(which) {
            case PAINT_INFO:
                mInfoPaint = p;
                break;
            case PAINT_DESCRIPTION:
                mDescPaint = p;
                break;
            case PAINT_VALUES:
                mValuePaint = p;
                break;
        }
    }

    public boolean isDrawMarkerViewEnabled() {
        return mDrawMarkerView;
    }

    public void setDrawMarkerView(boolean enabled) {
        mDrawMarkerView = enabled;
    }

    public void setValuePaintColor(int color) {
        mValuePaint.setColor(color);
    }

    public void setSeparateThousands(boolean enabled) {
        mSeparateTousands = enabled;
    }

    public boolean isDrawYValuesEnabled() {
        return mDrawYValues;
    }

    public String getXValue(int index) {
        if (mData == null || mData.getXValCount() <= index)
            return null;
        else
            return mData.getXVals().get(index);
    }

    public float getYValue(int index) {
        return mData.getDataSetByIndex(0).getYVals().get(index).getVal();
    }

    public float getYValue(int index, int type) {
        DataSet set = mData.getDataSetByType(type);
        return set.getYVals().get(index).getVal();
    }

    public float getYValueByDataSetIndex(int xIndex, int dataSet) {
        DataSet set = mData.getDataSetByIndex(dataSet);
        return set.getYValForXIndex(xIndex);
    }

    public DataSet getDataSetByIndex(int index) {
        return mData.getDataSetByIndex(index);
    }

    public DataSet getDataSetByType(int type) {
        return mData.getDataSetByType(type);
    }

    public Entry getEntry(int index) {
        return mData.getDataSetByIndex(0).getYVals().get(index);
    }

    public Entry getEntry(int index, int type) {
        return mData.getDataSetByType(type).getYVals().get(index);
    }

    public Entry getEntryByDataSetIndex(int xIndex, int dataSetIndex) {
        return mData.getDataSetByIndex(dataSetIndex).getEntryForXIndex(xIndex);
    }

    protected ArrayList<SelInfo> getYValsAtIndex(int xIndex) {
        ArrayList<SelInfo> vals = new ArrayList<SelInfo>();
        for (int i = 0; i < mData.getDataSetCount(); i++) {
            float yVal = mData.getDataSetByIndex(i).getYValForXIndex(xIndex);
            if (!Float.isNaN(yVal)) {
                vals.add(new SelInfo(yVal, i));
            }
        }
        return vals;
    }

    public ArrayList<Entry> getEntriesAtIndex(int xIndex) {
        ArrayList<Entry> vals = new ArrayList<Entry>();
        for (int i = 0; i < mData.getDataSetCount(); i++) {
            DataSet set = mData.getDataSetByIndex(i);
            Entry e = set.getEntryForXIndex(xIndex);
            if (e != null) {
                vals.add(e);
            }
        }
        return vals;
    }

    public ChartData getData() {
        return mData;
    }

    public float getPercentOfTotal(float val) {
        return val / mData.getYValueSum() * 100f;
    }

    public void setValueTypeface(Typeface t) {
        mValuePaint.setTypeface(t);
    }

    public void setDescriptionTypeface(Typeface t) {
        mDescPaint.setTypeface(t);
    }

    public void setValueDigits(int digits) {
        mValueDigitsToUse = digits;
    }

    public int getValueDigits() {
        return mValueDigitsToUse;
    }

    public void saveToGallery(String title) {
        MediaStore.Images.Media.insertImage(getContext().getContentResolver(), mDrawBitmap, title, "");
    }

    public void saveToPath(String title, String pathOnSD) {
        OutputStream stream = null;
        try {
            stream = new FileOutputStream(Environment.getExternalStorageDirectory().getPath() + pathOnSD + "/" + title + ".png");
            mDrawBitmap.compress(CompressFormat.PNG, 40, stream);
            stream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void onLayout(boolean changed, int left, int top, int right, int bottom) {
        super.onLayout(changed, left, top, right, bottom);
    }

    @Override
    protected void onSizeChanged(int w, int h, int oldw, int oldh) {
        super.onSizeChanged(w, h, oldw, oldh);
    }

    @Override
    protected void onAttachedToWindow() {
        super.onAttachedToWindow();
        if (isInEditMode()) {
            initWithDummyData();
        }
    }
}