package com.github.mikephil.charting.charts;

import android.animation.ValueAnimator;
import android.animation.ValueAnimator.AnimatorUpdateListener;
import android.annotation.SuppressLint;
import android.content.ContentValues;
import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.Bitmap.CompressFormat;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Paint.Align;
import android.graphics.PointF;
import android.graphics.RectF;
import android.graphics.Typeface;
import android.graphics.drawable.Drawable;
import android.os.Environment;
import android.provider.MediaStore.Images;
import android.text.TextUtils;
import android.util.AttributeSet;
import android.util.Log;
import android.view.View;
import android.view.ViewGroup;
import android.view.ViewParent;
import com.github.mikephil.charting.animation.ChartAnimator;
import com.github.mikephil.charting.animation.Easing;
import com.github.mikephil.charting.animation.EasingFunction;
import com.github.mikephil.charting.components.Legend;
import com.github.mikephil.charting.components.MarkerView;
import com.github.mikephil.charting.components.XAxis;
import com.github.mikephil.charting.data.ChartData;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.formatter.DefaultValueFormatter;
import com.github.mikephil.charting.formatter.ValueFormatter;
import com.github.mikephil.charting.highlight.ChartHighlighter;
import com.github.mikephil.charting.highlight.Highlight;
import com.github.mikephil.charting.highlight.Highlighter;
import com.github.mikephil.charting.interfaces.dataprovider.ChartInterface;
import com.github.mikephil.charting.interfaces.datasets.IDataSet;
import com.github.mikephil.charting.listener.ChartTouchListener;
import com.github.mikephil.charting.listener.OnChartGestureListener;
import com.github.mikephil.charting.listener.OnChartValueSelectedListener;
import com.github.mikephil.charting.renderer.DataRenderer;
import com.github.mikephil.charting.renderer.LegendRenderer;
import com.github.mikephil.charting.utils.Utils;
import com.github.mikephil.charting.utils.ViewPortHandler;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

@SuppressLint("NewApi")
public abstract class Chart<T extends ChartData<? extends IDataSet<? extends Entry>>> extends ViewGroup implements ChartInterface {

    public static final String LOG_TAG = "MPAndroidChart";

    protected boolean mLogEnabled = false;

    protected T mData = null;

    protected boolean mHighLightPerTapEnabled = true;

    private boolean mDragDecelerationEnabled = true;

    private float mDragDecelerationFrictionCoef = 0.9f;

    protected DefaultValueFormatter mDefaultFormatter = new DefaultValueFormatter(0);

    protected Paint mDescPaint;

    protected Paint mInfoPaint;

    protected String mDescription = "Description";

    protected XAxis mXAxis;

    protected boolean mTouchEnabled = true;

    protected Legend mLegend;

    protected OnChartValueSelectedListener mSelectionListener;

    protected ChartTouchListener mChartTouchListener;

    private String mNoDataText = "No chart data available.";

    private OnChartGestureListener mGestureListener;

    private String mNoDataTextDescription;

    protected LegendRenderer mLegendRenderer;

    protected DataRenderer mRenderer;

    protected Highlighter mHighlighter;

    protected ViewPortHandler mViewPortHandler = new ViewPortHandler();

    protected ChartAnimator mAnimator;

    private float mExtraTopOffset = 0.f, mExtraRightOffset = 0.f, mExtraBottomOffset = 0.f, mExtraLeftOffset = 0.f;

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
        setWillNotDraw(false);
        if (android.os.Build.VERSION.SDK_INT < 11)
            mAnimator = new ChartAnimator();
        else
            mAnimator = new ChartAnimator(new AnimatorUpdateListener() {

                @Override
                public void onAnimationUpdate(ValueAnimator animation) {
                    postInvalidate();
                }
            });
        Utils.init(getContext());
        mMaxHighlightDistance = Utils.convertDpToPixel(100f);
        mLegend = new Legend();
        mLegendRenderer = new LegendRenderer(mViewPortHandler, mLegend);
        mXAxis = new XAxis();
        mDescPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mDescPaint.setColor(Color.BLACK);
        mDescPaint.setTextAlign(Align.RIGHT);
        mDescPaint.setTextSize(Utils.convertDpToPixel(9f));
        mInfoPaint = new Paint(Paint.ANTI_ALIAS_FLAG);
        mInfoPaint.setColor(Color.rgb(247, 189, 51));
        mInfoPaint.setTextAlign(Align.CENTER);
        mInfoPaint.setTextSize(Utils.convertDpToPixel(12f));
        mDrawPaint = new Paint(Paint.DITHER_FLAG);
        if (mLogEnabled)
            Log.i("", "Chart.init()");
    }

    public void setData(T data) {
        mData = data;
        mOffsetsCalculated = false;
        if (data == null) {
            return;
        }
        setupDefaultFormatter(data.getYMin(), data.getYMax());
        for (IDataSet set : mData.getDataSets()) {
            if (set.needsFormatter() || set.getValueFormatter() == mDefaultFormatter)
                set.setValueFormatter(mDefaultFormatter);
        }
        notifyDataSetChanged();
        if (mLogEnabled)
            Log.i(LOG_TAG, "Data is set.");
    }

    public void clear() {
        mData = null;
        mOffsetsCalculated = false;
        mIndicesToHighlight = null;
        invalidate();
    }

    public void clearValues() {
        mData.clearValues();
        invalidate();
    }

    public boolean isEmpty() {
        if (mData == null)
            return true;
        else {
            if (mData.getEntryCount() <= 0)
                return true;
            else
                return false;
        }
    }

    public abstract void notifyDataSetChanged();

    protected abstract void calculateOffsets();

    protected abstract void calcMinMax();

    protected void setupDefaultFormatter(float min, float max) {
        float reference = 0f;
        if (mData == null || mData.getEntryCount() < 2) {
            reference = Math.max(Math.abs(min), Math.abs(max));
        } else {
            reference = Math.abs(max - min);
        }
        int digits = Utils.getDecimals(reference);
        mDefaultFormatter.setup(digits);
    }

    private boolean mOffsetsCalculated = false;

    protected Paint mDrawPaint;

    @Override
    protected void onDraw(Canvas canvas) {
        if (mData == null) {
            boolean hasText = !TextUtils.isEmpty(mNoDataText);
            boolean hasDescription = !TextUtils.isEmpty(mNoDataTextDescription);
            float line1height = hasText ? Utils.calcTextHeight(mInfoPaint, mNoDataText) : 0.f;
            float line2height = hasDescription ? Utils.calcTextHeight(mInfoPaint, mNoDataTextDescription) : 0.f;
            float lineSpacing = (hasText && hasDescription) ? (mInfoPaint.getFontSpacing() - line1height) : 0.f;
            float y = (getHeight() - (line1height + lineSpacing + line2height)) / 2.f + line1height;
            if (hasText) {
                canvas.drawText(mNoDataText, getWidth() / 2, y, mInfoPaint);
                if (hasDescription) {
                    y = y + line1height + lineSpacing;
                }
            }
            if (hasDescription) {
                canvas.drawText(mNoDataTextDescription, getWidth() / 2, y, mInfoPaint);
            }
            return;
        }
        if (!mOffsetsCalculated) {
            calculateOffsets();
            mOffsetsCalculated = true;
        }
    }

    private PointF mDescriptionPosition;

    protected void drawDescription(Canvas c) {
        if (!mDescription.equals("")) {
            if (mDescriptionPosition == null) {
                c.drawText(mDescription, getWidth() - mViewPortHandler.offsetRight() - 10, getHeight() - mViewPortHandler.offsetBottom() - 10, mDescPaint);
            } else {
                c.drawText(mDescription, mDescriptionPosition.x, mDescriptionPosition.y, mDescPaint);
            }
        }
    }

    protected Highlight[] mIndicesToHighlight;

    protected float mMaxHighlightDistance = 0f;

    @Override
    public float getMaxHighlightDistance() {
        return mMaxHighlightDistance;
    }

    public void setMaxHighlightDistance(float distDp) {
        mMaxHighlightDistance = Utils.convertDpToPixel(distDp);
    }

    public Highlight[] getHighlighted() {
        return mIndicesToHighlight;
    }

    public boolean isHighlightPerTapEnabled() {
        return mHighLightPerTapEnabled;
    }

    public void setHighlightPerTapEnabled(boolean enabled) {
        mHighLightPerTapEnabled = enabled;
    }

    public boolean valuesToHighlight() {
        return mIndicesToHighlight == null || mIndicesToHighlight.length <= 0 || mIndicesToHighlight[0] == null ? false : true;
    }

    protected void setLastHighlighted(Highlight[] highs) {
        if (highs == null || highs.length <= 0 || highs[0] == null) {
            mChartTouchListener.setLastHighlighted(null);
        } else {
            mChartTouchListener.setLastHighlighted(highs[0]);
        }
    }

    public void highlightValues(Highlight[] highs) {
        mIndicesToHighlight = highs;
        setLastHighlighted(highs);
        invalidate();
    }

    public void highlightValue(float x, int dataSetIndex) {
        highlightValue(x, dataSetIndex, true);
    }

    public void highlightValue(float x, int dataSetIndex, boolean callListener) {
        if (dataSetIndex < 0 || dataSetIndex >= mData.getDataSetCount()) {
            highlightValue(null, callListener);
        } else {
            highlightValue(new Highlight(x, dataSetIndex), callListener);
        }
    }

    public void highlightValue(Highlight highlight) {
        highlightValue(highlight, false);
    }

    public void highlightValue(Highlight high, boolean callListener) {
        Entry e = null;
        if (high == null)
            mIndicesToHighlight = null;
        else {
            if (mLogEnabled)
                Log.i(LOG_TAG, "Highlighted: " + high.toString());
            e = mData.getEntryForHighlight(high);
            if (e == null) {
                mIndicesToHighlight = null;
                high = null;
            } else {
                mIndicesToHighlight = new Highlight[] { high };
            }
        }
        setLastHighlighted(mIndicesToHighlight);
        if (callListener && mSelectionListener != null) {
            if (!valuesToHighlight())
                mSelectionListener.onNothingSelected();
            else {
                mSelectionListener.onValueSelected(e, high);
            }
        }
        invalidate();
    }

    public Highlight getHighlightByTouchPoint(float x, float y) {
        if (mData == null) {
            Log.e(LOG_TAG, "Can't select by touch. No data set.");
            return null;
        } else
            return getHighlighter().getHighlight(x, y);
    }

    public void setOnTouchListener(ChartTouchListener l) {
        this.mChartTouchListener = l;
    }

    public ChartTouchListener getOnTouchListener() {
        return mChartTouchListener;
    }

    protected boolean mDrawMarkerViews = true;

    protected MarkerView mMarkerView;

    protected void drawMarkers(Canvas canvas) {
        if (mMarkerView == null || !mDrawMarkerViews || !valuesToHighlight())
            return;
        for (int i = 0; i < mIndicesToHighlight.length; i++) {
            Highlight highlight = mIndicesToHighlight[i];
            IDataSet set = mData.getDataSetByIndex(highlight.getDataSetIndex());
            Entry e = mData.getEntryForHighlight(mIndicesToHighlight[i]);
            int entryIndex = set.getEntryIndex(e);
            if (e == null || entryIndex > set.getEntryCount() * mAnimator.getPhaseX())
                continue;
            float[] pos = getMarkerPosition(highlight);
            if (!mViewPortHandler.isInBounds(pos[0], pos[1]))
                continue;
            mMarkerView.refreshContent(e, highlight);
            mMarkerView.measure(MeasureSpec.makeMeasureSpec(0, MeasureSpec.UNSPECIFIED), MeasureSpec.makeMeasureSpec(0, MeasureSpec.UNSPECIFIED));
            mMarkerView.layout(0, 0, mMarkerView.getMeasuredWidth(), mMarkerView.getMeasuredHeight());
            if (pos[1] - mMarkerView.getHeight() <= 0) {
                float y = mMarkerView.getHeight() - pos[1];
                mMarkerView.draw(canvas, pos[0], pos[1] + y);
            } else {
                mMarkerView.draw(canvas, pos[0], pos[1]);
            }
        }
    }

    protected float[] getMarkerPosition(Highlight high) {
        return new float[] { high.getDrawX(), high.getDrawY() };
    }

    public ChartAnimator getAnimator() {
        return mAnimator;
    }

    public boolean isDragDecelerationEnabled() {
        return mDragDecelerationEnabled;
    }

    public void setDragDecelerationEnabled(boolean enabled) {
        mDragDecelerationEnabled = enabled;
    }

    public float getDragDecelerationFrictionCoef() {
        return mDragDecelerationFrictionCoef;
    }

    public void setDragDecelerationFrictionCoef(float newValue) {
        if (newValue < 0.f)
            newValue = 0.f;
        if (newValue >= 1f)
            newValue = 0.999f;
        mDragDecelerationFrictionCoef = newValue;
    }

    public void animateXY(int durationMillisX, int durationMillisY, EasingFunction easingX, EasingFunction easingY) {
        mAnimator.animateXY(durationMillisX, durationMillisY, easingX, easingY);
    }

    public void animateX(int durationMillis, EasingFunction easing) {
        mAnimator.animateX(durationMillis, easing);
    }

    public void animateY(int durationMillis, EasingFunction easing) {
        mAnimator.animateY(durationMillis, easing);
    }

    public void animateXY(int durationMillisX, int durationMillisY, Easing.EasingOption easingX, Easing.EasingOption easingY) {
        mAnimator.animateXY(durationMillisX, durationMillisY, easingX, easingY);
    }

    public void animateX(int durationMillis, Easing.EasingOption easing) {
        mAnimator.animateX(durationMillis, easing);
    }

    public void animateY(int durationMillis, Easing.EasingOption easing) {
        mAnimator.animateY(durationMillis, easing);
    }

    public void animateX(int durationMillis) {
        mAnimator.animateX(durationMillis);
    }

    public void animateY(int durationMillis) {
        mAnimator.animateY(durationMillis);
    }

    public void animateXY(int durationMillisX, int durationMillisY) {
        mAnimator.animateXY(durationMillisX, durationMillisY);
    }

    public XAxis getXAxis() {
        return mXAxis;
    }

    public ValueFormatter getDefaultValueFormatter() {
        return mDefaultFormatter;
    }

    public void setOnChartValueSelectedListener(OnChartValueSelectedListener l) {
        this.mSelectionListener = l;
    }

    public void setOnChartGestureListener(OnChartGestureListener l) {
        this.mGestureListener = l;
    }

    public OnChartGestureListener getOnChartGestureListener() {
        return mGestureListener;
    }

    public float getYMax() {
        return mData.getYMax();
    }

    public float getYMin() {
        return mData.getYMin();
    }

    @Override
    public float getXChartMax() {
        return mXAxis.mAxisMaximum;
    }

    @Override
    public float getXChartMin() {
        return mXAxis.mAxisMinimum;
    }

    @Override
    public float getXRange() {
        return mXAxis.mAxisRange;
    }

    public PointF getCenter() {
        return new PointF(getWidth() / 2f, getHeight() / 2f);
    }

    @Override
    public PointF getCenterOffsets() {
        return mViewPortHandler.getContentCenter();
    }

    public void setDescription(String desc) {
        if (desc == null)
            desc = "";
        this.mDescription = desc;
    }

    public void setDescriptionPosition(float x, float y) {
        mDescriptionPosition = new PointF(x, y);
    }

    public void setDescriptionTypeface(Typeface t) {
        mDescPaint.setTypeface(t);
    }

    public void setDescriptionTextSize(float size) {
        if (size > 16f)
            size = 16f;
        if (size < 6f)
            size = 6f;
        mDescPaint.setTextSize(Utils.convertDpToPixel(size));
    }

    public void setDescriptionColor(int color) {
        mDescPaint.setColor(color);
    }

    public void setExtraOffsets(float left, float top, float right, float bottom) {
        setExtraLeftOffset(left);
        setExtraTopOffset(top);
        setExtraRightOffset(right);
        setExtraBottomOffset(bottom);
    }

    public void setExtraTopOffset(float offset) {
        mExtraTopOffset = Utils.convertDpToPixel(offset);
    }

    public float getExtraTopOffset() {
        return mExtraTopOffset;
    }

    public void setExtraRightOffset(float offset) {
        mExtraRightOffset = Utils.convertDpToPixel(offset);
    }

    public float getExtraRightOffset() {
        return mExtraRightOffset;
    }

    public void setExtraBottomOffset(float offset) {
        mExtraBottomOffset = Utils.convertDpToPixel(offset);
    }

    public float getExtraBottomOffset() {
        return mExtraBottomOffset;
    }

    public void setExtraLeftOffset(float offset) {
        mExtraLeftOffset = Utils.convertDpToPixel(offset);
    }

    public float getExtraLeftOffset() {
        return mExtraLeftOffset;
    }

    public void setLogEnabled(boolean enabled) {
        mLogEnabled = enabled;
    }

    public boolean isLogEnabled() {
        return mLogEnabled;
    }

    public void setNoDataText(String text) {
        mNoDataText = text;
    }

    public void setNoDataTextColor(int color) {
        mInfoPaint.setColor(color);
    }

    public void setNoDataTextDescription(String text) {
        mNoDataTextDescription = text;
    }

    public void setTouchEnabled(boolean enabled) {
        this.mTouchEnabled = enabled;
    }

    public void setMarkerView(MarkerView v) {
        mMarkerView = v;
    }

    public MarkerView getMarkerView() {
        return mMarkerView;
    }

    public Legend getLegend() {
        return mLegend;
    }

    public LegendRenderer getLegendRenderer() {
        return mLegendRenderer;
    }

    @Override
    public RectF getContentRect() {
        return mViewPortHandler.getContentRect();
    }

    public void disableScroll() {
        ViewParent parent = getParent();
        if (parent != null)
            parent.requestDisallowInterceptTouchEvent(true);
    }

    public void enableScroll() {
        ViewParent parent = getParent();
        if (parent != null)
            parent.requestDisallowInterceptTouchEvent(false);
    }

    public static final int PAINT_GRID_BACKGROUND = 4;

    public static final int PAINT_INFO = 7;

    public static final int PAINT_DESCRIPTION = 11;

    public static final int PAINT_HOLE = 13;

    public static final int PAINT_CENTER_TEXT = 14;

    public static final int PAINT_LEGEND_LABEL = 18;

    public void setPaint(Paint p, int which) {
        switch(which) {
            case PAINT_INFO:
                mInfoPaint = p;
                break;
            case PAINT_DESCRIPTION:
                mDescPaint = p;
                break;
        }
    }

    public Paint getPaint(int which) {
        switch(which) {
            case PAINT_INFO:
                return mInfoPaint;
            case PAINT_DESCRIPTION:
                return mDescPaint;
        }
        return null;
    }

    public boolean isDrawMarkerViewEnabled() {
        return mDrawMarkerViews;
    }

    public void setDrawMarkerViews(boolean enabled) {
        mDrawMarkerViews = enabled;
    }

    public List<Entry> getEntriesAtIndex(int xIndex) {
        List<Entry> vals = new ArrayList<Entry>();
        for (int i = 0; i < mData.getDataSetCount(); i++) {
            IDataSet set = mData.getDataSetByIndex(i);
            Entry e = set.getEntryForXPos(xIndex);
            if (e != null) {
                vals.add(e);
            }
        }
        return vals;
    }

    public T getData() {
        return mData;
    }

    public ViewPortHandler getViewPortHandler() {
        return mViewPortHandler;
    }

    public DataRenderer getRenderer() {
        return mRenderer;
    }

    public void setRenderer(DataRenderer renderer) {
        if (renderer != null)
            mRenderer = renderer;
    }

    public Highlighter getHighlighter() {
        return mHighlighter;
    }

    public void setHighlighter(ChartHighlighter highlighter) {
        mHighlighter = highlighter;
    }

    @Override
    public PointF getCenterOfView() {
        return getCenter();
    }

    public Bitmap getChartBitmap() {
        Bitmap returnedBitmap = Bitmap.createBitmap(getWidth(), getHeight(), Bitmap.Config.RGB_565);
        Canvas canvas = new Canvas(returnedBitmap);
        Drawable bgDrawable = getBackground();
        if (bgDrawable != null)
            bgDrawable.draw(canvas);
        else
            canvas.drawColor(Color.WHITE);
        draw(canvas);
        return returnedBitmap;
    }

    public boolean saveToPath(String title, String pathOnSD) {
        Bitmap b = getChartBitmap();
        OutputStream stream = null;
        try {
            stream = new FileOutputStream(Environment.getExternalStorageDirectory().getPath() + pathOnSD + "/" + title + ".png");
            b.compress(CompressFormat.PNG, 40, stream);
            stream.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean saveToGallery(String fileName, String subFolderPath, String fileDescription, Bitmap.CompressFormat format, int quality) {
        if (quality < 0 || quality > 100)
            quality = 50;
        long currentTime = System.currentTimeMillis();
        File extBaseDir = Environment.getExternalStorageDirectory();
        File file = new File(extBaseDir.getAbsolutePath() + "/DCIM/" + subFolderPath);
        if (!file.exists()) {
            if (!file.mkdirs()) {
                return false;
            }
        }
        String mimeType = "";
        switch(format) {
            case PNG:
                mimeType = "image/png";
                if (!fileName.endsWith(".png"))
                    fileName += ".png";
                break;
            case WEBP:
                mimeType = "image/webp";
                if (!fileName.endsWith(".webp"))
                    fileName += ".webp";
                break;
            case JPEG:
            default:
                mimeType = "image/jpeg";
                if (!(fileName.endsWith(".jpg") || fileName.endsWith(".jpeg")))
                    fileName += ".jpg";
                break;
        }
        String filePath = file.getAbsolutePath() + "/" + fileName;
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(filePath);
            Bitmap b = getChartBitmap();
            b.compress(format, quality, out);
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        long size = new File(filePath).length();
        ContentValues values = new ContentValues(8);
        values.put(Images.Media.TITLE, fileName);
        values.put(Images.Media.DISPLAY_NAME, fileName);
        values.put(Images.Media.DATE_ADDED, currentTime);
        values.put(Images.Media.MIME_TYPE, mimeType);
        values.put(Images.Media.DESCRIPTION, fileDescription);
        values.put(Images.Media.ORIENTATION, 0);
        values.put(Images.Media.DATA, filePath);
        values.put(Images.Media.SIZE, size);
        return getContext().getContentResolver().insert(Images.Media.EXTERNAL_CONTENT_URI, values) != null;
    }

    public boolean saveToGallery(String fileName, int quality) {
        return saveToGallery(fileName, "", "MPAndroidChart-Library Save", Bitmap.CompressFormat.JPEG, quality);
    }

    protected ArrayList<Runnable> mJobs = new ArrayList<Runnable>();

    public void removeViewportJob(Runnable job) {
        mJobs.remove(job);
    }

    public void clearAllViewportJobs() {
        mJobs.clear();
    }

    public void addViewportJob(Runnable job) {
        if (mViewPortHandler.hasChartDimens()) {
            post(job);
        } else {
            mJobs.add(job);
        }
    }

    public ArrayList<Runnable> getJobs() {
        return mJobs;
    }

    @Override
    protected void onLayout(boolean changed, int left, int top, int right, int bottom) {
        for (int i = 0; i < getChildCount(); i++) {
            getChildAt(i).layout(left, top, right, bottom);
        }
    }

    @Override
    protected void onMeasure(int widthMeasureSpec, int heightMeasureSpec) {
        super.onMeasure(widthMeasureSpec, heightMeasureSpec);
        int size = (int) Utils.convertDpToPixel(50f);
        setMeasuredDimension(Math.max(getSuggestedMinimumWidth(), resolveSize(size, widthMeasureSpec)), Math.max(getSuggestedMinimumHeight(), resolveSize(size, heightMeasureSpec)));
    }

    @Override
    protected void onSizeChanged(int w, int h, int oldw, int oldh) {
        if (mLogEnabled)
            Log.i(LOG_TAG, "OnSizeChanged()");
        if (w > 0 && h > 0 && w < 10000 && h < 10000) {
            mViewPortHandler.setChartDimens(w, h);
            if (mLogEnabled)
                Log.i(LOG_TAG, "Setting chart dimens, width: " + w + ", height: " + h);
            for (Runnable r : mJobs) {
                post(r);
            }
            mJobs.clear();
        }
        notifyDataSetChanged();
        super.onSizeChanged(w, h, oldw, oldh);
    }

    public void setHardwareAccelerationEnabled(boolean enabled) {
        if (android.os.Build.VERSION.SDK_INT >= 11) {
            if (enabled)
                setLayerType(View.LAYER_TYPE_HARDWARE, null);
            else
                setLayerType(View.LAYER_TYPE_SOFTWARE, null);
        } else {
            Log.e(LOG_TAG, "Cannot enable/disable hardware acceleration for devices below API level 11.");
        }
    }

    @Override
    protected void onDetachedFromWindow() {
        super.onDetachedFromWindow();
        if (mUnbind)
            unbindDrawables(this);
    }

    private boolean mUnbind = false;

    private void unbindDrawables(View view) {
        if (view.getBackground() != null) {
            view.getBackground().setCallback(null);
        }
        if (view instanceof ViewGroup) {
            for (int i = 0; i < ((ViewGroup) view).getChildCount(); i++) {
                unbindDrawables(((ViewGroup) view).getChildAt(i));
            }
            ((ViewGroup) view).removeAllViews();
        }
    }

    public void setUnbindEnabled(boolean enabled) {
        this.mUnbind = enabled;
    }
}