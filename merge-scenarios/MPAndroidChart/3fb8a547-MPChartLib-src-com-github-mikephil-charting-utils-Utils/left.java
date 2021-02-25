package com.github.mikephil.charting.utils;

import android.annotation.SuppressLint;
import android.content.Context;
import android.content.res.Resources;
import android.graphics.Paint;
import android.graphics.PointF;
import android.graphics.Rect;
import android.os.Build;
import android.util.DisplayMetrics;
import android.util.Log;
import android.view.MotionEvent;
import android.view.VelocityTracker;
import android.view.View;
import android.view.ViewConfiguration;
import com.github.mikephil.charting.components.YAxis.AxisDependency;
import com.github.mikephil.charting.formatter.DefaultValueFormatter;
import com.github.mikephil.charting.formatter.ValueFormatter;
import java.util.List;

public abstract class Utils {

    private static DisplayMetrics mMetrics;

    private static int mMinimumFlingVelocity = 50;

    private static int mMaximumFlingVelocity = 8000;

    @SuppressWarnings("deprecation")
    public static void init(Context context) {
        if (context == null) {
            mMinimumFlingVelocity = ViewConfiguration.getMinimumFlingVelocity();
            mMaximumFlingVelocity = ViewConfiguration.getMaximumFlingVelocity();
            Log.e("MPChartLib-Utils", "Utils.init(...) PROVIDED CONTEXT OBJECT IS NULL");
        } else {
            ViewConfiguration viewConfiguration = ViewConfiguration.get(context);
            mMinimumFlingVelocity = viewConfiguration.getScaledMinimumFlingVelocity();
            mMaximumFlingVelocity = viewConfiguration.getScaledMaximumFlingVelocity();
            Resources res = context.getResources();
            mMetrics = res.getDisplayMetrics();
        }
    }

    @Deprecated
    public static void init(Resources res) {
        mMetrics = res.getDisplayMetrics();
        mMinimumFlingVelocity = ViewConfiguration.getMinimumFlingVelocity();
        mMaximumFlingVelocity = ViewConfiguration.getMaximumFlingVelocity();
    }

    public static float convertDpToPixel(float dp) {
        if (mMetrics == null) {
            Log.e("MPChartLib-Utils", "Utils NOT INITIALIZED. You need to call Utils.init(...) at least once before calling Utils.convertDpToPixel(...). Otherwise conversion does not take place.");
            return dp;
        }
        DisplayMetrics metrics = mMetrics;
        float px = dp * (metrics.densityDpi / 160f);
        return px;
    }

    public static float convertPixelsToDp(float px) {
        if (mMetrics == null) {
            Log.e("MPChartLib-Utils", "Utils NOT INITIALIZED. You need to call Utils.init(...) at least once before calling Utils.convertPixelsToDp(...). Otherwise conversion does not take place.");
            return px;
        }
        DisplayMetrics metrics = mMetrics;
        float dp = px / (metrics.densityDpi / 160f);
        return dp;
    }

    public static int calcTextWidth(Paint paint, String demoText) {
        return (int) paint.measureText(demoText);
    }

    public static int calcTextHeight(Paint paint, String demoText) {
        Rect r = new Rect();
        paint.getTextBounds(demoText, 0, demoText.length(), r);
        return r.height();
    }

    public static float getLineHeight(Paint paint) {
        Paint.FontMetrics metrics = paint.getFontMetrics();
        return metrics.descent - metrics.ascent;
    }

    public static float getLineSpacing(Paint paint) {
        Paint.FontMetrics metrics = paint.getFontMetrics();
        return metrics.ascent - metrics.top + metrics.bottom;
    }

    public static FSize calcTextSize(Paint paint, String demoText) {
        Rect r = new Rect();
        paint.getTextBounds(demoText, 0, demoText.length(), r);
        return new FSize(r.width(), r.height());
    }

    private static final int[] POW_10 = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };

    public static String formatNumber(float number, int digitCount, boolean separateThousands) {
        return formatNumber(number, digitCount, separateThousands, '.');
    }

    public static String formatNumber(float number, int digitCount, boolean separateThousands, char separateChar) {
        char[] out = new char[35];
        boolean neg = false;
        if (number == 0) {
            return "0";
        }
        boolean zero = false;
        if (number < 1 && number > -1) {
            zero = true;
        }
        if (number < 0) {
            neg = true;
            number = -number;
        }
        if (digitCount > POW_10.length) {
            digitCount = POW_10.length - 1;
        }
        number *= POW_10[digitCount];
        long lval = Math.round(number);
        int ind = out.length - 1;
        int charCount = 0;
        boolean decimalPointAdded = false;
        while (lval != 0 || charCount < (digitCount + 1)) {
            int digit = (int) (lval % 10);
            lval = lval / 10;
            out[ind--] = (char) (digit + '0');
            charCount++;
            if (charCount == digitCount) {
                out[ind--] = ',';
                charCount++;
                decimalPointAdded = true;
            } else if (separateThousands && lval != 0 && charCount > digitCount) {
                if (decimalPointAdded) {
                    if ((charCount - digitCount) % 4 == 0) {
                        out[ind--] = separateChar;
                        charCount++;
                    }
                } else {
                    if ((charCount - digitCount) % 4 == 3) {
                        out[ind--] = separateChar;
                        charCount++;
                    }
                }
            }
        }
        if (zero) {
            out[ind--] = '0';
            charCount += 1;
        }
        if (neg) {
            out[ind--] = '-';
            charCount += 1;
        }
        int start = out.length - charCount;
        return String.valueOf(out, start, out.length - start);
    }

    public static float roundToNextSignificant(double number) {
        final float d = (float) Math.ceil((float) Math.log10(number < 0 ? -number : number));
        final int pw = 1 - (int) d;
        final float magnitude = (float) Math.pow(10, pw);
        final long shifted = Math.round(number * magnitude);
        return shifted / magnitude;
    }

    public static int getDecimals(float number) {
        float i = roundToNextSignificant(number);
        return (int) Math.ceil(-Math.log10(i)) + 2;
    }

    public static int[] convertIntegers(List<Integer> integers) {
        int[] ret = new int[integers.size()];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = integers.get(i).intValue();
        }
        return ret;
    }

    public static String[] convertStrings(List<String> strings) {
        String[] ret = new String[strings.size()];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = strings.get(i);
        }
        return ret;
    }

    public static double nextUp(double d) {
        if (d == Double.POSITIVE_INFINITY)
            return d;
        else {
            d += 0.0d;
            return Double.longBitsToDouble(Double.doubleToRawLongBits(d) + ((d >= 0.0d) ? +1L : -1L));
        }
    }

    public static int getClosestDataSetIndex(List<SelectionDetail> valsAtIndex, float val, AxisDependency axis) {
        int index = -Integer.MAX_VALUE;
        float distance = Float.MAX_VALUE;
        for (int i = 0; i < valsAtIndex.size(); i++) {
            SelectionDetail sel = valsAtIndex.get(i);
            if (axis == null || sel.dataSet.getAxisDependency() == axis) {
                float cdistance = Math.abs((float) sel.val - val);
                if (cdistance < distance) {
                    index = valsAtIndex.get(i).dataSetIndex;
                    distance = cdistance;
                }
            }
        }
        return index;
    }

    public static float getMinimumDistance(List<SelectionDetail> valsAtIndex, float val, AxisDependency axis) {
        float distance = Float.MAX_VALUE;
        for (int i = 0; i < valsAtIndex.size(); i++) {
            SelectionDetail sel = valsAtIndex.get(i);
            if (sel.dataSet.getAxisDependency() == axis) {
                float cdistance = Math.abs(sel.val - val);
                if (cdistance < distance) {
                    distance = cdistance;
                }
            }
        }
        return distance;
    }

    public static boolean needsDefaultFormatter(ValueFormatter formatter) {
        if (formatter == null)
            return true;
        if (formatter instanceof DefaultValueFormatter)
            return true;
        return false;
    }

    public static PointF getPosition(PointF center, float dist, float angle) {
        PointF p = new PointF((float) (center.x + dist * Math.cos(Math.toRadians(angle))), (float) (center.y + dist * Math.sin(Math.toRadians(angle))));
        return p;
    }

    public static void velocityTrackerPointerUpCleanUpIfNecessary(MotionEvent ev, VelocityTracker tracker) {
        tracker.computeCurrentVelocity(1000, mMaximumFlingVelocity);
        final int upIndex = ev.getActionIndex();
        final int id1 = ev.getPointerId(upIndex);
        final float x1 = tracker.getXVelocity(id1);
        final float y1 = tracker.getYVelocity(id1);
        for (int i = 0, count = ev.getPointerCount(); i < count; i++) {
            if (i == upIndex)
                continue;
            final int id2 = ev.getPointerId(i);
            final float x = x1 * tracker.getXVelocity(id2);
            final float y = y1 * tracker.getYVelocity(id2);
            final float dot = x + y;
            if (dot < 0) {
                tracker.clear();
                break;
            }
        }
    }

    @SuppressLint("NewApi")
    public static void postInvalidateOnAnimation(View view) {
        if (Build.VERSION.SDK_INT >= 16)
            view.postInvalidateOnAnimation();
        else
            view.postInvalidateDelayed(10);
    }

    public static int getMinimumFlingVelocity() {
        return mMinimumFlingVelocity;
    }

    public static int getMaximumFlingVelocity() {
        return mMaximumFlingVelocity;
    }

    public static float getNormalizedAngle(float angle) {
        while (angle < 0.f) angle += 360.f;
        return angle % 360.f;
    }
}