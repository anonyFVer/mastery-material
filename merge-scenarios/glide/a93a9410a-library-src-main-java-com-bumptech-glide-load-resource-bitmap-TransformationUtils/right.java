package com.bumptech.glide.load.resource.bitmap;

import android.annotation.TargetApi;
import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Matrix;
import android.graphics.Paint;
import android.graphics.PorterDuff;
import android.graphics.PorterDuffXfermode;
import android.graphics.Rect;
import android.graphics.RectF;
import android.media.ExifInterface;
import android.os.Build;
import android.util.Log;
import com.bumptech.glide.load.engine.bitmap_recycle.BitmapPool;

public final class TransformationUtils {

    private static final String TAG = "TransformationUtils";

    public static final int PAINT_FLAGS = Paint.DITHER_FLAG | Paint.FILTER_BITMAP_FLAG;

    private TransformationUtils() {
    }

    public static Bitmap centerCrop(Bitmap recycled, Bitmap toCrop, int width, int height) {
        if (toCrop == null) {
            return null;
        } else if (toCrop.getWidth() == width && toCrop.getHeight() == height) {
            return toCrop;
        }
        final float scale;
        float dx = 0, dy = 0;
        Matrix m = new Matrix();
        if (toCrop.getWidth() * height > width * toCrop.getHeight()) {
            scale = (float) height / (float) toCrop.getHeight();
            dx = (width - toCrop.getWidth() * scale) * 0.5f;
        } else {
            scale = (float) width / (float) toCrop.getWidth();
            dy = (height - toCrop.getHeight() * scale) * 0.5f;
        }
        m.setScale(scale, scale);
        m.postTranslate((int) (dx + 0.5f), (int) (dy + 0.5f));
        final Bitmap result;
        if (recycled != null) {
            result = recycled;
        } else {
            result = Bitmap.createBitmap(width, height, getSafeConfig(toCrop));
        }
        TransformationUtils.setAlpha(toCrop, result);
        Canvas canvas = new Canvas(result);
        Paint paint = new Paint(PAINT_FLAGS);
        canvas.drawBitmap(toCrop, m, paint);
        return result;
    }

    public static Bitmap fitCenter(Bitmap toFit, BitmapPool pool, int width, int height) {
        if (toFit.getWidth() == width && toFit.getHeight() == height) {
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "requested target size matches input, returning input");
            }
            return toFit;
        }
        final float widthPercentage = width / (float) toFit.getWidth();
        final float heightPercentage = height / (float) toFit.getHeight();
        final float minPercentage = Math.min(widthPercentage, heightPercentage);
        final int targetWidth = (int) (minPercentage * toFit.getWidth());
        final int targetHeight = (int) (minPercentage * toFit.getHeight());
        if (toFit.getWidth() == targetWidth && toFit.getHeight() == targetHeight) {
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "adjusted target size matches input, returning input");
            }
            return toFit;
        }
        Bitmap.Config config = getSafeConfig(toFit);
        Bitmap toReuse = pool.get(targetWidth, targetHeight, config);
        if (toReuse == null) {
            toReuse = Bitmap.createBitmap(targetWidth, targetHeight, config);
        }
        TransformationUtils.setAlpha(toFit, toReuse);
        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            Log.v(TAG, "request: " + width + "x" + height);
            Log.v(TAG, "toFit:   " + toFit.getWidth() + "x" + toFit.getHeight());
            Log.v(TAG, "toReuse: " + toReuse.getWidth() + "x" + toReuse.getHeight());
            Log.v(TAG, "minPct:   " + minPercentage);
        }
        Canvas canvas = new Canvas(toReuse);
        Matrix matrix = new Matrix();
        matrix.setScale(minPercentage, minPercentage);
        Paint paint = new Paint(PAINT_FLAGS);
        canvas.drawBitmap(toFit, matrix, paint);
        return toReuse;
    }

    public static void setAlpha(Bitmap toTransform, Bitmap outBitmap) {
        setAlphaIfAvailable(outBitmap, toTransform.hasAlpha());
    }

    @TargetApi(Build.VERSION_CODES.HONEYCOMB_MR1)
    private static void setAlphaIfAvailable(Bitmap bitmap, boolean hasAlpha) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB_MR1 && bitmap != null) {
            bitmap.setHasAlpha(hasAlpha);
        }
    }

    @TargetApi(Build.VERSION_CODES.ECLAIR)
    @Deprecated
    public static int getOrientation(String pathToOriginal) {
        int degreesToRotate = 0;
        try {
            ExifInterface exif = new ExifInterface(pathToOriginal);
            int orientation = exif.getAttributeInt(ExifInterface.TAG_ORIENTATION, ExifInterface.ORIENTATION_UNDEFINED);
            return getExifOrientationDegrees(orientation);
        } catch (Exception e) {
            if (Log.isLoggable(TAG, Log.ERROR)) {
                Log.e(TAG, "Unable to get orientation for image with path=" + pathToOriginal, e);
            }
        }
        return degreesToRotate;
    }

    @Deprecated
    public static Bitmap orientImage(String pathToOriginal, Bitmap imageToOrient) {
        int degreesToRotate = getOrientation(pathToOriginal);
        return rotateImage(imageToOrient, degreesToRotate);
    }

    public static Bitmap rotateImage(Bitmap imageToOrient, int degreesToRotate) {
        Bitmap result = imageToOrient;
        try {
            if (degreesToRotate != 0) {
                Matrix matrix = new Matrix();
                matrix.setRotate(degreesToRotate);
                result = Bitmap.createBitmap(imageToOrient, 0, 0, imageToOrient.getWidth(), imageToOrient.getHeight(), matrix, true);
            }
        } catch (Exception e) {
            if (Log.isLoggable(TAG, Log.ERROR)) {
                Log.e(TAG, "Exception when trying to orient image", e);
            }
        }
        return result;
    }

    public static int getExifOrientationDegrees(int exifOrientation) {
        final int degreesToRotate;
        switch(exifOrientation) {
            case ExifInterface.ORIENTATION_TRANSPOSE:
            case ExifInterface.ORIENTATION_ROTATE_90:
                degreesToRotate = 90;
                break;
            case ExifInterface.ORIENTATION_ROTATE_180:
            case ExifInterface.ORIENTATION_FLIP_VERTICAL:
                degreesToRotate = 180;
                break;
            case ExifInterface.ORIENTATION_TRANSVERSE:
            case ExifInterface.ORIENTATION_ROTATE_270:
                degreesToRotate = 270;
                break;
            default:
                degreesToRotate = 0;
        }
        return degreesToRotate;
    }

    public static Bitmap rotateImageExif(Bitmap toOrient, BitmapPool pool, int exifOrientation) {
        final Matrix matrix = new Matrix();
        initializeMatrixForRotation(exifOrientation, matrix);
        if (matrix.isIdentity()) {
            return toOrient;
        }
        final RectF newRect = new RectF(0, 0, toOrient.getWidth(), toOrient.getHeight());
        matrix.mapRect(newRect);
        final int newWidth = Math.round(newRect.width());
        final int newHeight = Math.round(newRect.height());
        Bitmap.Config config = getSafeConfig(toOrient);
        Bitmap result = pool.get(newWidth, newHeight, config);
        if (result == null) {
            result = Bitmap.createBitmap(newWidth, newHeight, config);
        }
        matrix.postTranslate(-newRect.left, -newRect.top);
        final Canvas canvas = new Canvas(result);
        final Paint paint = new Paint(PAINT_FLAGS);
        canvas.drawBitmap(toOrient, matrix, paint);
        return result;
    }

    public static Bitmap circleCrop(Bitmap recycled, Bitmap toCrop, int destWidth, int destHeight) {
        if (toCrop == null) {
            return null;
        }
        Bitmap result = (recycled != null) ? recycled : Bitmap.createBitmap(destWidth, destHeight, getSafeConfig(toCrop));
        setAlphaIfAvailable(result, true);
        int destMinEdge = Math.min(destWidth, destHeight);
        float radius = destMinEdge / 2f;
        Rect destRect = new Rect((destWidth - destMinEdge) / 2, (destHeight - destMinEdge) / 2, destMinEdge, destMinEdge);
        int srcWidth = toCrop.getWidth();
        int srcHeight = toCrop.getHeight();
        int srcMinEdge = Math.min(srcWidth, srcHeight);
        Rect srcRect = new Rect((srcWidth - srcMinEdge) / 2, (srcHeight - srcMinEdge) / 2, srcMinEdge, srcMinEdge);
        Canvas canvas = new Canvas(result);
        Paint paint = new Paint(PAINT_FLAGS);
        paint.setAntiAlias(true);
        canvas.drawCircle(destRect.left + radius, destRect.top + radius, radius, paint);
        paint.setXfermode(new PorterDuffXfermode(PorterDuff.Mode.SRC_IN));
        canvas.drawBitmap(toCrop, srcRect, destRect, paint);
        return result;
    }

    private static Bitmap.Config getSafeConfig(Bitmap bitmap) {
        return bitmap.getConfig() != null ? bitmap.getConfig() : Bitmap.Config.ARGB_8888;
    }

    static void initializeMatrixForRotation(int exifOrientation, Matrix matrix) {
        switch(exifOrientation) {
            case ExifInterface.ORIENTATION_FLIP_HORIZONTAL:
                matrix.setScale(-1, 1);
                break;
            case ExifInterface.ORIENTATION_ROTATE_180:
                matrix.setRotate(180);
                break;
            case ExifInterface.ORIENTATION_FLIP_VERTICAL:
                matrix.setRotate(180);
                matrix.postScale(-1, 1);
                break;
            case ExifInterface.ORIENTATION_TRANSPOSE:
                matrix.setRotate(90);
                matrix.postScale(-1, 1);
                break;
            case ExifInterface.ORIENTATION_ROTATE_90:
                matrix.setRotate(90);
                break;
            case ExifInterface.ORIENTATION_TRANSVERSE:
                matrix.setRotate(-90);
                matrix.postScale(-1, 1);
                break;
            case ExifInterface.ORIENTATION_ROTATE_270:
                matrix.setRotate(-90);
                break;
            default:
        }
    }
}