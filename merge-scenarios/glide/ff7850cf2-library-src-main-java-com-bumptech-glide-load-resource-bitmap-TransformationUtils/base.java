package com.bumptech.glide.load.resource.bitmap;

import android.annotation.TargetApi;
import android.graphics.Bitmap;
import android.graphics.BitmapShader;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Matrix;
import android.graphics.Paint;
import android.graphics.PorterDuff;
import android.graphics.PorterDuffXfermode;
import android.graphics.Rect;
import android.graphics.RectF;
import android.graphics.Shader;
import android.media.ExifInterface;
import android.os.Build;
import android.support.annotation.NonNull;
import android.util.Log;
import com.bumptech.glide.load.engine.bitmap_recycle.BitmapPool;
import com.bumptech.glide.util.Preconditions;

public final class TransformationUtils {

    private static final String TAG = "TransformationUtils";

    public static final int PAINT_FLAGS = Paint.DITHER_FLAG | Paint.FILTER_BITMAP_FLAG;

    private static final Paint DEFAULT_PAINT = new Paint(PAINT_FLAGS);

    private static final int CIRCLE_CROP_PAINT_FLAGS = PAINT_FLAGS | Paint.ANTI_ALIAS_FLAG;

    private static final Paint CIRCLE_CROP_SHAPE_PAINT = new Paint(CIRCLE_CROP_PAINT_FLAGS);

    private static final Paint CIRCLE_CROP_BITMAP_PAINT;

    static {
        CIRCLE_CROP_BITMAP_PAINT = new Paint(CIRCLE_CROP_PAINT_FLAGS);
        CIRCLE_CROP_BITMAP_PAINT.setXfermode(new PorterDuffXfermode(PorterDuff.Mode.SRC_IN));
    }

    private TransformationUtils() {
    }

    public static Bitmap centerCrop(@NonNull BitmapPool pool, @NonNull Bitmap inBitmap, int width, int height) {
        if (inBitmap.getWidth() == width && inBitmap.getHeight() == height) {
            return inBitmap;
        }
        final float scale;
        float dx = 0, dy = 0;
        Matrix m = new Matrix();
        if (inBitmap.getWidth() * height > width * inBitmap.getHeight()) {
            scale = (float) height / (float) inBitmap.getHeight();
            dx = (width - inBitmap.getWidth() * scale) * 0.5f;
        } else {
            scale = (float) width / (float) inBitmap.getWidth();
            dy = (height - inBitmap.getHeight() * scale) * 0.5f;
        }
        m.setScale(scale, scale);
        m.postTranslate((int) (dx + 0.5f), (int) (dy + 0.5f));
        Bitmap result = pool.get(width, height, getSafeConfig(inBitmap));
        TransformationUtils.setAlpha(inBitmap, result);
        Canvas canvas = new Canvas(result);
        canvas.drawBitmap(inBitmap, m, DEFAULT_PAINT);
        clear(canvas);
        return result;
    }

    public static Bitmap fitCenter(@NonNull BitmapPool pool, @NonNull Bitmap inBitmap, int width, int height) {
        if (inBitmap.getWidth() == width && inBitmap.getHeight() == height) {
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "requested target size matches input, returning input");
            }
            return inBitmap;
        }
        final float widthPercentage = width / (float) inBitmap.getWidth();
        final float heightPercentage = height / (float) inBitmap.getHeight();
        final float minPercentage = Math.min(widthPercentage, heightPercentage);
        final int targetWidth = (int) (minPercentage * inBitmap.getWidth());
        final int targetHeight = (int) (minPercentage * inBitmap.getHeight());
        if (inBitmap.getWidth() == targetWidth && inBitmap.getHeight() == targetHeight) {
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "adjusted target size matches input, returning input");
            }
            return inBitmap;
        }
        Bitmap.Config config = getSafeConfig(inBitmap);
        Bitmap toReuse = pool.get(targetWidth, targetHeight, config);
        TransformationUtils.setAlpha(inBitmap, toReuse);
        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            Log.v(TAG, "request: " + width + "x" + height);
            Log.v(TAG, "toFit:   " + inBitmap.getWidth() + "x" + inBitmap.getHeight());
            Log.v(TAG, "toReuse: " + toReuse.getWidth() + "x" + toReuse.getHeight());
            Log.v(TAG, "minPct:   " + minPercentage);
        }
        Canvas canvas = new Canvas(toReuse);
        Matrix matrix = new Matrix();
        matrix.setScale(minPercentage, minPercentage);
        canvas.drawBitmap(inBitmap, matrix, DEFAULT_PAINT);
        clear(canvas);
        return toReuse;
    }

    public static void setAlpha(Bitmap inBitmap, Bitmap outBitmap) {
        setAlphaIfAvailable(outBitmap, inBitmap.hasAlpha());
    }

    @TargetApi(Build.VERSION_CODES.HONEYCOMB_MR1)
    private static void setAlphaIfAvailable(Bitmap bitmap, boolean hasAlpha) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB_MR1 && bitmap != null) {
            bitmap.setHasAlpha(hasAlpha);
        }
    }

    public static Bitmap rotateImage(@NonNull Bitmap imageToOrient, int degreesToRotate) {
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

    public static Bitmap rotateImageExif(@NonNull BitmapPool pool, @NonNull Bitmap inBitmap, int exifOrientation) {
        final Matrix matrix = new Matrix();
        initializeMatrixForRotation(exifOrientation, matrix);
        if (matrix.isIdentity()) {
            return inBitmap;
        }
        final RectF newRect = new RectF(0, 0, inBitmap.getWidth(), inBitmap.getHeight());
        matrix.mapRect(newRect);
        final int newWidth = Math.round(newRect.width());
        final int newHeight = Math.round(newRect.height());
        Bitmap.Config config = getSafeConfig(inBitmap);
        Bitmap result = pool.get(newWidth, newHeight, config);
        matrix.postTranslate(-newRect.left, -newRect.top);
        final Canvas canvas = new Canvas(result);
        canvas.drawBitmap(inBitmap, matrix, DEFAULT_PAINT);
        clear(canvas);
        return result;
    }

    public static Bitmap circleCrop(@NonNull BitmapPool pool, @NonNull Bitmap inBitmap, int destWidth, int destHeight) {
        int destMinEdge = Math.min(destWidth, destHeight);
        float radius = destMinEdge / 2f;
        Rect destRect = new Rect((destWidth - destMinEdge) / 2, (destHeight - destMinEdge) / 2, destMinEdge, destMinEdge);
        int srcWidth = inBitmap.getWidth();
        int srcHeight = inBitmap.getHeight();
        int srcMinEdge = Math.min(srcWidth, srcHeight);
        Rect srcRect = new Rect((srcWidth - srcMinEdge) / 2, (srcHeight - srcMinEdge) / 2, srcMinEdge, srcMinEdge);
        Bitmap toTransform = getAlphaSafeBitmap(pool, inBitmap);
        Bitmap result = pool.get(destWidth, destHeight, getSafeConfig(toTransform));
        setAlphaIfAvailable(result, true);
        Canvas canvas = new Canvas(result);
        canvas.drawCircle(destRect.left + radius, destRect.top + radius, radius, CIRCLE_CROP_SHAPE_PAINT);
        canvas.drawBitmap(toTransform, srcRect, destRect, CIRCLE_CROP_BITMAP_PAINT);
        clear(canvas);
        if (!toTransform.equals(inBitmap)) {
            pool.put(toTransform);
        }
        return result;
    }

    private static Bitmap getAlphaSafeBitmap(@NonNull BitmapPool pool, @NonNull Bitmap maybeAlphaSafe) {
        if (Bitmap.Config.ARGB_8888.equals(maybeAlphaSafe.getConfig())) {
            return maybeAlphaSafe;
        }
        Bitmap argbBitmap = pool.get(maybeAlphaSafe.getWidth(), maybeAlphaSafe.getHeight(), Bitmap.Config.ARGB_8888);
        new Canvas(argbBitmap).drawBitmap(maybeAlphaSafe, 0, 0, null);
        return argbBitmap;
    }

    public static Bitmap roundedCorners(@NonNull BitmapPool pool, @NonNull Bitmap inBitmap, int width, int height, int roundingRadius) {
        Preconditions.checkArgument(width > 0, "width must be greater than 0.");
        Preconditions.checkArgument(height > 0, "height must be greater than 0.");
        Preconditions.checkArgument(roundingRadius > 0, "roundingRadius must be greater than 0.");
        Bitmap toTransform = getAlphaSafeBitmap(pool, inBitmap);
        Bitmap result = pool.get(width, height, Bitmap.Config.ARGB_8888);
        setAlphaIfAvailable(result, true);
        BitmapShader shader = new BitmapShader(toTransform, Shader.TileMode.CLAMP, Shader.TileMode.CLAMP);
        Paint paint = new Paint();
        paint.setAntiAlias(true);
        paint.setShader(shader);
        RectF rect = new RectF(0, 0, result.getWidth(), result.getHeight());
        Canvas canvas = new Canvas(result);
        canvas.drawColor(Color.TRANSPARENT, PorterDuff.Mode.CLEAR);
        canvas.drawRoundRect(rect, roundingRadius, roundingRadius, paint);
        clear(canvas);
        if (!toTransform.equals(inBitmap)) {
            pool.put(toTransform);
        }
        return result;
    }

    private static void clear(Canvas canvas) {
        canvas.setBitmap(null);
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