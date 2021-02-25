package com.bumptech.glide.load.resource.bitmap;

import android.annotation.TargetApi;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Build;
import android.util.DisplayMetrics;
import android.util.Log;
import com.bumptech.glide.Logs;
import com.bumptech.glide.load.DecodeFormat;
import com.bumptech.glide.load.Option;
import com.bumptech.glide.load.Options;
import com.bumptech.glide.load.engine.Resource;
import com.bumptech.glide.load.engine.bitmap_recycle.BitmapPool;
import com.bumptech.glide.load.engine.bitmap_recycle.ByteArrayPool;
import com.bumptech.glide.load.resource.bitmap.DownsampleStrategy.SampleSizeRounding;
import com.bumptech.glide.request.target.Target;
import com.bumptech.glide.util.Preconditions;
import com.bumptech.glide.util.Util;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Queue;
import java.util.Set;

public final class Downsampler {

    private static final String TAG = "Downsampler";

    public static final Option<DecodeFormat> DECODE_FORMAT = Option.memory("com.bumptech.glide.load.resource.bitmap.Downsampler.DecodeFormat", DecodeFormat.DEFAULT);

    public static final Option<DownsampleStrategy> DOWNSAMPLE_STRATEGY = Option.memory("com.bumptech.glide.load.resource.bitmap.Downsampler.DownsampleStrategy", DownsampleStrategy.AT_LEAST);

    private static final DecodeCallbacks EMPTY_CALLBACKS = new DecodeCallbacks() {

        @Override
        public void onObtainBounds() {
        }

        @Override
        public void onDecodeComplete(BitmapPool bitmapPool, Bitmap downsampled) throws IOException {
        }
    };

    private static final Set<ImageHeaderParser.ImageType> TYPES_THAT_USE_POOL_PRE_KITKAT = Collections.unmodifiableSet(EnumSet.of(ImageHeaderParser.ImageType.JPEG, ImageHeaderParser.ImageType.PNG_A, ImageHeaderParser.ImageType.PNG));

    private static final Queue<BitmapFactory.Options> OPTIONS_QUEUE = Util.createQueue(0);

    private static final int MARK_POSITION = 5 * 1024 * 1024;

    private final BitmapPool bitmapPool;

    private final DisplayMetrics displayMetrics;

    private final ByteArrayPool byteArrayPool;

    public Downsampler(DisplayMetrics displayMetrics, BitmapPool bitmapPool, ByteArrayPool byteArrayPool) {
        this.displayMetrics = Preconditions.checkNotNull(displayMetrics);
        this.bitmapPool = Preconditions.checkNotNull(bitmapPool);
        this.byteArrayPool = Preconditions.checkNotNull(byteArrayPool);
    }

    public boolean handles(InputStream is) {
        return true;
    }

    public boolean handles(ByteBuffer byteBuffer) {
        return true;
    }

    public Resource<Bitmap> decode(InputStream is, int outWidth, int outHeight, Options options) throws IOException {
        return decode(is, outWidth, outHeight, options, EMPTY_CALLBACKS);
    }

    @SuppressWarnings("resource")
    public Resource<Bitmap> decode(InputStream is, int requestedWidth, int requestedHeight, Options options, DecodeCallbacks callbacks) throws IOException {
        Preconditions.checkArgument(is.markSupported(), "You must provide an InputStream that supports" + " mark()");
        byte[] bytesForOptions = byteArrayPool.get(ByteArrayPool.STANDARD_BUFFER_SIZE_BYTES);
        BitmapFactory.Options bitmapFactoryOptions = getDefaultOptions();
        bitmapFactoryOptions.inTempStorage = bytesForOptions;
        DecodeFormat decodeFormat = options.get(DECODE_FORMAT);
        DownsampleStrategy downsampleStrategy = options.get(DOWNSAMPLE_STRATEGY);
        try {
            Bitmap result = decodeFromWrappedStreams(is, bitmapFactoryOptions, downsampleStrategy, decodeFormat, requestedWidth, requestedHeight, callbacks);
            return BitmapResource.obtain(result, bitmapPool);
        } finally {
            releaseOptions(bitmapFactoryOptions);
            byteArrayPool.put(bytesForOptions);
        }
    }

    private Bitmap decodeFromWrappedStreams(InputStream is, BitmapFactory.Options options, DownsampleStrategy downsampleStrategy, DecodeFormat decodeFormat, int requestedWidth, int requestedHeight, DecodeCallbacks callbacks) throws IOException {
        int[] sourceDimensions = getDimensions(is, options, callbacks);
        int sourceWidth = sourceDimensions[0];
        int sourceHeight = sourceDimensions[1];
        String sourceMimeType = options.outMimeType;
        int orientation = getOrientation(is);
        int degreesToRotate = TransformationUtils.getExifOrientationDegrees(getOrientation(is));
        options.inPreferredConfig = getConfig(is, decodeFormat);
        if (options.inPreferredConfig != Bitmap.Config.ARGB_8888) {
            options.inDither = true;
        }
        calculateScaling(downsampleStrategy, degreesToRotate, sourceWidth, sourceHeight, requestedWidth, requestedHeight, options);
        Bitmap downsampled = downsampleWithSize(is, options, bitmapPool, sourceWidth, sourceHeight, callbacks);
        callbacks.onDecodeComplete(bitmapPool, downsampled);
        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            logDecode(sourceWidth, sourceHeight, sourceMimeType, options, downsampled, requestedWidth, requestedHeight);
        }
        Bitmap rotated = null;
        if (downsampled != null) {
            downsampled.setDensity(displayMetrics.densityDpi);
            rotated = TransformationUtils.rotateImageExif(downsampled, bitmapPool, orientation);
            if (!downsampled.equals(rotated) && !bitmapPool.put(downsampled)) {
                downsampled.recycle();
            }
        }
        return rotated;
    }

    static void calculateScaling(DownsampleStrategy downsampleStrategy, int degreesToRotate, int sourceWidth, int sourceHeight, int requestedWidth, int requestedHeight, BitmapFactory.Options options) {
        if (sourceWidth <= 0 || sourceHeight <= 0) {
            return;
        }
        int targetHeight = requestedHeight == Target.SIZE_ORIGINAL ? sourceHeight : requestedHeight;
        int targetWidth = requestedWidth == Target.SIZE_ORIGINAL ? sourceWidth : requestedWidth;
        final float exactScaleFactor;
        if (degreesToRotate == 90 || degreesToRotate == 270) {
            exactScaleFactor = downsampleStrategy.getScaleFactor(sourceHeight, sourceWidth, targetWidth, targetHeight);
        } else {
            exactScaleFactor = downsampleStrategy.getScaleFactor(sourceWidth, sourceHeight, targetWidth, targetHeight);
        }
        if (exactScaleFactor <= 0f) {
            throw new IllegalArgumentException("Cannot scale with factor: " + exactScaleFactor + " from: " + downsampleStrategy);
        }
        SampleSizeRounding rounding = downsampleStrategy.getSampleSizeRounding(sourceWidth, sourceHeight, targetWidth, targetHeight);
        if (rounding == null) {
            throw new IllegalArgumentException("Cannot round with null rounding");
        }
        int outWidth = (int) (exactScaleFactor * sourceWidth + 0.5f);
        int outHeight = (int) (exactScaleFactor * sourceHeight + 0.5f);
        int widthScaleFactor = sourceWidth / outWidth;
        int heightScaleFactor = sourceHeight / outHeight;
        int scaleFactor = rounding == SampleSizeRounding.MEMORY ? Math.max(widthScaleFactor, heightScaleFactor) : Math.min(widthScaleFactor, heightScaleFactor);
        int powerOfTwoSampleSize = Math.max(1, Integer.highestOneBit(scaleFactor));
        if (rounding == SampleSizeRounding.MEMORY && powerOfTwoSampleSize < (1.f / exactScaleFactor)) {
            powerOfTwoSampleSize = powerOfTwoSampleSize << 1;
        }
        float adjustedScaleFactor = powerOfTwoSampleSize * exactScaleFactor;
        options.inSampleSize = powerOfTwoSampleSize;
        options.inTargetDensity = (int) (1000 * adjustedScaleFactor + 0.5f);
        options.inDensity = 1000;
        if (isScaling(options)) {
            options.inScaled = true;
        } else {
            options.inDensity = options.inTargetDensity = 0;
        }
        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            Log.v(TAG, "Calculate scaling" + ", source: [" + sourceWidth + "x" + sourceHeight + "]" + ", target: [" + targetWidth + "x" + targetHeight + "]" + ", exact scale factor: " + exactScaleFactor + ", power of 2 sample size: " + powerOfTwoSampleSize + ", adjusted scale factor: " + adjustedScaleFactor + ", target density: " + options.inTargetDensity + ", density: " + options.inDensity);
        }
    }

    private int getOrientation(InputStream is) throws IOException {
        is.mark(MARK_POSITION);
        int orientation = 0;
        try {
            orientation = new ImageHeaderParser(is, byteArrayPool).getOrientation();
        } catch (IOException e) {
            if (Logs.isEnabled(Log.DEBUG)) {
                Logs.log(Log.DEBUG, "Cannot determine the image orientation from header", e);
            }
        } finally {
            is.reset();
        }
        return orientation;
    }

    private Bitmap downsampleWithSize(InputStream is, BitmapFactory.Options options, BitmapPool pool, int sourceWidth, int sourceHeight, DecodeCallbacks callbacks) throws IOException {
        if ((options.inSampleSize == 1 || Build.VERSION_CODES.KITKAT <= Build.VERSION.SDK_INT) && shouldUsePool(is)) {
            float densityMultiplier = isScaling(options) ? (float) options.inTargetDensity / options.inDensity : 1f;
            int sampleSize = options.inSampleSize;
            int downsampledWidth = (int) Math.ceil(sourceWidth / (float) sampleSize);
            int downsampledHeight = (int) Math.ceil(sourceHeight / (float) sampleSize);
            int expectedWidth = (int) Math.ceil(downsampledWidth * densityMultiplier);
            int expectedHeight = (int) Math.ceil(downsampledHeight * densityMultiplier);
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "Calculated target [" + expectedWidth + "x" + expectedHeight + "] for source" + " [" + sourceWidth + "x" + sourceHeight + "]" + ", sampleSize: " + sampleSize + ", targetDensity: " + options.inTargetDensity + ", density: " + options.inDensity + ", density multiplier: " + densityMultiplier);
            }
            setInBitmap(options, pool.getDirty(expectedWidth, expectedHeight, options.inPreferredConfig));
        }
        return decodeStream(is, options, callbacks);
    }

    private boolean shouldUsePool(InputStream is) throws IOException {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.KITKAT) {
            return true;
        }
        is.mark(MARK_POSITION);
        try {
            final ImageHeaderParser.ImageType type = new ImageHeaderParser(is, byteArrayPool).getType();
            return TYPES_THAT_USE_POOL_PRE_KITKAT.contains(type);
        } catch (IOException e) {
            if (Logs.isEnabled(Log.DEBUG)) {
                Logs.log(Log.DEBUG, "Cannot determine the image type from header", e);
            }
        } finally {
            is.reset();
        }
        return false;
    }

    private Bitmap.Config getConfig(InputStream is, DecodeFormat format) throws IOException {
        if (format == DecodeFormat.PREFER_ARGB_8888 || Build.VERSION.SDK_INT == Build.VERSION_CODES.JELLY_BEAN) {
            return Bitmap.Config.ARGB_8888;
        }
        boolean hasAlpha = false;
        is.mark(MARK_POSITION);
        try {
            hasAlpha = new ImageHeaderParser(is, byteArrayPool).hasAlpha();
        } catch (IOException e) {
            if (Logs.isEnabled(Log.DEBUG)) {
                Logs.log(Log.DEBUG, "Cannot determine whether the image has alpha or not from header for" + " format " + format, e);
            }
        } finally {
            is.reset();
        }
        return hasAlpha ? Bitmap.Config.ARGB_8888 : Bitmap.Config.RGB_565;
    }

    private static int[] getDimensions(InputStream is, BitmapFactory.Options options, DecodeCallbacks decodeCallbacks) throws IOException {
        options.inJustDecodeBounds = true;
        decodeStream(is, options, decodeCallbacks);
        options.inJustDecodeBounds = false;
        return new int[] { options.outWidth, options.outHeight };
    }

    private static Bitmap decodeStream(InputStream is, BitmapFactory.Options options, DecodeCallbacks callbacks) throws IOException {
        if (options.inJustDecodeBounds) {
            is.mark(MARK_POSITION);
        } else {
            callbacks.onObtainBounds();
        }
        int sourceWidth = options.outWidth;
        int sourceHeight = options.outHeight;
        String outMimeType = options.outMimeType;
        final Bitmap result;
        try {
            result = BitmapFactory.decodeStream(is, null, options);
        } catch (IllegalArgumentException e) {
            throw newIoExceptionForInBitmapAssertion(e, sourceWidth, sourceHeight, outMimeType, options);
        }
        if (options.inJustDecodeBounds) {
            is.reset();
        }
        return result;
    }

    private static boolean isScaling(BitmapFactory.Options options) {
        return options.inTargetDensity > 0 && options.inDensity > 0 && options.inTargetDensity != options.inDensity;
    }

    private static void logDecode(int sourceWidth, int sourceHeight, String outMimeType, BitmapFactory.Options options, Bitmap result, int requestedWidth, int requestedHeight) {
        Log.v(TAG, "Decoded " + getBitmapString(result) + " from [" + sourceWidth + "x" + sourceHeight + "] " + outMimeType + " with inBitmap " + getInBitmapString(options) + " for [" + requestedWidth + "x" + requestedHeight + "]" + ", sample size: " + options.inSampleSize + ", density: " + options.inDensity + ", target density: " + options.inTargetDensity + ", thread: " + Thread.currentThread().getName());
    }

    @TargetApi(Build.VERSION_CODES.HONEYCOMB)
    private static String getInBitmapString(BitmapFactory.Options options) {
        return Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB ? getBitmapString(options.inBitmap) : null;
    }

    @TargetApi(Build.VERSION_CODES.KITKAT)
    private static String getBitmapString(Bitmap bitmap) {
        final String result;
        if (bitmap == null) {
            result = null;
        } else {
            String sizeString = Build.VERSION.SDK_INT >= Build.VERSION_CODES.KITKAT ? " (" + bitmap.getAllocationByteCount() + ")" : "";
            result = "[" + bitmap.getWidth() + "x" + bitmap.getHeight() + "] " + bitmap.getConfig() + sizeString;
        }
        return result;
    }

    @TargetApi(Build.VERSION_CODES.HONEYCOMB)
    private static IOException newIoExceptionForInBitmapAssertion(IllegalArgumentException e, int outWidth, int outHeight, String outMimeType, BitmapFactory.Options options) {
        return new IOException("Exception decoding bitmap" + ", outWidth: " + outWidth + ", outHeight: " + outHeight + ", outMimeType: " + outMimeType + ", inBitmap: " + getInBitmapString(options), e);
    }

    @TargetApi(Build.VERSION_CODES.HONEYCOMB)
    private static void setInBitmap(BitmapFactory.Options options, Bitmap recycled) {
        if (Build.VERSION_CODES.HONEYCOMB <= Build.VERSION.SDK_INT) {
            options.inBitmap = recycled;
        }
    }

    @TargetApi(Build.VERSION_CODES.HONEYCOMB)
    private static synchronized BitmapFactory.Options getDefaultOptions() {
        BitmapFactory.Options decodeBitmapOptions;
        synchronized (OPTIONS_QUEUE) {
            decodeBitmapOptions = OPTIONS_QUEUE.poll();
        }
        if (decodeBitmapOptions == null) {
            decodeBitmapOptions = new BitmapFactory.Options();
            resetOptions(decodeBitmapOptions);
        }
        return decodeBitmapOptions;
    }

    private static void releaseOptions(BitmapFactory.Options decodeBitmapOptions) {
        resetOptions(decodeBitmapOptions);
        synchronized (OPTIONS_QUEUE) {
            OPTIONS_QUEUE.offer(decodeBitmapOptions);
        }
    }

    @TargetApi(Build.VERSION_CODES.HONEYCOMB)
    private static void resetOptions(BitmapFactory.Options decodeBitmapOptions) {
        decodeBitmapOptions.inTempStorage = null;
        decodeBitmapOptions.inDither = false;
        decodeBitmapOptions.inScaled = false;
        decodeBitmapOptions.inSampleSize = 1;
        decodeBitmapOptions.inPreferredConfig = null;
        decodeBitmapOptions.inJustDecodeBounds = false;
        decodeBitmapOptions.inDensity = 0;
        decodeBitmapOptions.inTargetDensity = 0;
        decodeBitmapOptions.outWidth = 0;
        decodeBitmapOptions.outHeight = 0;
        decodeBitmapOptions.outMimeType = null;
        if (Build.VERSION_CODES.HONEYCOMB <= Build.VERSION.SDK_INT) {
            decodeBitmapOptions.inBitmap = null;
            decodeBitmapOptions.inMutable = true;
        }
    }

    public interface DecodeCallbacks {

        void onObtainBounds();

        void onDecodeComplete(BitmapPool bitmapPool, Bitmap downsampled) throws IOException;
    }
}