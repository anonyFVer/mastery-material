package com.bumptech.glide.load.resource.bitmap;

import android.annotation.TargetApi;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Build;
import android.util.Log;
import com.bumptech.glide.load.DecodeFormat;
import com.bumptech.glide.load.engine.bitmap_recycle.BitmapPool;
import com.bumptech.glide.util.ByteArrayPool;
import com.bumptech.glide.util.ExceptionCatchingInputStream;
import com.bumptech.glide.util.Util;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.Queue;
import java.util.Set;

public abstract class Downsampler implements BitmapDecoder<InputStream> {

    private static final String TAG = "Downsampler";

    private static final Set<ImageHeaderParser.ImageType> TYPES_THAT_USE_POOL = EnumSet.of(ImageHeaderParser.ImageType.JPEG, ImageHeaderParser.ImageType.PNG_A, ImageHeaderParser.ImageType.PNG);

    private static final Queue<BitmapFactory.Options> OPTIONS_QUEUE = Util.createQueue(0);

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
        if (Build.VERSION_CODES.HONEYCOMB <= Build.VERSION.SDK_INT) {
            decodeBitmapOptions.inBitmap = null;
            decodeBitmapOptions.inMutable = true;
        }
    }

    public static final Downsampler AT_LEAST = new Downsampler() {

        @Override
        protected int getSampleSize(int inWidth, int inHeight, int outWidth, int outHeight) {
            return Math.min(inHeight / outHeight, inWidth / outWidth);
        }

        @Override
        public String getId() {
            return "AT_LEAST.com.bumptech.glide.load.data.bitmap";
        }
    };

    public static final Downsampler AT_MOST = new Downsampler() {

        @Override
        protected int getSampleSize(int inWidth, int inHeight, int outWidth, int outHeight) {
            return Math.max(inHeight / outHeight, inWidth / outWidth);
        }

        @Override
        public String getId() {
            return "AT_MOST.com.bumptech.glide.load.data.bitmap";
        }
    };

    public static final Downsampler NONE = new Downsampler() {

        @Override
        protected int getSampleSize(int inWidth, int inHeight, int outWidth, int outHeight) {
            return 0;
        }

        @Override
        public String getId() {
            return "NONE.com.bumptech.glide.load.data.bitmap";
        }
    };

    private static final int MARK_POSITION = 5 * 1024 * 1024;

    @SuppressWarnings("resource")
    @Override
    public Bitmap decode(InputStream is, BitmapPool pool, int outWidth, int outHeight, DecodeFormat decodeFormat) {
        final ByteArrayPool byteArrayPool = ByteArrayPool.get();
        final byte[] bytesForOptions = byteArrayPool.getBytes();
        final byte[] bytesForStream = byteArrayPool.getBytes();
        final BitmapFactory.Options options = getDefaultOptions();
        final ExceptionCatchingInputStream stream = ExceptionCatchingInputStream.obtain(new RecyclableBufferedInputStream(is, bytesForStream));
        try {
            stream.mark(MARK_POSITION);
            int orientation = 0;
            try {
                orientation = new ImageHeaderParser(stream).getOrientation();
            } catch (IOException e) {
                if (Log.isLoggable(TAG, Log.WARN)) {
                    Log.w(TAG, "Cannot determine the image orientation from header", e);
                }
            } finally {
                try {
                    stream.reset();
                } catch (IOException e) {
                    if (Log.isLoggable(TAG, Log.WARN)) {
                        Log.w(TAG, "Cannot reset the input stream", e);
                    }
                }
            }
            options.inTempStorage = bytesForOptions;
            final int[] inDimens = getDimensions(stream, options);
            final int inWidth = inDimens[0];
            final int inHeight = inDimens[1];
            final int degreesToRotate = TransformationUtils.getExifOrientationDegrees(orientation);
            final int sampleSize;
            if (degreesToRotate == 90 || degreesToRotate == 270) {
                sampleSize = getSampleSize(inHeight, inWidth, outWidth, outHeight);
            } else {
                sampleSize = getSampleSize(inWidth, inHeight, outWidth, outHeight);
            }
            final Bitmap downsampled = downsampleWithSize(stream, options, pool, inWidth, inHeight, sampleSize, decodeFormat);
            final Exception streamException = stream.getException();
            if (streamException != null) {
                throw new RuntimeException(streamException);
            }
            Bitmap rotated = null;
            if (downsampled != null) {
                rotated = TransformationUtils.rotateImageExif(downsampled, pool, orientation);
                if (downsampled != rotated && !pool.put(downsampled)) {
                    downsampled.recycle();
                }
            }
            return rotated;
        } finally {
            byteArrayPool.releaseBytes(bytesForOptions);
            byteArrayPool.releaseBytes(bytesForStream);
            stream.release();
            releaseOptions(options);
        }
    }

    protected Bitmap downsampleWithSize(InputStream is, BitmapFactory.Options options, BitmapPool pool, int inWidth, int inHeight, int sampleSize, DecodeFormat decodeFormat) {
        Bitmap.Config config = getConfig(is, decodeFormat);
        options.inSampleSize = sampleSize;
        options.inPreferredConfig = config;
        if (options.inSampleSize == 1 || Build.VERSION_CODES.KITKAT <= Build.VERSION.SDK_INT) {
            if (shouldUsePool(is)) {
                setInBitmap(options, pool.get(inWidth, inHeight, config));
            }
        }
        return decodeStream(is, options);
    }

    private static boolean shouldUsePool(InputStream is) {
        if (Build.VERSION_CODES.KITKAT <= Build.VERSION.SDK_INT) {
            return true;
        }
        is.mark(1024);
        try {
            final ImageHeaderParser.ImageType type = new ImageHeaderParser(is).getType();
            return TYPES_THAT_USE_POOL.contains(type);
        } catch (IOException e) {
            if (Log.isLoggable(TAG, Log.WARN)) {
                Log.w(TAG, "Cannot determine the image type from header", e);
            }
        } finally {
            try {
                is.reset();
            } catch (IOException e) {
                if (Log.isLoggable(TAG, Log.WARN)) {
                    Log.w(TAG, "Cannot reset the input stream", e);
                }
            }
        }
        return false;
    }

    private static Bitmap.Config getConfig(InputStream is, DecodeFormat format) {
        if (format == DecodeFormat.ALWAYS_ARGB_8888) {
            return Bitmap.Config.ARGB_8888;
        }
        boolean hasAlpha = false;
        is.mark(1024);
        try {
            hasAlpha = new ImageHeaderParser(is).hasAlpha();
        } catch (IOException e) {
            if (Log.isLoggable(TAG, Log.WARN)) {
                Log.w(TAG, "Cannot determine whether the image has alpha or not from header for format " + format, e);
            }
        } finally {
            try {
                is.reset();
            } catch (IOException e) {
                if (Log.isLoggable(TAG, Log.WARN)) {
                    Log.w(TAG, "Cannot reset the input stream", e);
                }
            }
        }
        return hasAlpha ? Bitmap.Config.ARGB_8888 : Bitmap.Config.RGB_565;
    }

    protected abstract int getSampleSize(int inWidth, int inHeight, int outWidth, int outHeight);

    public int[] getDimensions(InputStream is, BitmapFactory.Options options) {
        options.inJustDecodeBounds = true;
        decodeStream(is, options);
        options.inJustDecodeBounds = false;
        return new int[] { options.outWidth, options.outHeight };
    }

    private static Bitmap decodeStream(InputStream is, BitmapFactory.Options options) {
        if (options.inJustDecodeBounds) {
            is.mark(MARK_POSITION);
        }
        final Bitmap result = BitmapFactory.decodeStream(is, null, options);
        try {
            if (options.inJustDecodeBounds) {
                is.reset();
            }
        } catch (IOException e) {
            if (Log.isLoggable(TAG, Log.ERROR)) {
                Log.e(TAG, "Exception loading inDecodeBounds=" + options.inJustDecodeBounds + " sample=" + options.inSampleSize, e);
            }
        }
        return result;
    }

    @TargetApi(Build.VERSION_CODES.HONEYCOMB)
    private static void setInBitmap(BitmapFactory.Options options, Bitmap recycled) {
        if (Build.VERSION_CODES.HONEYCOMB <= Build.VERSION.SDK_INT) {
            options.inBitmap = recycled;
        }
    }
}