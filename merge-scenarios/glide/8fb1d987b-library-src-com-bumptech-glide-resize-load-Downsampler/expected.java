package com.bumptech.glide.resize.load;

import static com.bumptech.glide.resize.load.ImageHeaderParser.ImageType;
import static com.bumptech.glide.resize.load.ImageHeaderParser.ImageType.PNG_A;
import static com.bumptech.glide.resize.load.ImageHeaderParser.ImageType.JPEG;
import static com.bumptech.glide.resize.load.ImageHeaderParser.ImageType.PNG;
import android.annotation.TargetApi;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Build;
import android.util.Log;
import com.bumptech.glide.resize.RecyclableBufferedInputStream;
import com.bumptech.glide.resize.bitmap_recycle.BitmapPool;
import com.bumptech.glide.util.ByteArrayPool;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.Set;

public abstract class Downsampler implements BitmapDecoder<InputStream> {

    private static final String TAG = "Downsampler";

    private static final boolean CAN_RECYCLE = Build.VERSION.SDK_INT >= 11;

    private static final Set<ImageType> TYPES_THAT_USE_POOL = EnumSet.of(JPEG, PNG_A, PNG);

    @TargetApi(11)
    private static BitmapFactory.Options getDefaultOptions() {
        BitmapFactory.Options decodeBitmapOptions = new BitmapFactory.Options();
        decodeBitmapOptions.inDither = false;
        decodeBitmapOptions.inScaled = false;
        decodeBitmapOptions.inPreferredConfig = Bitmap.Config.RGB_565;
        decodeBitmapOptions.inSampleSize = 1;
        if (CAN_RECYCLE) {
            decodeBitmapOptions.inMutable = true;
        }
        return decodeBitmapOptions;
    }

    private final String id = getClass().toString();

    public static Downsampler AT_LEAST = new Downsampler() {

        @Override
        protected int getSampleSize(int inWidth, int inHeight, int outWidth, int outHeight) {
            return Math.min(inHeight / outHeight, inWidth / outWidth);
        }
    };

    public static Downsampler AT_MOST = new Downsampler() {

        @Override
        protected int getSampleSize(int inWidth, int inHeight, int outWidth, int outHeight) {
            return Math.max(inHeight / outHeight, inWidth / outWidth);
        }
    };

    public static Downsampler NONE = new Downsampler() {

        public Bitmap decode(InputStream is, BitmapPool pool) {
            return decode(is, pool, -1, -1);
        }

        @Override
        protected int getSampleSize(int inWidth, int inHeight, int outWidth, int outHeight) {
            return 0;
        }
    };

    private static final int MARK_POSITION = 5 * 1024 * 1024;

    @Override
    public Bitmap decode(InputStream is, BitmapPool pool, int outWidth, int outHeight) {
        final ByteArrayPool byteArrayPool = ByteArrayPool.get();
        byte[] bytesForOptions = byteArrayPool.getBytes();
        byte[] bytesForStream = byteArrayPool.getBytes();
        RecyclableBufferedInputStream bis = new RecyclableBufferedInputStream(is, bytesForStream);
        bis.mark(MARK_POSITION);
        int orientation = 0;
        try {
            orientation = new ImageHeaderParser(bis).getOrientation();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            bis.reset();
        } catch (IOException e) {
            e.printStackTrace();
        }
        final BitmapFactory.Options options = getDefaultOptions();
        options.inTempStorage = bytesForOptions;
        final int[] inDimens = getDimensions(bis, options);
        final int inWidth = inDimens[0];
        final int inHeight = inDimens[1];
        final int degreesToRotate = TransformationUtils.getExifOrientationDegrees(orientation);
        final int sampleSize;
        if (degreesToRotate == 90 || degreesToRotate == 270) {
            sampleSize = getSampleSize(inHeight, inWidth, outWidth, outHeight);
        } else {
            sampleSize = getSampleSize(inWidth, inHeight, outWidth, outHeight);
        }
        final Bitmap downsampled = downsampleWithSize(bis, options, pool, inWidth, inHeight, sampleSize);
        if (downsampled == null) {
            throw new IllegalArgumentException("Unable to decode image sample size: " + sampleSize + " inWidth: " + inWidth + " inHeight: " + inHeight);
        }
        final Bitmap rotated = TransformationUtils.rotateImageExif(downsampled, pool, orientation);
        if (downsampled != rotated && !pool.put(downsampled)) {
            downsampled.recycle();
        }
        byteArrayPool.releaseBytes(bytesForOptions);
        byteArrayPool.releaseBytes(bytesForStream);
        return rotated;
    }

    protected Bitmap downsampleWithSize(RecyclableBufferedInputStream bis, BitmapFactory.Options options, BitmapPool pool, int inWidth, int inHeight, int sampleSize) {
        options.inSampleSize = sampleSize;
        if (options.inSampleSize == 1 || Build.VERSION.SDK_INT >= 19) {
            if (shouldUsePool(bis)) {
                setInBitmap(options, pool.get(inWidth, inHeight, getConfig(bis)));
            }
        }
        return decodeStream(bis, options);
    }

    private boolean shouldUsePool(RecyclableBufferedInputStream bis) {
        if (Build.VERSION.SDK_INT >= 19) {
            return true;
        }
        bis.mark(1024);
        try {
            final ImageType type = new ImageHeaderParser(bis).getType();
            return TYPES_THAT_USE_POOL.contains(type);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bis.reset();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    private Bitmap.Config getConfig(RecyclableBufferedInputStream bis) {
        Bitmap.Config result = Bitmap.Config.RGB_565;
        bis.mark(1024);
        try {
            result = new ImageHeaderParser(bis).hasAlpha() ? Bitmap.Config.ARGB_8888 : Bitmap.Config.RGB_565;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bis.reset();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public String getId() {
        return id;
    }

    protected abstract int getSampleSize(int inWidth, int inHeight, int outWidth, int outHeight);

    public int[] getDimensions(RecyclableBufferedInputStream bis, BitmapFactory.Options options) {
        options.inJustDecodeBounds = true;
        decodeStream(bis, options);
        options.inJustDecodeBounds = false;
        return new int[] { options.outWidth, options.outHeight };
    }

    private Bitmap decodeStream(RecyclableBufferedInputStream bis, BitmapFactory.Options options) {
        if (options.inJustDecodeBounds) {
            bis.mark(MARK_POSITION);
        }
        final Bitmap result = BitmapFactory.decodeStream(bis, null, options);
        try {
            if (options.inJustDecodeBounds) {
                bis.reset();
                bis.clearMark();
            } else {
                bis.close();
            }
        } catch (IOException e) {
            if (Log.isLoggable(TAG, Log.ERROR)) {
                Log.e(TAG, "Exception loading inDecodeBounds=" + options.inJustDecodeBounds + " sample=" + options.inSampleSize, e);
            }
        }
        return result;
    }

    @TargetApi(11)
    private static void setInBitmap(BitmapFactory.Options options, Bitmap recycled) {
        if (Build.VERSION.SDK_INT >= 11) {
            options.inBitmap = recycled;
        }
    }
}