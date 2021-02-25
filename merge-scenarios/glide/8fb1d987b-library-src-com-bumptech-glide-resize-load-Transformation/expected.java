package com.bumptech.glide.resize.load;

import android.graphics.Bitmap;
import com.bumptech.glide.resize.bitmap_recycle.BitmapPool;

public abstract class Transformation {

    private final String id = getClass().toString();

    public static Transformation CENTER_CROP = new Transformation() {

        @Override
        public Bitmap transform(Bitmap bitmap, BitmapPool pool, int outWidth, int outHeight) {
            if (outWidth <= 0 || outHeight <= 0) {
                throw new IllegalArgumentException("Cannot center crop image to width=" + outWidth + " and height=" + outHeight);
            }
            final Bitmap toReuse = pool.get(outWidth, outHeight, bitmap.getConfig());
            return TransformationUtils.centerCrop(toReuse, bitmap, outWidth, outHeight);
        }
    };

    public static Transformation FIT_CENTER = new Transformation() {

        @Override
        public Bitmap transform(Bitmap bitmap, BitmapPool pool, int outWidth, int outHeight) {
            if (outWidth <= 0 || outHeight <= 0) {
                throw new IllegalArgumentException("Cannot fit center image to within width=" + outWidth + " or height=" + outHeight);
            }
            return TransformationUtils.fitCenter(bitmap, pool, outWidth, outHeight);
        }
    };

    public static Transformation NONE = new Transformation() {

        @Override
        public Bitmap transform(Bitmap bitmap, BitmapPool pool, int outWidth, int outHeight) {
            return bitmap;
        }
    };

    public abstract Bitmap transform(Bitmap bitmap, BitmapPool pool, int outWidth, int outHeight);

    public String getId() {
        return id;
    }
}