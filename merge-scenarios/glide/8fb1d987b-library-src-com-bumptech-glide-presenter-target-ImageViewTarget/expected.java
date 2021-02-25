package com.bumptech.glide.presenter.target;

import android.graphics.Bitmap;
import android.graphics.drawable.Drawable;
import android.widget.ImageView;

public class ImageViewTarget extends ViewTarget<ImageView> {

    private final ImageView view;

    public ImageViewTarget(ImageView view) {
        super(view);
        this.view = view;
    }

    @Override
    public void onImageReady(Bitmap bitmap) {
        view.setImageBitmap(bitmap);
    }

    @Override
    public void setPlaceholder(Drawable placeholder) {
        view.setImageDrawable(placeholder);
    }
}