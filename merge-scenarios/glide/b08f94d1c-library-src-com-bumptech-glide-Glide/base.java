package com.bumptech.glide;

import android.content.Context;
import android.view.animation.Animation;
import android.view.animation.AnimationUtils;
import android.widget.ImageView;
import com.android.volley.RequestQueue;
import com.android.volley.toolbox.Volley;
import com.bumptech.glide.loader.image.ImageLoader;
import com.bumptech.glide.loader.model.FileLoader;
import com.bumptech.glide.loader.model.ModelLoader;
import com.bumptech.glide.loader.model.UrlLoader;
import com.bumptech.glide.loader.model.VolleyModelLoader;
import com.bumptech.glide.presenter.ImagePresenter;
import com.bumptech.glide.presenter.ImageSetCallback;
import com.bumptech.glide.resize.ImageManager;
import com.bumptech.glide.resize.loader.Approximate;
import com.bumptech.glide.resize.loader.CenterCrop;
import com.bumptech.glide.resize.loader.FitCenter;
import java.io.File;
import java.net.URL;

public class Glide {

    private static final Glide GLIDE = new Glide();

    private ImageManager imageManager = null;

    private RequestQueue requestQueue = null;

    public static Glide get() {
        return GLIDE;
    }

    protected Glide() {
    }

    public RequestQueue getRequestQueue(Context context) {
        if (!isRequestQueueSet()) {
            setRequestQueue(Volley.newRequestQueue(context));
        }
        return requestQueue;
    }

    public boolean isRequestQueueSet() {
        return requestQueue != null;
    }

    public void setRequestQueue(RequestQueue requestQueue) {
        this.requestQueue = requestQueue;
    }

    public ImageManager getImageManager(Context context) {
        if (!isImageManagerSet()) {
            setImageManager(new ImageManager.Builder(context));
        }
        return imageManager;
    }

    public boolean isImageManagerSet() {
        return imageManager != null;
    }

    public void setImageManager(ImageManager.Builder builder) {
        setImageManager(builder.build());
    }

    public void setImageManager(ImageManager imageManager) {
        this.imageManager = imageManager;
    }

    public static <T> HalfRequest<T> load(T model) {
        if (model == null) {
            throw new IllegalArgumentException("Model can't be null");
        }
        return new HalfRequest<T>(model);
    }

    @SuppressWarnings("unchecked")
    private static <T> ModelLoader<T> getModelFor(T model, Context context) {
        if (model == URL.class) {
            return (ModelLoader<T>) new VolleyModelLoader<URL>(GLIDE.getRequestQueue(context)) {

                @Override
                protected String getUrl(URL model, int width, int height) {
                    return model.toString();
                }

                @Override
                public String getId(URL model) {
                    return model.toString();
                }
            };
        } else if (model == File.class) {
            return (ModelLoader<T>) new FileLoader();
        } else {
            throw new IllegalArgumentException("No default ModelLoader for class=" + model.getClass() + ", you need to provide one by calling with()");
        }
    }

    public static class HalfRequest<T> {

        private final T model;

        public HalfRequest(T model) {
            this.model = model;
        }

        public Request<T> into(ImageView imageView) {
            if (imageView == null) {
                throw new IllegalArgumentException("ImageView can't be null");
            }
            return new Request<T>(model, imageView);
        }
    }

    public static class Request<T> {

        private final T model;

        private final ImageView imageView;

        private final Context context;

        private ImagePresenter<T> presenter;

        private ImagePresenter.Builder<T> builder;

        private ModelLoader<T> modelLoader = null;

        @SuppressWarnings("unchecked")
        public Request(T model, ImageView imageView) {
            this.model = model;
            this.imageView = imageView;
            this.context = imageView.getContext();
            presenter = (ImagePresenter<T>) imageView.getTag(R.id.image_presenter_id);
            builder = new ImagePresenter.Builder<T>().setImageView(imageView).setImageLoader(new Approximate(getImageManager()));
        }

        public Request<T> with(ModelLoader<T> modelLoader) {
            this.modelLoader = modelLoader;
            builder.setModelLoader(modelLoader);
            return this;
        }

        public Request<T> centerCrop() {
            return resizeWith(new CenterCrop(getImageManager()));
        }

        public Request<T> fitCenter() {
            return resizeWith(new FitCenter(getImageManager()));
        }

        public Request<T> approximate() {
            return resizeWith(new Approximate(getImageManager()));
        }

        public Request<T> resizeWith(ImageLoader imageLoader) {
            if (presenter == null) {
                builder.setImageLoader(imageLoader);
            }
            return this;
        }

        public Request<T> animate(final Animation animation) {
            builder.setImageSetCallback(new ImageSetCallback() {

                @Override
                public void onImageSet(ImageView view, boolean fromCache) {
                    view.clearAnimation();
                    if (!fromCache) {
                        view.startAnimation(animation);
                    }
                }
            });
            return this;
        }

        public Request<T> animate(int animationId) {
            return animate(AnimationUtils.loadAnimation(context, animationId));
        }

        public void begin() {
            build();
            presenter.setModel(model);
        }

        private ImageManager getImageManager() {
            return GLIDE.getImageManager(context);
        }

        private void build() {
            if (presenter == null) {
                if (modelLoader == null) {
                    modelLoader = getModelFor(model, context);
                }
                presenter = builder.build();
                imageView.setTag(R.id.image_presenter_id, presenter);
            }
        }
    }
}