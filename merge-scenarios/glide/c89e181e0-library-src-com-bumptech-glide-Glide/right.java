package com.bumptech.glide;

import android.content.Context;
import android.net.Uri;
import android.view.ViewGroup;
import android.view.animation.Animation;
import android.view.animation.AnimationUtils;
import android.widget.ImageView;
import com.bumptech.glide.loader.image.ImageLoader;
import com.bumptech.glide.loader.image.ImageManagerLoader;
import com.bumptech.glide.loader.model.FileLoader;
import com.bumptech.glide.loader.model.GenericLoaderFactory;
import com.bumptech.glide.loader.model.ModelLoader;
import com.bumptech.glide.loader.model.ModelLoaderFactory;
import com.bumptech.glide.loader.model.ResourceLoader;
import com.bumptech.glide.loader.model.StringLoader;
import com.bumptech.glide.loader.model.UriLoader;
import com.bumptech.glide.loader.stream.StreamLoader;
import com.bumptech.glide.loader.transformation.CenterCrop;
import com.bumptech.glide.loader.transformation.FitCenter;
import com.bumptech.glide.loader.transformation.MultiTransformationLoader;
import com.bumptech.glide.loader.transformation.None;
import com.bumptech.glide.loader.transformation.TransformationLoader;
import com.bumptech.glide.presenter.ImagePresenter;
import com.bumptech.glide.presenter.target.ImageViewTarget;
import com.bumptech.glide.presenter.target.Target;
import com.bumptech.glide.resize.ImageManager;
import com.bumptech.glide.resize.load.Downsampler;
import com.bumptech.glide.resize.load.Transformation;
import com.bumptech.glide.util.Log;
import com.bumptech.glide.volley.VolleyUrlLoader;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.WeakHashMap;

public class Glide {

    private static final Glide GLIDE = new Glide();

    private final Map<Target, Metadata> metadataTracker = new WeakHashMap<Target, Metadata>();

    private final GenericLoaderFactory loaderFactory = new GenericLoaderFactory();

    private ImageManager imageManager = null;

    public static abstract class RequestListener<T> {

        public abstract void onException(Exception e, T model, Target target);

        public abstract void onImageReady(T model, Target target);

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            return true;
        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException();
        }
    }

    public static Glide get() {
        return GLIDE;
    }

    protected Glide() {
        loaderFactory.register(File.class, new FileLoader.Factory());
        loaderFactory.register(Integer.class, new ResourceLoader.Factory());
        loaderFactory.register(String.class, new StringLoader.Factory());
        loaderFactory.register(Uri.class, new UriLoader.Factory());
        try {
            Class.forName("com.bumptech.glide.volley.VolleyUrlLoader$Factory");
            loaderFactory.register(URL.class, new VolleyUrlLoader.Factory());
        } catch (ClassNotFoundException e) {
            Log.d("Volley not found, missing url loader");
            loaderFactory.register(URL.class, new ModelLoaderFactory<URL>() {

                ModelLoader<URL> errorUrlLoader = new ModelLoader<URL>() {

                    @Override
                    public StreamLoader getStreamLoader(URL model, int width, int height) {
                        throw new IllegalArgumentException("No ModelLoaderFactory for urls registered with Glide");
                    }

                    @Override
                    public String getId(URL model) {
                        throw new IllegalArgumentException("No ModelLoaderFactory for urls registered with Glide");
                    }
                };

                @Override
                public ModelLoader<URL> build(Context context, GenericLoaderFactory factories) {
                    return errorUrlLoader;
                }

                @Override
                @SuppressWarnings("unchecked")
                public Class<? extends ModelLoader<URL>> loaderClass() {
                    return (Class<ModelLoader<URL>>) errorUrlLoader.getClass();
                }

                @Override
                public void teardown() {
                }
            });
        }
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

    public <T> void register(Class<T> clazz, ModelLoaderFactory<T> factory) {
        ModelLoaderFactory<T> removed = loaderFactory.register(clazz, factory);
        if (removed != null) {
            removed.teardown();
        }
    }

    public static <T> ModelLoader<T> buildModelLoader(Class<T> clazz, Context context) {
        return GLIDE.loaderFactory.buildModelLoader(clazz, context);
    }

    @SuppressWarnings("unchecked")
    private <T> ModelLoaderFactory<T> getFactory(T model) {
        return loaderFactory.getFactory((Class<T>) model.getClass());
    }

    private ImageViewTarget getImageViewTarget(ImageView imageView) {
        Object tag = imageView.getTag();
        final ImageViewTarget result;
        if (tag != null && tag instanceof ImageViewTarget) {
            result = (ImageViewTarget) tag;
        } else {
            result = null;
        }
        return result;
    }

    private ImageViewTarget getImageViewTargetOrSet(ImageView imageView) {
        ImageViewTarget result = getImageViewTarget(imageView);
        if (result == null) {
            result = new ImageViewTarget(imageView);
            imageView.setTag(result);
        }
        return result;
    }

    public static <T> ModelRequest<T> using(ModelLoaderFactory<T> factory) {
        return new ModelRequest<T>(factory);
    }

    public static <T> ModelRequest<T> using(final ModelLoader<T> modelLoader) {
        return new ModelRequest<T>(new ModelLoaderFactory<T>() {

            @Override
            public ModelLoader<T> build(Context context, GenericLoaderFactory factories) {
                return modelLoader;
            }

            @Override
            @SuppressWarnings("unchecked")
            public Class<? extends ModelLoader<T>> loaderClass() {
                return (Class<ModelLoader<T>>) modelLoader.getClass();
            }

            @Override
            public void teardown() {
            }
        });
    }

    public static Request<String> load(String string) {
        return new Request<String>(string);
    }

    public static Request<Uri> load(Uri uri) {
        return new Request<Uri>(uri);
    }

    public static Request<URL> load(URL url) {
        return new Request<URL>(url);
    }

    public static Request<File> load(File file) {
        return new Request<File>(file);
    }

    public static Request<Integer> load(Integer resourceId) {
        return new Request<Integer>(resourceId);
    }

    public static <T> Request<T> load(T model) {
        return new Request<T>(model);
    }

    public static boolean cancel(ImageView imageView) {
        final Target target = GLIDE.getImageViewTarget(imageView);
        return target != null && cancel(target);
    }

    public static boolean cancel(Target target) {
        ImagePresenter current = target.getImagePresenter();
        final boolean cancelled = current != null;
        if (cancelled) {
            current.clear();
        }
        return cancelled;
    }

    public static class ModelRequest<T> {

        private final ModelLoaderFactory<T> factory;

        private ModelRequest(ModelLoaderFactory<T> factory) {
            this.factory = factory;
        }

        public Request<T> load(T model) {
            return new Request<T>(model, factory);
        }
    }

    @SuppressWarnings("unused")
    public static class Request<T> {

        private Context context;

        private Target target;

        private ModelLoaderFactory<T> modelLoaderFactory;

        private final T model;

        private int animationId = -1;

        private int placeholderId = -1;

        private int errorId = -1;

        private Downsampler downsampler = Downsampler.AT_LEAST;

        private ArrayList<TransformationLoader<T>> transformationLoaders = new ArrayList<TransformationLoader<T>>();

        private RequestListener<T> requestListener;

        private Request(T model) {
            this(model, GLIDE.getFactory(model));
        }

        private Request(T model, ModelLoaderFactory<T> factory) {
            if (model == null) {
                throw new IllegalArgumentException("Model can't be null");
            }
            this.model = model;
            if (factory == null) {
                throw new IllegalArgumentException("No ModelLoaderFactory registered for class=" + model.getClass());
            }
            this.modelLoaderFactory = factory;
        }

        public Request<T> approximate() {
            return downsample(Downsampler.AT_LEAST);
        }

        public Request<T> asIs() {
            return downsample(Downsampler.NONE);
        }

        public Request<T> downsample(Downsampler downsampler) {
            this.downsampler = downsampler;
            return this;
        }

        public Request<T> centerCrop() {
            return transform(new CenterCrop<T>());
        }

        public Request<T> fitCenter() {
            return transform(new FitCenter<T>());
        }

        public Request<T> transform(final Transformation transformation) {
            return transform(new TransformationLoader<T>() {

                @Override
                public Transformation getTransformation(T model) {
                    return transformation;
                }

                @Override
                public String getId() {
                    return transformation.getId();
                }
            });
        }

        public Request<T> transform(TransformationLoader<T> transformationLoader) {
            transformationLoaders.add(transformationLoader);
            return this;
        }

        public Request<T> animate(int animationId) {
            this.animationId = animationId;
            return this;
        }

        public Request<T> placeholder(int resourceId) {
            this.placeholderId = resourceId;
            return this;
        }

        public Request<T> error(int resourceId) {
            this.errorId = resourceId;
            return this;
        }

        public Request<T> listener(RequestListener<T> requestListener) {
            this.requestListener = requestListener;
            return this;
        }

        public void into(ImageView imageView) {
            final ViewGroup.LayoutParams layoutParams = imageView.getLayoutParams();
            if (layoutParams != null && (layoutParams.width == ViewGroup.LayoutParams.WRAP_CONTENT || layoutParams.height == ViewGroup.LayoutParams.WRAP_CONTENT)) {
                downsampler = Downsampler.NONE;
            }
            finish(imageView.getContext(), GLIDE.getImageViewTargetOrSet(imageView));
        }

        public ContextRequest into(Target target) {
            return new ContextRequest(this, target);
        }

        private void finish(Context context, Target target) {
            this.context = context;
            this.target = target;
            ImagePresenter<T> imagePresenter = getImagePresenter(target);
            imagePresenter.setModel(model);
        }

        @SuppressWarnings("unchecked")
        private ImagePresenter<T> getImagePresenter(Target target) {
            Metadata previous = GLIDE.metadataTracker.get(target);
            Metadata current = new Metadata(this);
            ImagePresenter<T> result = target.getImagePresenter();
            if (!current.isIdenticalTo(previous)) {
                if (result != null) {
                    result.clear();
                }
                result = buildImagePresenter(target);
                target.setImagePresenter(result);
                GLIDE.metadataTracker.put(target, current);
            }
            return result;
        }

        private ImagePresenter<T> buildImagePresenter(final Target target) {
            TransformationLoader<T> transformationLoader = getFinalTransformationLoader();
            ImagePresenter.Builder<T> builder = new ImagePresenter.Builder<T>().setTarget(target, context).setModelLoader(modelLoaderFactory.build(context, GLIDE.loaderFactory)).setImageLoader(new ImageManagerLoader(context, downsampler)).setTransformationLoader(transformationLoader);
            if (animationId != -1 || requestListener != null) {
                final Animation animation;
                if (animationId != -1) {
                    animation = AnimationUtils.loadAnimation(context, animationId);
                } else {
                    animation = null;
                }
                builder.setImageReadyCallback(new ImagePresenter.ImageReadyCallback<T>() {

                    @Override
                    public void onImageReady(T model, Target target, boolean fromCache) {
                        if (animation != null && !fromCache) {
                            target.startAnimation(animation);
                        }
                        if (requestListener != null) {
                            requestListener.onImageReady(null, target);
                        }
                    }
                });
            }
            if (placeholderId != -1) {
                builder.setPlaceholderResource(placeholderId);
            }
            if (errorId != -1) {
                builder.setErrorResource(errorId);
            }
            if (requestListener != null) {
                builder.setExceptionHandler(new ImagePresenter.ExceptionHandler<T>() {

                    @Override
                    public void onException(Exception e, T model, boolean isCurrent) {
                        if (isCurrent) {
                            requestListener.onException(e, model, target);
                        }
                    }
                });
            }
            return builder.build();
        }

        private TransformationLoader<T> getFinalTransformationLoader() {
            switch(transformationLoaders.size()) {
                case 0:
                    return new None<T>();
                case 1:
                    return transformationLoaders.get(0);
                default:
                    return new MultiTransformationLoader<T>(transformationLoaders);
            }
        }

        private String getFinalTransformationId() {
            switch(transformationLoaders.size()) {
                case 0:
                    return Transformation.NONE.getId();
                case 1:
                    return transformationLoaders.get(0).getId();
                default:
                    StringBuilder sb = new StringBuilder();
                    for (TransformationLoader transformationLoader : transformationLoaders) {
                        sb.append(transformationLoader.getId());
                    }
                    return sb.toString();
            }
        }
    }

    public static class ContextRequest {

        private final Request request;

        private final Target target;

        private ContextRequest(Request request, Target target) {
            this.request = request;
            this.target = target;
        }

        public void with(Context context) {
            request.finish(context, target);
        }
    }

    private static class Metadata {

        public final Class modelClass;

        public final Class modelLoaderClass;

        public final int animationId;

        public final int placeholderId;

        public final int errorId;

        private final String downsamplerId;

        private final String transformationId;

        private final RequestListener requestListener;

        public Metadata(Request request) {
            modelClass = request.model.getClass();
            modelLoaderClass = request.modelLoaderFactory.loaderClass();
            downsamplerId = request.downsampler.getId();
            transformationId = request.getFinalTransformationId();
            animationId = request.animationId;
            placeholderId = request.placeholderId;
            errorId = request.errorId;
            requestListener = request.requestListener;
        }

        public boolean isIdenticalTo(Metadata metadata) {
            if (metadata == null)
                return false;
            if (animationId != metadata.animationId)
                return false;
            if (errorId != metadata.errorId)
                return false;
            if (placeholderId != metadata.placeholderId)
                return false;
            if (!downsamplerId.equals(metadata.downsamplerId))
                return false;
            if (!modelClass.equals(metadata.modelClass))
                return false;
            if (!modelLoaderClass.equals(metadata.modelLoaderClass))
                return false;
            if (!transformationId.equals(metadata.transformationId))
                return false;
            if (requestListener == null ? metadata.requestListener != null : !requestListener.equals(metadata.requestListener))
                return false;
            return true;
        }
    }
}