package com.bumptech.glide;

import android.content.Context;
import android.net.Uri;
import android.os.Handler;
import android.os.Looper;
import android.os.ParcelFileDescriptor;
import com.bumptech.glide.load.Key;
import com.bumptech.glide.load.engine.DiskCacheStrategy;
import com.bumptech.glide.load.model.ModelLoader;
import com.bumptech.glide.load.model.file_descriptor.FileDescriptorModelLoader;
import com.bumptech.glide.load.model.stream.MediaStoreStreamLoader;
import com.bumptech.glide.load.model.stream.StreamByteArrayLoader;
import com.bumptech.glide.load.model.stream.StreamModelLoader;
import com.bumptech.glide.manager.ConnectivityMonitor;
import com.bumptech.glide.manager.ConnectivityMonitorFactory;
import com.bumptech.glide.manager.Lifecycle;
import com.bumptech.glide.manager.LifecycleListener;
import com.bumptech.glide.manager.RequestManagerTreeNode;
import com.bumptech.glide.manager.RequestTracker;
import com.bumptech.glide.signature.ApplicationVersionSignature;
import com.bumptech.glide.signature.MediaStoreSignature;
import com.bumptech.glide.signature.StringSignature;
import com.bumptech.glide.util.Util;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.UUID;

public class RequestManager implements LifecycleListener {

    private final Context context;

    private final Lifecycle lifecycle;

    private final RequestManagerTreeNode treeNode;

    private final RequestTracker requestTracker;

    private final Glide glide;

    private final OptionsApplier optionsApplier;

    private DefaultOptions options;

    public RequestManager(Context context, Lifecycle lifecycle, RequestManagerTreeNode treeNode) {
        this(context, lifecycle, treeNode, new RequestTracker(), new ConnectivityMonitorFactory());
    }

    RequestManager(Context context, final Lifecycle lifecycle, RequestManagerTreeNode treeNode, RequestTracker requestTracker, ConnectivityMonitorFactory factory) {
        this.context = context.getApplicationContext();
        this.lifecycle = lifecycle;
        this.treeNode = treeNode;
        this.requestTracker = requestTracker;
        this.glide = Glide.get(context);
        this.optionsApplier = new OptionsApplier();
        ConnectivityMonitor connectivityMonitor = factory.build(context, new RequestManagerConnectivityListener(requestTracker));
        if (Util.isOnBackgroundThread()) {
            new Handler(Looper.getMainLooper()).post(new Runnable() {

                @Override
                public void run() {
                    lifecycle.addListener(RequestManager.this);
                }
            });
        } else {
            lifecycle.addListener(this);
        }
        lifecycle.addListener(connectivityMonitor);
    }

    public void onTrimMemory(int level) {
        glide.trimMemory(level);
    }

    public void onLowMemory() {
        glide.clearMemory();
    }

    public interface DefaultOptions {

        <T> void apply(GenericRequestBuilder<T, ?, ?, ?> requestBuilder);
    }

    public void setDefaultOptions(DefaultOptions options) {
        this.options = options;
    }

    public boolean isPaused() {
        Util.assertMainThread();
        return requestTracker.isPaused();
    }

    public void pauseRequests() {
        Util.assertMainThread();
        requestTracker.pauseRequests();
    }

    public void pauseRequestsRecursive() {
        Util.assertMainThread();
        pauseRequests();
        for (RequestManager requestManager : treeNode.getDescendants()) {
            requestManager.pauseRequests();
        }
    }

    public void resumeRequests() {
        Util.assertMainThread();
        requestTracker.resumeRequests();
    }

    public void resumeRequestsRecursive() {
        Util.assertMainThread();
        resumeRequests();
        for (RequestManager requestManager : treeNode.getDescendants()) {
            requestManager.resumeRequests();
        }
    }

    @Override
    public void onStart() {
        resumeRequests();
    }

    @Override
    public void onStop() {
        pauseRequests();
    }

    @Override
    public void onDestroy() {
        requestTracker.clearRequests();
    }

    public <A, T> GenericModelRequest<A, T> using(ModelLoader<A, T> modelLoader, Class<T> dataClass) {
        return new GenericModelRequest<A, T>(modelLoader, dataClass);
    }

    public <T> ImageModelRequest<T> using(final StreamModelLoader<T> modelLoader) {
        return new ImageModelRequest<T>(modelLoader);
    }

    public ImageModelRequest<byte[]> using(StreamByteArrayLoader modelLoader) {
        return new ImageModelRequest<byte[]>(modelLoader);
    }

    public <T> VideoModelRequest<T> using(final FileDescriptorModelLoader<T> modelLoader) {
        return new VideoModelRequest<T>(modelLoader);
    }

    public DrawableTypeRequest<String> load(String string) {
        return (DrawableTypeRequest<String>) fromString().load(string);
    }

    public DrawableTypeRequest<String> fromString() {
        return loadGeneric(String.class);
    }

    public DrawableTypeRequest<Uri> load(Uri uri) {
        return (DrawableTypeRequest<Uri>) fromUri().load(uri);
    }

    public DrawableTypeRequest<Uri> fromUri() {
        return loadGeneric(Uri.class);
    }

    @Deprecated
    public DrawableTypeRequest<Uri> loadFromMediaStore(Uri uri, String mimeType, long dateModified, int orientation) {
        Key signature = new MediaStoreSignature(mimeType, dateModified, orientation);
        return (DrawableTypeRequest<Uri>) loadFromMediaStore(uri).signature(signature);
    }

    public DrawableTypeRequest<Uri> loadFromMediaStore(Uri uri) {
        return (DrawableTypeRequest<Uri>) fromMediaStore().load(uri);
    }

    public DrawableTypeRequest<Uri> fromMediaStore() {
        ModelLoader<Uri, InputStream> genericStreamLoader = Glide.buildStreamModelLoader(Uri.class, context);
        ModelLoader<Uri, InputStream> mediaStoreLoader = new MediaStoreStreamLoader(context, genericStreamLoader);
        ModelLoader<Uri, ParcelFileDescriptor> fileDescriptorModelLoader = Glide.buildFileDescriptorModelLoader(Uri.class, context);
        return optionsApplier.apply(new DrawableTypeRequest<Uri>(Uri.class, mediaStoreLoader, fileDescriptorModelLoader, context, glide, requestTracker, lifecycle, optionsApplier));
    }

    public DrawableTypeRequest<File> load(File file) {
        return (DrawableTypeRequest<File>) fromFile().load(file);
    }

    public DrawableTypeRequest<File> fromFile() {
        return loadGeneric(File.class);
    }

    public DrawableTypeRequest<Integer> load(Integer resourceId) {
        return (DrawableTypeRequest<Integer>) fromResource().load(resourceId);
    }

    public DrawableTypeRequest<Integer> fromResource() {
        return (DrawableTypeRequest<Integer>) loadGeneric(Integer.class).signature(ApplicationVersionSignature.obtain(context));
    }

    @Deprecated
    public DrawableTypeRequest<URL> load(URL url) {
        return (DrawableTypeRequest<URL>) fromUrl().load(url);
    }

    @Deprecated
    public DrawableTypeRequest<URL> fromUrl() {
        return loadGeneric(URL.class);
    }

    @Deprecated
    public DrawableTypeRequest<byte[]> load(byte[] model, final String id) {
        return (DrawableTypeRequest<byte[]>) load(model).signature(new StringSignature(id));
    }

    public DrawableTypeRequest<byte[]> load(byte[] model) {
        return (DrawableTypeRequest<byte[]>) fromBytes().load(model);
    }

    public DrawableTypeRequest<byte[]> fromBytes() {
        return (DrawableTypeRequest<byte[]>) loadGeneric(byte[].class).signature(new StringSignature(UUID.randomUUID().toString())).diskCacheStrategy(DiskCacheStrategy.NONE).skipMemoryCache(true);
    }

    public <T> DrawableTypeRequest<T> load(T model) {
        return (DrawableTypeRequest<T>) loadGeneric(getSafeClass(model)).load(model);
    }

    public <T> DrawableTypeRequest<T> from(Class<T> modelClass) {
        return loadGeneric(modelClass);
    }

    private <T> DrawableTypeRequest<T> loadGeneric(Class<T> modelClass) {
        ModelLoader<T, InputStream> streamModelLoader = Glide.buildStreamModelLoader(modelClass, context);
        ModelLoader<T, ParcelFileDescriptor> fileDescriptorModelLoader = Glide.buildFileDescriptorModelLoader(modelClass, context);
        if (modelClass != null && streamModelLoader == null && fileDescriptorModelLoader == null) {
            throw new IllegalArgumentException("Unknown type " + modelClass + ". You must provide a Model of a type for" + " which there is a registered ModelLoader, if you are using a custom model, you must first call" + " Glide#register with a ModelLoaderFactory for your custom model class");
        }
        return optionsApplier.apply(new DrawableTypeRequest<T>(modelClass, streamModelLoader, fileDescriptorModelLoader, context, glide, requestTracker, lifecycle, optionsApplier));
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<T> getSafeClass(T model) {
        return model != null ? (Class<T>) model.getClass() : null;
    }

    public final class VideoModelRequest<T> {

        private final ModelLoader<T, ParcelFileDescriptor> loader;

        VideoModelRequest(ModelLoader<T, ParcelFileDescriptor> loader) {
            this.loader = loader;
        }

        public DrawableTypeRequest<T> load(T model) {
            return (DrawableTypeRequest<T>) optionsApplier.apply(new DrawableTypeRequest<T>(getSafeClass(model), null, loader, context, glide, requestTracker, lifecycle, optionsApplier)).load(model);
        }
    }

    public final class ImageModelRequest<T> {

        private final ModelLoader<T, InputStream> loader;

        ImageModelRequest(ModelLoader<T, InputStream> loader) {
            this.loader = loader;
        }

        public DrawableTypeRequest<T> from(Class<T> modelClass) {
            return optionsApplier.apply(new DrawableTypeRequest<T>(modelClass, loader, null, context, glide, requestTracker, lifecycle, optionsApplier));
        }

        public DrawableTypeRequest<T> load(T model) {
            return (DrawableTypeRequest<T>) from(getSafeClass(model)).load(model);
        }
    }

    public final class GenericModelRequest<A, T> {

        private final ModelLoader<A, T> modelLoader;

        private final Class<T> dataClass;

        GenericModelRequest(ModelLoader<A, T> modelLoader, Class<T> dataClass) {
            this.modelLoader = modelLoader;
            this.dataClass = dataClass;
        }

        public GenericTypeRequest from(Class<A> modelClass) {
            return new GenericTypeRequest(modelClass);
        }

        public GenericTypeRequest load(A model) {
            return new GenericTypeRequest(model);
        }

        public final class GenericTypeRequest {

            private final A model;

            private final Class<A> modelClass;

            private final boolean providedModel;

            GenericTypeRequest(A model) {
                providedModel = true;
                this.model = model;
                this.modelClass = getSafeClass(model);
            }

            GenericTypeRequest(Class<A> modelClass) {
                providedModel = false;
                this.model = null;
                this.modelClass = modelClass;
            }

            public <Z> GenericTranscodeRequest<A, T, Z> as(Class<Z> resourceClass) {
                GenericTranscodeRequest<A, T, Z> result = optionsApplier.apply(new GenericTranscodeRequest<A, T, Z>(context, glide, modelClass, modelLoader, dataClass, resourceClass, requestTracker, lifecycle, optionsApplier));
                if (providedModel) {
                    result.load(model);
                }
                return result;
            }
        }
    }

    class OptionsApplier {

        public <A, X extends GenericRequestBuilder<A, ?, ?, ?>> X apply(X builder) {
            if (options != null) {
                options.apply(builder);
            }
            return builder;
        }
    }

    private static class RequestManagerConnectivityListener implements ConnectivityMonitor.ConnectivityListener {

        private final RequestTracker requestTracker;

        public RequestManagerConnectivityListener(RequestTracker requestTracker) {
            this.requestTracker = requestTracker;
        }

        @Override
        public void onConnectivityChanged(boolean isConnected) {
            if (isConnected) {
                requestTracker.restartRequests();
            }
        }
    }
}