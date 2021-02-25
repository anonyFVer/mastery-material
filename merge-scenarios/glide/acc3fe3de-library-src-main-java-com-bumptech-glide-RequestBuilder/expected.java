package com.bumptech.glide;

import static com.bumptech.glide.request.RequestOptions.signatureOf;
import android.net.Uri;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.widget.ImageView;
import com.bumptech.glide.load.engine.DiskCacheStrategy;
import com.bumptech.glide.request.BaseRequestOptions;
import com.bumptech.glide.request.FutureTarget;
import com.bumptech.glide.request.Request;
import com.bumptech.glide.request.RequestCoordinator;
import com.bumptech.glide.request.RequestFutureTarget;
import com.bumptech.glide.request.RequestListener;
import com.bumptech.glide.request.RequestOptions;
import com.bumptech.glide.request.SingleRequest;
import com.bumptech.glide.request.ThumbnailRequestCoordinator;
import com.bumptech.glide.request.target.PreloadTarget;
import com.bumptech.glide.request.target.Target;
import com.bumptech.glide.signature.ApplicationVersionSignature;
import com.bumptech.glide.signature.StringSignature;
import com.bumptech.glide.util.Preconditions;
import com.bumptech.glide.util.Util;
import java.io.File;
import java.net.URL;
import java.util.UUID;

public class RequestBuilder<TranscodeType> implements Cloneable {

    private static final TransitionOptions<?, ?> DEFAULT_ANIMATION_OPTIONS = new GenericTransitionOptions<Object>();

    private static final BaseRequestOptions<?> DOWNLOAD_ONLY_OPTIONS = new RequestOptions().diskCacheStrategy(DiskCacheStrategy.DATA).priority(Priority.LOW).skipMemoryCache(true);

    private final GlideContext context;

    private final RequestManager requestManager;

    private final Class<TranscodeType> transcodeClass;

    private final BaseRequestOptions<?> defaultRequestOptions;

    @NonNull
    private BaseRequestOptions<?> requestOptions;

    @SuppressWarnings("unchecked")
    private TransitionOptions<?, ? super TranscodeType> transitionOptions = (TransitionOptions<?, ? super TranscodeType>) DEFAULT_ANIMATION_OPTIONS;

    @Nullable
    private Object model;

    @Nullable
    private RequestListener<TranscodeType> requestListener;

    @Nullable
    private RequestBuilder<TranscodeType> thumbnailBuilder;

    @Nullable
    private Float thumbSizeMultiplier;

    private boolean isModelSet;

    private boolean isThumbnailBuilt;

    RequestBuilder(Class<TranscodeType> transcodeClass, RequestBuilder<?> other) {
        this(other.context, other.requestManager, transcodeClass);
        model = other.model;
        isModelSet = other.isModelSet;
        requestOptions = other.requestOptions;
    }

    RequestBuilder(GlideContext context, RequestManager requestManager, Class<TranscodeType> transcodeClass) {
        this.requestManager = requestManager;
        this.context = Preconditions.checkNotNull(context);
        this.transcodeClass = transcodeClass;
        this.defaultRequestOptions = requestManager.getDefaultRequestOptions();
        this.requestOptions = defaultRequestOptions;
    }

    public RequestBuilder<TranscodeType> apply(@NonNull BaseRequestOptions<?> requestOptions) {
        Preconditions.checkNotNull(requestOptions);
        BaseRequestOptions<?> toMutate = defaultRequestOptions == this.requestOptions ? this.requestOptions.clone() : this.requestOptions;
        this.requestOptions = toMutate.apply(requestOptions);
        return this;
    }

    public RequestBuilder<TranscodeType> transition(@NonNull TransitionOptions<?, ? super TranscodeType> transitionOptions) {
        this.transitionOptions = Preconditions.checkNotNull(transitionOptions);
        return this;
    }

    @SuppressWarnings("unchecked")
    public RequestBuilder<TranscodeType> listener(@Nullable RequestListener<TranscodeType> requestListener) {
        this.requestListener = requestListener;
        return this;
    }

    @SuppressWarnings("unchecked")
    public RequestBuilder<TranscodeType> thumbnail(@Nullable RequestBuilder<TranscodeType> thumbnailRequest) {
        this.thumbnailBuilder = thumbnailRequest;
        return this;
    }

    @SuppressWarnings("unchecked")
    public RequestBuilder<TranscodeType> thumbnail(float sizeMultiplier) {
        if (sizeMultiplier < 0f || sizeMultiplier > 1f) {
            throw new IllegalArgumentException("sizeMultiplier must be between 0 and 1");
        }
        this.thumbSizeMultiplier = sizeMultiplier;
        return this;
    }

    @SuppressWarnings("unchecked")
    public RequestBuilder<TranscodeType> load(@Nullable Object model) {
        return loadGeneric(model);
    }

    private RequestBuilder<TranscodeType> loadGeneric(@Nullable Object model) {
        this.model = model;
        isModelSet = true;
        return this;
    }

    public RequestBuilder<TranscodeType> load(@Nullable String string) {
        return loadGeneric(string);
    }

    public RequestBuilder<TranscodeType> load(@Nullable Uri uri) {
        return loadGeneric(uri);
    }

    public RequestBuilder<TranscodeType> load(@Nullable File file) {
        return loadGeneric(file);
    }

    public RequestBuilder<TranscodeType> load(@Nullable Integer resourceId) {
        return loadGeneric(resourceId).apply(signatureOf(ApplicationVersionSignature.obtain(context)));
    }

    @Deprecated
    public RequestBuilder<TranscodeType> load(@Nullable URL url) {
        return loadGeneric(url);
    }

    public RequestBuilder<TranscodeType> load(@Nullable byte[] model) {
        return loadGeneric(model).apply(signatureOf(new StringSignature(UUID.randomUUID().toString())).diskCacheStrategy(DiskCacheStrategy.NONE).skipMemoryCache(true));
    }

    @SuppressWarnings("unchecked")
    @Override
    public RequestBuilder<TranscodeType> clone() {
        try {
            RequestBuilder<TranscodeType> result = (RequestBuilder<TranscodeType>) super.clone();
            result.requestOptions = result.requestOptions.clone();
            result.transitionOptions = result.transitionOptions.clone();
            return result;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public <Y extends Target<TranscodeType>> Y into(@NonNull Y target) {
        Util.assertMainThread();
        Preconditions.checkNotNull(target);
        if (!isModelSet) {
            throw new IllegalArgumentException("You must call #load() before calling #into()");
        }
        Request previous = target.getRequest();
        if (previous != null) {
            requestManager.clear(target);
        }
        requestOptions.lock();
        Request request = buildRequest(target);
        target.setRequest(request);
        requestManager.track(target, request);
        return target;
    }

    public Target<TranscodeType> into(ImageView view) {
        Util.assertMainThread();
        Preconditions.checkNotNull(view);
        if (!requestOptions.isTransformationSet() && requestOptions.isTransformationAllowed() && view.getScaleType() != null) {
            if (requestOptions.isLocked()) {
                requestOptions = requestOptions.clone();
            }
            switch(view.getScaleType()) {
                case CENTER_CROP:
                    requestOptions.optionalCenterCrop(context);
                    break;
                case CENTER_INSIDE:
                    requestOptions.optionalCenterInside(context);
                    break;
                case FIT_CENTER:
                case FIT_START:
                case FIT_END:
                    requestOptions.optionalFitCenter(context);
                    break;
                default:
            }
        }
        return into(context.buildImageViewTarget(view, transcodeClass));
    }

    @Deprecated
    public FutureTarget<TranscodeType> into(int width, int height) {
        return submit(width, height);
    }

    public FutureTarget<TranscodeType> submit() {
        return submit(Target.SIZE_ORIGINAL, Target.SIZE_ORIGINAL);
    }

    public FutureTarget<TranscodeType> submit(int width, int height) {
        final RequestFutureTarget<TranscodeType> target = new RequestFutureTarget<>(context.getMainHandler(), width, height);
        if (Util.isOnBackgroundThread()) {
            context.getMainHandler().post(new Runnable() {

                @Override
                public void run() {
                    if (!target.isCancelled()) {
                        into(target);
                    }
                }
            });
        } else {
            into(target);
        }
        return target;
    }

    public Target<TranscodeType> preload(int width, int height) {
        final PreloadTarget<TranscodeType> target = PreloadTarget.obtain(requestManager, width, height);
        return into(target);
    }

    public Target<TranscodeType> preload() {
        return preload(Target.SIZE_ORIGINAL, Target.SIZE_ORIGINAL);
    }

    @Deprecated
    public <Y extends Target<File>> Y downloadOnly(Y target) {
        return getDownloadOnlyRequest().into(target);
    }

    @Deprecated
    public FutureTarget<File> downloadOnly(int width, int height) {
        return getDownloadOnlyRequest().submit(width, height);
    }

    private RequestBuilder<File> getDownloadOnlyRequest() {
        return new RequestBuilder<>(File.class, this).apply(DOWNLOAD_ONLY_OPTIONS);
    }

    private Priority getThumbnailPriority(Priority current) {
        switch(current) {
            case LOW:
                return Priority.NORMAL;
            case NORMAL:
                return Priority.HIGH;
            case HIGH:
            case IMMEDIATE:
                return Priority.IMMEDIATE;
            default:
                throw new IllegalArgumentException("unknown priority: " + requestOptions.getPriority());
        }
    }

    private Request buildRequest(Target<TranscodeType> target) {
        return buildRequestRecursive(target, null, transitionOptions, requestOptions.getPriority(), requestOptions.getOverrideWidth(), requestOptions.getOverrideHeight());
    }

    private Request buildRequestRecursive(Target<TranscodeType> target, @Nullable ThumbnailRequestCoordinator parentCoordinator, TransitionOptions<?, ? super TranscodeType> transitionOptions, Priority priority, int overrideWidth, int overrideHeight) {
        if (thumbnailBuilder != null) {
            if (isThumbnailBuilt) {
                throw new IllegalStateException("You cannot use a request as both the main request and a " + "thumbnail, consider using clone() on the request(s) passed to thumbnail()");
            }
            TransitionOptions<?, ? super TranscodeType> thumbTransitionOptions = thumbnailBuilder.transitionOptions;
            if (DEFAULT_ANIMATION_OPTIONS.equals(thumbTransitionOptions)) {
                thumbTransitionOptions = transitionOptions;
            }
            Priority thumbPriority = thumbnailBuilder.requestOptions.isPrioritySet() ? thumbnailBuilder.requestOptions.getPriority() : getThumbnailPriority(priority);
            int thumbOverrideWidth = thumbnailBuilder.requestOptions.getOverrideWidth();
            int thumbOverrideHeight = thumbnailBuilder.requestOptions.getOverrideHeight();
            if (Util.isValidDimensions(overrideWidth, overrideHeight) && !thumbnailBuilder.requestOptions.isValidOverride()) {
                thumbOverrideWidth = requestOptions.getOverrideWidth();
                thumbOverrideHeight = requestOptions.getOverrideHeight();
            }
            ThumbnailRequestCoordinator coordinator = new ThumbnailRequestCoordinator(parentCoordinator);
            Request fullRequest = obtainRequest(target, requestOptions, coordinator, transitionOptions, priority, overrideWidth, overrideHeight);
            isThumbnailBuilt = true;
            Request thumbRequest = thumbnailBuilder.buildRequestRecursive(target, coordinator, thumbTransitionOptions, thumbPriority, thumbOverrideWidth, thumbOverrideHeight);
            isThumbnailBuilt = false;
            coordinator.setRequests(fullRequest, thumbRequest);
            return coordinator;
        } else if (thumbSizeMultiplier != null) {
            ThumbnailRequestCoordinator coordinator = new ThumbnailRequestCoordinator(parentCoordinator);
            Request fullRequest = obtainRequest(target, requestOptions, coordinator, transitionOptions, priority, overrideWidth, overrideHeight);
            BaseRequestOptions<?> thumbnailOptions = requestOptions.clone().sizeMultiplier(thumbSizeMultiplier);
            Request thumbnailRequest = obtainRequest(target, thumbnailOptions, coordinator, transitionOptions, getThumbnailPriority(priority), overrideWidth, overrideHeight);
            coordinator.setRequests(fullRequest, thumbnailRequest);
            return coordinator;
        } else {
            return obtainRequest(target, requestOptions, parentCoordinator, transitionOptions, priority, overrideWidth, overrideHeight);
        }
    }

    private Request obtainRequest(Target<TranscodeType> target, BaseRequestOptions<?> requestOptions, RequestCoordinator requestCoordinator, TransitionOptions<?, ? super TranscodeType> transitionOptions, Priority priority, int overrideWidth, int overrideHeight) {
        requestOptions.lock();
        return SingleRequest.obtain(context, model, transcodeClass, requestOptions, overrideWidth, overrideHeight, priority, target, requestListener, requestCoordinator, context.getEngine(), transitionOptions.getTransitionFactory());
    }
}