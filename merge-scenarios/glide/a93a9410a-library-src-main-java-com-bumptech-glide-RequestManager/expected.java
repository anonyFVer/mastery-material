package com.bumptech.glide;

import static com.bumptech.glide.request.RequestOptions.decodeTypeOf;
import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.drawable.Drawable;
import android.os.Handler;
import android.os.Looper;
import android.view.View;
import com.bumptech.glide.load.resource.bitmap.BitmapTransitionOptions;
import com.bumptech.glide.load.resource.drawable.DrawableTransitionOptions;
import com.bumptech.glide.load.resource.gif.GifDrawable;
import com.bumptech.glide.manager.ConnectivityMonitor;
import com.bumptech.glide.manager.ConnectivityMonitorFactory;
import com.bumptech.glide.manager.Lifecycle;
import com.bumptech.glide.manager.LifecycleListener;
import com.bumptech.glide.manager.RequestManagerTreeNode;
import com.bumptech.glide.manager.RequestTracker;
import com.bumptech.glide.request.Request;
import com.bumptech.glide.request.RequestOptions;
import com.bumptech.glide.request.target.Target;
import com.bumptech.glide.request.target.ViewTarget;
import com.bumptech.glide.request.transition.Transition;
import com.bumptech.glide.util.Util;

public class RequestManager implements LifecycleListener {

    private static final RequestOptions DECODE_TYPE_BITMAP = decodeTypeOf(Bitmap.class).lock();

    private static final RequestOptions DECODE_TYPE_GIF = decodeTypeOf(GifDrawable.class).lock();

    private final GlideContext context;

    private final Lifecycle lifecycle;

    private final RequestTracker requestTracker;

    private final RequestManagerTreeNode treeNode;

    public RequestManager(Context context, Lifecycle lifecycle, RequestManagerTreeNode treeNode) {
        this(context, lifecycle, treeNode, new RequestTracker(), new ConnectivityMonitorFactory());
    }

    RequestManager(Context context, final Lifecycle lifecycle, RequestManagerTreeNode treeNode, RequestTracker requestTracker, ConnectivityMonitorFactory factory) {
        this.context = Glide.get(context).getGlideContext();
        this.lifecycle = lifecycle;
        this.treeNode = treeNode;
        this.requestTracker = requestTracker;
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
        Glide.get(context).registerRequestManager(this);
    }

    public void onTrimMemory(int level) {
        context.onTrimMemory(level);
    }

    public void onLowMemory() {
        context.onLowMemory();
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
        Glide.get(context).unregisterRequestManager(this);
        requestTracker.clearRequests();
    }

    public RequestBuilder<Bitmap> asBitmap() {
        return as(Bitmap.class).transition(new BitmapTransitionOptions()).apply(DECODE_TYPE_BITMAP);
    }

    public RequestBuilder<GifDrawable> asGif() {
        return as(GifDrawable.class).transition(new DrawableTransitionOptions()).apply(DECODE_TYPE_GIF);
    }

    public RequestBuilder<Drawable> asDrawable() {
        return as(Drawable.class).transition(new DrawableTransitionOptions());
    }

    public <ResourceType> RequestBuilder<ResourceType> as(Class<ResourceType> resourceClass) {
        return new RequestBuilder<>(context, this, resourceClass);
    }

    public void clear(View view) {
        clear(new ClearTarget(view));
    }

    public void clear(Target<?> target) {
        Util.assertMainThread();
        if (target == null) {
            return;
        }
        Request request = target.getRequest();
        target.setRequest(null);
        untrackOrDelegate(target, request);
    }

    private void untrackOrDelegate(Target<?> target, Request request) {
        boolean isOwnedByUs = untrack(target, request);
        if (!isOwnedByUs) {
            Glide.get(context).removeFromManagers(target, request);
        }
    }

    boolean untrack(Target<?> target, Request request) {
        lifecycle.removeListener(target);
        return requestTracker.clearRemoveAndRecycle(request);
    }

    void track(Target<?> target, Request request) {
        lifecycle.addListener(target);
        requestTracker.runRequest(request);
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

    private static class ClearTarget extends ViewTarget<View, Object> {

        public ClearTarget(View view) {
            super(view);
        }

        @Override
        public void onResourceReady(Object resource, Transition<? super Object> transition) {
        }
    }
}