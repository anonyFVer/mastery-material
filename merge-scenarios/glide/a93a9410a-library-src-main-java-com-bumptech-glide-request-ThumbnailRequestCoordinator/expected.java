package com.bumptech.glide.request;

public class ThumbnailRequestCoordinator implements RequestCoordinator, Request {

    private Request full;

    private Request thumb;

    private RequestCoordinator coordinator;

    private boolean isRunning;

    public ThumbnailRequestCoordinator() {
        this(null);
    }

    public ThumbnailRequestCoordinator(RequestCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    public void setRequests(Request full, Request thumb) {
        this.full = full;
        this.thumb = thumb;
    }

    @Override
    public boolean canSetImage(Request request) {
        return parentCanSetImage() && (request.equals(full) || !full.isResourceSet());
    }

    private boolean parentCanSetImage() {
        return coordinator == null || coordinator.canSetImage(this);
    }

    @Override
    public boolean canNotifyStatusChanged(Request request) {
        return parentCanNotifyStatusChanged() && request.equals(full) && !isAnyResourceSet();
    }

    private boolean parentCanNotifyStatusChanged() {
        return coordinator == null || coordinator.canNotifyStatusChanged(this);
    }

    @Override
    public boolean isAnyResourceSet() {
        return parentIsAnyResourceSet() || isResourceSet();
    }

    @Override
    public void onRequestSuccess(Request request) {
        if (request.equals(thumb)) {
            return;
        }
        if (coordinator != null) {
            coordinator.onRequestSuccess(this);
        }
        if (!thumb.isComplete()) {
            thumb.clear();
        }
    }

    private boolean parentIsAnyResourceSet() {
        return coordinator != null && coordinator.isAnyResourceSet();
    }

    @Override
    public void begin() {
        isRunning = true;
        if (!thumb.isRunning()) {
            thumb.begin();
        }
        if (isRunning && !full.isRunning()) {
            full.begin();
        }
    }

    @Override
    public void pause() {
        isRunning = false;
        full.pause();
        thumb.pause();
    }

    @Override
    public void clear() {
        isRunning = false;
        thumb.clear();
        full.clear();
    }

    @Override
    public boolean isPaused() {
        return full.isPaused();
    }

    @Override
    public boolean isRunning() {
        return full.isRunning();
    }

    @Override
    public boolean isComplete() {
        return full.isComplete() || thumb.isComplete();
    }

    @Override
    public boolean isResourceSet() {
        return full.isResourceSet() || thumb.isResourceSet();
    }

    @Override
    public boolean isCancelled() {
        return full.isCancelled();
    }

    @Override
    public boolean isFailed() {
        return full.isFailed();
    }

    @Override
    public void recycle() {
        full.recycle();
        thumb.recycle();
    }
}