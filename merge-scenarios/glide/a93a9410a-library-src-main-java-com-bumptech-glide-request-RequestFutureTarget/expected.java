package com.bumptech.glide.request;

import android.graphics.drawable.Drawable;
import android.os.Handler;
import com.bumptech.glide.request.target.SizeReadyCallback;
import com.bumptech.glide.request.transition.Transition;
import com.bumptech.glide.util.Util;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RequestFutureTarget<R> implements FutureTarget<R>, Runnable {

    private static final Waiter DEFAULT_WAITER = new Waiter();

    private final Handler mainHandler;

    private final int width;

    private final int height;

    private final boolean assertBackgroundThread;

    private final Waiter waiter;

    private R resource;

    private Request request;

    private boolean isCancelled;

    private boolean resultReceived;

    private boolean loadFailed;

    public RequestFutureTarget(Handler mainHandler, int width, int height) {
        this(mainHandler, width, height, true, DEFAULT_WAITER);
    }

    RequestFutureTarget(Handler mainHandler, int width, int height, boolean assertBackgroundThread, Waiter waiter) {
        this.mainHandler = mainHandler;
        this.width = width;
        this.height = height;
        this.assertBackgroundThread = assertBackgroundThread;
        this.waiter = waiter;
    }

    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
        if (isCancelled) {
            return true;
        }
        final boolean result = !isDone();
        if (result) {
            isCancelled = true;
            waiter.notifyAll(this);
        }
        clearOnMainThread();
        return result;
    }

    @Override
    public synchronized boolean isCancelled() {
        return isCancelled;
    }

    @Override
    public synchronized boolean isDone() {
        return isCancelled || resultReceived;
    }

    @Override
    public R get() throws InterruptedException, ExecutionException {
        try {
            return doGet(null);
        } catch (TimeoutException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public R get(long time, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        return doGet(timeUnit.toMillis(time));
    }

    @Override
    public void getSize(SizeReadyCallback cb) {
        cb.onSizeReady(width, height);
    }

    @Override
    public void setRequest(Request request) {
        this.request = request;
    }

    @Override
    public Request getRequest() {
        return request;
    }

    @Override
    public void onLoadCleared(Drawable placeholder) {
    }

    @Override
    public void onLoadStarted(Drawable placeholder) {
    }

    @Override
    public synchronized void onLoadFailed(Drawable errorDrawable) {
        loadFailed = true;
        waiter.notifyAll(this);
    }

    @Override
    public synchronized void onResourceReady(R resource, Transition<? super R> transition) {
        resultReceived = true;
        this.resource = resource;
        waiter.notifyAll(this);
    }

    private synchronized R doGet(Long timeoutMillis) throws ExecutionException, InterruptedException, TimeoutException {
        if (assertBackgroundThread) {
            Util.assertBackgroundThread();
        }
        if (isCancelled) {
            throw new CancellationException();
        } else if (loadFailed) {
            throw new ExecutionException(new IllegalStateException("Load failed"));
        } else if (resultReceived) {
            return resource;
        }
        if (timeoutMillis == null) {
            waiter.waitForTimeout(this, 0);
        } else if (timeoutMillis > 0) {
            waiter.waitForTimeout(this, timeoutMillis);
        }
        if (Thread.interrupted()) {
            throw new InterruptedException();
        } else if (loadFailed) {
            throw new ExecutionException(new IllegalStateException("Load failed"));
        } else if (isCancelled) {
            throw new CancellationException();
        } else if (!resultReceived) {
            throw new TimeoutException();
        }
        return resource;
    }

    @Override
    public void run() {
        if (request != null) {
            request.clear();
            request = null;
        }
    }

    private void clearOnMainThread() {
        mainHandler.post(this);
    }

    @Override
    public void onStart() {
    }

    @Override
    public void onStop() {
    }

    @Override
    public void onDestroy() {
    }

    static class Waiter {

        public void waitForTimeout(Object toWaitOn, long timeoutMillis) throws InterruptedException {
            toWaitOn.wait(timeoutMillis);
        }

        public void notifyAll(Object toNotify) {
            toNotify.notifyAll();
        }
    }
}