package com.bumptech.glide.request.target;

import android.annotation.TargetApi;
import android.content.Context;
import android.graphics.Point;
import android.os.Build;
import android.util.Log;
import android.view.Display;
import android.view.View;
import android.view.ViewGroup.LayoutParams;
import android.view.ViewTreeObserver;
import android.view.WindowManager;
import com.bumptech.glide.request.Request;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

public abstract class ViewTarget<T extends View, Z> extends BaseTarget<Z> {

    private static final String TAG = "ViewTarget";

    protected final T view;

    private final SizeDeterminer sizeDeterminer;

    public ViewTarget(T view) {
        if (view == null) {
            throw new NullPointerException("View must not be null!");
        }
        this.view = view;
        sizeDeterminer = new SizeDeterminer(view);
    }

    public T getView() {
        return view;
    }

    @Override
    public void getSize(SizeReadyCallback cb) {
        sizeDeterminer.getSize(cb);
    }

    @Override
    public void setRequest(Request request) {
        view.setTag(request);
    }

    @Override
    public Request getRequest() {
        Object tag = view.getTag();
        Request request = null;
        if (tag != null) {
            if (tag instanceof Request) {
                request = (Request) tag;
            } else {
                throw new IllegalArgumentException("You must not call setTag() on a view Glide is targeting");
            }
        }
        return request;
    }

    @Override
    public String toString() {
        return "Target for: " + view;
    }

    private static class SizeDeterminer {

        private static final int PENDING_SIZE = 0;

        private final View view;

        private final List<SizeReadyCallback> cbs = new ArrayList<SizeReadyCallback>();

        private SizeDeterminerLayoutListener layoutListener;

        private Point displayDimens;

        public SizeDeterminer(View view) {
            this.view = view;
        }

        private void notifyCbs(int width, int height) {
            for (SizeReadyCallback cb : cbs) {
                cb.onSizeReady(width, height);
            }
            cbs.clear();
        }

        private void checkCurrentDimens() {
            if (cbs.isEmpty()) {
                return;
            }
            int currentWidth = getViewWidthOrParam();
            int currentHeight = getViewHeightOrParam();
            if (!isSizeValid(currentWidth) || !isSizeValid(currentHeight)) {
                return;
            }
            notifyCbs(currentWidth, currentHeight);
            ViewTreeObserver observer = view.getViewTreeObserver();
            if (observer.isAlive()) {
                observer.removeOnPreDrawListener(layoutListener);
            }
            layoutListener = null;
        }

        public void getSize(SizeReadyCallback cb) {
            int currentWidth = getViewWidthOrParam();
            int currentHeight = getViewHeightOrParam();
            if (isSizeValid(currentWidth) && isSizeValid(currentHeight)) {
                cb.onSizeReady(currentWidth, currentHeight);
            } else {
                if (!cbs.contains(cb)) {
                    cbs.add(cb);
                }
                if (layoutListener == null) {
                    final ViewTreeObserver observer = view.getViewTreeObserver();
                    layoutListener = new SizeDeterminerLayoutListener(this);
                    observer.addOnPreDrawListener(layoutListener);
                }
            }
        }

        private int getViewHeightOrParam() {
            final LayoutParams layoutParams = view.getLayoutParams();
            if (isSizeValid(view.getHeight())) {
                return view.getHeight();
            } else if (layoutParams != null) {
                return getSizeForParam(layoutParams.height, true);
            } else {
                return PENDING_SIZE;
            }
        }

        private int getViewWidthOrParam() {
            final LayoutParams layoutParams = view.getLayoutParams();
            if (isSizeValid(view.getWidth())) {
                return view.getWidth();
            } else if (layoutParams != null) {
                return getSizeForParam(layoutParams.width, false);
            } else {
                return PENDING_SIZE;
            }
        }

        private int getSizeForParam(int param, boolean isHeight) {
            if (param == LayoutParams.WRAP_CONTENT) {
                Point displayDimens = getDisplayDimens();
                return isHeight ? displayDimens.y : displayDimens.x;
            } else {
                return param;
            }
        }

        @TargetApi(Build.VERSION_CODES.HONEYCOMB_MR2)
        @SuppressWarnings("deprecation")
        private Point getDisplayDimens() {
            if (displayDimens != null) {
                return displayDimens;
            }
            WindowManager windowManager = (WindowManager) view.getContext().getSystemService(Context.WINDOW_SERVICE);
            Display display = windowManager.getDefaultDisplay();
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB_MR2) {
                displayDimens = new Point();
                display.getSize(displayDimens);
            } else {
                displayDimens = new Point(display.getWidth(), display.getHeight());
            }
            return displayDimens;
        }

        private boolean isSizeValid(int size) {
            return size > 0 || size == LayoutParams.WRAP_CONTENT;
        }

        private static class SizeDeterminerLayoutListener implements ViewTreeObserver.OnPreDrawListener {

            private final WeakReference<SizeDeterminer> sizeDeterminerRef;

            public SizeDeterminerLayoutListener(SizeDeterminer sizeDeterminer) {
                sizeDeterminerRef = new WeakReference<SizeDeterminer>(sizeDeterminer);
            }

            @Override
            public boolean onPreDraw() {
                if (Log.isLoggable(TAG, Log.VERBOSE)) {
                    Log.v(TAG, "OnGlobalLayoutListener called listener=" + this);
                }
                SizeDeterminer sizeDeterminer = sizeDeterminerRef.get();
                if (sizeDeterminer != null) {
                    sizeDeterminer.checkCurrentDimens();
                }
                return true;
            }
        }
    }
}