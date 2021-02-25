package com.bumptech.glide.manager;

import com.bumptech.glide.request.Request;
import com.bumptech.glide.util.Util;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;

public class RequestTracker {

    private final Set<Request> requests = Collections.newSetFromMap(new WeakHashMap<Request, Boolean>());

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final List<Request> pendingRequests = new ArrayList<>();

    private boolean isPaused;

    public void runRequest(Request request) {
        requests.add(request);
        if (!isPaused) {
            request.begin();
        } else {
            pendingRequests.add(request);
        }
    }

    void addRequest(Request request) {
        requests.add(request);
    }

    public boolean clearRemoveAndRecycle(Request request) {
        boolean isOwnedByUs = request != null && (requests.remove(request) || pendingRequests.remove(request));
        if (isOwnedByUs) {
            request.clear();
            request.recycle();
        }
        return isOwnedByUs;
    }

    public boolean isPaused() {
        return isPaused;
    }

    public void pauseRequests() {
        isPaused = true;
        for (Request request : Util.getSnapshot(requests)) {
            if (request.isRunning()) {
                request.pause();
                pendingRequests.add(request);
            }
        }
    }

    public void resumeRequests() {
        isPaused = false;
        for (Request request : Util.getSnapshot(requests)) {
            if (!request.isComplete() && !request.isCancelled() && !request.isRunning()) {
                request.begin();
            }
        }
        pendingRequests.clear();
    }

    public void clearRequests() {
        for (Request request : Util.getSnapshot(requests)) {
            clearRemoveAndRecycle(request);
        }
        pendingRequests.clear();
    }

    public void restartRequests() {
        for (Request request : Util.getSnapshot(requests)) {
            if (!request.isComplete() && !request.isCancelled()) {
                request.pause();
                if (!isPaused) {
                    request.begin();
                } else {
                    pendingRequests.add(request);
                }
            }
        }
    }
}