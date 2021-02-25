package com.bumptech.glide.manager;

import com.bumptech.glide.request.Request;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;

public class RequestTracker {

    private final Set<Request> requests = Collections.newSetFromMap(new WeakHashMap<Request, Boolean>());

    private boolean isPaused;

    public void runRequest(Request request) {
        requests.add(request);
        if (!isPaused) {
            request.begin();
        }
    }

    void addRequest(Request request) {
        requests.add(request);
    }

    public void removeRequest(Request request) {
        requests.remove(request);
    }

    public boolean isPaused() {
        return isPaused;
    }

    public void pauseRequests() {
        isPaused = true;
        for (Request request : getSnapshot()) {
            if (request.isRunning()) {
                request.pause();
            }
        }
    }

    public void resumeRequests() {
        isPaused = false;
        for (Request request : getSnapshot()) {
            if (!request.isComplete() && !request.isCancelled() && !request.isRunning()) {
                request.begin();
            }
        }
    }

    public void clearRequests() {
        for (Request request : getSnapshot()) {
            request.clear();
        }
    }

    public void restartRequests() {
        for (Request request : getSnapshot()) {
            if (!request.isComplete() && !request.isCancelled()) {
                request.pause();
                if (!isPaused) {
                    request.begin();
                }
            }
        }
    }

    private List<Request> getSnapshot() {
        List<Request> result = new ArrayList<Request>(requests.size());
        for (Request request : requests) {
            result.add(request);
        }
        return result;
    }
}