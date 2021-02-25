package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class ListenableFuture<V> extends BaseFuture<V> implements ActionListener<V> {

    private volatile boolean done = false;

    private final List<Tuple<ActionListener<V>, ExecutorService>> listeners = new ArrayList<>();

    public void addListener(ActionListener<V> listener, ExecutorService executor) {
        if (done) {
            notifyListener(listener, EsExecutors.newDirectExecutorService());
        } else {
            final boolean run;
            synchronized (this) {
                if (done) {
                    run = true;
                } else {
                    listeners.add(new Tuple<>(listener, executor));
                    run = false;
                }
            }
            if (run) {
                notifyListener(listener, EsExecutors.newDirectExecutorService());
            }
        }
    }

    @Override
    protected synchronized void done() {
        done = true;
        listeners.forEach(t -> notifyListener(t.v1(), t.v2()));
        listeners.clear();
    }

    private void notifyListener(ActionListener<V> listener, ExecutorService executorService) {
        try {
            executorService.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        V value = FutureUtils.get(ListenableFuture.this, 0L, TimeUnit.NANOSECONDS);
                        listener.onResponse(value);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }

                @Override
                public String toString() {
                    return "ListenableFuture notification";
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void onResponse(V v) {
        final boolean set = set(v);
        if (set == false) {
            throw new IllegalStateException("did not set value, value or exception already set?");
        }
    }

    @Override
    public void onFailure(Exception e) {
        final boolean set = setException(e);
        if (set == false) {
            throw new IllegalStateException("did not set exception, value already set or exception already set?");
        }
    }
}