package io.netty.channel.group;

import static java.util.concurrent.TimeUnit.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DefaultChannelGroupFuture implements ChannelGroupFuture {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(DefaultChannelGroupFuture.class);

    private final ChannelGroup group;

    final Map<Integer, ChannelFuture> futures;

    private ChannelGroupFutureListener firstListener;

    private List<ChannelGroupFutureListener> otherListeners;

    private boolean done;

    int successCount;

    int failureCount;

    private int waiters;

    private final ChannelFutureListener childListener = new ChannelFutureListener() {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            boolean success = future.isSuccess();
            boolean callSetDone;
            synchronized (DefaultChannelGroupFuture.this) {
                if (success) {
                    successCount++;
                } else {
                    failureCount++;
                }
                callSetDone = successCount + failureCount == futures.size();
                assert successCount + failureCount <= futures.size();
            }
            if (callSetDone) {
                setDone();
            }
        }
    };

    public DefaultChannelGroupFuture(ChannelGroup group, Collection<ChannelFuture> futures) {
        if (group == null) {
            throw new NullPointerException("group");
        }
        if (futures == null) {
            throw new NullPointerException("futures");
        }
        this.group = group;
        Map<Integer, ChannelFuture> futureMap = new LinkedHashMap<Integer, ChannelFuture>();
        for (ChannelFuture f : futures) {
            futureMap.put(f.channel().id(), f);
        }
        this.futures = Collections.unmodifiableMap(futureMap);
        for (ChannelFuture f : this.futures.values()) {
            f.addListener(childListener);
        }
        if (this.futures.isEmpty()) {
            setDone();
        }
    }

    DefaultChannelGroupFuture(ChannelGroup group, Map<Integer, ChannelFuture> futures) {
        this.group = group;
        this.futures = Collections.unmodifiableMap(futures);
        for (ChannelFuture f : this.futures.values()) {
            f.addListener(childListener);
        }
        if (this.futures.isEmpty()) {
            setDone();
        }
    }

    @Override
    public ChannelGroup getGroup() {
        return group;
    }

    @Override
    public ChannelFuture find(Integer channelId) {
        return futures.get(channelId);
    }

    @Override
    public ChannelFuture find(Channel channel) {
        return futures.get(channel.id());
    }

    @Override
    public Iterator<ChannelFuture> iterator() {
        return futures.values().iterator();
    }

    @Override
    public synchronized boolean isDone() {
        return done;
    }

    @Override
    public synchronized boolean isCompleteSuccess() {
        return successCount == futures.size();
    }

    @Override
    public synchronized boolean isPartialSuccess() {
        return successCount != 0 && successCount != futures.size();
    }

    @Override
    public synchronized boolean isPartialFailure() {
        return failureCount != 0 && failureCount != futures.size();
    }

    @Override
    public synchronized boolean isCompleteFailure() {
        int futureCnt = futures.size();
        return futureCnt != 0 && failureCount == futureCnt;
    }

    @Override
    public void addListener(ChannelGroupFutureListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener");
        }
        boolean notifyNow = false;
        synchronized (this) {
            if (done) {
                notifyNow = true;
            } else {
                if (firstListener == null) {
                    firstListener = listener;
                } else {
                    if (otherListeners == null) {
                        otherListeners = new ArrayList<ChannelGroupFutureListener>(1);
                    }
                    otherListeners.add(listener);
                }
            }
        }
        if (notifyNow) {
            notifyListener(listener);
        }
    }

    @Override
    public void removeListener(ChannelGroupFutureListener listener) {
        if (listener == null) {
            throw new NullPointerException("listener");
        }
        synchronized (this) {
            if (!done) {
                if (listener == firstListener) {
                    if (otherListeners != null && !otherListeners.isEmpty()) {
                        firstListener = otherListeners.remove(0);
                    } else {
                        firstListener = null;
                    }
                } else if (otherListeners != null) {
                    otherListeners.remove(listener);
                }
            }
        }
    }

    @Override
    public ChannelGroupFuture await() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        synchronized (this) {
            while (!done) {
                waiters++;
                try {
                    wait();
                } finally {
                    waiters--;
                }
            }
        }
        return this;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return await0(unit.toNanos(timeout), true);
    }

    @Override
    public boolean await(long timeoutMillis) throws InterruptedException {
        return await0(MILLISECONDS.toNanos(timeoutMillis), true);
    }

    @Override
    public ChannelGroupFuture awaitUninterruptibly() {
        boolean interrupted = false;
        synchronized (this) {
            while (!done) {
                waiters++;
                try {
                    wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                } finally {
                    waiters--;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return this;
    }

    @Override
    public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
        try {
            return await0(unit.toNanos(timeout), false);
        } catch (InterruptedException e) {
            throw new InternalError();
        }
    }

    @Override
    public boolean awaitUninterruptibly(long timeoutMillis) {
        try {
            return await0(MILLISECONDS.toNanos(timeoutMillis), false);
        } catch (InterruptedException e) {
            throw new InternalError();
        }
    }

    private boolean await0(long timeoutNanos, boolean interruptable) throws InterruptedException {
        if (interruptable && Thread.interrupted()) {
            throw new InterruptedException();
        }
        long startTime = timeoutNanos <= 0 ? 0 : System.nanoTime();
        long waitTime = timeoutNanos;
        boolean interrupted = false;
        try {
            synchronized (this) {
                if (done) {
                    return done;
                } else if (waitTime <= 0) {
                    return done;
                }
                waiters++;
                try {
                    for (; ; ) {
                        try {
                            wait(waitTime / 1000000, (int) (waitTime % 1000000));
                        } catch (InterruptedException e) {
                            if (interruptable) {
                                throw e;
                            } else {
                                interrupted = true;
                            }
                        }
                        if (done) {
                            return true;
                        } else {
                            waitTime = timeoutNanos - (System.nanoTime() - startTime);
                            if (waitTime <= 0) {
                                return done;
                            }
                        }
                    }
                } finally {
                    waiters--;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void setDone() {
        synchronized (this) {
            if (done) {
                return;
            }
            done = true;
            if (waiters > 0) {
                notifyAll();
            }
        }
        notifyListeners();
    }

    private void notifyListeners() {
        if (firstListener != null) {
            notifyListener(firstListener);
            firstListener = null;
            if (otherListeners != null) {
                for (ChannelGroupFutureListener l : otherListeners) {
                    notifyListener(l);
                }
                otherListeners = null;
            }
        }
    }

    private void notifyListener(ChannelGroupFutureListener l) {
        try {
            l.operationComplete(this);
        } catch (Throwable t) {
            if (logger.isWarnEnabled()) {
                logger.warn("An exception was thrown by " + ChannelFutureListener.class.getSimpleName() + ".", t);
            }
        }
    }
}