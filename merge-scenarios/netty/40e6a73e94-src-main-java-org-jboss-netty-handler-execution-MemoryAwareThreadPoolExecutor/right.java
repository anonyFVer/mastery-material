package org.jboss.netty.handler.execution;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.logging.InternalLogger;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.util.DefaultObjectSizeEstimator;
import org.jboss.netty.util.ObjectSizeEstimator;
import org.jboss.netty.util.internal.ConcurrentIdentityHashMap;
import org.jboss.netty.util.internal.SharedResourceMisuseDetector;

public class MemoryAwareThreadPoolExecutor extends ThreadPoolExecutor {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(MemoryAwareThreadPoolExecutor.class);

    private static final SharedResourceMisuseDetector misuseDetector = new SharedResourceMisuseDetector(MemoryAwareThreadPoolExecutor.class);

    private volatile Settings settings;

    private final ConcurrentMap<Channel, AtomicLong> channelCounters = new ConcurrentIdentityHashMap<Channel, AtomicLong>();

    private final Limiter totalLimiter;

    public MemoryAwareThreadPoolExecutor(int corePoolSize, long maxChannelMemorySize, long maxTotalMemorySize) {
        this(corePoolSize, maxChannelMemorySize, maxTotalMemorySize, 30, TimeUnit.SECONDS);
    }

    public MemoryAwareThreadPoolExecutor(int corePoolSize, long maxChannelMemorySize, long maxTotalMemorySize, long keepAliveTime, TimeUnit unit) {
        this(corePoolSize, maxChannelMemorySize, maxTotalMemorySize, keepAliveTime, unit, Executors.defaultThreadFactory());
    }

    public MemoryAwareThreadPoolExecutor(int corePoolSize, long maxChannelMemorySize, long maxTotalMemorySize, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory) {
        this(corePoolSize, maxChannelMemorySize, maxTotalMemorySize, keepAliveTime, unit, new DefaultObjectSizeEstimator(), threadFactory);
    }

    public MemoryAwareThreadPoolExecutor(int corePoolSize, long maxChannelMemorySize, long maxTotalMemorySize, long keepAliveTime, TimeUnit unit, ObjectSizeEstimator objectSizeEstimator, ThreadFactory threadFactory) {
        super(corePoolSize, corePoolSize, keepAliveTime, unit, new LinkedTransferQueue<Runnable>(), threadFactory, new NewThreadRunsPolicy());
        if (objectSizeEstimator == null) {
            throw new NullPointerException("objectSizeEstimator");
        }
        if (maxChannelMemorySize < 0) {
            throw new IllegalArgumentException("maxChannelMemorySize: " + maxChannelMemorySize);
        }
        if (maxTotalMemorySize < 0) {
            throw new IllegalArgumentException("maxTotalMemorySize: " + maxTotalMemorySize);
        }
        try {
            Method m = getClass().getMethod("allowCoreThreadTimeOut", new Class[] { boolean.class });
            m.invoke(this, Boolean.TRUE);
        } catch (Throwable t) {
            logger.debug("ThreadPoolExecutor.allowCoreThreadTimeOut() is not " + "supported in this platform.");
        }
        settings = new Settings(objectSizeEstimator, maxChannelMemorySize);
        if (maxTotalMemorySize == 0) {
            totalLimiter = null;
        } else {
            totalLimiter = new Limiter(maxTotalMemorySize);
        }
        misuseDetector.increase();
    }

    @Override
    protected void terminated() {
        super.terminated();
        misuseDetector.decrease();
    }

    public ObjectSizeEstimator getObjectSizeEstimator() {
        return settings.objectSizeEstimator;
    }

    public void setObjectSizeEstimator(ObjectSizeEstimator objectSizeEstimator) {
        if (objectSizeEstimator == null) {
            throw new NullPointerException("objectSizeEstimator");
        }
        settings = new Settings(objectSizeEstimator, settings.maxChannelMemorySize);
    }

    public long getMaxChannelMemorySize() {
        return settings.maxChannelMemorySize;
    }

    public void setMaxChannelMemorySize(long maxChannelMemorySize) {
        if (maxChannelMemorySize < 0) {
            throw new IllegalArgumentException("maxChannelMemorySize: " + maxChannelMemorySize);
        }
        if (getTaskCount() > 0) {
            throw new IllegalStateException("can't be changed after a task is executed");
        }
        settings = new Settings(settings.objectSizeEstimator, maxChannelMemorySize);
    }

    public long getMaxTotalMemorySize() {
        return totalLimiter.limit;
    }

    @Deprecated
    public void setMaxTotalMemorySize(long maxTotalMemorySize) {
        if (maxTotalMemorySize < 0) {
            throw new IllegalArgumentException("maxTotalMemorySize: " + maxTotalMemorySize);
        }
        if (getTaskCount() > 0) {
            throw new IllegalStateException("can't be changed after a task is executed");
        }
    }

    @Override
    public void execute(Runnable command) {
        if (!(command instanceof ChannelEventRunnable)) {
            command = new MemoryAwareRunnable(command);
        }
        increaseCounter(command);
        doExecute(command);
    }

    protected void doExecute(Runnable task) {
        doUnorderedExecute(task);
    }

    protected final void doUnorderedExecute(Runnable task) {
        super.execute(task);
    }

    @Override
    public boolean remove(Runnable task) {
        boolean removed = super.remove(task);
        if (removed) {
            decreaseCounter(task);
        }
        return removed;
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        decreaseCounter(r);
    }

    protected void increaseCounter(Runnable task) {
        if (!shouldCount(task)) {
            return;
        }
        Settings settings = this.settings;
        long maxChannelMemorySize = settings.maxChannelMemorySize;
        int increment = settings.objectSizeEstimator.estimateSize(task);
        if (task instanceof ChannelEventRunnable) {
            ChannelEventRunnable eventTask = (ChannelEventRunnable) task;
            eventTask.estimatedSize = increment;
            Channel channel = eventTask.getEvent().getChannel();
            long channelCounter = getChannelCounter(channel).addAndGet(increment);
            if (maxChannelMemorySize != 0 && channelCounter >= maxChannelMemorySize && channel.isOpen()) {
                if (channel.isReadable()) {
                    ChannelHandlerContext ctx = eventTask.getContext();
                    if (ctx.getHandler() instanceof ExecutionHandler) {
                        ctx.setAttachment(Boolean.TRUE);
                    }
                    channel.setReadable(false);
                }
            }
        } else {
            ((MemoryAwareRunnable) task).estimatedSize = increment;
        }
        if (totalLimiter != null) {
            totalLimiter.increase(increment);
        }
    }

    protected void decreaseCounter(Runnable task) {
        if (!shouldCount(task)) {
            return;
        }
        Settings settings = this.settings;
        long maxChannelMemorySize = settings.maxChannelMemorySize;
        int increment;
        if (task instanceof ChannelEventRunnable) {
            increment = ((ChannelEventRunnable) task).estimatedSize;
        } else {
            increment = ((MemoryAwareRunnable) task).estimatedSize;
        }
        if (totalLimiter != null) {
            totalLimiter.decrease(increment);
        }
        if (task instanceof ChannelEventRunnable) {
            ChannelEventRunnable eventTask = (ChannelEventRunnable) task;
            Channel channel = eventTask.getEvent().getChannel();
            long channelCounter = getChannelCounter(channel).addAndGet(-increment);
            if (maxChannelMemorySize != 0 && channelCounter < maxChannelMemorySize && channel.isOpen()) {
                if (!channel.isReadable()) {
                    ChannelHandlerContext ctx = eventTask.getContext();
                    if (ctx.getHandler() instanceof ExecutionHandler) {
                        ctx.setAttachment(null);
                    }
                    channel.setReadable(true);
                }
            }
        }
    }

    private AtomicLong getChannelCounter(Channel channel) {
        AtomicLong counter = channelCounters.get(channel);
        if (counter == null) {
            counter = new AtomicLong();
            AtomicLong oldCounter = channelCounters.putIfAbsent(channel, counter);
            if (oldCounter != null) {
                counter = oldCounter;
            }
        }
        if (!channel.isOpen()) {
            channelCounters.remove(channel);
        }
        return counter;
    }

    protected boolean shouldCount(Runnable task) {
        if (task instanceof ChannelEventRunnable) {
            ChannelEventRunnable r = (ChannelEventRunnable) task;
            ChannelEvent e = r.getEvent();
            if (e instanceof WriteCompletionEvent) {
                return false;
            } else if (e instanceof ChannelStateEvent) {
                if (((ChannelStateEvent) e).getState() == ChannelState.INTEREST_OPS) {
                    return false;
                }
            }
        }
        return true;
    }

    private static final class Settings {

        final ObjectSizeEstimator objectSizeEstimator;

        final long maxChannelMemorySize;

        Settings(ObjectSizeEstimator objectSizeEstimator, long maxChannelMemorySize) {
            this.objectSizeEstimator = objectSizeEstimator;
            this.maxChannelMemorySize = maxChannelMemorySize;
        }
    }

    private static final class NewThreadRunsPolicy implements RejectedExecutionHandler {

        NewThreadRunsPolicy() {
            super();
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                final Thread t = new Thread(r, "Temporary task executor");
                t.start();
            } catch (Throwable e) {
                throw new RejectedExecutionException("Failed to start a new thread", e);
            }
        }
    }

    private static final class MemoryAwareRunnable implements Runnable {

        final Runnable task;

        int estimatedSize;

        MemoryAwareRunnable(Runnable task) {
            this.task = task;
        }

        @Override
        public void run() {
            task.run();
        }
    }

    private static class Limiter {

        final long limit;

        private long counter;

        private int waiters;

        Limiter(long limit) {
            super();
            this.limit = limit;
        }

        synchronized void increase(long amount) {
            while (counter >= limit) {
                waiters++;
                try {
                    wait();
                } catch (InterruptedException e) {
                } finally {
                    waiters--;
                }
            }
            counter += amount;
        }

        synchronized void decrease(long amount) {
            counter -= amount;
            if (counter < limit && waiters > 0) {
                notifyAll();
            }
        }
    }
}