package org.jboss.netty.handler.execution;

import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.util.ObjectSizeEstimator;
import org.jboss.netty.util.internal.ConcurrentIdentityWeakKeyHashMap;

public class OrderedMemoryAwareThreadPoolExecutor extends MemoryAwareThreadPoolExecutor {

    private final ConcurrentMap<Object, Executor> childExecutors = newChildExecutorMap();

    public OrderedMemoryAwareThreadPoolExecutor(int corePoolSize, long maxChannelMemorySize, long maxTotalMemorySize) {
        super(corePoolSize, maxChannelMemorySize, maxTotalMemorySize);
    }

    public OrderedMemoryAwareThreadPoolExecutor(int corePoolSize, long maxChannelMemorySize, long maxTotalMemorySize, long keepAliveTime, TimeUnit unit) {
        super(corePoolSize, maxChannelMemorySize, maxTotalMemorySize, keepAliveTime, unit);
    }

    public OrderedMemoryAwareThreadPoolExecutor(int corePoolSize, long maxChannelMemorySize, long maxTotalMemorySize, long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory) {
        super(corePoolSize, maxChannelMemorySize, maxTotalMemorySize, keepAliveTime, unit, threadFactory);
    }

    public OrderedMemoryAwareThreadPoolExecutor(int corePoolSize, long maxChannelMemorySize, long maxTotalMemorySize, long keepAliveTime, TimeUnit unit, ObjectSizeEstimator objectSizeEstimator, ThreadFactory threadFactory) {
        super(corePoolSize, maxChannelMemorySize, maxTotalMemorySize, keepAliveTime, unit, objectSizeEstimator, threadFactory);
    }

    protected ConcurrentMap<Object, Executor> newChildExecutorMap() {
        return new ConcurrentIdentityWeakKeyHashMap<Object, Executor>();
    }

    protected Object getChildExecutorKey(ChannelEvent e) {
        return e.getChannel();
    }

    protected Set<Object> getChildExecutorKeySet() {
        return childExecutors.keySet();
    }

    protected boolean removeChildExecutor(Object key) {
        return childExecutors.remove(key) != null;
    }

    @Override
    protected void doExecute(Runnable task) {
        if (!(task instanceof ChannelEventRunnable)) {
            doUnorderedExecute(task);
        } else {
            ChannelEventRunnable r = (ChannelEventRunnable) task;
            getChildExecutor(r.getEvent()).execute(task);
        }
    }

    private Executor getChildExecutor(ChannelEvent e) {
        Object key = getChildExecutorKey(e);
        Executor executor = childExecutors.get(key);
        if (executor == null) {
            executor = new ChildExecutor();
            Executor oldExecutor = childExecutors.putIfAbsent(key, executor);
            if (oldExecutor != null) {
                executor = oldExecutor;
            }
        }
        if (e instanceof ChannelStateEvent) {
            Channel channel = e.getChannel();
            ChannelStateEvent se = (ChannelStateEvent) e;
            if (se.getState() == ChannelState.OPEN && !channel.isOpen()) {
                childExecutors.remove(channel);
            }
        }
        return executor;
    }

    @Override
    protected boolean shouldCount(Runnable task) {
        if (task instanceof ChildExecutor) {
            return false;
        }
        return super.shouldCount(task);
    }

    void onAfterExecute(Runnable r, Throwable t) {
        afterExecute(r, t);
    }

    private final class ChildExecutor implements Executor, Runnable {

        private final LinkedList<Runnable> tasks = new LinkedList<Runnable>();

        ChildExecutor() {
        }

        @Override
        public void execute(Runnable command) {
            boolean needsExecution;
            synchronized (tasks) {
                needsExecution = tasks.isEmpty();
                tasks.add(command);
            }
            if (needsExecution) {
                doUnorderedExecute(this);
            }
        }

        @Override
        public void run() {
            Thread thread = Thread.currentThread();
            for (; ; ) {
                final Runnable task;
                synchronized (tasks) {
                    task = tasks.getFirst();
                }
                boolean ran = false;
                beforeExecute(thread, task);
                try {
                    task.run();
                    ran = true;
                    onAfterExecute(task, null);
                } catch (RuntimeException e) {
                    if (!ran) {
                        onAfterExecute(task, e);
                    }
                    throw e;
                } finally {
                    synchronized (tasks) {
                        tasks.removeFirst();
                        if (tasks.isEmpty()) {
                            break;
                        }
                    }
                }
            }
        }
    }
}