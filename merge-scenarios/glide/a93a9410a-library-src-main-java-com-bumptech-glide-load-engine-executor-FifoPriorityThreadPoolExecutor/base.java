package com.bumptech.glide.load.engine.executor;

import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FifoPriorityThreadPoolExecutor extends ThreadPoolExecutor {

    AtomicInteger ordering = new AtomicInteger();

    public FifoPriorityThreadPoolExecutor(int poolSize) {
        this(poolSize, poolSize, 0, TimeUnit.MILLISECONDS, new DefaultThreadFactory());
    }

    public FifoPriorityThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAlive, TimeUnit timeUnit, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAlive, timeUnit, new PriorityBlockingQueue<Runnable>(), threadFactory);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new LoadTask<T>(runnable, value, ordering.getAndIncrement());
    }

    public static class DefaultThreadFactory implements ThreadFactory {

        int threadNum = 0;

        @Override
        public Thread newThread(Runnable runnable) {
            final Thread result = new Thread(runnable, "fifo-pool-thread-" + threadNum) {

                @Override
                public void run() {
                    android.os.Process.setThreadPriority(android.os.Process.THREAD_PRIORITY_BACKGROUND);
                    super.run();
                }
            };
            threadNum++;
            return result;
        }
    }

    static class LoadTask<T> extends FutureTask<T> implements Comparable<LoadTask<?>> {

        private final int priority;

        private final int order;

        public LoadTask(Runnable runnable, T result, int order) {
            super(runnable, result);
            if (!(runnable instanceof Prioritized)) {
                throw new IllegalArgumentException("FifoPriorityThreadPoolExecutor must be given Runnables that " + "implement Prioritized");
            }
            priority = ((Prioritized) runnable).getPriority();
            this.order = order;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean equals(Object o) {
            if (o instanceof LoadTask) {
                LoadTask<Object> other = (LoadTask<Object>) o;
                return order == other.order && priority == other.priority;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int result = priority;
            result = 31 * result + order;
            return result;
        }

        @Override
        public int compareTo(LoadTask<?> loadTask) {
            int result = priority - loadTask.priority;
            if (result == 0) {
                result = order - loadTask.order;
            }
            return result;
        }
    }
}