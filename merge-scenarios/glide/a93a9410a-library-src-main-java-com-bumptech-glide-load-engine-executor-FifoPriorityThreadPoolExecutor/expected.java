package com.bumptech.glide.load.engine.executor;

import android.util.Log;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FifoPriorityThreadPoolExecutor extends ThreadPoolExecutor {

    private static final String TAG = "GlidePool";

    private static final String DEFAULT_NAME = "fifo-pool";

    private final AtomicInteger ordering = new AtomicInteger();

    private final UncaughtThrowableStrategy uncaughtThrowableStrategy;

    public FifoPriorityThreadPoolExecutor(int poolSize) {
        this(poolSize, UncaughtThrowableStrategy.LOG);
    }

    public FifoPriorityThreadPoolExecutor(int poolSize, UncaughtThrowableStrategy uncaughtThrowableStrategy) {
        this(poolSize, poolSize, 0, TimeUnit.MILLISECONDS, new DefaultThreadFactory(), uncaughtThrowableStrategy);
    }

    public FifoPriorityThreadPoolExecutor(String name, int poolSize) {
        this(poolSize, poolSize, 0, TimeUnit.MILLISECONDS, new DefaultThreadFactory(name), UncaughtThrowableStrategy.LOG);
    }

    public FifoPriorityThreadPoolExecutor(String name, int poolSize, UncaughtThrowableStrategy uncaughtThrowableStrategy) {
        this(poolSize, poolSize, 0, TimeUnit.MILLISECONDS, new DefaultThreadFactory(name), uncaughtThrowableStrategy);
    }

    public FifoPriorityThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAlive, TimeUnit timeUnit, ThreadFactory threadFactory, UncaughtThrowableStrategy uncaughtThrowableStrategy) {
        super(corePoolSize, maximumPoolSize, keepAlive, timeUnit, new PriorityBlockingQueue<Runnable>(), threadFactory);
        this.uncaughtThrowableStrategy = uncaughtThrowableStrategy;
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new LoadTask<>(runnable, value, ordering.getAndIncrement());
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        if (t == null && r instanceof Future<?>) {
            Future<?> future = (Future<?>) r;
            if (future.isDone() && !future.isCancelled()) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    uncaughtThrowableStrategy.handle(e);
                }
            }
        }
    }

    public enum UncaughtThrowableStrategy {

        IGNORE, LOG {

            @Override
            protected void handle(Throwable t) {
                if (Log.isLoggable(TAG, Log.ERROR)) {
                    Log.e(TAG, "Request threw uncaught throwable", t);
                }
            }
        }
        , THROW {

            @Override
            protected void handle(Throwable t) {
                super.handle(t);
                throw new RuntimeException(t);
            }
        }
        ;

        protected void handle(Throwable t) {
        }
    }

    public static class DefaultThreadFactory implements ThreadFactory {

        private final String name;

        private int threadNum = 0;

        public DefaultThreadFactory() {
            this(DEFAULT_NAME);
        }

        public DefaultThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            final Thread result = new Thread(runnable, name + "-thread-" + threadNum) {

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