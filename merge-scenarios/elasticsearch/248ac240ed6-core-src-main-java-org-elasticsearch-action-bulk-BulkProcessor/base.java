package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class BulkProcessor implements Closeable {

    public interface Listener {

        void beforeBulk(long executionId, BulkRequest request);

        void afterBulk(long executionId, BulkRequest request, BulkResponse response);

        void afterBulk(long executionId, BulkRequest request, Throwable failure);
    }

    public static class Builder {

        private final Client client;

        private final Listener listener;

        private String name;

        private int concurrentRequests = 1;

        private int bulkActions = 1000;

        private ByteSizeValue bulkSize = new ByteSizeValue(5, ByteSizeUnit.MB);

        private TimeValue flushInterval = null;

        private BackoffPolicy backoffPolicy = BackoffPolicy.exponentialBackoff();

        public Builder(Client client, Listener listener) {
            this.client = client;
            this.listener = listener;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setConcurrentRequests(int concurrentRequests) {
            this.concurrentRequests = concurrentRequests;
            return this;
        }

        public Builder setBulkActions(int bulkActions) {
            this.bulkActions = bulkActions;
            return this;
        }

        public Builder setBulkSize(ByteSizeValue bulkSize) {
            this.bulkSize = bulkSize;
            return this;
        }

        public Builder setFlushInterval(TimeValue flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder setBackoffPolicy(BackoffPolicy backoffPolicy) {
            if (backoffPolicy == null) {
                throw new NullPointerException("'backoffPolicy' must not be null. To disable backoff, pass BackoffPolicy.noBackoff()");
            }
            this.backoffPolicy = backoffPolicy;
            return this;
        }

        public BulkProcessor build() {
            return new BulkProcessor(client, backoffPolicy, listener, name, concurrentRequests, bulkActions, bulkSize, flushInterval);
        }
    }

    public static Builder builder(Client client, Listener listener) {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(listener, "listener");
        return new Builder(client, listener);
    }

    private final int bulkActions;

    private final long bulkSize;

    private final ScheduledThreadPoolExecutor scheduler;

    private final ScheduledFuture<?> scheduledFuture;

    private final AtomicLong executionIdGen = new AtomicLong();

    private BulkRequest bulkRequest;

    private final BulkRequestHandler bulkRequestHandler;

    private volatile boolean closed = false;

    BulkProcessor(Client client, BackoffPolicy backoffPolicy, Listener listener, @Nullable String name, int concurrentRequests, int bulkActions, ByteSizeValue bulkSize, @Nullable TimeValue flushInterval) {
        this.bulkActions = bulkActions;
        this.bulkSize = bulkSize.bytes();
        this.bulkRequest = new BulkRequest();
        this.bulkRequestHandler = (concurrentRequests == 0) ? BulkRequestHandler.syncHandler(client, backoffPolicy, listener) : BulkRequestHandler.asyncHandler(client, backoffPolicy, listener, concurrentRequests);
        if (flushInterval != null) {
            this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, EsExecutors.daemonThreadFactory(client.settings(), (name != null ? "[" + name + "]" : "") + "bulk_processor"));
            this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(new Flush(), flushInterval.millis(), flushInterval.millis(), TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
            this.scheduledFuture = null;
        }
    }

    @Override
    public void close() {
        try {
            awaitClose(0, TimeUnit.NANOSECONDS);
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
        }
    }

    public synchronized boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        if (closed) {
            return true;
        }
        closed = true;
        if (this.scheduledFuture != null) {
            FutureUtils.cancel(this.scheduledFuture);
            this.scheduler.shutdown();
        }
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
        return this.bulkRequestHandler.awaitClose(timeout, unit);
    }

    public BulkProcessor add(IndexRequest request) {
        return add((ActionRequest<?>) request);
    }

    public BulkProcessor add(DeleteRequest request) {
        return add((ActionRequest<?>) request);
    }

    public BulkProcessor add(ActionRequest<?> request) {
        return add(request, null);
    }

    public BulkProcessor add(ActionRequest<?> request, @Nullable Object payload) {
        internalAdd(request, payload);
        return this;
    }

    boolean isOpen() {
        return closed == false;
    }

    protected void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("bulk process already closed");
        }
    }

    private synchronized void internalAdd(ActionRequest<?> request, @Nullable Object payload) {
        ensureOpen();
        bulkRequest.add(request, payload);
        executeIfNeeded();
    }

    public BulkProcessor add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        return add(data, defaultIndex, defaultType, null, null);
    }

    public synchronized BulkProcessor add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType, @Nullable String defaultPipeline, @Nullable Object payload) throws Exception {
        bulkRequest.add(data, defaultIndex, defaultType, null, null, defaultPipeline, payload, true);
        executeIfNeeded();
        return this;
    }

    private void executeIfNeeded() {
        ensureOpen();
        if (!isOverTheLimit()) {
            return;
        }
        execute();
    }

    private void execute() {
        final BulkRequest bulkRequest = this.bulkRequest;
        final long executionId = executionIdGen.incrementAndGet();
        this.bulkRequest = new BulkRequest();
        this.bulkRequestHandler.execute(bulkRequest, executionId);
    }

    private boolean isOverTheLimit() {
        if (bulkActions != -1 && bulkRequest.numberOfActions() >= bulkActions) {
            return true;
        }
        if (bulkSize != -1 && bulkRequest.estimatedSizeInBytes() >= bulkSize) {
            return true;
        }
        return false;
    }

    public synchronized void flush() {
        ensureOpen();
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
    }

    class Flush implements Runnable {

        @Override
        public void run() {
            synchronized (BulkProcessor.this) {
                if (closed) {
                    return;
                }
                if (bulkRequest.numberOfActions() == 0) {
                    return;
                }
                execute();
            }
        }
    }
}