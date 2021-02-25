package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.Retry;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.action.bulk.BackoffPolicy.exponentialBackoff;
import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;
import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.SIZE_ALL_MATCHES;
import static org.elasticsearch.rest.RestStatus.CONFLICT;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;

public abstract class AbstractAsyncBulkByScrollAction<Request extends AbstractBulkByScrollRequest<Request>> {

    protected final Logger logger;

    protected final BulkByScrollTask task;

    protected final ThreadPool threadPool;

    protected final Request mainRequest;

    private final AtomicLong startTime = new AtomicLong(-1);

    private final Set<String> destinationIndices = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ParentTaskAssigningClient client;

    private final ActionListener<BulkIndexByScrollResponse> listener;

    private final Retry bulkRetry;

    private final ScrollableHitSource scrollSource;

    public AbstractAsyncBulkByScrollAction(BulkByScrollTask task, Logger logger, ParentTaskAssigningClient client, ThreadPool threadPool, Request mainRequest, ActionListener<BulkIndexByScrollResponse> listener) {
        this.task = task;
        this.logger = logger;
        this.client = client;
        this.threadPool = threadPool;
        this.mainRequest = mainRequest;
        this.listener = listener;
        BackoffPolicy backoffPolicy = buildBackoffPolicy();
        bulkRetry = Retry.on(EsRejectedExecutionException.class).policy(BackoffPolicy.wrap(backoffPolicy, task::countBulkRetry));
        scrollSource = buildScrollableResultSource(backoffPolicy);
        List<SortBuilder<?>> sorts = mainRequest.getSearchRequest().source().sorts();
        if (sorts == null || sorts.isEmpty()) {
            mainRequest.getSearchRequest().source().sort(fieldSort("_doc"));
        }
        mainRequest.getSearchRequest().source().version(needsSourceDocumentVersions());
    }

    protected abstract boolean needsSourceDocumentVersions();

    protected abstract BulkRequest buildBulk(Iterable<? extends ScrollableHitSource.Hit> docs);

    protected ScrollableHitSource buildScrollableResultSource(BackoffPolicy backoffPolicy) {
        return new ClientScrollableHitSource(logger, backoffPolicy, threadPool, task::countSearchRetry, this::finishHim, client, mainRequest.getSearchRequest());
    }

    protected BulkIndexByScrollResponse buildResponse(TimeValue took, List<BulkItemResponse.Failure> indexingFailures, List<SearchFailure> searchFailures, boolean timedOut) {
        return new BulkIndexByScrollResponse(took, task.getStatus(), indexingFailures, searchFailures, timedOut);
    }

    public void start() {
        if (task.isCancelled()) {
            finishHim(null);
            return;
        }
        try {
            startTime.set(System.nanoTime());
            scrollSource.start(response -> onScrollResponse(timeValueNanos(System.nanoTime()), 0, response));
        } catch (Exception e) {
            finishHim(e);
        }
    }

    void onScrollResponse(TimeValue lastBatchStartTime, int lastBatchSize, ScrollableHitSource.Response response) {
        if (task.isCancelled()) {
            finishHim(null);
            return;
        }
        if ((response.getFailures().size() > 0) || response.isTimedOut()) {
            refreshAndFinish(emptyList(), response.getFailures(), response.isTimedOut());
            return;
        }
        long total = response.getTotalHits();
        if (mainRequest.getSize() > 0) {
            total = min(total, mainRequest.getSize());
        }
        task.setTotal(total);
        AbstractRunnable prepareBulkRequestRunnable = new AbstractRunnable() {

            @Override
            protected void doRun() throws Exception {
                prepareBulkRequest(timeValueNanos(System.nanoTime()), response);
            }

            @Override
            public void onFailure(Exception e) {
                finishHim(e);
            }
        };
        prepareBulkRequestRunnable = (AbstractRunnable) threadPool.getThreadContext().preserveContext(prepareBulkRequestRunnable);
        task.delayPrepareBulkRequest(threadPool, lastBatchStartTime, lastBatchSize, prepareBulkRequestRunnable);
    }

    void prepareBulkRequest(TimeValue thisBatchStartTime, ScrollableHitSource.Response response) {
        if (task.isCancelled()) {
            finishHim(null);
            return;
        }
        if (response.getHits().isEmpty()) {
            refreshAndFinish(emptyList(), emptyList(), false);
            return;
        }
        task.countBatch();
        List<? extends ScrollableHitSource.Hit> hits = response.getHits();
        if (mainRequest.getSize() != SIZE_ALL_MATCHES) {
            long remaining = max(0, mainRequest.getSize() - task.getSuccessfullyProcessed());
            if (remaining < hits.size()) {
                hits = hits.subList(0, (int) remaining);
            }
        }
        BulkRequest request = buildBulk(hits);
        if (request.requests().isEmpty()) {
            startNextScroll(thisBatchStartTime, 0);
            return;
        }
        request.timeout(mainRequest.getTimeout());
        request.waitForActiveShards(mainRequest.getWaitForActiveShards());
        if (logger.isDebugEnabled()) {
            logger.debug("sending [{}] entry, [{}] bulk request", request.requests().size(), new ByteSizeValue(request.estimatedSizeInBytes()));
        }
        sendBulkRequest(thisBatchStartTime, request);
    }

    void sendBulkRequest(TimeValue thisBatchStartTime, BulkRequest request) {
        if (task.isCancelled()) {
            finishHim(null);
            return;
        }
        bulkRetry.withAsyncBackoff(client, request, new ActionListener<BulkResponse>() {

            @Override
            public void onResponse(BulkResponse response) {
                onBulkResponse(thisBatchStartTime, response);
            }

            @Override
            public void onFailure(Exception e) {
                finishHim(e);
            }
        });
    }

    void onBulkResponse(TimeValue thisBatchStartTime, BulkResponse response) {
        try {
            List<Failure> failures = new ArrayList<Failure>();
            Set<String> destinationIndicesThisBatch = new HashSet<>();
            for (BulkItemResponse item : response) {
                if (item.isFailed()) {
                    recordFailure(item.getFailure(), failures);
                    continue;
                }
                switch(item.getOpType()) {
                    case "index":
                    case "create":
                        IndexResponse ir = item.getResponse();
                        if (ir.getResult() == DocWriteResponse.Result.CREATED) {
                            task.countCreated();
                        } else {
                            task.countUpdated();
                        }
                        break;
                    case "delete":
                        task.countDeleted();
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown op type:  " + item.getOpType());
                }
                destinationIndicesThisBatch.add(item.getIndex());
            }
            if (task.isCancelled()) {
                finishHim(null);
                return;
            }
            addDestinationIndices(destinationIndicesThisBatch);
            if (false == failures.isEmpty()) {
                refreshAndFinish(unmodifiableList(failures), emptyList(), false);
                return;
            }
            if (mainRequest.getSize() != SIZE_ALL_MATCHES && task.getSuccessfullyProcessed() >= mainRequest.getSize()) {
                refreshAndFinish(emptyList(), emptyList(), false);
                return;
            }
            startNextScroll(thisBatchStartTime, response.getItems().length);
        } catch (Exception t) {
            finishHim(t);
        }
    }

    void startNextScroll(TimeValue lastBatchStartTime, int lastBatchSize) {
        if (task.isCancelled()) {
            finishHim(null);
            return;
        }
        TimeValue extraKeepAlive = task.throttleWaitTime(lastBatchStartTime, lastBatchSize);
        scrollSource.startNextScroll(extraKeepAlive, response -> {
            onScrollResponse(lastBatchStartTime, lastBatchSize, response);
        });
    }

    private void recordFailure(Failure failure, List<Failure> failures) {
        if (failure.getStatus() == CONFLICT) {
            task.countVersionConflict();
            if (false == mainRequest.isAbortOnVersionConflict()) {
                return;
            }
        }
        failures.add(failure);
    }

    void refreshAndFinish(List<Failure> indexingFailures, List<SearchFailure> searchFailures, boolean timedOut) {
        if (task.isCancelled() || false == mainRequest.isRefresh() || destinationIndices.isEmpty()) {
            finishHim(null, indexingFailures, searchFailures, timedOut);
            return;
        }
        RefreshRequest refresh = new RefreshRequest();
        refresh.indices(destinationIndices.toArray(new String[destinationIndices.size()]));
        client.admin().indices().refresh(refresh, new ActionListener<RefreshResponse>() {

            @Override
            public void onResponse(RefreshResponse response) {
                finishHim(null, indexingFailures, searchFailures, timedOut);
            }

            @Override
            public void onFailure(Exception e) {
                finishHim(e);
            }
        });
    }

    void finishHim(Exception failure) {
        finishHim(failure, emptyList(), emptyList(), false);
    }

    void finishHim(Exception failure, List<Failure> indexingFailures, List<SearchFailure> searchFailures, boolean timedOut) {
        scrollSource.close();
        if (failure == null) {
            listener.onResponse(buildResponse(timeValueNanos(System.nanoTime() - startTime.get()), indexingFailures, searchFailures, timedOut));
        } else {
            listener.onFailure(failure);
        }
    }

    BackoffPolicy buildBackoffPolicy() {
        return exponentialBackoff(mainRequest.getRetryBackoffInitialTime(), mainRequest.getMaxRetries());
    }

    void addDestinationIndices(Collection<String> indices) {
        destinationIndices.addAll(indices);
    }

    void setScroll(String scroll) {
        scrollSource.setScroll(scroll);
    }
}