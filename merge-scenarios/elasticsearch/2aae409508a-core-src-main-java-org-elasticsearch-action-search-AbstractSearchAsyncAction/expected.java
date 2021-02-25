package org.elasticsearch.action.search;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.Transport;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

abstract class AbstractSearchAsyncAction<FirstResult extends SearchPhaseResult> extends AbstractAsyncAction {

    private static final float DEFAULT_INDEX_BOOST = 1.0f;

    protected final Logger logger;

    protected final SearchTransportService searchTransportService;

    private final Executor executor;

    protected final ActionListener<SearchResponse> listener;

    private final GroupShardsIterator shardsIts;

    protected final SearchRequest request;

    protected final Function<String, Transport.Connection> nodeIdToConnection;

    protected final SearchTask task;

    protected final int expectedSuccessfulOps;

    private final int expectedTotalOps;

    protected final AtomicInteger successfulOps = new AtomicInteger();

    private final AtomicInteger totalOps = new AtomicInteger();

    protected final AtomicArray<FirstResult> firstResults;

    private final Map<String, AliasFilter> aliasFilter;

    private final Map<String, Float> concreteIndexBoosts;

    private final long clusterStateVersion;

    private volatile AtomicArray<ShardSearchFailure> shardFailures;

    private final Object shardFailuresMutex = new Object();

    protected volatile ScoreDoc[] sortedShardDocs;

    protected AbstractSearchAsyncAction(Logger logger, SearchTransportService searchTransportService, Function<String, Transport.Connection> nodeIdToConnection, Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts, Executor executor, SearchRequest request, ActionListener<SearchResponse> listener, GroupShardsIterator shardsIts, long startTime, long clusterStateVersion, SearchTask task) {
        super(startTime);
        this.logger = logger;
        this.searchTransportService = searchTransportService;
        this.executor = executor;
        this.request = request;
        this.task = task;
        this.listener = listener;
        this.nodeIdToConnection = nodeIdToConnection;
        this.clusterStateVersion = clusterStateVersion;
        this.shardsIts = shardsIts;
        expectedSuccessfulOps = shardsIts.size();
        expectedTotalOps = shardsIts.totalSizeWith1ForEmpty();
        firstResults = new AtomicArray<>(shardsIts.size());
        this.aliasFilter = aliasFilter;
        this.concreteIndexBoosts = concreteIndexBoosts;
    }

    public void start() {
        if (expectedSuccessfulOps == 0) {
            listener.onResponse(new SearchResponse(InternalSearchResponse.empty(), null, 0, 0, buildTookInMillis(), ShardSearchFailure.EMPTY_ARRAY));
            return;
        }
        int shardIndex = -1;
        for (final ShardIterator shardIt : shardsIts) {
            shardIndex++;
            final ShardRouting shard = shardIt.nextOrNull();
            if (shard != null) {
                performFirstPhase(shardIndex, shardIt, shard);
            } else {
                onFirstPhaseResult(shardIndex, null, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
            }
        }
    }

    void performFirstPhase(final int shardIndex, final ShardIterator shardIt, final ShardRouting shard) {
        if (shard == null) {
            onFirstPhaseResult(shardIndex, null, null, shardIt, new NoShardAvailableActionException(shardIt.shardId()));
        } else {
            try {
                final Transport.Connection connection = nodeIdToConnection.apply(shard.currentNodeId());
                AliasFilter filter = this.aliasFilter.get(shard.index().getUUID());
                assert filter != null;
                float indexBoost = concreteIndexBoosts.getOrDefault(shard.index().getUUID(), DEFAULT_INDEX_BOOST);
                ShardSearchTransportRequest transportRequest = new ShardSearchTransportRequest(request, shardIt.shardId(), shardsIts.size(), filter, indexBoost, startTime());
                sendExecuteFirstPhase(connection, transportRequest, new ActionListener<FirstResult>() {

                    @Override
                    public void onResponse(FirstResult result) {
                        onFirstPhaseResult(shardIndex, shard.currentNodeId(), result, shardIt);
                    }

                    @Override
                    public void onFailure(Exception t) {
                        onFirstPhaseResult(shardIndex, shard, connection.getNode().getId(), shardIt, t);
                    }
                });
            } catch (ConnectTransportException | IllegalArgumentException ex) {
                onFirstPhaseResult(shardIndex, shard, shard.currentNodeId(), shardIt, ex);
            }
        }
    }

    private void onFirstPhaseResult(int shardIndex, String nodeId, FirstResult result, ShardIterator shardIt) {
        result.shardTarget(new SearchShardTarget(nodeId, shardIt.shardId()));
        processFirstPhaseResult(shardIndex, result);
        successfulOps.incrementAndGet();
        final int xTotalOps = totalOps.addAndGet(shardIt.remaining() + 1);
        if (xTotalOps == expectedTotalOps) {
            try {
                innerMoveToSecondPhase();
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("{}: Failed to execute [{}] while moving to second phase", shardIt.shardId(), request), e);
                }
                raiseEarlyFailure(new ReduceSearchPhaseException(firstPhaseName(), "", e, buildShardFailures()));
            }
        } else if (xTotalOps > expectedTotalOps) {
            raiseEarlyFailure(new IllegalStateException("unexpected higher total ops [" + xTotalOps + "] compared " + "to expected [" + expectedTotalOps + "]"));
        }
    }

    private void onFirstPhaseResult(final int shardIndex, @Nullable ShardRouting shard, @Nullable String nodeId, final ShardIterator shardIt, Exception e) {
        SearchShardTarget shardTarget = new SearchShardTarget(nodeId, shardIt.shardId());
        addShardFailure(shardIndex, shardTarget, e);
        if (totalOps.incrementAndGet() == expectedTotalOps) {
            if (logger.isDebugEnabled()) {
                if (e != null && !TransportActions.isShardNotAvailableException(e)) {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("{}: Failed to execute [{}]", shard != null ? shard.shortSummary() : shardIt.shardId(), request), e);
                } else if (logger.isTraceEnabled()) {
                    logger.trace((Supplier<?>) () -> new ParameterizedMessage("{}: Failed to execute [{}]", shard, request), e);
                }
            }
            final ShardSearchFailure[] shardSearchFailures = buildShardFailures();
            if (successfulOps.get() == 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("All shards failed for phase: [{}]", firstPhaseName()), e);
                }
                raiseEarlyFailure(new SearchPhaseExecutionException(firstPhaseName(), "all shards failed", e, shardSearchFailures));
            } else {
                try {
                    innerMoveToSecondPhase();
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    raiseEarlyFailure(new ReduceSearchPhaseException(firstPhaseName(), "", inner, shardSearchFailures));
                }
            }
        } else {
            final ShardRouting nextShard = shardIt.nextOrNull();
            final boolean lastShard = nextShard == null;
            logger.trace((Supplier<?>) () -> new ParameterizedMessage("{}: Failed to execute [{}] lastShard [{}]", shard != null ? shard.shortSummary() : shardIt.shardId(), request, lastShard), e);
            if (!lastShard) {
                try {
                    performFirstPhase(shardIndex, shardIt, nextShard);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    onFirstPhaseResult(shardIndex, shard, shard.currentNodeId(), shardIt, inner);
                }
            } else {
                if (logger.isDebugEnabled() && !logger.isTraceEnabled()) {
                    if (e != null && !TransportActions.isShardNotAvailableException(e)) {
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage("{}: Failed to execute [{}] lastShard [{}]", shard != null ? shard.shortSummary() : shardIt.shardId(), request, lastShard), e);
                    }
                }
            }
        }
    }

    protected final ShardSearchFailure[] buildShardFailures() {
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures;
        if (shardFailures == null) {
            return ShardSearchFailure.EMPTY_ARRAY;
        }
        List<AtomicArray.Entry<ShardSearchFailure>> entries = shardFailures.asList();
        ShardSearchFailure[] failures = new ShardSearchFailure[entries.size()];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = entries.get(i).value;
        }
        return failures;
    }

    protected final void addShardFailure(final int shardIndex, @Nullable SearchShardTarget shardTarget, Exception e) {
        if (TransportActions.isShardNotAvailableException(e)) {
            return;
        }
        if (shardFailures == null) {
            synchronized (shardFailuresMutex) {
                if (shardFailures == null) {
                    shardFailures = new AtomicArray<>(shardsIts.size());
                }
            }
        }
        ShardSearchFailure failure = shardFailures.get(shardIndex);
        if (failure == null) {
            shardFailures.set(shardIndex, new ShardSearchFailure(e, shardTarget));
        } else {
            if (TransportActions.isReadOverrideException(e)) {
                shardFailures.set(shardIndex, new ShardSearchFailure(e, shardTarget));
            }
        }
    }

    private void raiseEarlyFailure(Exception e) {
        for (AtomicArray.Entry<FirstResult> entry : firstResults.asList()) {
            try {
                Transport.Connection connection = nodeIdToConnection.apply(entry.value.shardTarget().getNodeId());
                sendReleaseSearchContext(entry.value.id(), connection);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.trace("failed to release context", inner);
            }
        }
        listener.onFailure(e);
    }

    protected void releaseIrrelevantSearchContexts(AtomicArray<? extends QuerySearchResultProvider> queryResults, AtomicArray<IntArrayList> docIdsToLoad) {
        if (docIdsToLoad == null) {
            return;
        }
        if (request.scroll() == null) {
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults.asList()) {
                QuerySearchResult queryResult = entry.value.queryResult();
                if (queryResult.hasHits() && docIdsToLoad.get(entry.index) == null) {
                    try {
                        Transport.Connection connection = nodeIdToConnection.apply(entry.value.queryResult().shardTarget().getNodeId());
                        sendReleaseSearchContext(entry.value.queryResult().id(), connection);
                    } catch (Exception e) {
                        logger.trace("failed to release context", e);
                    }
                }
            }
        }
    }

    protected void sendReleaseSearchContext(long contextId, Transport.Connection connection) {
        if (connection != null) {
            searchTransportService.sendFreeContext(connection, contextId, request);
        }
    }

    protected ShardFetchSearchRequest createFetchRequest(QuerySearchResult queryResult, AtomicArray.Entry<IntArrayList> entry, ScoreDoc[] lastEmittedDocPerShard) {
        final ScoreDoc lastEmittedDoc = (lastEmittedDocPerShard != null) ? lastEmittedDocPerShard[entry.index] : null;
        return new ShardFetchSearchRequest(request, queryResult.id(), entry.value, lastEmittedDoc);
    }

    protected abstract void sendExecuteFirstPhase(Transport.Connection connection, ShardSearchTransportRequest request, ActionListener<FirstResult> listener);

    protected final void processFirstPhaseResult(int shardIndex, FirstResult result) {
        firstResults.set(shardIndex, result);
        if (logger.isTraceEnabled()) {
            logger.trace("got first-phase result from {}", result != null ? result.shardTarget() : null);
        }
        AtomicArray<ShardSearchFailure> shardFailures = this.shardFailures;
        if (shardFailures != null) {
            shardFailures.set(shardIndex, null);
        }
    }

    final void innerMoveToSecondPhase() throws Exception {
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            boolean hadOne = false;
            for (int i = 0; i < firstResults.length(); i++) {
                FirstResult result = firstResults.get(i);
                if (result == null) {
                    continue;
                }
                if (hadOne) {
                    sb.append(",");
                } else {
                    hadOne = true;
                }
                sb.append(result.shardTarget());
            }
            logger.trace("Moving to second phase, based on results from: {} (cluster state version: {})", sb, clusterStateVersion);
        }
        moveToSecondPhase();
    }

    protected abstract void moveToSecondPhase() throws Exception;

    protected abstract String firstPhaseName();

    protected Executor getExecutor() {
        return executor;
    }
}