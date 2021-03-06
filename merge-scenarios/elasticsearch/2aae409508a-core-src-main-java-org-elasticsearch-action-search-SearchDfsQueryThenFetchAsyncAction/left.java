package org.elasticsearch.action.search;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.transport.Transport;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class SearchDfsQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<DfsSearchResult> {

    final AtomicArray<QuerySearchResult> queryResults;

    final AtomicArray<FetchSearchResult> fetchResults;

    final AtomicArray<IntArrayList> docIdsToLoad;

    private final SearchPhaseController searchPhaseController;

    SearchDfsQueryThenFetchAsyncAction(Logger logger, SearchTransportService searchTransportService, Function<String, Transport.Connection> nodeIdToConnection, Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts, SearchPhaseController searchPhaseController, Executor executor, SearchRequest request, ActionListener<SearchResponse> listener, GroupShardsIterator shardsIts, long startTime, long clusterStateVersion, SearchTask task) {
        super(logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, executor, request, listener, shardsIts, startTime, clusterStateVersion, task);
        this.searchPhaseController = searchPhaseController;
        queryResults = new AtomicArray<>(firstResults.length());
        fetchResults = new AtomicArray<>(firstResults.length());
        docIdsToLoad = new AtomicArray<>(firstResults.length());
    }

    @Override
    protected String firstPhaseName() {
        return "dfs";
    }

    @Override
    protected void sendExecuteFirstPhase(Transport.Connection connection, ShardSearchTransportRequest request, ActionListener<DfsSearchResult> listener) {
        searchTransportService.sendExecuteDfs(connection, request, task, listener);
    }

    @Override
    protected void moveToSecondPhase() {
        final AggregatedDfs dfs = searchPhaseController.aggregateDfs(firstResults);
        final AtomicInteger counter = new AtomicInteger(firstResults.asList().size());
        for (final AtomicArray.Entry<DfsSearchResult> entry : firstResults.asList()) {
            DfsSearchResult dfsResult = entry.value;
            Transport.Connection connection = nodeIdToConnection.apply(dfsResult.shardTarget().nodeId());
            QuerySearchRequest querySearchRequest = new QuerySearchRequest(request, dfsResult.id(), dfs);
            executeQuery(entry.index, dfsResult, counter, querySearchRequest, connection);
        }
    }

    void executeQuery(final int shardIndex, final DfsSearchResult dfsResult, final AtomicInteger counter, final QuerySearchRequest querySearchRequest, final Transport.Connection connection) {
        searchTransportService.sendExecuteQuery(connection, querySearchRequest, task, new ActionListener<QuerySearchResult>() {

            @Override
            public void onResponse(QuerySearchResult result) {
                result.shardTarget(dfsResult.shardTarget());
                queryResults.set(shardIndex, result);
                if (counter.decrementAndGet() == 0) {
                    executeFetchPhase();
                }
            }

            @Override
            public void onFailure(Exception t) {
                try {
                    onQueryFailure(t, querySearchRequest, shardIndex, dfsResult, counter);
                } finally {
                    sendReleaseSearchContext(querySearchRequest.id(), connection);
                }
            }
        });
    }

    void onQueryFailure(Exception e, QuerySearchRequest querySearchRequest, int shardIndex, DfsSearchResult dfsResult, AtomicInteger counter) {
        if (logger.isDebugEnabled()) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] Failed to execute query phase", querySearchRequest.id()), e);
        }
        this.addShardFailure(shardIndex, dfsResult.shardTarget(), e);
        successfulOps.decrementAndGet();
        if (counter.decrementAndGet() == 0) {
            if (successfulOps.get() == 0) {
                listener.onFailure(new SearchPhaseExecutionException("query", "all shards failed", buildShardFailures()));
            } else {
                executeFetchPhase();
            }
        }
    }

    void executeFetchPhase() {
        try {
            innerExecuteFetchPhase();
        } catch (Exception e) {
            listener.onFailure(new ReduceSearchPhaseException("query", "", e, buildShardFailures()));
        }
    }

    void innerExecuteFetchPhase() throws Exception {
        final boolean isScrollRequest = request.scroll() != null;
        sortedShardDocs = searchPhaseController.sortDocs(isScrollRequest, queryResults);
        searchPhaseController.fillDocIdsToLoad(docIdsToLoad, sortedShardDocs);
        if (docIdsToLoad.asList().isEmpty()) {
            finishHim();
            return;
        }
        final ScoreDoc[] lastEmittedDocPerShard = (request.scroll() != null) ? searchPhaseController.getLastEmittedDocPerShard(queryResults.asList(), sortedShardDocs, firstResults.length()) : null;
        final AtomicInteger counter = new AtomicInteger(docIdsToLoad.asList().size());
        for (final AtomicArray.Entry<IntArrayList> entry : docIdsToLoad.asList()) {
            QuerySearchResult queryResult = queryResults.get(entry.index);
            Transport.Connection connection = nodeIdToConnection.apply(queryResult.shardTarget().nodeId());
            ShardFetchSearchRequest fetchSearchRequest = createFetchRequest(queryResult, entry, lastEmittedDocPerShard);
            executeFetch(entry.index, queryResult.shardTarget(), counter, fetchSearchRequest, connection);
        }
    }

    void executeFetch(final int shardIndex, final SearchShardTarget shardTarget, final AtomicInteger counter, final ShardFetchSearchRequest fetchSearchRequest, Transport.Connection connection) {
        searchTransportService.sendExecuteFetch(connection, fetchSearchRequest, task, new ActionListener<FetchSearchResult>() {

            @Override
            public void onResponse(FetchSearchResult result) {
                result.shardTarget(shardTarget);
                fetchResults.set(shardIndex, result);
                if (counter.decrementAndGet() == 0) {
                    finishHim();
                }
            }

            @Override
            public void onFailure(Exception t) {
                docIdsToLoad.set(shardIndex, null);
                onFetchFailure(t, fetchSearchRequest, shardIndex, shardTarget, counter);
            }
        });
    }

    void onFetchFailure(Exception e, ShardFetchSearchRequest fetchSearchRequest, int shardIndex, SearchShardTarget shardTarget, AtomicInteger counter) {
        if (logger.isDebugEnabled()) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] Failed to execute fetch phase", fetchSearchRequest.id()), e);
        }
        this.addShardFailure(shardIndex, shardTarget, e);
        successfulOps.decrementAndGet();
        if (counter.decrementAndGet() == 0) {
            finishHim();
        }
    }

    private void finishHim() {
        getExecutor().execute(new ActionRunnable<SearchResponse>(listener) {

            @Override
            public void doRun() throws IOException {
                final boolean isScrollRequest = request.scroll() != null;
                final InternalSearchResponse internalResponse = searchPhaseController.merge(isScrollRequest, sortedShardDocs, queryResults, fetchResults);
                String scrollId = isScrollRequest ? TransportSearchHelper.buildScrollId(request.searchType(), firstResults) : null;
                listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(), buildTookInMillis(), buildShardFailures()));
                releaseIrrelevantSearchContexts(queryResults, docIdsToLoad);
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    ReduceSearchPhaseException failure = new ReduceSearchPhaseException("merge", "", e, buildShardFailures());
                    if (logger.isDebugEnabled()) {
                        logger.debug("failed to reduce search", failure);
                    }
                    super.onFailure(failure);
                } finally {
                    releaseIrrelevantSearchContexts(queryResults, docIdsToLoad);
                }
            }
        });
    }
}