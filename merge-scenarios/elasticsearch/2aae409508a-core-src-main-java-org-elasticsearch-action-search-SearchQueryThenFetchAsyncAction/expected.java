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
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.transport.Transport;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class SearchQueryThenFetchAsyncAction extends AbstractSearchAsyncAction<QuerySearchResultProvider> {

    final AtomicArray<FetchSearchResult> fetchResults;

    final AtomicArray<IntArrayList> docIdsToLoad;

    private final SearchPhaseController searchPhaseController;

    SearchQueryThenFetchAsyncAction(Logger logger, SearchTransportService searchTransportService, Function<String, Transport.Connection> nodeIdToConnection, Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts, SearchPhaseController searchPhaseController, Executor executor, SearchRequest request, ActionListener<SearchResponse> listener, GroupShardsIterator shardsIts, long startTime, long clusterStateVersion, SearchTask task) {
        super(logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, executor, request, listener, shardsIts, startTime, clusterStateVersion, task);
        this.searchPhaseController = searchPhaseController;
        fetchResults = new AtomicArray<>(firstResults.length());
        docIdsToLoad = new AtomicArray<>(firstResults.length());
    }

    @Override
    protected String firstPhaseName() {
        return "query";
    }

    @Override
    protected void sendExecuteFirstPhase(Transport.Connection connection, ShardSearchTransportRequest request, ActionListener<QuerySearchResultProvider> listener) {
        searchTransportService.sendExecuteQuery(connection, request, task, listener);
    }

    @Override
    protected void moveToSecondPhase() throws Exception {
        final boolean isScrollRequest = request.scroll() != null;
        sortedShardDocs = searchPhaseController.sortDocs(isScrollRequest, firstResults);
        searchPhaseController.fillDocIdsToLoad(docIdsToLoad, sortedShardDocs);
        if (docIdsToLoad.asList().isEmpty()) {
            finishHim();
            return;
        }
        final ScoreDoc[] lastEmittedDocPerShard = isScrollRequest ? searchPhaseController.getLastEmittedDocPerShard(firstResults.asList(), sortedShardDocs, firstResults.length()) : null;
        final AtomicInteger counter = new AtomicInteger(docIdsToLoad.asList().size());
        for (AtomicArray.Entry<IntArrayList> entry : docIdsToLoad.asList()) {
            QuerySearchResultProvider queryResult = firstResults.get(entry.index);
            Transport.Connection connection = nodeIdToConnection.apply(queryResult.shardTarget().getNodeId());
            ShardFetchSearchRequest fetchSearchRequest = createFetchRequest(queryResult.queryResult(), entry, lastEmittedDocPerShard);
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
                final InternalSearchResponse internalResponse = searchPhaseController.merge(isScrollRequest, sortedShardDocs, firstResults, fetchResults);
                String scrollId = isScrollRequest ? TransportSearchHelper.buildScrollId(request.searchType(), firstResults) : null;
                listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(), buildTookInMillis(), buildShardFailures()));
                releaseIrrelevantSearchContexts(firstResults, docIdsToLoad);
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    ReduceSearchPhaseException failure = new ReduceSearchPhaseException("fetch", "", e, buildShardFailures());
                    if (logger.isDebugEnabled()) {
                        logger.debug("failed to reduce search", failure);
                    }
                    super.onFailure(failure);
                } finally {
                    releaseIrrelevantSearchContexts(firstResults, docIdsToLoad);
                }
            }
        });
    }
}