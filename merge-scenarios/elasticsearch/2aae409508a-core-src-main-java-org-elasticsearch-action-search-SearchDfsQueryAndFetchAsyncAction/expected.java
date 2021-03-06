package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.transport.Transport;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class SearchDfsQueryAndFetchAsyncAction extends AbstractSearchAsyncAction<DfsSearchResult> {

    private final AtomicArray<QueryFetchSearchResult> queryFetchResults;

    private final SearchPhaseController searchPhaseController;

    SearchDfsQueryAndFetchAsyncAction(Logger logger, SearchTransportService searchTransportService, Function<String, Transport.Connection> nodeIdToConnection, Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts, SearchPhaseController searchPhaseController, Executor executor, SearchRequest request, ActionListener<SearchResponse> listener, GroupShardsIterator shardsIts, long startTime, long clusterStateVersion, SearchTask task) {
        super(logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, executor, request, listener, shardsIts, startTime, clusterStateVersion, task);
        this.searchPhaseController = searchPhaseController;
        queryFetchResults = new AtomicArray<>(firstResults.length());
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
            Transport.Connection connection = nodeIdToConnection.apply(dfsResult.shardTarget().getNodeId());
            QuerySearchRequest querySearchRequest = new QuerySearchRequest(request, dfsResult.id(), dfs);
            executeSecondPhase(entry.index, dfsResult, counter, connection, querySearchRequest);
        }
    }

    void executeSecondPhase(final int shardIndex, final DfsSearchResult dfsResult, final AtomicInteger counter, final Transport.Connection connection, final QuerySearchRequest querySearchRequest) {
        searchTransportService.sendExecuteFetch(connection, querySearchRequest, task, new ActionListener<QueryFetchSearchResult>() {

            @Override
            public void onResponse(QueryFetchSearchResult result) {
                result.shardTarget(dfsResult.shardTarget());
                queryFetchResults.set(shardIndex, result);
                if (counter.decrementAndGet() == 0) {
                    finishHim();
                }
            }

            @Override
            public void onFailure(Exception t) {
                try {
                    onSecondPhaseFailure(t, querySearchRequest, shardIndex, dfsResult, counter);
                } finally {
                    sendReleaseSearchContext(querySearchRequest.id(), connection);
                }
            }
        });
    }

    void onSecondPhaseFailure(Exception e, QuerySearchRequest querySearchRequest, int shardIndex, DfsSearchResult dfsResult, AtomicInteger counter) {
        if (logger.isDebugEnabled()) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] Failed to execute query phase", querySearchRequest.id()), e);
        }
        this.addShardFailure(shardIndex, dfsResult.shardTarget(), e);
        successfulOps.decrementAndGet();
        if (counter.decrementAndGet() == 0) {
            finishHim();
        }
    }

    private void finishHim() {
        getExecutor().execute(new ActionRunnable<SearchResponse>(listener) {

            @Override
            public void doRun() throws IOException {
                sortedShardDocs = searchPhaseController.sortDocs(true, queryFetchResults);
                final InternalSearchResponse internalResponse = searchPhaseController.merge(true, sortedShardDocs, queryFetchResults, queryFetchResults);
                String scrollId = null;
                if (request.scroll() != null) {
                    scrollId = TransportSearchHelper.buildScrollId(request.searchType(), firstResults);
                }
                listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successfulOps.get(), buildTookInMillis(), buildShardFailures()));
            }

            @Override
            public void onFailure(Exception e) {
                ReduceSearchPhaseException failure = new ReduceSearchPhaseException("query_fetch", "", e, buildShardFailures());
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to reduce search", failure);
                }
                super.onFailure(e);
            }
        });
    }
}