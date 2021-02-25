package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import static org.elasticsearch.action.search.SearchType.QUERY_AND_FETCH;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;

public class TransportSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    public static final Setting<Long> SHARD_COUNT_LIMIT_SETTING = Setting.longSetting("action.search.shard_count.limit", 1000L, 1L, Property.Dynamic, Property.NodeScope);

    private final ClusterService clusterService;

    private final SearchTransportService searchTransportService;

    private final SearchPhaseController searchPhaseController;

    private final SearchService searchService;

    @Inject
    public TransportSearchAction(Settings settings, ThreadPool threadPool, TransportService transportService, SearchService searchService, SearchTransportService searchTransportService, SearchPhaseController searchPhaseController, ClusterService clusterService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, SearchAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, SearchRequest::new);
        this.searchPhaseController = searchPhaseController;
        this.searchTransportService = searchTransportService;
        SearchTransportService.registerRequestHandler(transportService, searchService);
        this.clusterService = clusterService;
        this.searchService = searchService;
    }

    private Map<String, AliasFilter> buildPerIndexAliasFilter(SearchRequest request, ClusterState clusterState, Index[] concreteIndices) {
        final Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        for (Index index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index.getName());
            AliasFilter aliasFilter = searchService.buildAliasFilter(clusterState, index.getName(), request.indices());
            assert aliasFilter != null;
            aliasFilterMap.put(index.getUUID(), aliasFilter);
        }
        return aliasFilterMap;
    }

    @Override
    protected void doExecute(Task task, SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        final long startTimeInMillis = Math.max(0, System.currentTimeMillis());
        ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        Index[] indices = indexNameExpressionResolver.concreteIndices(clusterState, searchRequest.indicesOptions(), startTimeInMillis, searchRequest.indices());
        Map<String, AliasFilter> aliasFilter = buildPerIndexAliasFilter(searchRequest, clusterState, indices);
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, searchRequest.routing(), searchRequest.indices());
        String[] concreteIndices = new String[indices.length];
        for (int i = 0; i < indices.length; i++) {
            concreteIndices[i] = indices[i].getName();
        }
        GroupShardsIterator shardIterators = clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, searchRequest.preference());
        failIfOverShardCountLimit(clusterService, shardIterators.size());
        if (shardIterators.size() == 1) {
            searchRequest.searchType(QUERY_AND_FETCH);
        }
        if (searchRequest.isSuggestOnly()) {
            searchRequest.requestCache(false);
            switch(searchRequest.searchType()) {
                case DFS_QUERY_AND_FETCH:
                case DFS_QUERY_THEN_FETCH:
                    searchRequest.searchType(QUERY_THEN_FETCH);
                    break;
            }
        }
        searchAsyncAction((SearchTask) task, searchRequest, shardIterators, startTimeInMillis, clusterState, Collections.unmodifiableMap(aliasFilter), listener).start();
    }

    @Override
    protected final void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        throw new UnsupportedOperationException("the task parameter is required");
    }

    private AbstractSearchAsyncAction searchAsyncAction(SearchTask task, SearchRequest searchRequest, GroupShardsIterator shardIterators, long startTime, ClusterState state, Map<String, AliasFilter> aliasFilter, ActionListener<SearchResponse> listener) {
        final Function<String, DiscoveryNode> nodesLookup = state.nodes()::get;
        final long clusterStateVersion = state.version();
        Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);
        AbstractSearchAsyncAction searchAsyncAction;
        switch(searchRequest.searchType()) {
            case DFS_QUERY_THEN_FETCH:
                searchAsyncAction = new SearchDfsQueryThenFetchAsyncAction(logger, searchTransportService, nodesLookup, aliasFilter, searchPhaseController, executor, searchRequest, listener, shardIterators, startTime, clusterStateVersion, task);
                break;
            case QUERY_THEN_FETCH:
                searchAsyncAction = new SearchQueryThenFetchAsyncAction(logger, searchTransportService, nodesLookup, aliasFilter, searchPhaseController, executor, searchRequest, listener, shardIterators, startTime, clusterStateVersion, task);
                break;
            case DFS_QUERY_AND_FETCH:
                searchAsyncAction = new SearchDfsQueryAndFetchAsyncAction(logger, searchTransportService, nodesLookup, aliasFilter, searchPhaseController, executor, searchRequest, listener, shardIterators, startTime, clusterStateVersion, task);
                break;
            case QUERY_AND_FETCH:
                searchAsyncAction = new SearchQueryAndFetchAsyncAction(logger, searchTransportService, nodesLookup, aliasFilter, searchPhaseController, executor, searchRequest, listener, shardIterators, startTime, clusterStateVersion, task);
                break;
            default:
                throw new IllegalStateException("Unknown search type: [" + searchRequest.searchType() + "]");
        }
        return searchAsyncAction;
    }

    private void failIfOverShardCountLimit(ClusterService clusterService, int shardCount) {
        final long shardCountLimit = clusterService.getClusterSettings().get(SHARD_COUNT_LIMIT_SETTING);
        if (shardCount > shardCountLimit) {
            throw new IllegalArgumentException("Trying to query " + shardCount + " shards, which is over the limit of " + shardCountLimit + ". This limit exists because querying many shards at the same time can make the " + "job of the coordinating node very CPU and/or memory intensive. It is usually a better idea to " + "have a smaller number of larger shards. Update [" + SHARD_COUNT_LIMIT_SETTING.getKey() + "] to a greater value if you really want to query that many shards at the same time.");
        }
    }
}