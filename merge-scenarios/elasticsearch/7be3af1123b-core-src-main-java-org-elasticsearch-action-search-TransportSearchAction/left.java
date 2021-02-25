package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import static org.elasticsearch.action.search.SearchType.QUERY_AND_FETCH;
import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;

public class TransportSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    public static final Setting<Long> SHARD_COUNT_LIMIT_SETTING = Setting.longSetting("action.search.shard_count.limit", 1000L, 1L, Property.Dynamic, Property.NodeScope);

    private static final char REMOTE_CLUSTER_INDEX_SEPARATOR = '|';

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

    private Map<String, AliasFilter> buildPerIndexAliasFilter(SearchRequest request, ClusterState clusterState, Index[] concreteIndices, Map<String, AliasFilter> remoteAliasMap) {
        final Map<String, AliasFilter> aliasFilterMap = new HashMap<>();
        for (Index index : concreteIndices) {
            clusterState.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, index.getName());
            AliasFilter aliasFilter = searchService.buildAliasFilter(clusterState, index.getName(), request.indices());
            assert aliasFilter != null;
            aliasFilterMap.put(index.getUUID(), aliasFilter);
        }
        aliasFilterMap.putAll(remoteAliasMap);
        return aliasFilterMap;
    }

    @Override
    protected void doExecute(Task task, SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        final long startTimeInMillis = Math.max(0, System.currentTimeMillis());
        final String[] localIndices;
        final Map<String, List<String>> remoteIndicesByCluster = new HashMap<>();
        if (searchTransportService.isCrossClusterSearchEnabled()) {
            List<String> localIndicesList = new ArrayList<>();
            for (String index : searchRequest.indices()) {
                int i = index.indexOf(REMOTE_CLUSTER_INDEX_SEPARATOR);
                if (i >= 0) {
                    String remoteCluster = index.substring(0, i);
                    if (searchTransportService.isRemoteClusterRegistered(remoteCluster)) {
                        String remoteIndex = index.substring(i + 1);
                        List<String> indices = remoteIndicesByCluster.get(remoteCluster);
                        if (indices == null) {
                            indices = new ArrayList<>();
                            remoteIndicesByCluster.put(remoteCluster, indices);
                        }
                        indices.add(remoteIndex);
                    } else {
                        localIndicesList.add(index);
                    }
                } else {
                    localIndicesList.add(index);
                }
            }
            localIndices = localIndicesList.toArray(new String[localIndicesList.size()]);
        } else {
            localIndices = searchRequest.indices();
        }
        if (remoteIndicesByCluster.isEmpty()) {
            executeSearch((SearchTask) task, startTimeInMillis, searchRequest, localIndices, Collections.emptyList(), Collections.emptySet(), Collections.emptyMap(), listener);
        } else {
            searchTransportService.sendSearchShards(searchRequest, remoteIndicesByCluster, ActionListener.wrap((searchShardsResponses) -> {
                List<ShardIterator> remoteShardIterators = new ArrayList<>();
                Set<DiscoveryNode> remoteNodes = new HashSet<>();
                Map<String, AliasFilter> remoteAliasFilters = new HashMap<>();
                processRemoteShards(searchShardsResponses, remoteShardIterators, remoteNodes, remoteAliasFilters);
                executeSearch((SearchTask) task, startTimeInMillis, searchRequest, localIndices, remoteShardIterators, remoteNodes, remoteAliasFilters, listener);
            }, listener::onFailure));
        }
    }

    private void processRemoteShards(Map<String, ClusterSearchShardsResponse> searchShardsResponses, List<ShardIterator> remoteShardIterators, Set<DiscoveryNode> remoteNodes, Map<String, AliasFilter> aliasFilterMap) {
        for (Map.Entry<String, ClusterSearchShardsResponse> entry : searchShardsResponses.entrySet()) {
            String clusterName = entry.getKey();
            ClusterSearchShardsResponse searchShardsResponse = entry.getValue();
            Collections.addAll(remoteNodes, searchShardsResponse.getNodes());
            Map<String, AliasFilter> indicesAndFilters = searchShardsResponse.getIndicesAndFilters();
            for (ClusterSearchShardsGroup clusterSearchShardsGroup : searchShardsResponse.getGroups()) {
                ShardId shardId = clusterSearchShardsGroup.getShardId();
                Index index = new Index(clusterName + REMOTE_CLUSTER_INDEX_SEPARATOR + shardId.getIndex().getName(), shardId.getIndex().getUUID());
                ShardIterator shardIterator = new PlainShardIterator(new ShardId(index, shardId.getId()), Arrays.asList(clusterSearchShardsGroup.getShards()));
                remoteShardIterators.add(shardIterator);
                AliasFilter aliasFilter;
                if (indicesAndFilters == null) {
                    aliasFilter = new AliasFilter(null, Strings.EMPTY_ARRAY);
                } else {
                    aliasFilter = indicesAndFilters.get(shardId.getIndexName());
                    assert aliasFilter != null;
                }
                aliasFilterMap.put(shardId.getIndex().getUUID(), aliasFilter);
            }
        }
    }

    private void executeSearch(SearchTask task, long startTimeInMillis, SearchRequest searchRequest, String[] localIndices, List<ShardIterator> remoteShardIterators, Set<DiscoveryNode> remoteNodes, Map<String, AliasFilter> remoteAliasMap, ActionListener<SearchResponse> listener) {
        ClusterState clusterState = clusterService.state();
        clusterState.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        final Index[] indices;
        if (localIndices.length == 0 && remoteShardIterators.size() > 0) {
            indices = new Index[0];
        } else {
            indices = indexNameExpressionResolver.concreteIndices(clusterState, searchRequest.indicesOptions(), startTimeInMillis, localIndices);
        }
        Map<String, AliasFilter> aliasFilter = buildPerIndexAliasFilter(searchRequest, clusterState, indices, remoteAliasMap);
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, searchRequest.routing(), searchRequest.indices());
        String[] concreteIndices = new String[indices.length];
        for (int i = 0; i < indices.length; i++) {
            concreteIndices[i] = indices[i].getName();
        }
        GroupShardsIterator localShardsIterator = clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, searchRequest.preference());
        GroupShardsIterator shardIterators = mergeShardsIterators(localShardsIterator, remoteShardIterators);
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
        Function<String, DiscoveryNode> nodesLookup = mergeNodesLookup(clusterState.nodes(), remoteNodes);
        searchAsyncAction(task, searchRequest, shardIterators, startTimeInMillis, nodesLookup, clusterState.version(), Collections.unmodifiableMap(aliasFilter), listener).start();
    }

    private static GroupShardsIterator mergeShardsIterators(GroupShardsIterator localShardsIterator, List<ShardIterator> remoteShardIterators) {
        if (remoteShardIterators.isEmpty()) {
            return localShardsIterator;
        }
        List<ShardIterator> shards = new ArrayList<>();
        for (ShardIterator shardIterator : remoteShardIterators) {
            shards.add(shardIterator);
        }
        for (ShardIterator shardIterator : localShardsIterator) {
            shards.add(shardIterator);
        }
        return new GroupShardsIterator(shards);
    }

    private Function<String, DiscoveryNode> mergeNodesLookup(DiscoveryNodes nodes, Set<DiscoveryNode> remoteNodes) {
        if (remoteNodes.isEmpty()) {
            return nodes::get;
        }
        ImmutableOpenMap.Builder<String, DiscoveryNode> builder = ImmutableOpenMap.builder(nodes.getNodes());
        for (DiscoveryNode remoteNode : remoteNodes) {
            searchTransportService.connectToRemoteNode(remoteNode);
            builder.put(remoteNode.getId(), remoteNode);
        }
        return builder.build()::get;
    }

    @Override
    protected final void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        throw new UnsupportedOperationException("the task parameter is required");
    }

    private AbstractSearchAsyncAction searchAsyncAction(SearchTask task, SearchRequest searchRequest, GroupShardsIterator shardIterators, long startTime, Function<String, DiscoveryNode> nodesLookup, long clusterStateVersion, Map<String, AliasFilter> aliasFilter, ActionListener<SearchResponse> listener) {
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

    private static void failIfOverShardCountLimit(ClusterService clusterService, int shardCount) {
        final long shardCountLimit = clusterService.getClusterSettings().get(SHARD_COUNT_LIMIT_SETTING);
        if (shardCount > shardCountLimit) {
            throw new IllegalArgumentException("Trying to query " + shardCount + " shards, which is over the limit of " + shardCountLimit + ". This limit exists because querying many shards at the same time can make the " + "job of the coordinating node very CPU and/or memory intensive. It is usually a better idea to " + "have a smaller number of larger shards. Update [" + SHARD_COUNT_LIMIT_SETTING.getKey() + "] to a greater value if you really want to query that many shards at the same time.");
        }
    }
}