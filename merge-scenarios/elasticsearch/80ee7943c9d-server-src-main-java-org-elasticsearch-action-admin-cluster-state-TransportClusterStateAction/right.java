package org.elasticsearch.action.admin.cluster.state;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaData.Custom;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import java.io.IOException;
import java.util.function.Predicate;
import static org.elasticsearch.discovery.zen.PublishClusterStateAction.serializeFullClusterState;

public class TransportClusterStateAction extends TransportMasterNodeReadAction<ClusterStateRequest, ClusterStateResponse> {

    @Inject
    public TransportClusterStateAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ClusterStateAction.NAME, false, transportService, clusterService, threadPool, actionFilters, ClusterStateRequest::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterStateRequest request, ClusterState state) {
        return null;
    }

    @Override
    protected ClusterStateResponse newResponse() {
        return new ClusterStateResponse();
    }

    @Override
    protected void masterOperation(final ClusterStateRequest request, final ClusterState state, final ActionListener<ClusterStateResponse> listener) throws IOException {
        if (request.waitForMetaDataVersion() != null) {
            final Predicate<ClusterState> metadataVersionPredicate = clusterState -> {
                return clusterState.metaData().version() >= request.waitForMetaDataVersion();
            };
            final ClusterStateObserver observer = new ClusterStateObserver(clusterService, request.waitForTimeout(), logger, threadPool.getThreadContext());
            final ClusterState clusterState = observer.setAndGetObservedState();
            if (metadataVersionPredicate.test(clusterState)) {
                buildResponse(request, clusterState, listener);
            } else {
                observer.waitForNextChange(new ClusterStateObserver.Listener() {

                    @Override
                    public void onNewClusterState(ClusterState state) {
                        try {
                            buildResponse(request, state, listener);
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        try {
                            listener.onResponse(new ClusterStateResponse(clusterState.getClusterName(), null, 0L, true));
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }
                }, metadataVersionPredicate);
            }
        } else {
            ClusterState currentState = clusterService.state();
            buildResponse(request, currentState, listener);
        }
    }

    private void buildResponse(final ClusterStateRequest request, final ClusterState currentState, final ActionListener<ClusterStateResponse> listener) throws IOException {
        logger.trace("Serving cluster state request using version {}", currentState.version());
        ClusterState.Builder builder = ClusterState.builder(currentState.getClusterName());
        builder.version(currentState.version());
        builder.stateUUID(currentState.stateUUID());
        if (request.nodes()) {
            builder.nodes(currentState.nodes());
        }
        if (request.routingTable()) {
            if (request.indices().length > 0) {
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
                String[] indices = indexNameExpressionResolver.concreteIndexNames(currentState, request);
                for (String filteredIndex : indices) {
                    if (currentState.routingTable().getIndicesRouting().containsKey(filteredIndex)) {
                        routingTableBuilder.add(currentState.routingTable().getIndicesRouting().get(filteredIndex));
                    }
                }
                builder.routingTable(routingTableBuilder.build());
            } else {
                builder.routingTable(currentState.routingTable());
            }
        }
        if (request.blocks()) {
            builder.blocks(currentState.blocks());
        }
        MetaData.Builder mdBuilder = MetaData.builder();
        mdBuilder.clusterUUID(currentState.metaData().clusterUUID());
        if (request.metaData()) {
            if (request.indices().length > 0) {
                String[] indices = indexNameExpressionResolver.concreteIndexNames(currentState, request);
                for (String filteredIndex : indices) {
                    IndexMetaData indexMetaData = currentState.metaData().index(filteredIndex);
                    if (indexMetaData != null) {
                        mdBuilder.put(indexMetaData, false);
                    }
                }
            } else {
                mdBuilder = MetaData.builder(currentState.metaData());
            }
            for (ObjectObjectCursor<String, Custom> custom : currentState.metaData().customs()) {
                if (custom.value.context().contains(MetaData.XContentContext.API) == false) {
                    mdBuilder.removeCustom(custom.key);
                }
            }
        }
        builder.metaData(mdBuilder);
        if (request.customs()) {
            for (ObjectObjectCursor<String, ClusterState.Custom> custom : currentState.customs()) {
                if (custom.value.isPrivate() == false) {
                    builder.putCustom(custom.key, custom.value);
                }
            }
        }
        listener.onResponse(new ClusterStateResponse(currentState.getClusterName(), builder.build(), serializeFullClusterState(currentState, Version.CURRENT).length(), false));
    }
}