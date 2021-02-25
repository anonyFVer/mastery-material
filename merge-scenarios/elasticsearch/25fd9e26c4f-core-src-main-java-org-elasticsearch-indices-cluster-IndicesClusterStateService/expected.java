package org.elasticsearch.indices.cluster;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.Type;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexComponent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexShardAlreadyExistsException;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardRelocatedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class IndicesClusterStateService extends AbstractLifecycleComponent implements ClusterStateListener {

    final AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>> indicesService;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private final PeerRecoveryTargetService recoveryTargetService;

    private final ShardStateAction shardStateAction;

    private final NodeMappingRefreshAction nodeMappingRefreshAction;

    private final NodeServicesProvider nodeServicesProvider;

    private final GlobalCheckpointSyncAction globalCheckpointSyncAction;

    private static final ShardStateAction.Listener SHARD_STATE_ACTION_LISTENER = new ShardStateAction.Listener() {
    };

    final ConcurrentMap<ShardId, ShardRouting> failedShardsCache = ConcurrentCollections.newConcurrentMap();

    private final RestoreService restoreService;

    private final RepositoriesService repositoriesService;

    private final FailedShardHandler failedShardHandler = new FailedShardHandler();

    private final boolean sendRefreshMapping;

    private final List<IndexEventListener> buildInIndexListener;

    @Inject
    public IndicesClusterStateService(Settings settings, IndicesService indicesService, ClusterService clusterService, ThreadPool threadPool, PeerRecoveryTargetService recoveryTargetService, ShardStateAction shardStateAction, NodeMappingRefreshAction nodeMappingRefreshAction, RepositoriesService repositoriesService, RestoreService restoreService, SearchService searchService, SyncedFlushService syncedFlushService, PeerRecoverySourceService peerRecoverySourceService, NodeServicesProvider nodeServicesProvider, GlobalCheckpointSyncAction globalCheckpointSyncAction) {
        this(settings, (AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>>) indicesService, clusterService, threadPool, recoveryTargetService, shardStateAction, nodeMappingRefreshAction, repositoriesService, restoreService, searchService, syncedFlushService, peerRecoverySourceService, nodeServicesProvider, globalCheckpointSyncAction);
    }

    IndicesClusterStateService(Settings settings, AllocatedIndices<? extends Shard, ? extends AllocatedIndex<? extends Shard>> indicesService, ClusterService clusterService, ThreadPool threadPool, PeerRecoveryTargetService recoveryTargetService, ShardStateAction shardStateAction, NodeMappingRefreshAction nodeMappingRefreshAction, RepositoriesService repositoriesService, RestoreService restoreService, SearchService searchService, SyncedFlushService syncedFlushService, PeerRecoverySourceService peerRecoverySourceService, NodeServicesProvider nodeServicesProvider, GlobalCheckpointSyncAction globalCheckpointSyncAction) {
        super(settings);
        this.buildInIndexListener = Arrays.asList(peerRecoverySourceService, recoveryTargetService, searchService, syncedFlushService);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.recoveryTargetService = recoveryTargetService;
        this.shardStateAction = shardStateAction;
        this.nodeMappingRefreshAction = nodeMappingRefreshAction;
        this.restoreService = restoreService;
        this.repositoriesService = repositoriesService;
        this.sendRefreshMapping = this.settings.getAsBoolean("indices.cluster.send_refresh_mapping", true);
        this.nodeServicesProvider = nodeServicesProvider;
        this.globalCheckpointSyncAction = globalCheckpointSyncAction;
    }

    @Override
    protected void doStart() {
        clusterService.addFirst(this);
    }

    @Override
    protected void doStop() {
        clusterService.remove(this);
    }

    @Override
    protected void doClose() {
    }

    @Override
    public synchronized void clusterChanged(final ClusterChangedEvent event) {
        if (!lifecycle.started()) {
            return;
        }
        final ClusterState state = event.state();
        if (state.blocks().disableStatePersistence()) {
            for (AllocatedIndex<? extends Shard> indexService : indicesService) {
                indicesService.removeIndex(indexService.index(), "cleaning index (disabled block persistence)");
            }
            return;
        }
        updateFailedShardsCache(state);
        deleteIndices(event);
        removeUnallocatedIndices(event);
        failMissingShards(state);
        removeShards(state);
        updateIndices(event);
        createIndices(state);
        createOrUpdateShards(state);
    }

    private void updateFailedShardsCache(final ClusterState state) {
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            failedShardsCache.clear();
            return;
        }
        DiscoveryNode masterNode = state.nodes().getMasterNode();
        for (Iterator<Map.Entry<ShardId, ShardRouting>> iterator = failedShardsCache.entrySet().iterator(); iterator.hasNext(); ) {
            ShardRouting failedShardRouting = iterator.next().getValue();
            ShardRouting matchedRouting = localRoutingNode.getByShardId(failedShardRouting.shardId());
            if (matchedRouting == null || matchedRouting.isSameAllocation(failedShardRouting) == false) {
                iterator.remove();
            } else {
                if (masterNode != null) {
                    String message = "master " + masterNode + " has not removed previously failed shard. resending shard failure";
                    logger.trace("[{}] re-sending failed shard [{}], reason [{}]", matchedRouting.shardId(), matchedRouting, message);
                    shardStateAction.localShardFailed(matchedRouting, message, null, SHARD_STATE_ACTION_LISTENER);
                }
            }
        }
    }

    private void deleteIndices(final ClusterChangedEvent event) {
        final ClusterState previousState = event.previousState();
        final ClusterState state = event.state();
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;
        for (Index index : event.indicesDeleted()) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] cleaning index, no longer part of the metadata", index);
            }
            AllocatedIndex<? extends Shard> indexService = indicesService.indexService(index);
            final IndexSettings indexSettings;
            if (indexService != null) {
                indexSettings = indexService.getIndexSettings();
                indicesService.deleteIndex(index, "index no longer part of the metadata");
            } else if (previousState.metaData().hasIndex(index.getName())) {
                final IndexMetaData metaData = previousState.metaData().index(index);
                indexSettings = new IndexSettings(metaData, settings);
                indicesService.deleteUnassignedIndex("deleted index was not assigned to local node", metaData, state);
            } else {
                assert previousState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
                final IndexMetaData metaData = indicesService.verifyIndexIsDeleted(index, event.state());
                if (metaData != null) {
                    indexSettings = new IndexSettings(metaData, settings);
                } else {
                    indexSettings = null;
                }
            }
            if (indexSettings != null) {
                threadPool.generic().execute(new AbstractRunnable() {

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to complete pending deletion for index", index), e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        try {
                            indicesService.processPendingDeletes(index, indexSettings, new TimeValue(30, TimeUnit.MINUTES));
                        } catch (LockObtainFailedException exc) {
                            logger.warn("[{}] failed to lock all shards for index - timed out after 30 seconds", index);
                        } catch (InterruptedException e) {
                            logger.warn("[{}] failed to lock all shards for index - interrupted", index);
                        }
                    }
                });
            }
        }
    }

    private void removeUnallocatedIndices(final ClusterChangedEvent event) {
        final ClusterState state = event.state();
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;
        Set<Index> indicesWithShards = new HashSet<>();
        RoutingNode localRoutingNode = state.getRoutingNodes().node(localNodeId);
        if (localRoutingNode != null) {
            for (ShardRouting shardRouting : localRoutingNode) {
                indicesWithShards.add(shardRouting.index());
            }
        }
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            Index index = indexService.index();
            if (indicesWithShards.contains(index) == false) {
                assert state.metaData().index(index) != null || event.isNewCluster() : "index " + index + " does not exist in the cluster state, it should either " + "have been deleted or the cluster must be new";
                logger.debug("{} removing index, no shards allocated", index);
                indicesService.removeIndex(index, "removing index (no shards allocated)");
            }
        }
    }

    private void failMissingShards(final ClusterState state) {
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }
        for (final ShardRouting shardRouting : localRoutingNode) {
            ShardId shardId = shardRouting.shardId();
            if (shardRouting.initializing() == false && failedShardsCache.containsKey(shardId) == false && indicesService.getShardOrNull(shardId) == null) {
                sendFailShard(shardRouting, "master marked shard as active, but shard has not been created, mark shard as failed", null);
            }
        }
    }

    private void removeShards(final ClusterState state) {
        final RoutingTable routingTable = state.routingTable();
        final DiscoveryNodes nodes = state.nodes();
        final String localNodeId = state.nodes().getLocalNodeId();
        assert localNodeId != null;
        RoutingNode localRoutingNode = state.getRoutingNodes().node(localNodeId);
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            for (Shard shard : indexService) {
                ShardRouting currentRoutingEntry = shard.routingEntry();
                ShardId shardId = currentRoutingEntry.shardId();
                ShardRouting newShardRouting = localRoutingNode == null ? null : localRoutingNode.getByShardId(shardId);
                if (newShardRouting == null || newShardRouting.isSameAllocation(currentRoutingEntry) == false) {
                    logger.debug("{} removing shard (not allocated)", shardId);
                    indexService.removeShard(shardId.id(), "removing shard (not allocated)");
                } else {
                    if (newShardRouting.recoverySource() != null && newShardRouting.recoverySource().getType() == Type.PEER) {
                        RecoveryState recoveryState = shard.recoveryState();
                        final DiscoveryNode sourceNode = findSourceNodeForPeerRecovery(logger, routingTable, nodes, newShardRouting);
                        if (recoveryState.getSourceNode().equals(sourceNode) == false) {
                            if (recoveryTargetService.cancelRecoveriesForShard(shardId, "recovery source node changed")) {
                                logger.debug("{} removing shard (recovery source changed), current [{}], global [{}], shard [{}])", shardId, recoveryState.getSourceNode(), sourceNode, newShardRouting);
                                indexService.removeShard(shardId.id(), "removing shard (recovery source node changed)");
                            }
                        }
                    }
                }
            }
        }
    }

    private void createIndices(final ClusterState state) {
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }
        final Map<Index, List<ShardRouting>> indicesToCreate = new HashMap<>();
        for (ShardRouting shardRouting : localRoutingNode) {
            if (failedShardsCache.containsKey(shardRouting.shardId()) == false) {
                final Index index = shardRouting.index();
                if (indicesService.indexService(index) == null) {
                    indicesToCreate.computeIfAbsent(index, k -> new ArrayList<>()).add(shardRouting);
                }
            }
        }
        for (Map.Entry<Index, List<ShardRouting>> entry : indicesToCreate.entrySet()) {
            final Index index = entry.getKey();
            final IndexMetaData indexMetaData = state.metaData().index(index);
            logger.debug("[{}] creating index", index);
            AllocatedIndex<? extends Shard> indexService = null;
            try {
                indexService = indicesService.createIndex(nodeServicesProvider, indexMetaData, buildInIndexListener, globalCheckpointSyncAction::updateCheckpointForShard);
                if (indexService.updateMapping(indexMetaData) && sendRefreshMapping) {
                    nodeMappingRefreshAction.nodeMappingRefresh(state.nodes().getMasterNode(), new NodeMappingRefreshAction.NodeMappingRefreshRequest(indexMetaData.getIndex().getName(), indexMetaData.getIndexUUID(), state.nodes().getLocalNodeId()));
                }
            } catch (Exception e) {
                final String failShardReason;
                if (indexService == null) {
                    failShardReason = "failed to create index";
                } else {
                    failShardReason = "failed to update mapping for index";
                    indicesService.removeIndex(index, "removing index (mapping update failed)");
                }
                for (ShardRouting shardRouting : entry.getValue()) {
                    sendFailShard(shardRouting, failShardReason, e);
                }
            }
        }
    }

    private void updateIndices(ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }
        final ClusterState state = event.state();
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            final Index index = indexService.index();
            final IndexMetaData currentIndexMetaData = indexService.getIndexSettings().getIndexMetaData();
            final IndexMetaData newIndexMetaData = state.metaData().index(index);
            assert newIndexMetaData != null : "index " + index + " should have been removed by deleteIndices";
            if (ClusterChangedEvent.indexMetaDataChanged(currentIndexMetaData, newIndexMetaData)) {
                indexService.updateMetaData(newIndexMetaData);
                try {
                    if (indexService.updateMapping(newIndexMetaData) && sendRefreshMapping) {
                        nodeMappingRefreshAction.nodeMappingRefresh(state.nodes().getMasterNode(), new NodeMappingRefreshAction.NodeMappingRefreshRequest(newIndexMetaData.getIndex().getName(), newIndexMetaData.getIndexUUID(), state.nodes().getLocalNodeId()));
                    }
                } catch (Exception e) {
                    indicesService.removeIndex(indexService.index(), "removing index (mapping update failed)");
                    RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
                    if (localRoutingNode != null) {
                        for (final ShardRouting shardRouting : localRoutingNode) {
                            if (shardRouting.index().equals(index) && failedShardsCache.containsKey(shardRouting.shardId()) == false) {
                                sendFailShard(shardRouting, "failed to update mapping for index", e);
                            }
                        }
                    }
                }
            }
        }
    }

    private void createOrUpdateShards(final ClusterState state) {
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return;
        }
        DiscoveryNodes nodes = state.nodes();
        RoutingTable routingTable = state.routingTable();
        for (final ShardRouting shardRouting : localRoutingNode) {
            ShardId shardId = shardRouting.shardId();
            if (failedShardsCache.containsKey(shardId) == false) {
                AllocatedIndex<? extends Shard> indexService = indicesService.indexService(shardId.getIndex());
                assert indexService != null : "index " + shardId.getIndex() + " should have been created by createIndices";
                Shard shard = indexService.getShardOrNull(shardId.id());
                if (shard == null) {
                    assert shardRouting.initializing() : shardRouting + " should have been removed by failMissingShards";
                    createShard(nodes, routingTable, shardRouting);
                } else {
                    updateShard(nodes, shardRouting, shard);
                }
            }
        }
    }

    private void createShard(DiscoveryNodes nodes, RoutingTable routingTable, ShardRouting shardRouting) {
        assert shardRouting.initializing() : "only allow shard creation for initializing shard but was " + shardRouting;
        DiscoveryNode sourceNode = null;
        if (shardRouting.recoverySource().getType() == Type.PEER) {
            sourceNode = findSourceNodeForPeerRecovery(logger, routingTable, nodes, shardRouting);
            if (sourceNode == null) {
                logger.trace("ignoring initializing shard {} - no source node can be found.", shardRouting.shardId());
                return;
            }
        }
        try {
            logger.debug("{} creating shard", shardRouting.shardId());
            RecoveryState recoveryState = new RecoveryState(shardRouting, nodes.getLocalNode(), sourceNode);
            indicesService.createShard(shardRouting, recoveryState, recoveryTargetService, new RecoveryListener(shardRouting), repositoriesService, nodeServicesProvider, failedShardHandler);
        } catch (IndexShardAlreadyExistsException e) {
            logger.debug("Trying to create shard that already exists", e);
            assert false;
        } catch (Exception e) {
            failAndRemoveShard(shardRouting, true, "failed to create shard", e);
        }
    }

    private void updateShard(DiscoveryNodes nodes, ShardRouting shardRouting, Shard shard) {
        final ShardRouting currentRoutingEntry = shard.routingEntry();
        assert currentRoutingEntry.isSameAllocation(shardRouting) : "local shard has a different allocation id but wasn't cleaning by removeShards. " + "cluster state: " + shardRouting + " local: " + currentRoutingEntry;
        try {
            shard.updateRoutingEntry(shardRouting);
        } catch (Exception e) {
            failAndRemoveShard(shardRouting, true, "failed updating shard routing entry", e);
            return;
        }
        final IndexShardState state = shard.state();
        if (shardRouting.initializing() && (state == IndexShardState.STARTED || state == IndexShardState.POST_RECOVERY)) {
            if (logger.isTraceEnabled()) {
                logger.trace("{} master marked shard as initializing, but shard has state [{}], resending shard started to {}", shardRouting.shardId(), state, nodes.getMasterNode());
            }
            if (nodes.getMasterNode() != null) {
                shardStateAction.shardStarted(shardRouting, "master " + nodes.getMasterNode() + " marked shard as initializing, but shard state is [" + state + "], mark shard as started", SHARD_STATE_ACTION_LISTENER);
            }
        }
    }

    private static DiscoveryNode findSourceNodeForPeerRecovery(Logger logger, RoutingTable routingTable, DiscoveryNodes nodes, ShardRouting shardRouting) {
        DiscoveryNode sourceNode = null;
        if (!shardRouting.primary()) {
            ShardRouting primary = routingTable.shardRoutingTable(shardRouting.shardId()).primaryShard();
            if (primary.active()) {
                sourceNode = nodes.get(primary.currentNodeId());
                if (sourceNode == null) {
                    logger.trace("can't find replica source node because primary shard {} is assigned to an unknown node.", primary);
                }
            } else {
                logger.trace("can't find replica source node because primary shard {} is not active.", primary);
            }
        } else if (shardRouting.relocatingNodeId() != null) {
            sourceNode = nodes.get(shardRouting.relocatingNodeId());
            if (sourceNode == null) {
                logger.trace("can't find relocation source node for shard {} because it is assigned to an unknown node [{}].", shardRouting.shardId(), shardRouting.relocatingNodeId());
            }
        } else {
            throw new IllegalStateException("trying to find source node for peer recovery when routing state means no peer recovery: " + shardRouting);
        }
        return sourceNode;
    }

    private class RecoveryListener implements PeerRecoveryTargetService.RecoveryListener {

        private final ShardRouting shardRouting;

        private RecoveryListener(ShardRouting shardRouting) {
            this.shardRouting = shardRouting;
        }

        @Override
        public void onRecoveryDone(RecoveryState state) {
            if (state.getRecoverySource().getType() == Type.SNAPSHOT) {
                SnapshotRecoverySource snapshotRecoverySource = (SnapshotRecoverySource) state.getRecoverySource();
                restoreService.indexShardRestoreCompleted(snapshotRecoverySource.snapshot(), shardRouting.shardId());
            }
            shardStateAction.shardStarted(shardRouting, "after " + state.getRecoverySource(), SHARD_STATE_ACTION_LISTENER);
        }

        @Override
        public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
            if (state.getRecoverySource().getType() == Type.SNAPSHOT) {
                try {
                    if (Lucene.isCorruptionException(e.getCause())) {
                        SnapshotRecoverySource snapshotRecoverySource = (SnapshotRecoverySource) state.getRecoverySource();
                        restoreService.failRestore(snapshotRecoverySource.snapshot(), shardRouting.shardId());
                    }
                } catch (Exception inner) {
                    e.addSuppressed(inner);
                } finally {
                    handleRecoveryFailure(shardRouting, sendShardFailure, e);
                }
            } else {
                handleRecoveryFailure(shardRouting, sendShardFailure, e);
            }
        }
    }

    private synchronized void handleRecoveryFailure(ShardRouting shardRouting, boolean sendShardFailure, Exception failure) {
        failAndRemoveShard(shardRouting, sendShardFailure, "failed recovery", failure);
    }

    private void failAndRemoveShard(ShardRouting shardRouting, boolean sendShardFailure, String message, @Nullable Exception failure) {
        try {
            AllocatedIndex<? extends Shard> indexService = indicesService.indexService(shardRouting.shardId().getIndex());
            if (indexService != null) {
                indexService.removeShard(shardRouting.shardId().id(), message);
            }
        } catch (ShardNotFoundException e) {
        } catch (Exception inner) {
            inner.addSuppressed(failure);
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}][{}] failed to remove shard after failure ([{}])", shardRouting.getIndexName(), shardRouting.getId(), message), inner);
        }
        if (sendShardFailure) {
            sendFailShard(shardRouting, message, failure);
        }
    }

    private void sendFailShard(ShardRouting shardRouting, String message, @Nullable Exception failure) {
        try {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] marking and sending shard failed due to [{}]", shardRouting.shardId(), message), failure);
            failedShardsCache.put(shardRouting.shardId(), shardRouting);
            shardStateAction.localShardFailed(shardRouting, message, failure, SHARD_STATE_ACTION_LISTENER);
        } catch (Exception inner) {
            if (failure != null)
                inner.addSuppressed(failure);
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}][{}] failed to mark shard as failed (because of [{}])", shardRouting.getIndexName(), shardRouting.getId(), message), inner);
        }
    }

    private class FailedShardHandler implements Callback<IndexShard.ShardFailure> {

        @Override
        public void handle(final IndexShard.ShardFailure shardFailure) {
            final ShardRouting shardRouting = shardFailure.routing;
            threadPool.generic().execute(() -> {
                synchronized (IndicesClusterStateService.this) {
                    failAndRemoveShard(shardRouting, true, "shard failure, reason [" + shardFailure.reason + "]", shardFailure.cause);
                }
            });
        }
    }

    public interface Shard {

        ShardId shardId();

        ShardRouting routingEntry();

        IndexShardState state();

        RecoveryState recoveryState();

        void updateRoutingEntry(ShardRouting shardRouting) throws IOException;
    }

    public interface AllocatedIndex<T extends Shard> extends Iterable<T>, IndexComponent {

        IndexSettings getIndexSettings();

        void updateMetaData(IndexMetaData indexMetaData);

        boolean updateMapping(IndexMetaData indexMetaData) throws IOException;

        @Nullable
        T getShardOrNull(int shardId);

        void removeShard(int shardId, String message);
    }

    public interface AllocatedIndices<T extends Shard, U extends AllocatedIndex<T>> extends Iterable<U> {

        U createIndex(NodeServicesProvider nodeServicesProvider, IndexMetaData indexMetaData, List<IndexEventListener> builtInIndexListener, Consumer<ShardId> globalCheckpointSyncer) throws IOException;

        IndexMetaData verifyIndexIsDeleted(Index index, ClusterState clusterState);

        void deleteIndex(Index index, String reason);

        void deleteUnassignedIndex(String reason, IndexMetaData metaData, ClusterState clusterState);

        void removeIndex(Index index, String reason);

        @Nullable
        U indexService(Index index);

        T createShard(ShardRouting shardRouting, RecoveryState recoveryState, PeerRecoveryTargetService recoveryTargetService, PeerRecoveryTargetService.RecoveryListener recoveryListener, RepositoriesService repositoriesService, NodeServicesProvider nodeServicesProvider, Callback<IndexShard.ShardFailure> onShardFailure) throws IOException;

        default T getShardOrNull(ShardId shardId) {
            U indexRef = indexService(shardId.getIndex());
            if (indexRef != null) {
                return indexRef.getShardOrNull(shardId.id());
            }
            return null;
        }

        void processPendingDeletes(Index index, IndexSettings indexSettings, TimeValue timeValue) throws IOException, InterruptedException, ShardLockObtainFailedException;
    }
}