package org.elasticsearch.action.support.replication;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ReplicationOperation<Request extends ReplicationRequest<Request>, ReplicaRequest extends ReplicationRequest<ReplicaRequest>, PrimaryResultT extends ReplicationOperation.PrimaryResult<ReplicaRequest>> {

    private final Logger logger;

    private final Request request;

    private final Supplier<ClusterState> clusterStateSupplier;

    private final String opType;

    private final AtomicInteger totalShards = new AtomicInteger();

    private final AtomicInteger pendingActions = new AtomicInteger();

    private final AtomicInteger successfulShards = new AtomicInteger();

    private final boolean executeOnReplicas;

    private final Primary<Request, ReplicaRequest, PrimaryResultT> primary;

    private final Replicas<ReplicaRequest> replicasProxy;

    private final AtomicBoolean finished = new AtomicBoolean();

    protected final ActionListener<PrimaryResultT> resultListener;

    private volatile PrimaryResultT primaryResult = null;

    private final List<ReplicationResponse.ShardInfo.Failure> shardReplicaFailures = Collections.synchronizedList(new ArrayList<>());

    public ReplicationOperation(Request request, Primary<Request, ReplicaRequest, PrimaryResultT> primary, ActionListener<PrimaryResultT> listener, boolean executeOnReplicas, Replicas<ReplicaRequest> replicas, Supplier<ClusterState> clusterStateSupplier, Logger logger, String opType) {
        this.executeOnReplicas = executeOnReplicas;
        this.replicasProxy = replicas;
        this.primary = primary;
        this.resultListener = listener;
        this.logger = logger;
        this.request = request;
        this.clusterStateSupplier = clusterStateSupplier;
        this.opType = opType;
    }

    public void execute() throws Exception {
        final String activeShardCountFailure = checkActiveShardCount();
        final ShardRouting primaryRouting = primary.routingEntry();
        final ShardId primaryId = primaryRouting.shardId();
        if (activeShardCountFailure != null) {
            finishAsFailed(new UnavailableShardsException(primaryId, "{} Timeout: [{}], request: [{}]", activeShardCountFailure, request.timeout(), request));
            return;
        }
        totalShards.incrementAndGet();
        pendingActions.incrementAndGet();
        primaryResult = primary.perform(request);
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] op [{}] completed on primary for request [{}]", primaryId, opType, request);
        }
        final ReplicaRequest replicaRequest = primaryResult.replicaRequest();
        if (replicaRequest != null) {
            assert replicaRequest.primaryTerm() > 0 : "replicaRequest doesn't have a primary term";
            ClusterState clusterState = clusterStateSupplier.get();
            final List<ShardRouting> shards = getShards(primaryId, clusterState);
            Set<String> inSyncAllocationIds = getInSyncAllocationIds(primaryId, clusterState);
            markUnavailableShardsAsStale(replicaRequest, inSyncAllocationIds, shards);
            performOnReplicas(replicaRequest, shards);
        }
        successfulShards.incrementAndGet();
        decPendingAndFinishIfNeeded();
    }

    private void markUnavailableShardsAsStale(ReplicaRequest replicaRequest, Set<String> inSyncAllocationIds, List<ShardRouting> shards) {
        if (inSyncAllocationIds.isEmpty() == false && shards.isEmpty() == false) {
            Set<String> availableAllocationIds = shards.stream().map(ShardRouting::allocationId).filter(Objects::nonNull).map(AllocationId::getId).collect(Collectors.toSet());
            for (String allocationId : Sets.difference(inSyncAllocationIds, availableAllocationIds)) {
                pendingActions.incrementAndGet();
                replicasProxy.markShardCopyAsStale(replicaRequest.shardId(), allocationId, replicaRequest.primaryTerm(), ReplicationOperation.this::decPendingAndFinishIfNeeded, ReplicationOperation.this::onPrimaryDemoted, throwable -> decPendingAndFinishIfNeeded());
            }
        }
    }

    private void performOnReplicas(ReplicaRequest replicaRequest, List<ShardRouting> shards) {
        final String localNodeId = primary.routingEntry().currentNodeId();
        for (final ShardRouting shard : shards) {
            if (executeOnReplicas == false || shard.unassigned()) {
                if (shard.primary() == false) {
                    totalShards.incrementAndGet();
                }
                continue;
            }
            if (shard.currentNodeId().equals(localNodeId) == false) {
                performOnReplica(shard, replicaRequest);
            }
            if (shard.relocating() && shard.relocatingNodeId().equals(localNodeId) == false) {
                performOnReplica(shard.getTargetRelocatingShard(), replicaRequest);
            }
        }
    }

    private void performOnReplica(final ShardRouting shard, final ReplicaRequest replicaRequest) {
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] sending op [{}] to replica {} for request [{}]", shard.shardId(), opType, shard, replicaRequest);
        }
        totalShards.incrementAndGet();
        pendingActions.incrementAndGet();
        replicasProxy.performOn(shard, replicaRequest, new ActionListener<TransportResponse.Empty>() {

            @Override
            public void onResponse(TransportResponse.Empty empty) {
                successfulShards.incrementAndGet();
                decPendingAndFinishIfNeeded();
            }

            @Override
            public void onFailure(Exception replicaException) {
                logger.trace((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage("[{}] failure while performing [{}] on replica {}, request [{}]", shard.shardId(), opType, shard, replicaRequest), replicaException);
                if (ignoreReplicaException(replicaException)) {
                    decPendingAndFinishIfNeeded();
                } else {
                    RestStatus restStatus = ExceptionsHelper.status(replicaException);
                    shardReplicaFailures.add(new ReplicationResponse.ShardInfo.Failure(shard.shardId(), shard.currentNodeId(), replicaException, restStatus, false));
                    String message = String.format(Locale.ROOT, "failed to perform %s on replica %s", opType, shard);
                    logger.warn((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage("[{}] {}", shard.shardId(), message), replicaException);
                    replicasProxy.failShard(shard, replicaRequest.primaryTerm(), message, replicaException, ReplicationOperation.this::decPendingAndFinishIfNeeded, ReplicationOperation.this::onPrimaryDemoted, throwable -> decPendingAndFinishIfNeeded());
                }
            }
        });
    }

    private void onPrimaryDemoted(Exception demotionFailure) {
        String primaryFail = String.format(Locale.ROOT, "primary shard [%s] was demoted while failing replica shard", primary.routingEntry());
        primary.failShard(primaryFail, demotionFailure);
        finishAsFailed(new RetryOnPrimaryException(primary.routingEntry().shardId(), primaryFail, demotionFailure));
    }

    protected String checkActiveShardCount() {
        final ShardId shardId = primary.routingEntry().shardId();
        final String indexName = shardId.getIndexName();
        final ClusterState state = clusterStateSupplier.get();
        assert state != null : "replication operation must have access to the cluster state";
        final ActiveShardCount waitForActiveShards = request.waitForActiveShards();
        if (waitForActiveShards == ActiveShardCount.NONE) {
            return null;
        }
        IndexRoutingTable indexRoutingTable = state.getRoutingTable().index(indexName);
        if (indexRoutingTable == null) {
            logger.trace("[{}] index not found in the routing table", shardId);
            return "Index " + indexName + " not found in the routing table";
        }
        IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId.getId());
        if (shardRoutingTable == null) {
            logger.trace("[{}] shard not found in the routing table", shardId);
            return "Shard " + shardId + " not found in the routing table";
        }
        if (waitForActiveShards.enoughShardsActive(shardRoutingTable)) {
            return null;
        } else {
            final String resolvedShards = waitForActiveShards == ActiveShardCount.ALL ? Integer.toString(shardRoutingTable.shards().size()) : waitForActiveShards.toString();
            logger.trace("[{}] not enough active copies to meet shard count of [{}] (have {}, needed {}), scheduling a retry. op [{}], " + "request [{}]", shardId, waitForActiveShards, shardRoutingTable.activeShards().size(), resolvedShards, opType, request);
            return "Not enough active copies to meet shard count of [" + waitForActiveShards + "] (have " + shardRoutingTable.activeShards().size() + ", needed " + resolvedShards + ").";
        }
    }

    protected Set<String> getInSyncAllocationIds(ShardId shardId, ClusterState clusterState) {
        IndexMetaData indexMetaData = clusterState.metaData().index(shardId.getIndex());
        if (indexMetaData != null) {
            return indexMetaData.inSyncAllocationIds(shardId.id());
        }
        return Collections.emptySet();
    }

    protected List<ShardRouting> getShards(ShardId shardId, ClusterState state) {
        final IndexShardRoutingTable shardRoutingTable = state.getRoutingTable().shardRoutingTableOrNull(shardId);
        List<ShardRouting> shards = shardRoutingTable == null ? Collections.emptyList() : shardRoutingTable.shards();
        return shards;
    }

    private void decPendingAndFinishIfNeeded() {
        assert pendingActions.get() > 0;
        if (pendingActions.decrementAndGet() == 0) {
            finish();
        }
    }

    private void finish() {
        if (finished.compareAndSet(false, true)) {
            final ReplicationResponse.ShardInfo.Failure[] failuresArray;
            if (shardReplicaFailures.isEmpty()) {
                failuresArray = ReplicationResponse.EMPTY;
            } else {
                failuresArray = new ReplicationResponse.ShardInfo.Failure[shardReplicaFailures.size()];
                shardReplicaFailures.toArray(failuresArray);
            }
            primaryResult.setShardInfo(new ReplicationResponse.ShardInfo(totalShards.get(), successfulShards.get(), failuresArray));
            resultListener.onResponse(primaryResult);
        }
    }

    private void finishAsFailed(Exception exception) {
        if (finished.compareAndSet(false, true)) {
            resultListener.onFailure(exception);
        }
    }

    public static boolean ignoreReplicaException(Exception e) {
        if (TransportActions.isShardNotAvailableException(e)) {
            return true;
        }
        if (isConflictException(e)) {
            return true;
        }
        return false;
    }

    public static boolean isConflictException(Throwable t) {
        final Throwable cause = ExceptionsHelper.unwrapCause(t);
        return cause instanceof VersionConflictEngineException;
    }

    public interface Primary<Request extends ReplicationRequest<Request>, ReplicaRequest extends ReplicationRequest<ReplicaRequest>, PrimaryResultT extends PrimaryResult<ReplicaRequest>> {

        ShardRouting routingEntry();

        void failShard(String message, Exception exception);

        PrimaryResultT perform(Request request) throws Exception;
    }

    public interface Replicas<ReplicaRequest extends ReplicationRequest<ReplicaRequest>> {

        void performOn(ShardRouting replica, ReplicaRequest replicaRequest, ActionListener<TransportResponse.Empty> listener);

        void failShard(ShardRouting replica, long primaryTerm, String message, Exception exception, Runnable onSuccess, Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure);

        void markShardCopyAsStale(ShardId shardId, String allocationId, long primaryTerm, Runnable onSuccess, Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure);
    }

    public static class RetryOnPrimaryException extends ElasticsearchException {

        public RetryOnPrimaryException(ShardId shardId, String msg) {
            this(shardId, msg, null);
        }

        public RetryOnPrimaryException(ShardId shardId, String msg, Throwable cause) {
            super(msg, cause);
            setShard(shardId);
        }

        public RetryOnPrimaryException(StreamInput in) throws IOException {
            super(in);
        }
    }

    public interface PrimaryResult<R extends ReplicationRequest<R>> {

        @Nullable
        R replicaRequest();

        void setShardInfo(ReplicationResponse.ShardInfo shardInfo);
    }
}