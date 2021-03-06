package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ReplicationOperation<Request extends ReplicationRequest<Request>, ReplicaRequest extends ReplicationRequest<ReplicaRequest>, PrimaryResultT extends ReplicationOperation.PrimaryResult<ReplicaRequest>> {

    private final ESLogger logger;

    private final Request request;

    private final Supplier<ClusterState> clusterStateSupplier;

    private final String opType;

    private final AtomicInteger totalShards = new AtomicInteger();

    private final AtomicInteger pendingShards = new AtomicInteger();

    private final AtomicInteger successfulShards = new AtomicInteger();

    private final boolean executeOnReplicas;

    private final boolean checkWriteConsistency;

    private final Primary<Request, ReplicaRequest, PrimaryResultT> primary;

    private final Replicas<ReplicaRequest> replicasProxy;

    private final AtomicBoolean finished = new AtomicBoolean();

    protected final ActionListener<PrimaryResultT> resultListener;

    private volatile PrimaryResultT primaryResult = null;

    private final List<ReplicationResponse.ShardInfo.Failure> shardReplicaFailures = Collections.synchronizedList(new ArrayList<>());

    public ReplicationOperation(Request request, Primary<Request, ReplicaRequest, PrimaryResultT> primary, ActionListener<PrimaryResultT> listener, boolean executeOnReplicas, boolean checkWriteConsistency, Replicas<ReplicaRequest> replicas, Supplier<ClusterState> clusterStateSupplier, ESLogger logger, String opType) {
        this.checkWriteConsistency = checkWriteConsistency;
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
        final String writeConsistencyFailure = checkWriteConsistency ? checkWriteConsistency() : null;
        final ShardRouting primaryRouting = primary.routingEntry();
        final ShardId primaryId = primaryRouting.shardId();
        if (writeConsistencyFailure != null) {
            finishAsFailed(new UnavailableShardsException(primaryId, "{} Timeout: [{}], request: [{}]", writeConsistencyFailure, request.timeout(), request));
            return;
        }
        totalShards.incrementAndGet();
        pendingShards.incrementAndGet();
        primaryResult = primary.perform(request);
        final ReplicaRequest replicaRequest = primaryResult.replicaRequest();
        assert replicaRequest.primaryTerm() > 0 : "replicaRequest doesn't have a primary term";
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] op [{}] completed on primary for request [{}]", primaryId, opType, request);
        }
        performOnReplicas(primaryId, replicaRequest);
        successfulShards.incrementAndGet();
        decPendingAndFinishIfNeeded();
    }

    private void performOnReplicas(ShardId primaryId, ReplicaRequest replicaRequest) {
        final List<ShardRouting> shards = getShards(primaryId, clusterStateSupplier.get());
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
                performOnReplica(shard.buildTargetRelocatingShard(), replicaRequest);
            }
        }
    }

    private void performOnReplica(final ShardRouting shard, final ReplicaRequest replicaRequest) {
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] sending op [{}] to replica {} for request [{}]", shard.shardId(), opType, shard, replicaRequest);
        }
        totalShards.incrementAndGet();
        pendingShards.incrementAndGet();
        replicasProxy.performOn(shard, replicaRequest, new ActionListener<TransportResponse.Empty>() {

            @Override
            public void onResponse(TransportResponse.Empty empty) {
                successfulShards.incrementAndGet();
                decPendingAndFinishIfNeeded();
            }

            @Override
            public void onFailure(Exception replicaException) {
                logger.trace("[{}] failure while performing [{}] on replica {}, request [{}]", replicaException, shard.shardId(), opType, shard, replicaRequest);
                if (ignoreReplicaException(replicaException)) {
                    decPendingAndFinishIfNeeded();
                } else {
                    RestStatus restStatus = ExceptionsHelper.status(replicaException);
                    shardReplicaFailures.add(new ReplicationResponse.ShardInfo.Failure(shard.shardId(), shard.currentNodeId(), replicaException, restStatus, false));
                    String message = String.format(Locale.ROOT, "failed to perform %s on replica %s", opType, shard);
                    logger.warn("[{}] {}", replicaException, shard.shardId(), message);
                    replicasProxy.failShard(shard, primary.routingEntry(), message, replicaException, ReplicationOperation.this::decPendingAndFinishIfNeeded, ReplicationOperation.this::onPrimaryDemoted, throwable -> decPendingAndFinishIfNeeded());
                }
            }
        });
    }

    private void onPrimaryDemoted(Exception demotionFailure) {
        String primaryFail = String.format(Locale.ROOT, "primary shard [%s] was demoted while failing replica shard", primary.routingEntry());
        primary.failShard(primaryFail, demotionFailure);
        finishAsFailed(new RetryOnPrimaryException(primary.routingEntry().shardId(), primaryFail, demotionFailure));
    }

    String checkWriteConsistency() {
        assert request.consistencyLevel() != WriteConsistencyLevel.DEFAULT : "consistency level should be set";
        final ShardId shardId = primary.routingEntry().shardId();
        final ClusterState state = clusterStateSupplier.get();
        final WriteConsistencyLevel consistencyLevel = request.consistencyLevel();
        final int sizeActive;
        final int requiredNumber;
        IndexRoutingTable indexRoutingTable = state.getRoutingTable().index(shardId.getIndexName());
        if (indexRoutingTable != null) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId.getId());
            if (shardRoutingTable != null) {
                sizeActive = shardRoutingTable.activeShards().size();
                if (consistencyLevel == WriteConsistencyLevel.QUORUM && shardRoutingTable.getSize() > 2) {
                    requiredNumber = (shardRoutingTable.getSize() / 2) + 1;
                } else if (consistencyLevel == WriteConsistencyLevel.ALL) {
                    requiredNumber = shardRoutingTable.getSize();
                } else {
                    requiredNumber = 1;
                }
            } else {
                sizeActive = 0;
                requiredNumber = 1;
            }
        } else {
            sizeActive = 0;
            requiredNumber = 1;
        }
        if (sizeActive < requiredNumber) {
            logger.trace("[{}] not enough active copies to meet write consistency of [{}] (have {}, needed {}), scheduling a retry." + " op [{}], request [{}]", shardId, consistencyLevel, sizeActive, requiredNumber, opType, request);
            return "Not enough active copies to meet write consistency of [" + consistencyLevel + "] (have " + sizeActive + ", needed " + requiredNumber + ").";
        } else {
            return null;
        }
    }

    protected List<ShardRouting> getShards(ShardId shardId, ClusterState state) {
        final IndexShardRoutingTable shardRoutingTable = state.getRoutingTable().shardRoutingTableOrNull(shardId);
        List<ShardRouting> shards = shardRoutingTable == null ? Collections.emptyList() : shardRoutingTable.shards();
        return shards;
    }

    private void decPendingAndFinishIfNeeded() {
        assert pendingShards.get() > 0;
        if (pendingShards.decrementAndGet() == 0) {
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

        void failShard(ShardRouting replica, ShardRouting primary, String message, Exception exception, Runnable onSuccess, Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure);
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

        R replicaRequest();

        void setShardInfo(ReplicationResponse.ShardInfo shardInfo);
    }
}