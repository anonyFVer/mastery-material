package org.elasticsearch.action.support.replication;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.flush.ShardFlushRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportChannelResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

public abstract class TransportReplicationAction<Request extends ReplicationRequest<Request>, ReplicaRequest extends ReplicationRequest<ReplicaRequest>, Response extends ReplicationResponse> extends TransportAction<Request, Response> {

    private final TransportService transportService;

    protected final ClusterService clusterService;

    private final IndicesService indicesService;

    private final ShardStateAction shardStateAction;

    private final TransportRequestOptions transportOptions;

    private final String executor;

    private final String transportReplicaAction;

    private final String transportPrimaryAction;

    private final ReplicasProxy replicasProxy;

    protected TransportReplicationAction(Settings settings, String actionName, TransportService transportService, ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request, Supplier<ReplicaRequest> replicaRequest, String executor) {
        super(settings, actionName, threadPool, actionFilters, indexNameExpressionResolver, transportService.getTaskManager());
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.shardStateAction = shardStateAction;
        this.executor = executor;
        this.transportPrimaryAction = actionName + "[p]";
        this.transportReplicaAction = actionName + "[r]";
        transportService.registerRequestHandler(actionName, request, ThreadPool.Names.SAME, new OperationTransportHandler());
        transportService.registerRequestHandler(transportPrimaryAction, () -> new ConcreteShardRequest<>(request), executor, new PrimaryOperationTransportHandler());
        transportService.registerRequestHandler(transportReplicaAction, () -> new ConcreteShardRequest<>(replicaRequest), executor, true, true, new ReplicaOperationTransportHandler());
        this.transportOptions = transportOptions();
        this.replicasProxy = new ReplicasProxy();
    }

    @Override
    protected final void doExecute(Request request, ActionListener<Response> listener) {
        throw new UnsupportedOperationException("the task parameter is required for this operation");
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        new ReroutePhase((ReplicationTask) task, request, listener).run();
    }

    protected abstract Response newResponseInstance();

    protected void resolveRequest(MetaData metaData, IndexMetaData indexMetaData, Request request) {
        if (request.waitForActiveShards() == ActiveShardCount.DEFAULT) {
            request.waitForActiveShards(indexMetaData.getWaitForActiveShards());
        }
    }

    protected abstract PrimaryResult shardOperationOnPrimary(Request shardRequest, IndexShard primary) throws Exception;

    protected abstract ReplicaResult shardOperationOnReplica(ReplicaRequest shardRequest, IndexShard replica) throws Exception;

    protected ClusterBlockLevel globalBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    protected ClusterBlockLevel indexBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    protected boolean resolveIndex() {
        return true;
    }

    protected TransportRequestOptions transportOptions() {
        return TransportRequestOptions.EMPTY;
    }

    protected boolean retryPrimaryException(final Throwable e) {
        return e.getClass() == ReplicationOperation.RetryOnPrimaryException.class || TransportActions.isShardNotAvailableException(e);
    }

    class OperationTransportHandler implements TransportRequestHandler<Request> {

        @Override
        public void messageReceived(final Request request, final TransportChannel channel, Task task) throws Exception {
            execute(task, request, new ActionListener<Response>() {

                @Override
                public void onResponse(Response result) {
                    try {
                        channel.sendResponse(result);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage("Failed to send response for {}", actionName), inner);
                    }
                }
            });
        }

        @Override
        public void messageReceived(Request request, TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("the task parameter is required for this operation");
        }
    }

    class PrimaryOperationTransportHandler implements TransportRequestHandler<ConcreteShardRequest<Request>> {

        @Override
        public void messageReceived(final ConcreteShardRequest<Request> request, final TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("the task parameter is required for this operation");
        }

        @Override
        public void messageReceived(ConcreteShardRequest<Request> request, TransportChannel channel, Task task) {
            new AsyncPrimaryAction(request.request, request.targetAllocationID, channel, (ReplicationTask) task).run();
        }
    }

    class AsyncPrimaryAction extends AbstractRunnable implements ActionListener<PrimaryShardReference> {

        private final Request request;

        private final String targetAllocationID;

        private final TransportChannel channel;

        private final ReplicationTask replicationTask;

        AsyncPrimaryAction(Request request, String targetAllocationID, TransportChannel channel, ReplicationTask replicationTask) {
            this.request = request;
            this.targetAllocationID = targetAllocationID;
            this.channel = channel;
            this.replicationTask = replicationTask;
        }

        @Override
        protected void doRun() throws Exception {
            acquirePrimaryShardReference(request.shardId(), targetAllocationID, this);
        }

        @Override
        public void onResponse(PrimaryShardReference primaryShardReference) {
            try {
                if (primaryShardReference.isRelocated()) {
                    primaryShardReference.close();
                    setPhase(replicationTask, "primary_delegation");
                    final ShardRouting primary = primaryShardReference.routingEntry();
                    assert primary.relocating() : "indexShard is marked as relocated but routing isn't" + primary;
                    DiscoveryNode relocatingNode = clusterService.state().nodes().get(primary.relocatingNodeId());
                    transportService.sendRequest(relocatingNode, transportPrimaryAction, new ConcreteShardRequest<>(request, primary.allocationId().getRelocationId()), transportOptions, new TransportChannelResponseHandler<Response>(logger, channel, "rerouting indexing to target primary " + primary, TransportReplicationAction.this::newResponseInstance) {

                        @Override
                        public void handleResponse(Response response) {
                            setPhase(replicationTask, "finished");
                            super.handleResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            setPhase(replicationTask, "finished");
                            super.handleException(exp);
                        }
                    });
                } else {
                    setPhase(replicationTask, "primary");
                    final IndexMetaData indexMetaData = clusterService.state().getMetaData().index(request.shardId().getIndex());
                    final boolean executeOnReplicas = (indexMetaData == null) || shouldExecuteReplication(indexMetaData.getSettings());
                    final ActionListener<Response> listener = createResponseListener(primaryShardReference);
                    createReplicatedOperation(request, ActionListener.wrap(result -> result.respond(listener), listener::onFailure), primaryShardReference, executeOnReplicas).execute();
                }
            } catch (Exception e) {
                Releasables.closeWhileHandlingException(primaryShardReference);
                onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            setPhase(replicationTask, "finished");
            try {
                channel.sendResponse(e);
            } catch (IOException inner) {
                inner.addSuppressed(e);
                logger.warn("failed to send response", inner);
            }
        }

        private ActionListener<Response> createResponseListener(final PrimaryShardReference primaryShardReference) {
            return new ActionListener<Response>() {

                @Override
                public void onResponse(Response response) {
                    primaryShardReference.close();
                    setPhase(replicationTask, "finished");
                    try {
                        channel.sendResponse(response);
                    } catch (IOException e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    primaryShardReference.close();
                    setPhase(replicationTask, "finished");
                    try {
                        channel.sendResponse(e);
                    } catch (IOException e1) {
                        logger.warn("failed to send response", e);
                    }
                }
            };
        }

        protected ReplicationOperation<Request, ReplicaRequest, PrimaryResult> createReplicatedOperation(Request request, ActionListener<PrimaryResult> listener, PrimaryShardReference primaryShardReference, boolean executeOnReplicas) {
            return new ReplicationOperation<>(request, primaryShardReference, listener, executeOnReplicas, replicasProxy, clusterService::state, logger, actionName);
        }
    }

    protected class PrimaryResult implements ReplicationOperation.PrimaryResult<ReplicaRequest> {

        final ReplicaRequest replicaRequest;

        final Response finalResponseIfSuccessful;

        final Exception finalFailure;

        public PrimaryResult(ReplicaRequest replicaRequest, Response finalResponseIfSuccessful, Exception finalFailure) {
            assert finalFailure != null ^ finalResponseIfSuccessful != null : "either a response or a failure has to be not null";
            this.replicaRequest = replicaRequest;
            this.finalResponseIfSuccessful = finalResponseIfSuccessful;
            this.finalFailure = finalFailure;
        }

        public PrimaryResult(ReplicaRequest replicaRequest, Response replicationResponse) {
            this(replicaRequest, replicationResponse, null);
        }

        @Override
        public ReplicaRequest replicaRequest() {
            return replicaRequest;
        }

        @Override
        public void setShardInfo(ReplicationResponse.ShardInfo shardInfo) {
            if (finalResponseIfSuccessful != null) {
                finalResponseIfSuccessful.setShardInfo(shardInfo);
            }
        }

        public void respond(ActionListener<Response> listener) {
            if (finalResponseIfSuccessful != null) {
                listener.onResponse(finalResponseIfSuccessful);
            } else {
                listener.onFailure(finalFailure);
            }
        }
    }

    protected class ReplicaResult {

        final Exception finalFailure;

        public ReplicaResult(Exception finalFailure) {
            this.finalFailure = finalFailure;
        }

        public ReplicaResult() {
            this(null);
        }

        public void respond(ActionListener<TransportResponse.Empty> listener) {
            if (finalFailure == null) {
                listener.onResponse(TransportResponse.Empty.INSTANCE);
            } else {
                listener.onFailure(finalFailure);
            }
        }
    }

    class ReplicaOperationTransportHandler implements TransportRequestHandler<ConcreteShardRequest<ReplicaRequest>> {

        @Override
        public void messageReceived(final ConcreteShardRequest<ReplicaRequest> request, final TransportChannel channel) throws Exception {
            throw new UnsupportedOperationException("the task parameter is required for this operation");
        }

        @Override
        public void messageReceived(ConcreteShardRequest<ReplicaRequest> requestWithAID, TransportChannel channel, Task task) throws Exception {
            new AsyncReplicaAction(requestWithAID.request, requestWithAID.targetAllocationID, channel, (ReplicationTask) task).run();
        }
    }

    public static class RetryOnReplicaException extends ElasticsearchException {

        public RetryOnReplicaException(ShardId shardId, String msg) {
            super(msg);
            setShard(shardId);
        }

        public RetryOnReplicaException(StreamInput in) throws IOException {
            super(in);
        }
    }

    private final class AsyncReplicaAction extends AbstractRunnable implements ActionListener<Releasable> {

        private final ReplicaRequest request;

        private final String targetAllocationID;

        private final TransportChannel channel;

        private final IndexShard replica;

        private final ReplicationTask task;

        private final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());

        AsyncReplicaAction(ReplicaRequest request, String targetAllocationID, TransportChannel channel, ReplicationTask task) {
            this.request = request;
            this.channel = channel;
            this.task = task;
            this.targetAllocationID = targetAllocationID;
            final ShardId shardId = request.shardId();
            assert shardId != null : "request shardId must be set";
            this.replica = getIndexShard(shardId);
        }

        @Override
        public void onResponse(Releasable releasable) {
            try {
                ReplicaResult replicaResult = shardOperationOnReplica(request, replica);
                releasable.close();
                replicaResult.respond(new ResponseListener());
            } catch (Exception e) {
                Releasables.closeWhileHandlingException(releasable);
                AsyncReplicaAction.this.onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (e instanceof RetryOnReplicaException) {
                logger.trace((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage("Retrying operation on replica, action [{}], request [{}]", transportReplicaAction, request), e);
                request.onRetry();
                final ThreadContext.StoredContext context = threadPool.getThreadContext().newStoredContext();
                observer.waitForNextChange(new ClusterStateObserver.Listener() {

                    @Override
                    public void onNewClusterState(ClusterState state) {
                        context.close();
                        String extraMessage = "action [" + transportReplicaAction + "], request[" + request + "]";
                        TransportChannelResponseHandler<TransportResponse.Empty> handler = new TransportChannelResponseHandler<>(logger, channel, extraMessage, () -> TransportResponse.Empty.INSTANCE);
                        transportService.sendRequest(clusterService.localNode(), transportReplicaAction, new ConcreteShardRequest<>(request, targetAllocationID), handler);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        responseWithFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        throw new AssertionError("Cannot happen: there is not timeout");
                    }
                });
            } else {
                responseWithFailure(e);
            }
        }

        protected void responseWithFailure(Exception e) {
            try {
                setPhase(task, "finished");
                channel.sendResponse(e);
            } catch (IOException responseException) {
                responseException.addSuppressed(e);
                logger.warn((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage("failed to send error message back to client for action [{}]", transportReplicaAction), responseException);
            }
        }

        @Override
        protected void doRun() throws Exception {
            setPhase(task, "replica");
            final String actualAllocationId = this.replica.routingEntry().allocationId().getId();
            if (actualAllocationId.equals(targetAllocationID) == false) {
                throw new ShardNotFoundException(this.replica.shardId(), "expected aID [{}] but found [{}]", targetAllocationID, actualAllocationId);
            }
            replica.acquireReplicaOperationLock(request.primaryTerm, this, executor);
        }

        private class ResponseListener implements ActionListener<TransportResponse.Empty> {

            @Override
            public void onResponse(Empty response) {
                if (logger.isTraceEnabled()) {
                    logger.trace("action [{}] completed on shard [{}] for request [{}]", transportReplicaAction, request.shardId(), request);
                }
                setPhase(task, "finished");
                try {
                    channel.sendResponse(response);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                responseWithFailure(e);
            }
        }
    }

    private IndexShard getIndexShard(ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        return indexService.getShard(shardId.id());
    }

    final class ReroutePhase extends AbstractRunnable {

        private final ActionListener<Response> listener;

        private final Request request;

        private final ReplicationTask task;

        private final ClusterStateObserver observer;

        private final AtomicBoolean finished = new AtomicBoolean();

        ReroutePhase(ReplicationTask task, Request request, ActionListener<Response> listener) {
            this.request = request;
            if (task != null) {
                this.request.setParentTask(clusterService.localNode().getId(), task.getId());
            }
            this.listener = listener;
            this.task = task;
            this.observer = new ClusterStateObserver(clusterService, request.timeout(), logger, threadPool.getThreadContext());
        }

        @Override
        public void onFailure(Exception e) {
            finishWithUnexpectedFailure(e);
        }

        @Override
        protected void doRun() {
            setPhase(task, "routing");
            final ClusterState state = observer.observedState();
            if (handleBlockExceptions(state)) {
                return;
            }
            final String concreteIndex = concreteIndex(state);
            final IndexMetaData indexMetaData = state.metaData().index(concreteIndex);
            if (indexMetaData == null) {
                retry(new IndexNotFoundException(concreteIndex));
                return;
            }
            resolveRequest(state.metaData(), indexMetaData, request);
            assert request.shardId() != null : "request shardId must be set in resolveRequest";
            assert request.waitForActiveShards() != ActiveShardCount.DEFAULT : "request waitForActiveShards must be set in resolveRequest";
            final ShardRouting primary = primary(state);
            if (retryIfUnavailable(state, primary)) {
                return;
            }
            final DiscoveryNode node = state.nodes().get(primary.currentNodeId());
            taskManager.registerChildTask(task, node.getId());
            if (primary.currentNodeId().equals(state.nodes().getLocalNodeId())) {
                performLocalAction(state, primary, node);
            } else {
                performRemoteAction(state, primary, node);
            }
        }

        private void performLocalAction(ClusterState state, ShardRouting primary, DiscoveryNode node) {
            setPhase(task, "waiting_on_primary");
            if (logger.isTraceEnabled()) {
                logger.trace("send action [{}] on primary [{}] for request [{}] with cluster state version [{}] to [{}] ", transportPrimaryAction, request.shardId(), request, state.version(), primary.currentNodeId());
            }
            performAction(node, transportPrimaryAction, true, new ConcreteShardRequest<>(request, primary.allocationId().getId()));
        }

        private void performRemoteAction(ClusterState state, ShardRouting primary, DiscoveryNode node) {
            if (state.version() < request.routedBasedOnClusterVersion()) {
                logger.trace("failed to find primary [{}] for request [{}] despite sender thinking it would be here. Local cluster state " + "version [{}]] is older than on sending node (version [{}]), scheduling a retry...", request.shardId(), request, state.version(), request.routedBasedOnClusterVersion());
                retryBecauseUnavailable(request.shardId(), "failed to find primary as current cluster state with version [" + state.version() + "] is stale (expected at least [" + request.routedBasedOnClusterVersion() + "]");
                return;
            } else {
                request.routedBasedOnClusterVersion(state.version());
            }
            if (logger.isTraceEnabled()) {
                logger.trace("send action [{}] on primary [{}] for request [{}] with cluster state version [{}] to [{}]", actionName, request.shardId(), request, state.version(), primary.currentNodeId());
            }
            setPhase(task, "rerouted");
            performAction(node, actionName, false, request);
        }

        private boolean retryIfUnavailable(ClusterState state, ShardRouting primary) {
            if (primary == null || primary.active() == false) {
                logger.trace("primary shard [{}] is not yet active, scheduling a retry: action [{}], request [{}], " + "cluster state version [{}]", request.shardId(), actionName, request, state.version());
                retryBecauseUnavailable(request.shardId(), "primary shard is not active");
                return true;
            }
            if (state.nodes().nodeExists(primary.currentNodeId()) == false) {
                logger.trace("primary shard [{}] is assigned to an unknown node [{}], scheduling a retry: action [{}], request [{}], " + "cluster state version [{}]", request.shardId(), primary.currentNodeId(), actionName, request, state.version());
                retryBecauseUnavailable(request.shardId(), "primary shard isn't assigned to a known node.");
                return true;
            }
            return false;
        }

        private String concreteIndex(ClusterState state) {
            return resolveIndex() ? indexNameExpressionResolver.concreteSingleIndex(state, request).getName() : request.index();
        }

        private ShardRouting primary(ClusterState state) {
            IndexShardRoutingTable indexShard = state.getRoutingTable().shardRoutingTable(request.shardId());
            return indexShard.primaryShard();
        }

        private boolean handleBlockExceptions(ClusterState state) {
            ClusterBlockException blockException = state.blocks().globalBlockedException(globalBlockLevel());
            if (blockException != null) {
                handleBlockException(blockException);
                return true;
            }
            blockException = state.blocks().indexBlockedException(indexBlockLevel(), concreteIndex(state));
            if (blockException != null) {
                handleBlockException(blockException);
                return true;
            }
            return false;
        }

        private void handleBlockException(ClusterBlockException blockException) {
            if (blockException.retryable()) {
                logger.trace("cluster is blocked, scheduling a retry", blockException);
                retry(blockException);
            } else {
                finishAsFailed(blockException);
            }
        }

        private void performAction(final DiscoveryNode node, final String action, final boolean isPrimaryAction, final TransportRequest requestToPerform) {
            transportService.sendRequest(node, action, requestToPerform, transportOptions, new TransportResponseHandler<Response>() {

                @Override
                public Response newInstance() {
                    return newResponseInstance();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public void handleResponse(Response response) {
                    finishOnSuccess(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    try {
                        final Throwable cause = exp.unwrapCause();
                        if (cause instanceof ConnectTransportException || cause instanceof NodeClosedException || (isPrimaryAction && retryPrimaryException(cause))) {
                            logger.trace((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage("received an error from node [{}] for request [{}], scheduling a retry", node.getId(), requestToPerform), exp);
                            retry(exp);
                        } else {
                            finishAsFailed(exp);
                        }
                    } catch (Exception e) {
                        e.addSuppressed(exp);
                        finishWithUnexpectedFailure(e);
                    }
                }
            });
        }

        void retry(Exception failure) {
            assert failure != null;
            if (observer.isTimedOut()) {
                finishAsFailed(failure);
                return;
            }
            setPhase(task, "waiting_for_retry");
            request.onRetry();
            final ThreadContext.StoredContext context = threadPool.getThreadContext().newStoredContext();
            observer.waitForNextChange(new ClusterStateObserver.Listener() {

                @Override
                public void onNewClusterState(ClusterState state) {
                    context.close();
                    run();
                }

                @Override
                public void onClusterServiceClose() {
                    finishAsFailed(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    context.close();
                    run();
                }
            });
        }

        void finishAsFailed(Exception failure) {
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "failed");
                logger.trace((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage("operation failed. action [{}], request [{}]", actionName, request), failure);
                listener.onFailure(failure);
            } else {
                assert false : "finishAsFailed called but operation is already finished";
            }
        }

        void finishWithUnexpectedFailure(Exception failure) {
            logger.warn((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage("unexpected error during the primary phase for action [{}], request [{}]", actionName, request), failure);
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "failed");
                listener.onFailure(failure);
            } else {
                assert false : "finishWithUnexpectedFailure called but operation is already finished";
            }
        }

        void finishOnSuccess(Response response) {
            if (finished.compareAndSet(false, true)) {
                setPhase(task, "finished");
                if (logger.isTraceEnabled()) {
                    logger.trace("operation succeeded. action [{}],request [{}]", actionName, request);
                }
                listener.onResponse(response);
            } else {
                assert false : "finishOnSuccess called but operation is already finished";
            }
        }

        void retryBecauseUnavailable(ShardId shardId, String message) {
            retry(new UnavailableShardsException(shardId, "{} Timeout: [{}], request: [{}]", message, request.timeout(), request));
        }
    }

    private void acquirePrimaryShardReference(ShardId shardId, String allocationId, ActionListener<PrimaryShardReference> onReferenceAcquired) {
        IndexShard indexShard = getIndexShard(shardId);
        if (indexShard.routingEntry().primary() == false) {
            throw new ReplicationOperation.RetryOnPrimaryException(indexShard.shardId(), "actual shard is not a primary " + indexShard.routingEntry());
        }
        final String actualAllocationId = indexShard.routingEntry().allocationId().getId();
        if (actualAllocationId.equals(allocationId) == false) {
            throw new ShardNotFoundException(shardId, "expected aID [{}] but found [{}]", allocationId, actualAllocationId);
        }
        ActionListener<Releasable> onAcquired = new ActionListener<Releasable>() {

            @Override
            public void onResponse(Releasable releasable) {
                onReferenceAcquired.onResponse(new PrimaryShardReference(indexShard, releasable));
            }

            @Override
            public void onFailure(Exception e) {
                onReferenceAcquired.onFailure(e);
            }
        };
        indexShard.acquirePrimaryOperationLock(onAcquired, executor);
    }

    protected boolean shouldExecuteReplication(Settings settings) {
        return IndexMetaData.isIndexUsingShadowReplicas(settings) == false;
    }

    class PrimaryShardReference implements ReplicationOperation.Primary<Request, ReplicaRequest, PrimaryResult>, Releasable {

        private final IndexShard indexShard;

        private final Releasable operationLock;

        PrimaryShardReference(IndexShard indexShard, Releasable operationLock) {
            this.indexShard = indexShard;
            this.operationLock = operationLock;
        }

        @Override
        public void close() {
            operationLock.close();
        }

        public boolean isRelocated() {
            return indexShard.state() == IndexShardState.RELOCATED;
        }

        @Override
        public void failShard(String reason, Exception e) {
            try {
                indexShard.failShard(reason, e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
        }

        @Override
        public PrimaryResult perform(Request request) throws Exception {
            PrimaryResult result = shardOperationOnPrimary(request, indexShard);
            if (result.replicaRequest() != null) {
                result.replicaRequest().primaryTerm(indexShard.getPrimaryTerm());
            }
            return result;
        }

        @Override
        public ShardRouting routingEntry() {
            return indexShard.routingEntry();
        }
    }

    final class ReplicasProxy implements ReplicationOperation.Replicas<ReplicaRequest> {

        @Override
        public void performOn(ShardRouting replica, ReplicaRequest request, ActionListener<TransportResponse.Empty> listener) {
            String nodeId = replica.currentNodeId();
            final DiscoveryNode node = clusterService.state().nodes().get(nodeId);
            if (node == null) {
                listener.onFailure(new NoNodeAvailableException("unknown node [" + nodeId + "]"));
                return;
            }
            transportService.sendRequest(node, transportReplicaAction, new ConcreteShardRequest<>(request, replica.allocationId().getId()), transportOptions, new ActionListenerResponseHandler<>(listener, () -> TransportResponse.Empty.INSTANCE));
        }

        @Override
        public void failShard(ShardRouting replica, long primaryTerm, String message, Exception exception, Runnable onSuccess, Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure) {
            shardStateAction.remoteShardFailed(replica.shardId(), replica.allocationId().getId(), primaryTerm, message, exception, createListener(onSuccess, onPrimaryDemoted, onIgnoredFailure));
        }

        @Override
        public void markShardCopyAsStale(ShardId shardId, String allocationId, long primaryTerm, Runnable onSuccess, Consumer<Exception> onPrimaryDemoted, Consumer<Exception> onIgnoredFailure) {
            shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, "mark copy as stale", null, createListener(onSuccess, onPrimaryDemoted, onIgnoredFailure));
        }

        private ShardStateAction.Listener createListener(final Runnable onSuccess, final Consumer<Exception> onPrimaryDemoted, final Consumer<Exception> onIgnoredFailure) {
            return new ShardStateAction.Listener() {

                @Override
                public void onSuccess() {
                    onSuccess.run();
                }

                @Override
                public void onFailure(Exception shardFailedError) {
                    if (shardFailedError instanceof ShardStateAction.NoLongerPrimaryShardException) {
                        onPrimaryDemoted.accept(shardFailedError);
                    } else {
                        assert shardFailedError instanceof TransportException || shardFailedError instanceof NodeClosedException : shardFailedError;
                        onIgnoredFailure.accept(shardFailedError);
                    }
                }
            };
        }
    }

    public static final class ConcreteShardRequest<R extends TransportRequest> extends TransportRequest {

        private String targetAllocationID;

        private R request;

        ConcreteShardRequest(Supplier<R> requestSupplier) {
            request = requestSupplier.get();
            targetAllocationID = null;
        }

        ConcreteShardRequest(R request, String targetAllocationID) {
            Objects.requireNonNull(request);
            Objects.requireNonNull(targetAllocationID);
            this.request = request;
            this.targetAllocationID = targetAllocationID;
        }

        @Override
        public void setParentTask(String parentTaskNode, long parentTaskId) {
            request.setParentTask(parentTaskNode, parentTaskId);
        }

        @Override
        public void setParentTask(TaskId taskId) {
            request.setParentTask(taskId);
        }

        @Override
        public TaskId getParentTask() {
            return request.getParentTask();
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return request.createTask(id, type, action, parentTaskId);
        }

        @Override
        public String getDescription() {
            return "[" + request.getDescription() + "] for aID [" + targetAllocationID + "]";
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            targetAllocationID = in.readString();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(targetAllocationID);
            request.writeTo(out);
        }

        public R getRequest() {
            return request;
        }

        public String getTargetAllocationID() {
            return targetAllocationID;
        }
    }

    static void setPhase(ReplicationTask task, String phase) {
        if (task != null) {
            task.setPhase(phase);
        }
    }
}