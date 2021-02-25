package org.elasticsearch.action.support.replication;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public abstract class TransportWriteAction<Request extends ReplicatedWriteRequest<Request>, ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>, Response extends ReplicationResponse & WriteResponse> extends TransportReplicationAction<Request, ReplicaRequest, Response> {

    protected TransportWriteAction(Settings settings, String actionName, TransportService transportService, ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request, Supplier<ReplicaRequest> replicaRequest, String executor) {
        super(settings, actionName, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters, indexNameExpressionResolver, request, replicaRequest, executor);
    }

    @Override
    protected abstract WritePrimaryResult<ReplicaRequest, Response> shardOperationOnPrimary(Request request, IndexShard primary) throws Exception;

    @Override
    protected abstract WriteReplicaResult<ReplicaRequest> shardOperationOnReplica(ReplicaRequest request, IndexShard replica) throws Exception;

    protected static class WritePrimaryResult<ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>, Response extends ReplicationResponse & WriteResponse> extends PrimaryResult<ReplicaRequest, Response> implements RespondingWriteResult {

        boolean finishedAsyncActions;

        public final Location location;

        ActionListener<Response> listener = null;

        public WritePrimaryResult(ReplicaRequest request, @Nullable Response finalResponse, @Nullable Location location, @Nullable Exception operationFailure, IndexShard primary, Logger logger) {
            super(request, finalResponse, operationFailure);
            this.location = location;
            assert location == null || operationFailure == null : "expected either failure to be null or translog location to be null, " + "but found: [" + location + "] translog location and [" + operationFailure + "] failure";
            if (operationFailure != null) {
                this.finishedAsyncActions = true;
            } else {
                new AsyncAfterWriteAction(primary, request, location, this, logger).run();
            }
        }

        @Override
        public synchronized void respond(ActionListener<Response> listener) {
            this.listener = listener;
            respondIfPossible(null);
        }

        protected void respondIfPossible(Exception ex) {
            if (finishedAsyncActions && listener != null) {
                if (ex == null) {
                    super.respond(listener);
                } else {
                    listener.onFailure(ex);
                }
            }
        }

        public synchronized void onFailure(Exception exception) {
            finishedAsyncActions = true;
            respondIfPossible(exception);
        }

        @Override
        public synchronized void onSuccess(boolean forcedRefresh) {
            finalResponseIfSuccessful.setForcedRefresh(forcedRefresh);
            finishedAsyncActions = true;
            respondIfPossible(null);
        }
    }

    protected static class WriteReplicaResult<ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>> extends ReplicaResult implements RespondingWriteResult {

        public final Location location;

        boolean finishedAsyncActions;

        private ActionListener<TransportResponse.Empty> listener;

        public WriteReplicaResult(ReplicaRequest request, @Nullable Location location, @Nullable Exception operationFailure, IndexShard replica, Logger logger) {
            super(operationFailure);
            this.location = location;
            if (operationFailure != null) {
                this.finishedAsyncActions = true;
            } else {
                new AsyncAfterWriteAction(replica, request, location, this, logger).run();
            }
        }

        @Override
        public void respond(ActionListener<TransportResponse.Empty> listener) {
            this.listener = listener;
            respondIfPossible(null);
        }

        protected void respondIfPossible(Exception ex) {
            if (finishedAsyncActions && listener != null) {
                if (ex == null) {
                    super.respond(listener);
                } else {
                    listener.onFailure(ex);
                }
            }
        }

        @Override
        public void onFailure(Exception ex) {
            finishedAsyncActions = true;
            respondIfPossible(ex);
        }

        @Override
        public synchronized void onSuccess(boolean forcedRefresh) {
            finishedAsyncActions = true;
            respondIfPossible(null);
        }
    }

    @Override
    protected ClusterBlockLevel globalBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    @Override
    protected ClusterBlockLevel indexBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    interface RespondingWriteResult {

        void onSuccess(boolean forcedRefresh);

        void onFailure(Exception ex);
    }

    static final class AsyncAfterWriteAction {

        private final Location location;

        private final boolean waitUntilRefresh;

        private final boolean sync;

        private final AtomicInteger pendingOps = new AtomicInteger(1);

        private final AtomicBoolean refreshed = new AtomicBoolean(false);

        private final AtomicReference<Exception> syncFailure = new AtomicReference<>(null);

        private final RespondingWriteResult respond;

        private final IndexShard indexShard;

        private final WriteRequest<?> request;

        private final Logger logger;

        AsyncAfterWriteAction(final IndexShard indexShard, final WriteRequest<?> request, @Nullable final Translog.Location location, final RespondingWriteResult respond, final Logger logger) {
            this.indexShard = indexShard;
            this.request = request;
            boolean waitUntilRefresh = false;
            switch(request.getRefreshPolicy()) {
                case IMMEDIATE:
                    indexShard.refresh("refresh_flag_index");
                    refreshed.set(true);
                    break;
                case WAIT_UNTIL:
                    if (location != null) {
                        waitUntilRefresh = true;
                        pendingOps.incrementAndGet();
                    }
                    break;
                case NONE:
                    break;
                default:
                    throw new IllegalArgumentException("unknown refresh policy: " + request.getRefreshPolicy());
            }
            this.waitUntilRefresh = waitUntilRefresh;
            this.respond = respond;
            this.location = location;
            if ((sync = indexShard.getTranslogDurability() == Translog.Durability.REQUEST && location != null)) {
                pendingOps.incrementAndGet();
            }
            this.logger = logger;
            assert pendingOps.get() >= 0 && pendingOps.get() <= 3 : "pendingOpts was: " + pendingOps.get();
        }

        private void maybeFinish() {
            final int numPending = pendingOps.decrementAndGet();
            if (numPending == 0) {
                if (syncFailure.get() != null) {
                    respond.onFailure(syncFailure.get());
                } else {
                    respond.onSuccess(refreshed.get());
                }
            }
            assert numPending >= 0 && numPending <= 2 : "numPending must either 2, 1 or 0 but was " + numPending;
        }

        void run() {
            indexShard.maybeFlush();
            maybeFinish();
            if (waitUntilRefresh) {
                assert pendingOps.get() > 0;
                indexShard.addRefreshListener(location, forcedRefresh -> {
                    if (forcedRefresh) {
                        logger.warn("block_until_refresh request ran out of slots and forced a refresh: [{}]", request);
                    }
                    refreshed.set(forcedRefresh);
                    maybeFinish();
                });
            }
            if (sync) {
                assert pendingOps.get() > 0;
                indexShard.sync(location, (ex) -> {
                    syncFailure.set(ex);
                    maybeFinish();
                });
            }
        }
    }
}