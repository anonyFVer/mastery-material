package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public class TransportBulkAction extends HandledTransportAction<BulkRequest, BulkResponse> {

    private final AutoCreateIndex autoCreateIndex;

    private final boolean allowIdGeneration;

    private final ClusterService clusterService;

    private final TransportShardBulkAction shardBulkAction;

    private final TransportCreateIndexAction createIndexAction;

    private final LongSupplier relativeTimeProvider;

    @Inject
    public TransportBulkAction(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterService clusterService, TransportShardBulkAction shardBulkAction, TransportCreateIndexAction createIndexAction, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, AutoCreateIndex autoCreateIndex) {
        this(settings, threadPool, transportService, clusterService, shardBulkAction, createIndexAction, actionFilters, indexNameExpressionResolver, autoCreateIndex, System::nanoTime);
    }

    public TransportBulkAction(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterService clusterService, TransportShardBulkAction shardBulkAction, TransportCreateIndexAction createIndexAction, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, AutoCreateIndex autoCreateIndex, LongSupplier relativeTimeProvider) {
        super(settings, BulkAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, BulkRequest::new);
        Objects.requireNonNull(relativeTimeProvider);
        this.clusterService = clusterService;
        this.shardBulkAction = shardBulkAction;
        this.createIndexAction = createIndexAction;
        this.autoCreateIndex = autoCreateIndex;
        this.allowIdGeneration = this.settings.getAsBoolean("action.bulk.action.allow_id_generation", true);
        this.relativeTimeProvider = relativeTimeProvider;
    }

    @Override
    protected final void doExecute(final BulkRequest bulkRequest, final ActionListener<BulkResponse> listener) {
        throw new UnsupportedOperationException("task parameter is required for this operation");
    }

    @Override
    protected void doExecute(Task task, BulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
        final long startTime = relativeTime();
        final AtomicArray<BulkItemResponse> responses = new AtomicArray<>(bulkRequest.requests.size());
        if (needToCheck()) {
            final Set<String> autoCreateIndices = bulkRequest.requests.stream().map(DocWriteRequest::index).collect(Collectors.toSet());
            final AtomicInteger counter = new AtomicInteger(autoCreateIndices.size());
            ClusterState state = clusterService.state();
            for (String index : autoCreateIndices) {
                if (shouldAutoCreate(index, state)) {
                    CreateIndexRequest createIndexRequest = new CreateIndexRequest();
                    createIndexRequest.index(index);
                    createIndexRequest.cause("auto(bulk api)");
                    createIndexRequest.masterNodeTimeout(bulkRequest.timeout());
                    createIndexAction.execute(createIndexRequest, new ActionListener<CreateIndexResponse>() {

                        @Override
                        public void onResponse(CreateIndexResponse result) {
                            if (counter.decrementAndGet() == 0) {
                                new BulkOperation(task, bulkRequest, listener, responses, startTime).run();
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (!(ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException)) {
                                for (int i = 0; i < bulkRequest.requests.size(); i++) {
                                    DocWriteRequest request = bulkRequest.requests.get(i);
                                    if (request != null && setResponseFailureIfIndexMatches(responses, i, request, index, e)) {
                                        bulkRequest.requests.set(i, null);
                                    }
                                }
                            }
                            if (counter.decrementAndGet() == 0) {
                                new BulkOperation(task, bulkRequest, ActionListener.wrap(listener::onResponse, inner -> {
                                    inner.addSuppressed(e);
                                    listener.onFailure(inner);
                                }), responses, startTime).run();
                            }
                        }
                    });
                } else {
                    if (counter.decrementAndGet() == 0) {
                        new BulkOperation(task, bulkRequest, listener, responses, startTime).run();
                    }
                }
            }
        } else {
            new BulkOperation(task, bulkRequest, listener, responses, startTime).run();
        }
    }

    boolean needToCheck() {
        return autoCreateIndex.needToCheck();
    }

    boolean shouldAutoCreate(String index, ClusterState state) {
        return autoCreateIndex.shouldAutoCreate(index, state);
    }

    private boolean setResponseFailureIfIndexMatches(AtomicArray<BulkItemResponse> responses, int idx, DocWriteRequest request, String index, Exception e) {
        if (index.equals(request.index())) {
            responses.set(idx, new BulkItemResponse(idx, request.opType(), new BulkItemResponse.Failure(request.index(), request.type(), request.id(), e)));
            return true;
        }
        return false;
    }

    private long buildTookInMillis(long startTimeNanos) {
        return TimeUnit.NANOSECONDS.toMillis(relativeTime() - startTimeNanos);
    }

    private final class BulkOperation extends AbstractRunnable {

        private final Task task;

        private final BulkRequest bulkRequest;

        private final ActionListener<BulkResponse> listener;

        private final AtomicArray<BulkItemResponse> responses;

        private final long startTimeNanos;

        private final ClusterStateObserver observer;

        BulkOperation(Task task, BulkRequest bulkRequest, ActionListener<BulkResponse> listener, AtomicArray<BulkItemResponse> responses, long startTimeNanos) {
            this.task = task;
            this.bulkRequest = bulkRequest;
            this.listener = listener;
            this.responses = responses;
            this.startTimeNanos = startTimeNanos;
            this.observer = new ClusterStateObserver(clusterService, bulkRequest.timeout(), logger, threadPool.getThreadContext());
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        protected void doRun() throws Exception {
            final ClusterState clusterState = observer.observedState().getClusterState();
            if (handleBlockExceptions(clusterState)) {
                return;
            }
            final ConcreteIndices concreteIndices = new ConcreteIndices(clusterState, indexNameExpressionResolver);
            MetaData metaData = clusterState.metaData();
            for (int i = 0; i < bulkRequest.requests.size(); i++) {
                DocWriteRequest docWriteRequest = bulkRequest.requests.get(i);
                if (docWriteRequest == null) {
                    continue;
                }
                if (addFailureIfIndexIsUnavailable(docWriteRequest, bulkRequest, responses, i, concreteIndices, metaData)) {
                    continue;
                }
                Index concreteIndex = concreteIndices.resolveIfAbsent(docWriteRequest);
                try {
                    switch(docWriteRequest.opType()) {
                        case CREATE:
                        case INDEX:
                            IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                            MappingMetaData mappingMd = null;
                            final IndexMetaData indexMetaData = metaData.index(concreteIndex);
                            if (indexMetaData != null) {
                                mappingMd = indexMetaData.mappingOrDefault(indexRequest.type());
                            }
                            indexRequest.resolveRouting(metaData);
                            indexRequest.process(mappingMd, allowIdGeneration, concreteIndex.getName());
                            break;
                        case UPDATE:
                            TransportUpdateAction.resolveAndValidateRouting(metaData, concreteIndex.getName(), (UpdateRequest) docWriteRequest);
                            break;
                        case DELETE:
                            docWriteRequest.routing(metaData.resolveIndexRouting(docWriteRequest.parent(), docWriteRequest.routing(), docWriteRequest.index()));
                            if (docWriteRequest.routing() == null && metaData.routingRequired(concreteIndex.getName(), docWriteRequest.type())) {
                                throw new RoutingMissingException(concreteIndex.getName(), docWriteRequest.type(), docWriteRequest.id());
                            }
                            break;
                        default:
                            throw new AssertionError("request type not supported: [" + docWriteRequest.opType() + "]");
                    }
                } catch (ElasticsearchParseException | RoutingMissingException e) {
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(concreteIndex.getName(), docWriteRequest.type(), docWriteRequest.id(), e);
                    BulkItemResponse bulkItemResponse = new BulkItemResponse(i, docWriteRequest.opType(), failure);
                    responses.set(i, bulkItemResponse);
                    bulkRequest.requests.set(i, null);
                }
            }
            Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();
            for (int i = 0; i < bulkRequest.requests.size(); i++) {
                DocWriteRequest request = bulkRequest.requests.get(i);
                if (request == null) {
                    continue;
                }
                String concreteIndex = concreteIndices.getConcreteIndex(request.index()).getName();
                ShardId shardId = clusterService.operationRouting().indexShards(clusterState, concreteIndex, request.id(), request.routing()).shardId();
                List<BulkItemRequest> shardRequests = requestsByShard.computeIfAbsent(shardId, shard -> new ArrayList<>());
                shardRequests.add(new BulkItemRequest(i, request));
            }
            if (requestsByShard.isEmpty()) {
                listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTimeNanos)));
                return;
            }
            final AtomicInteger counter = new AtomicInteger(requestsByShard.size());
            String nodeId = clusterService.localNode().getId();
            for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
                final ShardId shardId = entry.getKey();
                final List<BulkItemRequest> requests = entry.getValue();
                BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, bulkRequest.getRefreshPolicy(), requests.toArray(new BulkItemRequest[requests.size()]));
                bulkShardRequest.waitForActiveShards(bulkRequest.waitForActiveShards());
                bulkShardRequest.timeout(bulkRequest.timeout());
                if (task != null) {
                    bulkShardRequest.setParentTask(nodeId, task.getId());
                }
                shardBulkAction.execute(bulkShardRequest, new ActionListener<BulkShardResponse>() {

                    @Override
                    public void onResponse(BulkShardResponse bulkShardResponse) {
                        for (BulkItemResponse bulkItemResponse : bulkShardResponse.getResponses()) {
                            if (bulkItemResponse.getResponse() != null) {
                                bulkItemResponse.getResponse().setShardInfo(bulkShardResponse.getShardInfo());
                            }
                            responses.set(bulkItemResponse.getItemId(), bulkItemResponse);
                        }
                        if (counter.decrementAndGet() == 0) {
                            finishHim();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        for (BulkItemRequest request : requests) {
                            final String indexName = concreteIndices.getConcreteIndex(request.index()).getName();
                            DocWriteRequest docWriteRequest = request.request();
                            responses.set(request.id(), new BulkItemResponse(request.id(), docWriteRequest.opType(), new BulkItemResponse.Failure(indexName, docWriteRequest.type(), docWriteRequest.id(), e)));
                        }
                        if (counter.decrementAndGet() == 0) {
                            finishHim();
                        }
                    }

                    private void finishHim() {
                        listener.onResponse(new BulkResponse(responses.toArray(new BulkItemResponse[responses.length()]), buildTookInMillis(startTimeNanos)));
                    }
                });
            }
        }

        private boolean handleBlockExceptions(ClusterState state) {
            ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
            if (blockException != null) {
                if (blockException.retryable()) {
                    logger.trace("cluster is blocked, scheduling a retry", blockException);
                    retry(blockException);
                } else {
                    onFailure(blockException);
                }
                return true;
            }
            return false;
        }

        void retry(Exception failure) {
            assert failure != null;
            if (observer.isTimedOut()) {
                onFailure(failure);
                return;
            }
            final ThreadContext.StoredContext context = threadPool.getThreadContext().newStoredContext();
            observer.waitForNextChange(new ClusterStateObserver.Listener() {

                @Override
                public void onNewClusterState(ClusterState state) {
                    context.close();
                    run();
                }

                @Override
                public void onClusterServiceClose() {
                    onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    context.close();
                    run();
                }
            });
        }
    }

    void executeBulk(Task task, final BulkRequest bulkRequest, final long startTimeNanos, final ActionListener<BulkResponse> listener, final AtomicArray<BulkItemResponse> responses) {
        new BulkOperation(task, bulkRequest, listener, responses, startTimeNanos).run();
    }

    private boolean addFailureIfIndexIsUnavailable(DocWriteRequest request, BulkRequest bulkRequest, AtomicArray<BulkItemResponse> responses, int idx, final ConcreteIndices concreteIndices, final MetaData metaData) {
        Index concreteIndex = concreteIndices.getConcreteIndex(request.index());
        Exception unavailableException = null;
        if (concreteIndex == null) {
            try {
                concreteIndex = concreteIndices.resolveIfAbsent(request);
            } catch (IndexClosedException | IndexNotFoundException ex) {
                unavailableException = ex;
            }
        }
        if (unavailableException == null) {
            IndexMetaData indexMetaData = metaData.getIndexSafe(concreteIndex);
            if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                unavailableException = new IndexClosedException(concreteIndex);
            }
        }
        if (unavailableException != null) {
            BulkItemResponse.Failure failure = new BulkItemResponse.Failure(request.index(), request.type(), request.id(), unavailableException);
            BulkItemResponse bulkItemResponse = new BulkItemResponse(idx, request.opType(), failure);
            responses.set(idx, bulkItemResponse);
            bulkRequest.requests.set(idx, null);
            return true;
        }
        return false;
    }

    private static class ConcreteIndices {

        private final ClusterState state;

        private final IndexNameExpressionResolver indexNameExpressionResolver;

        private final Map<String, Index> indices = new HashMap<>();

        ConcreteIndices(ClusterState state, IndexNameExpressionResolver indexNameExpressionResolver) {
            this.state = state;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
        }

        Index getConcreteIndex(String indexOrAlias) {
            return indices.get(indexOrAlias);
        }

        Index resolveIfAbsent(DocWriteRequest request) {
            Index concreteIndex = indices.get(request.index());
            if (concreteIndex == null) {
                concreteIndex = indexNameExpressionResolver.concreteSingleIndex(state, request);
                indices.put(request.index(), concreteIndex);
            }
            return concreteIndex;
        }
    }

    private long relativeTime() {
        return relativeTimeProvider.getAsLong();
    }
}