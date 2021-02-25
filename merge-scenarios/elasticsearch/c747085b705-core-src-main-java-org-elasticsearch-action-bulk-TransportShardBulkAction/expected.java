package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import java.util.Map;
import static org.elasticsearch.action.support.replication.ReplicationOperation.ignoreReplicaException;
import static org.elasticsearch.action.support.replication.ReplicationOperation.isConflictException;

public class TransportShardBulkAction extends TransportWriteAction<BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = BulkAction.NAME + "[s]";

    private final UpdateHelper updateHelper;

    private final boolean allowIdGeneration;

    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction, MappingUpdatedAction mappingUpdatedAction, UpdateHelper updateHelper, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters, indexNameExpressionResolver, BulkShardRequest::new, ThreadPool.Names.BULK);
        this.updateHelper = updateHelper;
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);
        this.mappingUpdatedAction = mappingUpdatedAction;
    }

    @Override
    protected TransportRequestOptions transportOptions() {
        return BulkAction.INSTANCE.transportOptions(settings);
    }

    @Override
    protected BulkShardResponse newResponseInstance() {
        return new BulkShardResponse();
    }

    @Override
    protected boolean resolveIndex() {
        return false;
    }

    @Override
    protected WriteResult<BulkShardResponse> onPrimaryShard(BulkShardRequest request, IndexShard primary) throws Exception {
        final IndexMetaData metaData = primary.indexSettings().getIndexMetaData();
        long[] preVersions = new long[request.items().length];
        VersionType[] preVersionTypes = new VersionType[request.items().length];
        Translog.Location location = null;
        for (int requestIndex = 0; requestIndex < request.items().length; requestIndex++) {
            location = executeBulkItemRequest(metaData, primary, request, preVersions, preVersionTypes, location, requestIndex);
        }
        BulkItemResponse[] responses = new BulkItemResponse[request.items().length];
        BulkItemRequest[] items = request.items();
        for (int i = 0; i < items.length; i++) {
            responses[i] = items[i].getPrimaryResponse();
        }
        BulkShardResponse response = new BulkShardResponse(request.shardId(), responses);
        return new WriteResult<>(response, location);
    }

    private Translog.Location executeBulkItemRequest(IndexMetaData metaData, IndexShard indexShard, BulkShardRequest request, long[] preVersions, VersionType[] preVersionTypes, Translog.Location location, int requestIndex) {
        preVersions[requestIndex] = request.items()[requestIndex].request().version();
        preVersionTypes[requestIndex] = request.items()[requestIndex].request().versionType();
        DocumentRequest.OpType opType = request.items()[requestIndex].request().opType();
        try {
            WriteResult<? extends DocWriteResponse> writeResult = innerExecuteBulkItemRequest(metaData, indexShard, request, requestIndex);
            if (writeResult.getLocation() != null) {
                location = locationToSync(location, writeResult.getLocation());
            } else {
                assert writeResult.getResponse().getResult() == DocWriteResponse.Result.NOOP : "only noop operation can have null next operation";
            }
            BulkItemRequest item = request.items()[requestIndex];
            setResponse(item, new BulkItemResponse(item.id(), opType, writeResult.getResponse()));
        } catch (Exception e) {
            if (retryPrimaryException(e)) {
                for (int j = 0; j < requestIndex; j++) {
                    DocumentRequest<?> documentRequest = request.items()[j].request();
                    documentRequest.version(preVersions[j]);
                    documentRequest.versionType(preVersionTypes[j]);
                }
                throw (ElasticsearchException) e;
            }
            BulkItemRequest item = request.items()[requestIndex];
            DocumentRequest<?> documentRequest = item.request();
            if (isConflictException(e)) {
                logger.trace((Supplier<?>) () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}", request.shardId(), documentRequest.opType().getLowercase(), request), e);
            } else {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}", request.shardId(), documentRequest.opType().getLowercase(), request), e);
            }
            if (item.getPrimaryResponse() != null && isConflictException(e)) {
                setResponse(item, item.getPrimaryResponse());
            } else {
                setResponse(item, new BulkItemResponse(item.id(), documentRequest.opType(), new BulkItemResponse.Failure(request.index(), documentRequest.type(), documentRequest.id(), e)));
            }
        }
        assert request.items()[requestIndex].getPrimaryResponse() != null;
        assert preVersionTypes[requestIndex] != null;
        return location;
    }

    private WriteResult<? extends DocWriteResponse> innerExecuteBulkItemRequest(IndexMetaData metaData, IndexShard indexShard, BulkShardRequest request, int requestIndex) throws Exception {
        DocumentRequest<?> itemRequest = request.items()[requestIndex].request();
        switch(itemRequest.opType()) {
            case CREATE:
            case INDEX:
                return TransportIndexAction.executeIndexRequestOnPrimary(((IndexRequest) itemRequest), indexShard, mappingUpdatedAction);
            case UPDATE:
                int maxAttempts = ((UpdateRequest) itemRequest).retryOnConflict();
                for (int attemptCount = 0; attemptCount <= maxAttempts; attemptCount++) {
                    try {
                        return shardUpdateOperation(metaData, indexShard, request, requestIndex, ((UpdateRequest) itemRequest));
                    } catch (Exception e) {
                        final Throwable cause = ExceptionsHelper.unwrapCause(e);
                        if (attemptCount == maxAttempts || (cause instanceof VersionConflictEngineException) == false) {
                            throw e;
                        }
                    }
                }
                throw new IllegalStateException("version conflict exception should bubble up on last attempt");
            case DELETE:
                return TransportDeleteAction.executeDeleteRequestOnPrimary(((DeleteRequest) itemRequest), indexShard);
            default:
                throw new IllegalStateException("unexpected opType [" + itemRequest.opType() + "] found");
        }
    }

    private void setResponse(BulkItemRequest request, BulkItemResponse response) {
        request.setPrimaryResponse(response);
        if (response.isFailed()) {
            request.setIgnoreOnReplica();
        } else {
            response.getResponse().setShardInfo(new ShardInfo());
        }
    }

    private WriteResult<? extends DocWriteResponse> shardUpdateOperation(IndexMetaData metaData, IndexShard indexShard, BulkShardRequest request, int requestIndex, UpdateRequest updateRequest) throws Exception {
        UpdateHelper.Result translate = updateHelper.prepare(updateRequest, indexShard);
        switch(translate.getResponseResult()) {
            case CREATED:
            case UPDATED:
                IndexRequest indexRequest = translate.action();
                MappingMetaData mappingMd = metaData.mappingOrDefault(indexRequest.type());
                indexRequest.process(mappingMd, allowIdGeneration, request.index());
                WriteResult<IndexResponse> writeResult = TransportIndexAction.executeIndexRequestOnPrimary(indexRequest, indexShard, mappingUpdatedAction);
                BytesReference indexSourceAsBytes = indexRequest.source();
                IndexResponse indexResponse = writeResult.getResponse();
                UpdateResponse writeUpdateResponse = new UpdateResponse(indexResponse.getShardInfo(), indexResponse.getShardId(), indexResponse.getType(), indexResponse.getId(), indexResponse.getVersion(), indexResponse.getResult());
                if (updateRequest.fields() != null && updateRequest.fields().length > 0) {
                    Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(indexSourceAsBytes, true);
                    writeUpdateResponse.setGetResult(updateHelper.extractGetResult(updateRequest, request.index(), indexResponse.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceAsBytes));
                }
                request.items()[requestIndex] = new BulkItemRequest(request.items()[requestIndex].id(), indexRequest);
                return new WriteResult<>(writeUpdateResponse, writeResult.getLocation());
            case DELETED:
                DeleteRequest deleteRequest = translate.action();
                WriteResult<DeleteResponse> deleteResult = TransportDeleteAction.executeDeleteRequestOnPrimary(deleteRequest, indexShard);
                DeleteResponse response = deleteResult.getResponse();
                UpdateResponse deleteUpdateResponse = new UpdateResponse(response.getShardInfo(), response.getShardId(), response.getType(), response.getId(), response.getVersion(), response.getResult());
                deleteUpdateResponse.setGetResult(updateHelper.extractGetResult(updateRequest, request.index(), response.getVersion(), translate.updatedSourceAsMap(), translate.updateSourceContentType(), null));
                request.items()[requestIndex] = new BulkItemRequest(request.items()[requestIndex].id(), deleteRequest);
                return new WriteResult<>(deleteUpdateResponse, deleteResult.getLocation());
            case NOOP:
                BulkItemRequest item = request.items()[requestIndex];
                indexShard.noopUpdate(updateRequest.type());
                item.setIgnoreOnReplica();
                return new WriteResult<>(translate.action(), null);
            default:
                throw new IllegalStateException("Illegal update operation " + translate.getResponseResult());
        }
    }

    @Override
    protected Location onReplicaShard(BulkShardRequest request, IndexShard indexShard) {
        Translog.Location location = null;
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            if (item == null || item.isIgnoreOnReplica()) {
                continue;
            }
            DocumentRequest<?> documentRequest = item.request();
            final Engine.Operation operation;
            try {
                switch(documentRequest.opType()) {
                    case CREATE:
                    case INDEX:
                        operation = TransportIndexAction.executeIndexRequestOnReplica(((IndexRequest) documentRequest), indexShard);
                        break;
                    case DELETE:
                        operation = TransportDeleteAction.executeDeleteRequestOnReplica(((DeleteRequest) documentRequest), indexShard);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected request operation type on replica: " + documentRequest.opType().getLowercase());
                }
                location = locationToSync(location, operation.getTranslogLocation());
            } catch (Exception e) {
                if (!ignoreReplicaException(e)) {
                    throw e;
                }
            }
        }
        return location;
    }

    private Translog.Location locationToSync(Translog.Location current, Translog.Location next) {
        assert next != null : "next operation can't be null";
        assert current == null || current.compareTo(next) < 0 : "translog locations are not increasing";
        return next;
    }
}