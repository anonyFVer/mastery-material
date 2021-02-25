package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.action.support.replication.TransportWriteAction;
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
import org.elasticsearch.index.engine.EngineClosedException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import java.util.Map;
import static org.elasticsearch.action.support.replication.ReplicationOperation.ignoreReplicaException;
import static org.elasticsearch.action.support.replication.ReplicationOperation.isConflictException;

public class TransportShardBulkAction extends TransportWriteAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = BulkAction.NAME + "[s]";

    private final UpdateHelper updateHelper;

    private final boolean allowIdGeneration;

    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction, MappingUpdatedAction mappingUpdatedAction, UpdateHelper updateHelper, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters, indexNameExpressionResolver, BulkShardRequest::new, BulkShardRequest::new, ThreadPool.Names.BULK);
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
    protected WritePrimaryResult<BulkShardRequest, BulkShardResponse> shardOperationOnPrimary(BulkShardRequest request, IndexShard primary) throws Exception {
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
        return new WritePrimaryResult<>(request, response, location, null, primary, logger);
    }

    private Translog.Location executeBulkItemRequest(IndexMetaData metaData, IndexShard primary, BulkShardRequest request, long[] preVersions, VersionType[] preVersionTypes, Translog.Location location, int requestIndex) throws Exception {
        final DocWriteRequest itemRequest = request.items()[requestIndex].request();
        preVersions[requestIndex] = itemRequest.version();
        preVersionTypes[requestIndex] = itemRequest.versionType();
        DocWriteRequest.OpType opType = itemRequest.opType();
        try {
            final Engine.Result operationResult;
            final DocWriteResponse response;
            final BulkItemRequest replicaRequest;
            switch(itemRequest.opType()) {
                case CREATE:
                case INDEX:
                    final IndexRequest indexRequest = (IndexRequest) itemRequest;
                    Engine.IndexResult indexResult = executeIndexRequestOnPrimary(indexRequest, primary, mappingUpdatedAction);
                    if (indexResult.hasFailure()) {
                        response = null;
                    } else {
                        final long version = indexResult.getVersion();
                        indexRequest.version(version);
                        indexRequest.versionType(indexRequest.versionType().versionTypeForReplicationAndRecovery());
                        indexRequest.seqNo(indexResult.getSeqNo());
                        assert indexRequest.versionType().validateVersionForWrites(indexRequest.version());
                        response = new IndexResponse(primary.shardId(), indexRequest.type(), indexRequest.id(), indexResult.getSeqNo(), indexResult.getVersion(), indexResult.isCreated());
                    }
                    operationResult = indexResult;
                    replicaRequest = request.items()[requestIndex];
                    break;
                case UPDATE:
                    UpdateResultHolder updateResultHolder = executeUpdateRequest(((UpdateRequest) itemRequest), primary, metaData, request, requestIndex);
                    operationResult = updateResultHolder.operationResult;
                    response = updateResultHolder.response;
                    replicaRequest = updateResultHolder.replicaRequest;
                    break;
                case DELETE:
                    final DeleteRequest deleteRequest = (DeleteRequest) itemRequest;
                    Engine.DeleteResult deleteResult = executeDeleteRequestOnPrimary(deleteRequest, primary);
                    if (deleteResult.hasFailure()) {
                        response = null;
                    } else {
                        deleteRequest.versionType(deleteRequest.versionType().versionTypeForReplicationAndRecovery());
                        deleteRequest.version(deleteResult.getVersion());
                        deleteRequest.seqNo(deleteResult.getSeqNo());
                        assert deleteRequest.versionType().validateVersionForWrites(deleteRequest.version());
                        response = new DeleteResponse(request.shardId(), deleteRequest.type(), deleteRequest.id(), deleteResult.getSeqNo(), deleteResult.getVersion(), deleteResult.isFound());
                    }
                    operationResult = deleteResult;
                    replicaRequest = request.items()[requestIndex];
                    break;
                default:
                    throw new IllegalStateException("unexpected opType [" + itemRequest.opType() + "] found");
            }
            request.items()[requestIndex] = replicaRequest;
            if (operationResult == null) {
                assert response.getResult() == DocWriteResponse.Result.NOOP : "only noop update can have null operation";
                replicaRequest.setIgnoreOnReplica();
                replicaRequest.setPrimaryResponse(new BulkItemResponse(replicaRequest.id(), opType, response));
            } else if (operationResult.hasFailure() == false) {
                location = locationToSync(location, operationResult.getTranslogLocation());
                BulkItemResponse primaryResponse = new BulkItemResponse(replicaRequest.id(), opType, response);
                replicaRequest.setPrimaryResponse(primaryResponse);
                primaryResponse.getResponse().setShardInfo(new ShardInfo());
            } else {
                DocWriteRequest docWriteRequest = replicaRequest.request();
                Exception failure = operationResult.getFailure();
                if (isConflictException(failure)) {
                    logger.trace((Supplier<?>) () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}", request.shardId(), docWriteRequest.opType().getLowercase(), request), failure);
                } else {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}", request.shardId(), docWriteRequest.opType().getLowercase(), request), failure);
                }
                if (replicaRequest.getPrimaryResponse() == null || isConflictException(failure) == false) {
                    replicaRequest.setIgnoreOnReplica();
                    replicaRequest.setPrimaryResponse(new BulkItemResponse(replicaRequest.id(), docWriteRequest.opType(), new BulkItemResponse.Failure(request.index(), docWriteRequest.type(), docWriteRequest.id(), failure)));
                }
            }
            assert replicaRequest.getPrimaryResponse() != null;
            assert preVersionTypes[requestIndex] != null;
        } catch (Exception e) {
            if (retryPrimaryException(e)) {
                for (int j = 0; j < requestIndex; j++) {
                    DocWriteRequest docWriteRequest = request.items()[j].request();
                    docWriteRequest.version(preVersions[j]);
                    docWriteRequest.versionType(preVersionTypes[j]);
                }
            }
            throw e;
        }
        return location;
    }

    private static class UpdateResultHolder {

        final BulkItemRequest replicaRequest;

        final Engine.Result operationResult;

        final DocWriteResponse response;

        private UpdateResultHolder(BulkItemRequest replicaRequest, Engine.Result operationResult, DocWriteResponse response) {
            this.replicaRequest = replicaRequest;
            this.operationResult = operationResult;
            this.response = response;
        }
    }

    private UpdateResultHolder executeUpdateRequest(UpdateRequest updateRequest, IndexShard primary, IndexMetaData metaData, BulkShardRequest request, int requestIndex) throws Exception {
        Engine.Result updateOperationResult = null;
        UpdateResponse updateResponse = null;
        BulkItemRequest replicaRequest = request.items()[requestIndex];
        int maxAttempts = updateRequest.retryOnConflict();
        for (int attemptCount = 0; attemptCount <= maxAttempts; attemptCount++) {
            final UpdateHelper.Result translate;
            try {
                translate = updateHelper.prepare(updateRequest, primary, threadPool::estimatedTimeInMillis);
            } catch (Exception failure) {
                updateOperationResult = new Engine.IndexResult(failure, updateRequest.version(), SequenceNumbersService.UNASSIGNED_SEQ_NO);
                break;
            }
            switch(translate.getResponseResult()) {
                case CREATED:
                case UPDATED:
                    IndexRequest indexRequest = translate.action();
                    MappingMetaData mappingMd = metaData.mappingOrDefault(indexRequest.type());
                    indexRequest.process(mappingMd, allowIdGeneration, request.index());
                    updateOperationResult = executeIndexRequestOnPrimary(indexRequest, primary, mappingUpdatedAction);
                    if (updateOperationResult.hasFailure() == false) {
                        final long version = updateOperationResult.getVersion();
                        indexRequest.version(version);
                        indexRequest.versionType(indexRequest.versionType().versionTypeForReplicationAndRecovery());
                        assert indexRequest.versionType().validateVersionForWrites(indexRequest.version());
                    }
                    break;
                case DELETED:
                    DeleteRequest deleteRequest = translate.action();
                    updateOperationResult = executeDeleteRequestOnPrimary(deleteRequest, primary);
                    if (updateOperationResult.hasFailure() == false) {
                        deleteRequest.versionType(deleteRequest.versionType().versionTypeForReplicationAndRecovery());
                        deleteRequest.version(updateOperationResult.getVersion());
                        assert deleteRequest.versionType().validateVersionForWrites(deleteRequest.version());
                    }
                    break;
                case NOOP:
                    primary.noopUpdate(updateRequest.type());
                    break;
                default:
                    throw new IllegalStateException("Illegal update operation " + translate.getResponseResult());
            }
            if (updateOperationResult == null) {
                updateResponse = translate.action();
                break;
            } else if (updateOperationResult.hasFailure() == false) {
                switch(updateOperationResult.getOperationType()) {
                    case INDEX:
                        IndexRequest updateIndexRequest = translate.action();
                        final IndexResponse indexResponse = new IndexResponse(primary.shardId(), updateIndexRequest.type(), updateIndexRequest.id(), updateOperationResult.getSeqNo(), updateOperationResult.getVersion(), ((Engine.IndexResult) updateOperationResult).isCreated());
                        BytesReference indexSourceAsBytes = updateIndexRequest.source();
                        updateResponse = new UpdateResponse(indexResponse.getShardInfo(), indexResponse.getShardId(), indexResponse.getType(), indexResponse.getId(), indexResponse.getSeqNo(), indexResponse.getVersion(), indexResponse.getResult());
                        if ((updateRequest.fetchSource() != null && updateRequest.fetchSource().fetchSource()) || (updateRequest.fields() != null && updateRequest.fields().length > 0)) {
                            Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(indexSourceAsBytes, true);
                            updateResponse.setGetResult(updateHelper.extractGetResult(updateRequest, request.index(), indexResponse.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceAsBytes));
                        }
                        replicaRequest = new BulkItemRequest(request.items()[requestIndex].id(), updateIndexRequest);
                        break;
                    case DELETE:
                        DeleteRequest updateDeleteRequest = translate.action();
                        DeleteResponse deleteResponse = new DeleteResponse(primary.shardId(), updateDeleteRequest.type(), updateDeleteRequest.id(), updateOperationResult.getSeqNo(), updateOperationResult.getVersion(), ((Engine.DeleteResult) updateOperationResult).isFound());
                        updateResponse = new UpdateResponse(deleteResponse.getShardInfo(), deleteResponse.getShardId(), deleteResponse.getType(), deleteResponse.getId(), deleteResponse.getSeqNo(), deleteResponse.getVersion(), deleteResponse.getResult());
                        updateResponse.setGetResult(updateHelper.extractGetResult(updateRequest, request.index(), deleteResponse.getVersion(), translate.updatedSourceAsMap(), translate.updateSourceContentType(), null));
                        replicaRequest = new BulkItemRequest(request.items()[requestIndex].id(), updateDeleteRequest);
                        break;
                }
                break;
            } else if (updateOperationResult.getFailure() instanceof VersionConflictEngineException == false) {
                break;
            }
        }
        return new UpdateResultHolder(replicaRequest, updateOperationResult, updateResponse);
    }

    @Override
    protected WriteReplicaResult<BulkShardRequest> shardOperationOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        Translog.Location location = null;
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            if (item.isIgnoreOnReplica() == false) {
                DocWriteRequest docWriteRequest = item.request();
                final Engine.Result operationResult;
                try {
                    switch(docWriteRequest.opType()) {
                        case CREATE:
                        case INDEX:
                            operationResult = executeIndexRequestOnReplica(((IndexRequest) docWriteRequest), replica);
                            break;
                        case DELETE:
                            operationResult = executeDeleteRequestOnReplica(((DeleteRequest) docWriteRequest), replica);
                            break;
                        default:
                            throw new IllegalStateException("Unexpected request operation type on replica: " + docWriteRequest.opType().getLowercase());
                    }
                    if (operationResult.hasFailure()) {
                        Exception failure = operationResult.getFailure();
                        assert failure instanceof VersionConflictEngineException || failure instanceof MapperParsingException || failure instanceof EngineClosedException || failure instanceof IndexShardClosedException : "expected any one of [version conflict, mapper parsing, engine closed, index shard closed]" + " failures. got " + failure;
                        if (!ignoreReplicaException(failure)) {
                            throw failure;
                        }
                    } else {
                        location = locationToSync(location, operationResult.getTranslogLocation());
                    }
                } catch (Exception e) {
                    if (!ignoreReplicaException(e)) {
                        throw e;
                    }
                }
            }
        }
        return new WriteReplicaResult<>(request, location, null, replica, logger);
    }

    private Translog.Location locationToSync(Translog.Location current, Translog.Location next) {
        assert next != null : "next operation can't be null";
        assert current == null || current.compareTo(next) < 0 : "translog locations are not increasing";
        return next;
    }

    public <Request extends ReplicatedWriteRequest<Request>, Response extends ReplicationResponse & WriteResponse> WritePrimaryResult<Request, Response> executeSingleItemBulkRequestOnPrimary(Request request, IndexShard primary) throws Exception {
        BulkItemRequest[] itemRequests = new BulkItemRequest[1];
        WriteRequest.RefreshPolicy refreshPolicy = request.getRefreshPolicy();
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        itemRequests[0] = new BulkItemRequest(0, ((DocWriteRequest) request));
        BulkShardRequest bulkShardRequest = new BulkShardRequest(request.shardId(), refreshPolicy, itemRequests);
        WritePrimaryResult<BulkShardRequest, BulkShardResponse> result = shardOperationOnPrimary(bulkShardRequest, primary);
        BulkShardResponse bulkShardResponse = result.finalResponseIfSuccessful;
        assert bulkShardResponse.getResponses().length == 1 : "expected only one bulk shard response";
        BulkItemResponse itemResponse = bulkShardResponse.getResponses()[0];
        final Response response;
        final Exception failure;
        if (itemResponse.isFailed()) {
            failure = itemResponse.getFailure().getCause();
            response = null;
        } else {
            response = (Response) itemResponse.getResponse();
            failure = null;
        }
        return new WritePrimaryResult<>(request, response, result.location, failure, primary, logger);
    }

    public <ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>> WriteReplicaResult<ReplicaRequest> executeSingleItemBulkRequestOnReplica(ReplicaRequest request, IndexShard replica) throws Exception {
        BulkItemRequest[] itemRequests = new BulkItemRequest[1];
        WriteRequest.RefreshPolicy refreshPolicy = request.getRefreshPolicy();
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        itemRequests[0] = new BulkItemRequest(0, ((DocWriteRequest) request));
        BulkShardRequest bulkShardRequest = new BulkShardRequest(request.shardId(), refreshPolicy, itemRequests);
        WriteReplicaResult<BulkShardRequest> result = shardOperationOnReplica(bulkShardRequest, replica);
        return new WriteReplicaResult<>(request, result.location, null, replica, logger);
    }

    public static Engine.IndexResult executeIndexRequestOnReplica(IndexRequest request, IndexShard replica) {
        final ShardId shardId = replica.shardId();
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.REPLICA, shardId.getIndexName(), request.type(), request.id(), request.source()).routing(request.routing()).parent(request.parent());
        final Engine.Index operation;
        try {
            operation = replica.prepareIndexOnReplica(sourceToParse, request.seqNo(), request.version(), request.versionType(), request.getAutoGeneratedTimestamp(), request.isRetry());
        } catch (MapperParsingException e) {
            return new Engine.IndexResult(e, request.version(), request.seqNo());
        }
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        if (update != null) {
            throw new RetryOnReplicaException(shardId, "Mappings are not available on the replica yet, triggered update: " + update);
        }
        return replica.index(operation);
    }

    static Engine.Index prepareIndexOperationOnPrimary(IndexRequest request, IndexShard primary) {
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, request.index(), request.type(), request.id(), request.source()).routing(request.routing()).parent(request.parent());
        return primary.prepareIndexOnPrimary(sourceToParse, request.version(), request.versionType(), request.getAutoGeneratedTimestamp(), request.isRetry());
    }

    public static Engine.IndexResult executeIndexRequestOnPrimary(IndexRequest request, IndexShard primary, MappingUpdatedAction mappingUpdatedAction) throws Exception {
        Engine.Index operation;
        try {
            operation = prepareIndexOperationOnPrimary(request, primary);
        } catch (MapperParsingException | IllegalArgumentException e) {
            return new Engine.IndexResult(e, request.version(), request.seqNo());
        }
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        final ShardId shardId = primary.shardId();
        if (update != null) {
            try {
                mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), request.type(), update);
            } catch (IllegalArgumentException e) {
                return new Engine.IndexResult(e, request.version(), request.seqNo());
            }
            try {
                operation = prepareIndexOperationOnPrimary(request, primary);
            } catch (MapperParsingException | IllegalArgumentException e) {
                return new Engine.IndexResult(e, request.version(), request.seqNo());
            }
            update = operation.parsedDoc().dynamicMappingsUpdate();
            if (update != null) {
                throw new ReplicationOperation.RetryOnPrimaryException(shardId, "Dynamic mappings are not available on the node that holds the primary yet");
            }
        }
        return primary.index(operation);
    }

    public static Engine.DeleteResult executeDeleteRequestOnPrimary(DeleteRequest request, IndexShard primary) {
        final Engine.Delete delete = primary.prepareDeleteOnPrimary(request.type(), request.id(), request.version(), request.versionType());
        return primary.delete(delete);
    }

    public static Engine.DeleteResult executeDeleteRequestOnReplica(DeleteRequest request, IndexShard replica) {
        final Engine.Delete delete = replica.prepareDeleteOnReplica(request.type(), request.id(), request.seqNo(), request.primaryTerm(), request.version(), request.versionType());
        return replica.delete(delete);
    }
}