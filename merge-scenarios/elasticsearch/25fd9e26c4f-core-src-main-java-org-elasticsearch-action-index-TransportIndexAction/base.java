package org.elasticsearch.action.index;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportIndexAction extends TransportWriteAction<IndexRequest, IndexResponse> {

    private final AutoCreateIndex autoCreateIndex;

    private final boolean allowIdGeneration;

    private final TransportCreateIndexAction createIndexAction;

    private final ClusterService clusterService;

    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportIndexAction(Settings settings, TransportService transportService, ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction, TransportCreateIndexAction createIndexAction, MappingUpdatedAction mappingUpdatedAction, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, AutoCreateIndex autoCreateIndex) {
        super(settings, IndexAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters, indexNameExpressionResolver, IndexRequest::new, ThreadPool.Names.INDEX);
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.createIndexAction = createIndexAction;
        this.autoCreateIndex = autoCreateIndex;
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, final IndexRequest request, final ActionListener<IndexResponse> listener) {
        ClusterState state = clusterService.state();
        if (autoCreateIndex.shouldAutoCreate(request.index(), state)) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest();
            createIndexRequest.index(request.index());
            createIndexRequest.mapping(request.type());
            createIndexRequest.cause("auto(index api)");
            createIndexRequest.masterNodeTimeout(request.timeout());
            createIndexAction.execute(task, createIndexRequest, new ActionListener<CreateIndexResponse>() {

                @Override
                public void onResponse(CreateIndexResponse result) {
                    innerExecute(task, request, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                        try {
                            innerExecute(task, request, listener);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            listener.onFailure(inner);
                        }
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } else {
            innerExecute(task, request, listener);
        }
    }

    @Override
    protected void resolveRequest(MetaData metaData, IndexMetaData indexMetaData, IndexRequest request) {
        MappingMetaData mappingMd = indexMetaData.mappingOrDefault(request.type());
        request.resolveRouting(metaData);
        request.process(mappingMd, allowIdGeneration, indexMetaData.getIndex().getName());
        ShardId shardId = clusterService.operationRouting().shardId(clusterService.state(), indexMetaData.getIndex().getName(), request.id(), request.routing());
        request.setShardId(shardId);
    }

    private void innerExecute(Task task, final IndexRequest request, final ActionListener<IndexResponse> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected IndexResponse newResponseInstance() {
        return new IndexResponse();
    }

    @Override
    protected WriteResult<IndexResponse> onPrimaryShard(IndexRequest request, IndexShard indexShard) throws Exception {
        return executeIndexRequestOnPrimary(request, indexShard, mappingUpdatedAction);
    }

    @Override
    protected Location onReplicaShard(IndexRequest request, IndexShard indexShard) {
        return executeIndexRequestOnReplica(request, indexShard).getTranslogLocation();
    }

    public static Engine.Index executeIndexRequestOnReplica(IndexRequest request, IndexShard indexShard) {
        final ShardId shardId = indexShard.shardId();
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.REPLICA, shardId.getIndexName(), request.type(), request.id(), request.source()).routing(request.routing()).parent(request.parent()).timestamp(request.timestamp()).ttl(request.ttl());
        final Engine.Index operation = indexShard.prepareIndexOnReplica(sourceToParse, request.version(), request.versionType());
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        if (update != null) {
            throw new RetryOnReplicaException(shardId, "Mappings are not available on the replica yet, triggered update: " + update);
        }
        indexShard.index(operation);
        return operation;
    }

    public static Engine.Index prepareIndexOperationOnPrimary(IndexRequest request, IndexShard indexShard) {
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, request.index(), request.type(), request.id(), request.source()).routing(request.routing()).parent(request.parent()).timestamp(request.timestamp()).ttl(request.ttl());
        return indexShard.prepareIndexOnPrimary(sourceToParse, request.version(), request.versionType());
    }

    public static WriteResult<IndexResponse> executeIndexRequestOnPrimary(IndexRequest request, IndexShard indexShard, MappingUpdatedAction mappingUpdatedAction) throws Exception {
        Engine.Index operation = prepareIndexOperationOnPrimary(request, indexShard);
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        final ShardId shardId = indexShard.shardId();
        if (update != null) {
            mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), request.type(), update);
            operation = prepareIndexOperationOnPrimary(request, indexShard);
            update = operation.parsedDoc().dynamicMappingsUpdate();
            if (update != null) {
                throw new ReplicationOperation.RetryOnPrimaryException(shardId, "Dynamic mappings are not available on the node that holds the primary yet");
            }
        }
        final boolean created = indexShard.index(operation);
        final long version = operation.version();
        request.version(version);
        request.versionType(request.versionType().versionTypeForReplicationAndRecovery());
        assert request.versionType().validateVersionForWrites(request.version());
        IndexResponse response = new IndexResponse(shardId, request.type(), request.id(), request.version(), created);
        return new WriteResult<>(response, operation.getTranslogLocation());
    }
}