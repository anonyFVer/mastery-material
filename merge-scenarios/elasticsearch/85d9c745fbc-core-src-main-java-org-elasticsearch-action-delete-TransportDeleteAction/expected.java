package org.elasticsearch.action.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteAction extends TransportWriteAction<DeleteRequest, DeleteRequest, DeleteResponse> {

    private final TransportBulkAction bulkAction;

    private final TransportShardBulkAction shardBulkAction;

    @Inject
    public TransportDeleteAction(Settings settings, TransportService transportService, ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, TransportBulkAction bulkAction, TransportShardBulkAction shardBulkAction) {
        super(settings, DeleteAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters, indexNameExpressionResolver, DeleteRequest::new, DeleteRequest::new, ThreadPool.Names.INDEX);
        this.bulkAction = bulkAction;
        this.shardBulkAction = shardBulkAction;
    }

    @Override
    protected void doExecute(Task task, final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(request);
        bulkRequest.setRefreshPolicy(request.getRefreshPolicy());
        bulkRequest.timeout(request.timeout());
        bulkRequest.waitForActiveShards(request.waitForActiveShards());
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        bulkAction.execute(task, bulkRequest, new ActionListener<BulkResponse>() {

            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                assert bulkItemResponses.getItems().length == 1 : "expected only one item in bulk request";
                BulkItemResponse bulkItemResponse = bulkItemResponses.getItems()[0];
                if (bulkItemResponse.isFailed() == false) {
                    DeleteResponse response = bulkItemResponse.getResponse();
                    listener.onResponse(response);
                } else {
                    listener.onFailure(bulkItemResponse.getFailure().getCause());
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected DeleteResponse newResponseInstance() {
        return new DeleteResponse();
    }

    @Override
    protected WritePrimaryResult<DeleteRequest, DeleteResponse> shardOperationOnPrimary(DeleteRequest request, IndexShard primary) throws Exception {
        return shardBulkAction.executeSingleItemBulkRequestOnPrimary(request, primary);
    }

    @Override
    protected WriteReplicaResult<DeleteRequest> shardOperationOnReplica(DeleteRequest request, IndexShard replica) throws Exception {
        return shardBulkAction.executeSingleItemBulkRequestOnReplica(request, replica);
    }
}