package org.elasticsearch.action.delete;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import java.io.IOException;

public class DeleteResponse extends DocWriteResponse {

    public DeleteResponse() {
    }

    public DeleteResponse(ShardId shardId, String type, String id, long version, boolean found) {
        super(shardId, type, id, version, found ? Result.DELETED : Result.NOT_FOUND);
    }

    @Override
    public RestStatus status() {
        return result == Result.DELETED ? super.status() : RestStatus.NOT_FOUND;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("found", result == Result.DELETED);
        super.toXContent(builder, params);
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DeleteResponse[");
        builder.append("index=").append(getIndex());
        builder.append(",type=").append(getType());
        builder.append(",id=").append(getId());
        builder.append(",version=").append(getVersion());
        builder.append(",result=").append(getResult().getLowercase());
        builder.append(",shards=").append(getShardInfo());
        return builder.append("]").toString();
    }
}