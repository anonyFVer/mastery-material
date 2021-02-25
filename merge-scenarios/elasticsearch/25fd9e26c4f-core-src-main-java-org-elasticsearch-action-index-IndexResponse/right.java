package org.elasticsearch.action.index;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import java.io.IOException;

public class IndexResponse extends DocWriteResponse {

    public IndexResponse() {
    }

    public IndexResponse(ShardId shardId, String type, String id, long version, boolean created) {
        super(shardId, type, id, version, created ? Result.CREATED : Result.UPDATED);
    }

    @Override
    public RestStatus status() {
        return result == Result.CREATED ? RestStatus.CREATED : super.status();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("IndexResponse[");
        builder.append("index=").append(getIndex());
        builder.append(",type=").append(getType());
        builder.append(",id=").append(getId());
        builder.append(",version=").append(getVersion());
        builder.append(",result=").append(getResult().getLowercase());
        builder.append(",shards=").append(Strings.toString(getShardInfo(), true));
        return builder.append("]").toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        super.toXContent(builder, params);
        builder.field("created", result == Result.CREATED);
        return builder;
    }
}