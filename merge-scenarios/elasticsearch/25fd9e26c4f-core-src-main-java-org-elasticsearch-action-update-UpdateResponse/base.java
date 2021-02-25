package org.elasticsearch.action.update;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import java.io.IOException;

public class UpdateResponse extends DocWriteResponse {

    private boolean created;

    private GetResult getResult;

    public UpdateResponse() {
    }

    public UpdateResponse(ShardId shardId, String type, String id, long version, boolean created) {
        this(new ShardInfo(0, 0), shardId, type, id, version, created);
    }

    public UpdateResponse(ShardInfo shardInfo, ShardId shardId, String type, String id, long version, boolean created) {
        super(shardId, type, id, version);
        setShardInfo(shardInfo);
        this.created = created;
    }

    public void setGetResult(GetResult getResult) {
        this.getResult = getResult;
    }

    public GetResult getGetResult() {
        return this.getResult;
    }

    public boolean isCreated() {
        return this.created;
    }

    @Override
    public RestStatus status() {
        if (created) {
            return RestStatus.CREATED;
        }
        return super.status();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        created = in.readBoolean();
        if (in.readBoolean()) {
            getResult = GetResult.readGetResult(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(created);
        if (getResult == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            getResult.writeTo(out);
        }
    }

    static final class Fields {

        static final String GET = "get";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        super.toXContent(builder, params);
        if (getGetResult() != null) {
            builder.startObject(Fields.GET);
            getGetResult().toXContentEmbedded(builder, params);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("UpdateResponse[");
        builder.append("index=").append(getIndex());
        builder.append(",type=").append(getType());
        builder.append(",id=").append(getId());
        builder.append(",version=").append(getVersion());
        builder.append(",created=").append(created);
        builder.append(",shards=").append(getShardInfo());
        return builder.append("]").toString();
    }
}