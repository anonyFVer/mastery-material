package org.elasticsearch.action.delete;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import java.io.IOException;

public class DeleteResponse extends DocWriteResponse {

    private boolean found;

    public DeleteResponse() {
    }

    public DeleteResponse(ShardId shardId, String type, String id, long version, boolean found) {
        super(shardId, type, id, version);
        this.found = found;
    }

    public boolean isFound() {
        return found;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        found = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(found);
    }

    @Override
    public RestStatus status() {
        if (found == false) {
            return RestStatus.NOT_FOUND;
        }
        return super.status();
    }

    static final class Fields {

        static final String FOUND = "found";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.FOUND, isFound());
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
        builder.append(",found=").append(found);
        builder.append(",shards=").append(getShardInfo());
        return builder.append("]").toString();
    }
}