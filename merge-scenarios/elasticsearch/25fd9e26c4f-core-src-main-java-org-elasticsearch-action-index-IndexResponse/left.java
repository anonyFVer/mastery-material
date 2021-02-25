package org.elasticsearch.action.index;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import java.io.IOException;

public class IndexResponse extends DocWriteResponse {

    private boolean created;

    public IndexResponse() {
    }

    public IndexResponse(ShardId shardId, String type, String id, long seqNo, long version, boolean created) {
        super(shardId, type, id, seqNo, version);
        this.created = created;
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(created);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("IndexResponse[");
        builder.append("index=").append(getIndex());
        builder.append(",type=").append(getType());
        builder.append(",id=").append(getId());
        builder.append(",version=").append(getVersion());
        builder.append(",created=").append(created);
        builder.append(",seqNo=").append(getSeqNo());
        builder.append(",shards=").append(getShardInfo());
        return builder.append("]").toString();
    }

    static final class Fields {

        static final String CREATED = "created";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        super.toXContent(builder, params);
        builder.field(Fields.CREATED, isCreated());
        return builder;
    }
}