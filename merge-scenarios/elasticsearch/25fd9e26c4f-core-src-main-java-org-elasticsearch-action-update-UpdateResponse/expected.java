package org.elasticsearch.action.update;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import java.io.IOException;

public class UpdateResponse extends DocWriteResponse {

    private GetResult getResult;

    public UpdateResponse() {
    }

    public UpdateResponse(ShardId shardId, String type, String id, long version, Result result) {
        this(new ShardInfo(0, 0), shardId, type, id, SequenceNumbersService.UNASSIGNED_SEQ_NO, version, result);
    }

    public UpdateResponse(ShardInfo shardInfo, ShardId shardId, String type, String id, long seqNo, long version, Result result) {
        super(shardId, type, id, seqNo, version, result);
        setShardInfo(shardInfo);
    }

    public void setGetResult(GetResult getResult) {
        this.getResult = getResult;
    }

    public GetResult getGetResult() {
        return this.getResult;
    }

    @Override
    public RestStatus status() {
        return this.result == Result.CREATED ? RestStatus.CREATED : super.status();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            getResult = GetResult.readGetResult(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
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
        builder.append(",result=").append(getResult().getLowercase());
        builder.append(",shards=").append(getShardInfo());
        return builder.append("]").toString();
    }
}