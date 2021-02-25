package org.elasticsearch.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import java.io.IOException;
import java.util.Locale;

public abstract class DocWriteResponse extends ReplicationResponse implements WriteResponse, StatusToXContent {

    public enum Result implements Writeable {

        CREATED(0), UPDATED(1), DELETED(2), NOT_FOUND(3), NOOP(4);

        private final byte op;

        private final String lowercase;

        Result(int op) {
            this.op = (byte) op;
            this.lowercase = this.toString().toLowerCase(Locale.ENGLISH);
        }

        public byte getOp() {
            return op;
        }

        public String getLowercase() {
            return lowercase;
        }

        public static Result readFrom(StreamInput in) throws IOException {
            Byte opcode = in.readByte();
            switch(opcode) {
                case 0:
                    return CREATED;
                case 1:
                    return UPDATED;
                case 2:
                    return DELETED;
                case 3:
                    return NOT_FOUND;
                case 4:
                    return NOOP;
                default:
                    throw new IllegalArgumentException("Unknown result code: " + opcode);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(op);
        }
    }

    private ShardId shardId;

    private String id;

    private String type;

    private long version;

    private boolean forcedRefresh;

    protected Result result;

    public DocWriteResponse(ShardId shardId, String type, String id, long version, Result result) {
        this.shardId = shardId;
        this.type = type;
        this.id = id;
        this.version = version;
        this.result = result;
    }

    protected DocWriteResponse() {
    }

    public Result getResult() {
        return result;
    }

    public String getIndex() {
        return this.shardId.getIndexName();
    }

    public ShardId getShardId() {
        return this.shardId;
    }

    public String getType() {
        return this.type;
    }

    public String getId() {
        return this.id;
    }

    public long getVersion() {
        return this.version;
    }

    public boolean forcedRefresh() {
        return forcedRefresh;
    }

    @Override
    public void setForcedRefresh(boolean forcedRefresh) {
        this.forcedRefresh = forcedRefresh;
    }

    public RestStatus status() {
        return getShardInfo().status();
    }

    public String getLocation(@Nullable String routing) {
        String index = getIndex();
        String type = getType();
        String id = getId();
        String routingStart = "?routing=";
        int bufferSize = 3 + index.length() + type.length() + id.length();
        if (routing != null) {
            bufferSize += routingStart.length() + routing.length();
        }
        StringBuilder location = new StringBuilder(bufferSize);
        location.append('/').append(index);
        location.append('/').append(type);
        location.append('/').append(id);
        if (routing != null) {
            location.append(routingStart).append(routing);
        }
        return location.toString();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = ShardId.readShardId(in);
        type = in.readString();
        id = in.readString();
        version = in.readZLong();
        forcedRefresh = in.readBoolean();
        result = Result.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeString(type);
        out.writeString(id);
        out.writeZLong(version);
        out.writeBoolean(forcedRefresh);
        result.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        ReplicationResponse.ShardInfo shardInfo = getShardInfo();
        builder.field("_index", shardId.getIndexName()).field("_type", type).field("_id", id).field("_version", version).field("result", getResult().getLowercase());
        if (forcedRefresh) {
            builder.field("forced_refresh", forcedRefresh);
        }
        shardInfo.toXContent(builder, params);
        return builder;
    }
}