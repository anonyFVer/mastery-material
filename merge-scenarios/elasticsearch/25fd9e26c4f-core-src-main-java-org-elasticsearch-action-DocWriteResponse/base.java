package org.elasticsearch.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import java.io.IOException;

public abstract class DocWriteResponse extends ReplicationResponse implements WriteResponse, StatusToXContent {

    private ShardId shardId;

    private String id;

    private String type;

    private long version;

    private boolean forcedRefresh;

    public DocWriteResponse(ShardId shardId, String type, String id, long version) {
        this.shardId = shardId;
        this.type = type;
        this.id = id;
        this.version = version;
    }

    protected DocWriteResponse() {
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

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = ShardId.readShardId(in);
        type = in.readString();
        id = in.readString();
        version = in.readZLong();
        forcedRefresh = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeString(type);
        out.writeString(id);
        out.writeZLong(version);
        out.writeBoolean(forcedRefresh);
    }

    static final class Fields {

        static final String _INDEX = "_index";

        static final String _TYPE = "_type";

        static final String _ID = "_id";

        static final String _VERSION = "_version";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        ReplicationResponse.ShardInfo shardInfo = getShardInfo();
        builder.field(Fields._INDEX, shardId.getIndexName()).field(Fields._TYPE, type).field(Fields._ID, id).field(Fields._VERSION, version).field("forced_refresh", forcedRefresh);
        shardInfo.toXContent(builder, params);
        return builder;
    }
}