package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import static org.elasticsearch.action.ValidateActions.addValidationError;

public abstract class ReplicationRequest<Request extends ReplicationRequest<Request>> extends ActionRequest<Request> implements IndicesRequest {

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    protected ShardId shardId;

    long seqNo;

    long primaryTerm;

    protected TimeValue timeout = DEFAULT_TIMEOUT;

    protected String index;

    private WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;

    private long routedBasedOnClusterVersion = 0;

    public ReplicationRequest() {
    }

    public ReplicationRequest(ShardId shardId) {
        this.index = shardId.getIndexName();
        this.shardId = shardId;
    }

    @SuppressWarnings("unchecked")
    public final Request timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    public final Request timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout"));
    }

    public TimeValue timeout() {
        return timeout;
    }

    public String index() {
        return this.index;
    }

    @SuppressWarnings("unchecked")
    public final Request index(String index) {
        this.index = index;
        return (Request) this;
    }

    @Override
    public String[] indices() {
        return new String[] { index };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    public WriteConsistencyLevel consistencyLevel() {
        return this.consistencyLevel;
    }

    @Nullable
    public ShardId shardId() {
        return shardId;
    }

    @SuppressWarnings("unchecked")
    public final Request consistencyLevel(WriteConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return (Request) this;
    }

    @SuppressWarnings("unchecked")
    Request routedBasedOnClusterVersion(long routedBasedOnClusterVersion) {
        this.routedBasedOnClusterVersion = routedBasedOnClusterVersion;
        return (Request) this;
    }

    long routedBasedOnClusterVersion() {
        return routedBasedOnClusterVersion;
    }

    public long seqNo() {
        return seqNo;
    }

    public void seqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    public long primaryTerm() {
        return primaryTerm;
    }

    public void primaryTerm(long term) {
        primaryTerm = term;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            shardId = ShardId.readShardId(in);
        } else {
            shardId = null;
        }
        consistencyLevel = WriteConsistencyLevel.fromId(in.readByte());
        timeout = new TimeValue(in);
        index = in.readString();
        routedBasedOnClusterVersion = in.readVLong();
        seqNo = in.readVLong();
        primaryTerm = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (shardId != null) {
            out.writeBoolean(true);
            shardId.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeByte(consistencyLevel.id());
        timeout.writeTo(out);
        out.writeString(index);
        out.writeVLong(routedBasedOnClusterVersion);
        out.writeVLong(seqNo);
        out.writeVLong(primaryTerm);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId) {
        return new ReplicationTask(id, type, action, getDescription(), parentTaskId);
    }

    @SuppressWarnings("unchecked")
    public Request setShardId(ShardId shardId) {
        this.shardId = shardId;
        return (Request) this;
    }

    @Override
    public String toString() {
        if (shardId != null) {
            return shardId.toString();
        } else {
            return index;
        }
    }

    @Override
    public String getDescription() {
        return toString();
    }
}