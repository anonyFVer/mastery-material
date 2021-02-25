package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import java.io.IOException;

public class BulkItemResponse implements Streamable, StatusToXContent {

    @Override
    public RestStatus status() {
        return failure == null ? response.status() : failure.getStatus();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(opType.getLowercase());
        if (failure == null) {
            response.toXContent(builder, params);
            builder.field(Fields.STATUS, response.status().getStatus());
        } else {
            builder.field(Fields._INDEX, failure.getIndex());
            builder.field(Fields._TYPE, failure.getType());
            builder.field(Fields._ID, failure.getId());
            builder.field(Fields.STATUS, failure.getStatus().getStatus());
            builder.startObject(Fields.ERROR);
            ElasticsearchException.toXContent(builder, params, failure.getCause());
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {

        static final String _INDEX = "_index";

        static final String _TYPE = "_type";

        static final String _ID = "_id";

        static final String STATUS = "status";

        static final String ERROR = "error";
    }

    public static class Failure implements Writeable, ToXContent {

        static final String INDEX_FIELD = "index";

        static final String TYPE_FIELD = "type";

        static final String ID_FIELD = "id";

        static final String CAUSE_FIELD = "cause";

        static final String STATUS_FIELD = "status";

        private final String index;

        private final String type;

        private final String id;

        private final Exception cause;

        private final RestStatus status;

        public Failure(String index, String type, String id, Exception cause) {
            this.index = index;
            this.type = type;
            this.id = id;
            this.cause = cause;
            this.status = ExceptionsHelper.status(cause);
        }

        public Failure(StreamInput in) throws IOException {
            index = in.readString();
            type = in.readString();
            id = in.readOptionalString();
            cause = in.readException();
            status = ExceptionsHelper.status(cause);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(getIndex());
            out.writeString(getType());
            out.writeOptionalString(getId());
            out.writeException(getCause());
        }

        public String getIndex() {
            return this.index;
        }

        public String getType() {
            return type;
        }

        public String getId() {
            return id;
        }

        public String getMessage() {
            return this.cause.toString();
        }

        public RestStatus getStatus() {
            return this.status;
        }

        public Exception getCause() {
            return cause;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(INDEX_FIELD, index);
            builder.field(TYPE_FIELD, type);
            if (id != null) {
                builder.field(ID_FIELD, id);
            }
            builder.startObject(CAUSE_FIELD);
            ElasticsearchException.toXContent(builder, params, cause);
            builder.endObject();
            builder.field(STATUS_FIELD, status.getStatus());
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this, true);
        }
    }

    private int id;

    private OpType opType;

    private DocWriteResponse response;

    private Failure failure;

    BulkItemResponse() {
    }

    public BulkItemResponse(int id, OpType opType, DocWriteResponse response) {
        this.id = id;
        this.response = response;
        this.opType = opType;
    }

    public BulkItemResponse(int id, OpType opType, Failure failure) {
        this.id = id;
        this.opType = opType;
        this.failure = failure;
    }

    public int getItemId() {
        return id;
    }

    public OpType getOpType() {
        return this.opType;
    }

    public String getIndex() {
        if (failure != null) {
            return failure.getIndex();
        }
        return response.getIndex();
    }

    public String getType() {
        if (failure != null) {
            return failure.getType();
        }
        return response.getType();
    }

    public String getId() {
        if (failure != null) {
            return failure.getId();
        }
        return response.getId();
    }

    public long getVersion() {
        if (failure != null) {
            return -1;
        }
        return response.getVersion();
    }

    public <T extends DocWriteResponse> T getResponse() {
        return (T) response;
    }

    public boolean isFailed() {
        return failure != null;
    }

    public String getFailureMessage() {
        if (failure != null) {
            return failure.getMessage();
        }
        return null;
    }

    public Failure getFailure() {
        return this.failure;
    }

    public static BulkItemResponse readBulkItem(StreamInput in) throws IOException {
        BulkItemResponse response = new BulkItemResponse();
        response.readFrom(in);
        return response;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readVInt();
        if (in.getVersion().onOrAfter(Version.V_6_0_0_alpha1_UNRELEASED)) {
            opType = OpType.fromId(in.readByte());
        } else {
            opType = OpType.fromString(in.readString());
        }
        byte type = in.readByte();
        if (type == 0) {
            response = new IndexResponse();
            response.readFrom(in);
        } else if (type == 1) {
            response = new DeleteResponse();
            response.readFrom(in);
        } else if (type == 3) {
            response = new UpdateResponse();
            response.readFrom(in);
        }
        if (in.readBoolean()) {
            failure = new Failure(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        if (out.getVersion().onOrAfter(Version.V_6_0_0_alpha1_UNRELEASED)) {
            out.writeByte(opType.getId());
        } else {
            out.writeString(opType.getLowercase());
        }
        if (response == null) {
            out.writeByte((byte) 2);
        } else {
            if (response instanceof IndexResponse) {
                out.writeByte((byte) 0);
            } else if (response instanceof DeleteResponse) {
                out.writeByte((byte) 1);
            } else if (response instanceof UpdateResponse) {
                out.writeByte((byte) 3);
            }
            response.writeTo(out);
        }
        if (failure == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            failure.writeTo(out);
        }
    }
}