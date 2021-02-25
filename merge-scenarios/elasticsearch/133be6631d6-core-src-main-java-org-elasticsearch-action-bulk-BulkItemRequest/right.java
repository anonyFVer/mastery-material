package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import java.io.IOException;

public class BulkItemRequest implements Streamable {

    private int id;

    private ActionRequest request;

    private volatile BulkItemResponse primaryResponse;

    private volatile boolean ignoreOnReplica;

    BulkItemRequest() {
    }

    public BulkItemRequest(int id, ActionRequest request) {
        assert request instanceof IndicesRequest;
        this.id = id;
        this.request = request;
    }

    public int id() {
        return id;
    }

    public ActionRequest request() {
        return request;
    }

    public String index() {
        IndicesRequest indicesRequest = (IndicesRequest) request;
        assert indicesRequest.indices().length == 1;
        return indicesRequest.indices()[0];
    }

    BulkItemResponse getPrimaryResponse() {
        return primaryResponse;
    }

    void setPrimaryResponse(BulkItemResponse primaryResponse) {
        this.primaryResponse = primaryResponse;
    }

    void setIgnoreOnReplica() {
        this.ignoreOnReplica = true;
    }

    boolean isIgnoreOnReplica() {
        return ignoreOnReplica;
    }

    public static BulkItemRequest readBulkItem(StreamInput in) throws IOException {
        BulkItemRequest item = new BulkItemRequest();
        item.readFrom(in);
        return item;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readVInt();
        byte type = in.readByte();
        if (type == 0) {
            request = new IndexRequest();
        } else if (type == 1) {
            request = new DeleteRequest();
        } else if (type == 2) {
            request = new UpdateRequest();
        }
        request.readFrom(in);
        if (in.readBoolean()) {
            primaryResponse = BulkItemResponse.readBulkItem(in);
        }
        ignoreOnReplica = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        if (request instanceof IndexRequest) {
            out.writeByte((byte) 0);
        } else if (request instanceof DeleteRequest) {
            out.writeByte((byte) 1);
        } else if (request instanceof UpdateRequest) {
            out.writeByte((byte) 2);
        }
        request.writeTo(out);
        out.writeOptionalStreamable(primaryResponse);
        out.writeBoolean(ignoreOnReplica);
    }
}