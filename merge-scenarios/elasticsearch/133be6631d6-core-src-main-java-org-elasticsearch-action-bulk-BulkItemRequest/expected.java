package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import java.io.IOException;

public class BulkItemRequest implements Streamable {

    private int id;

    private DocWriteRequest request;

    private volatile BulkItemResponse primaryResponse;

    private volatile boolean ignoreOnReplica;

    BulkItemRequest() {
    }

    public BulkItemRequest(int id, DocWriteRequest request) {
        this.id = id;
        this.request = request;
    }

    public int id() {
        return id;
    }

    public DocWriteRequest request() {
        return request;
    }

    public String index() {
        assert request.indices().length == 1;
        return request.indices()[0];
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
        request = DocWriteRequest.readDocumentRequest(in);
        if (in.readBoolean()) {
            primaryResponse = BulkItemResponse.readBulkItem(in);
        }
        ignoreOnReplica = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        DocWriteRequest.writeDocumentRequest(out, request);
        out.writeOptionalStreamable(primaryResponse);
        out.writeBoolean(ignoreOnReplica);
    }
}