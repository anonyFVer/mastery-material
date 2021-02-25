package org.elasticsearch.action.index;

import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import java.util.Map;

public class IndexRequestBuilder extends ReplicationRequestBuilder<IndexRequest, IndexResponse, IndexRequestBuilder> implements WriteRequestBuilder<IndexRequestBuilder> {

    public IndexRequestBuilder(ElasticsearchClient client, IndexAction action) {
        super(client, action, new IndexRequest());
    }

    public IndexRequestBuilder(ElasticsearchClient client, IndexAction action, @Nullable String index) {
        super(client, action, new IndexRequest(index));
    }

    public IndexRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    public IndexRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    public IndexRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    public IndexRequestBuilder setParent(String parent) {
        request.parent(parent);
        return this;
    }

    public IndexRequestBuilder setSource(BytesReference source) {
        request.source(source);
        return this;
    }

    public IndexRequestBuilder setSource(Map<String, ?> source) {
        request.source(source);
        return this;
    }

    public IndexRequestBuilder setSource(Map<String, ?> source, XContentType contentType) {
        request.source(source, contentType);
        return this;
    }

    public IndexRequestBuilder setSource(String source) {
        request.source(source);
        return this;
    }

    public IndexRequestBuilder setSource(XContentBuilder sourceBuilder) {
        request.source(sourceBuilder);
        return this;
    }

    public IndexRequestBuilder setSource(byte[] source) {
        request.source(source);
        return this;
    }

    public IndexRequestBuilder setSource(byte[] source, int offset, int length) {
        request.source(source, offset, length);
        return this;
    }

    public IndexRequestBuilder setSource(String field1, Object value1) {
        request.source(field1, value1);
        return this;
    }

    public IndexRequestBuilder setSource(String field1, Object value1, String field2, Object value2) {
        request.source(field1, value1, field2, value2);
        return this;
    }

    public IndexRequestBuilder setSource(String field1, Object value1, String field2, Object value2, String field3, Object value3) {
        request.source(field1, value1, field2, value2, field3, value3);
        return this;
    }

    public IndexRequestBuilder setSource(String field1, Object value1, String field2, Object value2, String field3, Object value3, String field4, Object value4) {
        request.source(field1, value1, field2, value2, field3, value3, field4, value4);
        return this;
    }

    public IndexRequestBuilder setSource(Object... source) {
        request.source(source);
        return this;
    }

    public IndexRequestBuilder setContentType(XContentType contentType) {
        request.contentType(contentType);
        return this;
    }

    public IndexRequestBuilder setOpType(IndexRequest.OpType opType) {
        request.opType(opType);
        return this;
    }

    public IndexRequestBuilder setOpType(String opType) {
        request.opType(IndexRequest.OpType.fromString(opType));
        return this;
    }

    public IndexRequestBuilder setCreate(boolean create) {
        request.create(create);
        return this;
    }

    public IndexRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }

    public IndexRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }

    public IndexRequestBuilder setTimestamp(String timestamp) {
        request.timestamp(timestamp);
        return this;
    }

    public IndexRequestBuilder setTTL(String ttl) {
        request.ttl(ttl);
        return this;
    }

    public IndexRequestBuilder setTTL(long ttl) {
        request.ttl(ttl);
        return this;
    }

    public IndexRequestBuilder setTTL(TimeValue ttl) {
        request.ttl(ttl);
        return this;
    }

    public IndexRequestBuilder setPipeline(String pipeline) {
        request.setPipeline(pipeline);
        return this;
    }
}