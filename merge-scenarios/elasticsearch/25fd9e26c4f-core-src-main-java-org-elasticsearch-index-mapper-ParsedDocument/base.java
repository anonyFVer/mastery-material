package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.ParseContext.Document;
import java.util.List;

public class ParsedDocument {

    private final Field version;

    private final String uid, id, type;

    private final String routing;

    private final long timestamp;

    private final long ttl;

    private final List<Document> documents;

    private BytesReference source;

    private Mapping dynamicMappingsUpdate;

    private String parent;

    public ParsedDocument(Field version, String id, String type, String routing, long timestamp, long ttl, List<Document> documents, BytesReference source, Mapping dynamicMappingsUpdate) {
        this.version = version;
        this.id = id;
        this.type = type;
        this.uid = Uid.createUid(type, id);
        this.routing = routing;
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.documents = documents;
        this.source = source;
        this.dynamicMappingsUpdate = dynamicMappingsUpdate;
    }

    public Field version() {
        return version;
    }

    public String uid() {
        return uid;
    }

    public String id() {
        return this.id;
    }

    public String type() {
        return this.type;
    }

    public String routing() {
        return this.routing;
    }

    public long timestamp() {
        return this.timestamp;
    }

    public long ttl() {
        return this.ttl;
    }

    public Document rootDoc() {
        return documents.get(documents.size() - 1);
    }

    public List<Document> docs() {
        return this.documents;
    }

    public BytesReference source() {
        return this.source;
    }

    public void setSource(BytesReference source) {
        this.source = source;
    }

    public ParsedDocument parent(String parent) {
        this.parent = parent;
        return this;
    }

    public String parent() {
        return this.parent;
    }

    public Mapping dynamicMappingsUpdate() {
        return dynamicMappingsUpdate;
    }

    public void addDynamicMappingsUpdate(Mapping update) {
        if (dynamicMappingsUpdate == null) {
            dynamicMappingsUpdate = update;
        } else {
            dynamicMappingsUpdate = dynamicMappingsUpdate.merge(update, false);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Document ").append("uid[").append(uid).append("] doc [").append(documents).append("]");
        return sb.toString();
    }
}