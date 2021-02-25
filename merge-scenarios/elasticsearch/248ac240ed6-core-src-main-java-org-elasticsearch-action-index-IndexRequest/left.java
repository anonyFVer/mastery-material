package org.elasticsearch.action.index;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.TimestampParsingException;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.TimestampFieldMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import static org.elasticsearch.action.ValidateActions.addValidationError;

public class IndexRequest extends ReplicatedWriteRequest<IndexRequest> implements DocumentRequest<IndexRequest> {

    private String type;

    private String id;

    @Nullable
    private String routing;

    @Nullable
    private String parent;

    @Nullable
    private String timestamp;

    @Nullable
    private TimeValue ttl;

    private BytesReference source;

    private OpType opType = OpType.INDEX;

    private long version = Versions.MATCH_ANY;

    private VersionType versionType = VersionType.INTERNAL;

    private XContentType contentType = Requests.INDEX_CONTENT_TYPE;

    private String pipeline;

    public IndexRequest() {
    }

    public IndexRequest(String index) {
        this.index = index;
    }

    public IndexRequest(String index, String type) {
        this.index = index;
        this.type = type;
    }

    public IndexRequest(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (source == null) {
            validationException = addValidationError("source is missing", validationException);
        }
        if (opType() == OpType.CREATE) {
            if (versionType != VersionType.INTERNAL || version != Versions.MATCH_DELETED) {
                validationException = addValidationError("create operations do not support versioning. use index instead", validationException);
                return validationException;
            }
        }
        if (!versionType.validateVersionForWrites(version)) {
            validationException = addValidationError("illegal version value [" + version + "] for version type [" + versionType.name() + "]", validationException);
        }
        if (ttl != null) {
            if (ttl.millis() < 0) {
                validationException = addValidationError("ttl must not be negative", validationException);
            }
        }
        if (id != null && id.getBytes(StandardCharsets.UTF_8).length > 512) {
            validationException = addValidationError("id is too long, must be no longer than 512 bytes but was: " + id.getBytes(StandardCharsets.UTF_8).length, validationException);
        }
        return validationException;
    }

    public XContentType getContentType() {
        return contentType;
    }

    public IndexRequest contentType(XContentType contentType) {
        this.contentType = contentType;
        return this;
    }

    @Override
    public String type() {
        return type;
    }

    public IndexRequest type(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String id() {
        return id;
    }

    public IndexRequest id(String id) {
        this.id = id;
        return this;
    }

    @Override
    public IndexRequest routing(String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    @Override
    public String routing() {
        return this.routing;
    }

    public IndexRequest parent(String parent) {
        this.parent = parent;
        return this;
    }

    @Override
    public String parent() {
        return this.parent;
    }

    public IndexRequest timestamp(String timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public String timestamp() {
        return this.timestamp;
    }

    public IndexRequest ttl(String ttl) {
        this.ttl = TimeValue.parseTimeValue(ttl, null, "ttl");
        return this;
    }

    public IndexRequest ttl(TimeValue ttl) {
        this.ttl = ttl;
        return this;
    }

    public IndexRequest ttl(long ttl) {
        this.ttl = new TimeValue(ttl);
        return this;
    }

    public TimeValue ttl() {
        return this.ttl;
    }

    public IndexRequest setPipeline(String pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    public String getPipeline() {
        return this.pipeline;
    }

    public BytesReference source() {
        return source;
    }

    public Map<String, Object> sourceAsMap() {
        return XContentHelper.convertToMap(source, false).v2();
    }

    public IndexRequest source(Map source) throws ElasticsearchGenerationException {
        return source(source, contentType);
    }

    public IndexRequest source(Map source, XContentType contentType) throws ElasticsearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public IndexRequest source(String source) {
        this.source = new BytesArray(source.getBytes(StandardCharsets.UTF_8));
        return this;
    }

    public IndexRequest source(XContentBuilder sourceBuilder) {
        source = sourceBuilder.bytes();
        return this;
    }

    public IndexRequest source(String field1, Object value1) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.startObject().field(field1, value1).endObject();
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    public IndexRequest source(String field1, Object value1, String field2, Object value2) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.startObject().field(field1, value1).field(field2, value2).endObject();
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    public IndexRequest source(String field1, Object value1, String field2, Object value2, String field3, Object value3) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.startObject().field(field1, value1).field(field2, value2).field(field3, value3).endObject();
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    public IndexRequest source(String field1, Object value1, String field2, Object value2, String field3, Object value3, String field4, Object value4) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.startObject().field(field1, value1).field(field2, value2).field(field3, value3).field(field4, value4).endObject();
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    public IndexRequest source(Object... source) {
        if (source.length % 2 != 0) {
            throw new IllegalArgumentException("The number of object passed must be even but was [" + source.length + "]");
        }
        if (source.length == 2 && source[0] instanceof BytesReference && source[1] instanceof Boolean) {
            throw new IllegalArgumentException("you are using the removed method for source with bytes and unsafe flag, the unsafe flag was removed, please just use source(BytesReference)");
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.startObject();
            for (int i = 0; i < source.length; i++) {
                builder.field(source[i++].toString(), source[i]);
            }
            builder.endObject();
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    public IndexRequest source(BytesReference source) {
        this.source = source;
        return this;
    }

    public IndexRequest source(byte[] source) {
        return source(source, 0, source.length);
    }

    public IndexRequest source(byte[] source, int offset, int length) {
        this.source = new BytesArray(source, offset, length);
        return this;
    }

    public IndexRequest opType(OpType opType) {
        if (opType != OpType.CREATE && opType != OpType.INDEX) {
            throw new IllegalArgumentException("opType must be 'create' or 'index', found: [" + opType + "]");
        }
        this.opType = opType;
        if (opType == OpType.CREATE) {
            version(Versions.MATCH_DELETED);
            versionType(VersionType.INTERNAL);
        }
        return this;
    }

    public IndexRequest opType(String opType) {
        String op = opType.toLowerCase(Locale.ROOT);
        if (op.equals("create")) {
            opType(OpType.CREATE);
        } else if (op.equals("index")) {
            opType(OpType.INDEX);
        } else {
            throw new IllegalArgumentException("opType must be 'create' or 'index', found: [" + opType + "]");
        }
        return this;
    }

    public IndexRequest create(boolean create) {
        if (create) {
            return opType(OpType.CREATE);
        } else {
            return opType(OpType.INDEX);
        }
    }

    @Override
    public OpType opType() {
        return this.opType;
    }

    @Override
    public IndexRequest version(long version) {
        this.version = version;
        return this;
    }

    @Override
    public long version() {
        return this.version;
    }

    @Override
    public IndexRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    @Override
    public VersionType versionType() {
        return this.versionType;
    }

    public void process(@Nullable MappingMetaData mappingMd, boolean allowIdGeneration, String concreteIndex) {
        if (timestamp != null) {
            timestamp = MappingMetaData.Timestamp.parseStringTimestamp(timestamp, mappingMd != null ? mappingMd.timestamp().dateTimeFormatter() : TimestampFieldMapper.Defaults.DATE_TIME_FORMATTER);
        }
        if (mappingMd != null) {
            if (mappingMd.routing().required() && routing == null) {
                throw new RoutingMissingException(concreteIndex, type, id);
            }
            if (parent != null && !mappingMd.hasParentField()) {
                throw new IllegalArgumentException("Can't specify parent if no parent field has been configured");
            }
        } else {
            if (parent != null) {
                throw new IllegalArgumentException("Can't specify parent if no parent field has been configured");
            }
        }
        if (allowIdGeneration) {
            if (id == null) {
                id(UUIDs.base64UUID());
            }
        }
        if (timestamp == null) {
            String defaultTimestamp = TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP;
            if (mappingMd != null && mappingMd.timestamp() != null) {
                if (mappingMd.timestamp().ignoreMissing() != null && mappingMd.timestamp().ignoreMissing() == false) {
                    throw new TimestampParsingException("timestamp is required by mapping");
                }
                defaultTimestamp = mappingMd.timestamp().defaultTimestamp();
            }
            if (defaultTimestamp.equals(TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP)) {
                timestamp = Long.toString(System.currentTimeMillis());
            } else {
                assert mappingMd != null;
                timestamp = MappingMetaData.Timestamp.parseStringTimestamp(defaultTimestamp, mappingMd.timestamp().dateTimeFormatter());
            }
        }
    }

    public void resolveRouting(MetaData metaData) {
        routing(metaData.resolveIndexRouting(parent, routing, index));
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        type = in.readOptionalString();
        id = in.readOptionalString();
        routing = in.readOptionalString();
        parent = in.readOptionalString();
        timestamp = in.readOptionalString();
        ttl = in.readOptionalWriteable(TimeValue::new);
        source = in.readBytesReference();
        opType = OpType.fromId(in.readByte());
        version = in.readLong();
        versionType = VersionType.fromValue(in.readByte());
        pipeline = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(type);
        out.writeOptionalString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(parent);
        out.writeOptionalString(timestamp);
        out.writeOptionalWriteable(ttl);
        out.writeBytesReference(source);
        out.writeByte(opType.getId());
        out.writeLong(version);
        out.writeByte(versionType.getValue());
        out.writeOptionalString(pipeline);
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(source, false);
        } catch (Exception e) {
        }
        return "index {[" + index + "][" + type + "][" + id + "], source[" + sSource + "]}";
    }
}