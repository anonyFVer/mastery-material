package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import static org.elasticsearch.action.ValidateActions.addValidationError;

public class BulkRequest extends ActionRequest<BulkRequest> implements CompositeIndicesRequest, WriteRequest<BulkRequest> {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(BulkRequest.class));

    private static final int REQUEST_OVERHEAD = 50;

    final List<DocumentRequest<?>> requests = new ArrayList<>();

    List<Object> payloads = null;

    protected TimeValue timeout = BulkShardRequest.DEFAULT_TIMEOUT;

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    private RefreshPolicy refreshPolicy = RefreshPolicy.NONE;

    private long sizeInBytes = 0;

    public BulkRequest() {
    }

    public BulkRequest add(DocumentRequest<?>... requests) {
        for (DocumentRequest<?> request : requests) {
            add(request, null);
        }
        return this;
    }

    public BulkRequest add(DocumentRequest<?> request) {
        return add(request, null);
    }

    public BulkRequest add(DocumentRequest<?> request, @Nullable Object payload) {
        if (request instanceof IndexRequest) {
            add((IndexRequest) request, payload);
        } else if (request instanceof DeleteRequest) {
            add((DeleteRequest) request, payload);
        } else if (request instanceof UpdateRequest) {
            add((UpdateRequest) request, payload);
        } else {
            throw new IllegalArgumentException("No support for request [" + request + "]");
        }
        return this;
    }

    public BulkRequest add(Iterable<DocumentRequest<?>> requests) {
        for (DocumentRequest<?> request : requests) {
            add(request);
        }
        return this;
    }

    public BulkRequest add(IndexRequest request) {
        return internalAdd(request, null);
    }

    public BulkRequest add(IndexRequest request, @Nullable Object payload) {
        return internalAdd(request, payload);
    }

    BulkRequest internalAdd(IndexRequest request, @Nullable Object payload) {
        Objects.requireNonNull(request, "'request' must not be null");
        requests.add(request);
        addPayload(payload);
        sizeInBytes += (request.source() != null ? request.source().length() : 0) + REQUEST_OVERHEAD;
        return this;
    }

    public BulkRequest add(UpdateRequest request) {
        return internalAdd(request, null);
    }

    public BulkRequest add(UpdateRequest request, @Nullable Object payload) {
        return internalAdd(request, payload);
    }

    BulkRequest internalAdd(UpdateRequest request, @Nullable Object payload) {
        Objects.requireNonNull(request, "'request' must not be null");
        requests.add(request);
        addPayload(payload);
        if (request.doc() != null) {
            sizeInBytes += request.doc().source().length();
        }
        if (request.upsertRequest() != null) {
            sizeInBytes += request.upsertRequest().source().length();
        }
        if (request.script() != null) {
            sizeInBytes += request.script().getScript().length() * 2;
        }
        return this;
    }

    public BulkRequest add(DeleteRequest request) {
        return add(request, null);
    }

    public BulkRequest add(DeleteRequest request, @Nullable Object payload) {
        Objects.requireNonNull(request, "'request' must not be null");
        requests.add(request);
        addPayload(payload);
        sizeInBytes += REQUEST_OVERHEAD;
        return this;
    }

    private void addPayload(Object payload) {
        if (payloads == null) {
            if (payload == null) {
                return;
            }
            payloads = new ArrayList<>(requests.size() + 10);
            for (int i = 1; i < requests.size(); i++) {
                payloads.add(null);
            }
        }
        payloads.add(payload);
    }

    public List<DocumentRequest<?>> requests() {
        return this.requests;
    }

    @Override
    public List<? extends IndicesRequest> subRequests() {
        return requests.stream().collect(Collectors.toList());
    }

    @Nullable
    public List<Object> payloads() {
        return this.payloads;
    }

    public int numberOfActions() {
        return requests.size();
    }

    public long estimatedSizeInBytes() {
        return sizeInBytes;
    }

    public BulkRequest add(byte[] data, int from, int length) throws Exception {
        return add(data, from, length, null, null);
    }

    public BulkRequest add(byte[] data, int from, int length, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        return add(new BytesArray(data, from, length), defaultIndex, defaultType);
    }

    public BulkRequest add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        return add(data, defaultIndex, defaultType, null, null, null, null, null, true);
    }

    public BulkRequest add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType, boolean allowExplicitIndex) throws Exception {
        return add(data, defaultIndex, defaultType, null, null, null, null, null, allowExplicitIndex);
    }

    public BulkRequest add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType, @Nullable String defaultRouting, @Nullable String[] defaultFields, @Nullable FetchSourceContext defaultFetchSourceContext, @Nullable String defaultPipeline, @Nullable Object payload, boolean allowExplicitIndex) throws Exception {
        XContent xContent = XContentFactory.xContent(data);
        int line = 0;
        int from = 0;
        int length = data.length();
        byte marker = xContent.streamSeparator();
        while (true) {
            int nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }
            line++;
            try (XContentParser parser = xContent.createParser(data.slice(from, nextMarker - from))) {
                from = nextMarker + 1;
                XContentParser.Token token = parser.nextToken();
                if (token == null) {
                    continue;
                }
                assert token == XContentParser.Token.START_OBJECT;
                token = parser.nextToken();
                assert token == XContentParser.Token.FIELD_NAME;
                String action = parser.currentName();
                String index = defaultIndex;
                String type = defaultType;
                String id = null;
                String routing = defaultRouting;
                String parent = null;
                FetchSourceContext fetchSourceContext = defaultFetchSourceContext;
                String[] fields = defaultFields;
                String timestamp = null;
                TimeValue ttl = null;
                String opType = null;
                long version = Versions.MATCH_ANY;
                VersionType versionType = VersionType.INTERNAL;
                int retryOnConflict = 0;
                String pipeline = defaultPipeline;
                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    String currentFieldName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if ("_index".equals(currentFieldName)) {
                                if (!allowExplicitIndex) {
                                    throw new IllegalArgumentException("explicit index in bulk is not allowed");
                                }
                                index = parser.text();
                            } else if ("_type".equals(currentFieldName)) {
                                type = parser.text();
                            } else if ("_id".equals(currentFieldName)) {
                                id = parser.text();
                            } else if ("_routing".equals(currentFieldName) || "routing".equals(currentFieldName)) {
                                routing = parser.text();
                            } else if ("_parent".equals(currentFieldName) || "parent".equals(currentFieldName)) {
                                parent = parser.text();
                            } else if ("_timestamp".equals(currentFieldName) || "timestamp".equals(currentFieldName)) {
                                timestamp = parser.text();
                            } else if ("_ttl".equals(currentFieldName) || "ttl".equals(currentFieldName)) {
                                if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                                    ttl = TimeValue.parseTimeValue(parser.text(), null, currentFieldName);
                                } else {
                                    ttl = new TimeValue(parser.longValue());
                                }
                            } else if ("op_type".equals(currentFieldName) || "opType".equals(currentFieldName)) {
                                opType = parser.text();
                            } else if ("_version".equals(currentFieldName) || "version".equals(currentFieldName)) {
                                version = parser.longValue();
                            } else if ("_version_type".equals(currentFieldName) || "_versionType".equals(currentFieldName) || "version_type".equals(currentFieldName) || "versionType".equals(currentFieldName)) {
                                versionType = VersionType.fromString(parser.text());
                            } else if ("_retry_on_conflict".equals(currentFieldName) || "_retryOnConflict".equals(currentFieldName)) {
                                retryOnConflict = parser.intValue();
                            } else if ("pipeline".equals(currentFieldName)) {
                                pipeline = parser.text();
                            } else if ("fields".equals(currentFieldName)) {
                                throw new IllegalArgumentException("Action/metadata line [" + line + "] contains a simple value for parameter [fields] while a list is expected");
                            } else if ("_source".equals(currentFieldName)) {
                                fetchSourceContext = FetchSourceContext.parse(parser);
                            } else {
                                throw new IllegalArgumentException("Action/metadata line [" + line + "] contains an unknown parameter [" + currentFieldName + "]");
                            }
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            if ("fields".equals(currentFieldName)) {
                                DEPRECATION_LOGGER.deprecated("Deprecated field [fields] used, expected [_source] instead");
                                List<Object> values = parser.list();
                                fields = values.toArray(new String[values.size()]);
                            } else {
                                throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected a simple value for field [" + currentFieldName + "] but found [" + token + "]");
                            }
                        } else if (token == XContentParser.Token.START_OBJECT && "_source".equals(currentFieldName)) {
                            fetchSourceContext = FetchSourceContext.parse(parser);
                        } else if (token != XContentParser.Token.VALUE_NULL) {
                            throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected a simple value for field [" + currentFieldName + "] but found [" + token + "]");
                        }
                    }
                } else if (token != XContentParser.Token.END_OBJECT) {
                    throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected " + XContentParser.Token.START_OBJECT + " or " + XContentParser.Token.END_OBJECT + " but found [" + token + "]");
                }
                if ("delete".equals(action)) {
                    add(new DeleteRequest(index, type, id).routing(routing).parent(parent).version(version).versionType(versionType), payload);
                } else {
                    nextMarker = findNextMarker(marker, from, data, length);
                    if (nextMarker == -1) {
                        break;
                    }
                    line++;
                    if ("index".equals(action)) {
                        if (opType == null) {
                            internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).timestamp(timestamp).ttl(ttl).version(version).versionType(versionType).setPipeline(pipeline).source(data.slice(from, nextMarker - from)), payload);
                        } else {
                            internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).timestamp(timestamp).ttl(ttl).version(version).versionType(versionType).create("create".equals(opType)).setPipeline(pipeline).source(data.slice(from, nextMarker - from)), payload);
                        }
                    } else if ("create".equals(action)) {
                        internalAdd(new IndexRequest(index, type, id).routing(routing).parent(parent).timestamp(timestamp).ttl(ttl).version(version).versionType(versionType).create(true).setPipeline(pipeline).source(data.slice(from, nextMarker - from)), payload);
                    } else if ("update".equals(action)) {
                        UpdateRequest updateRequest = new UpdateRequest(index, type, id).routing(routing).parent(parent).retryOnConflict(retryOnConflict).version(version).versionType(versionType).routing(routing).parent(parent).fromXContent(data.slice(from, nextMarker - from));
                        if (fetchSourceContext != null) {
                            updateRequest.fetchSource(fetchSourceContext);
                        }
                        if (fields != null) {
                            updateRequest.fields(fields);
                        }
                        IndexRequest upsertRequest = updateRequest.upsertRequest();
                        if (upsertRequest != null) {
                            upsertRequest.timestamp(timestamp);
                            upsertRequest.ttl(ttl);
                            upsertRequest.version(version);
                            upsertRequest.versionType(versionType);
                        }
                        IndexRequest doc = updateRequest.doc();
                        if (doc != null) {
                            doc.timestamp(timestamp);
                            doc.ttl(ttl);
                            doc.version(version);
                            doc.versionType(versionType);
                        }
                        internalAdd(updateRequest, payload);
                    }
                    from = nextMarker + 1;
                }
            }
        }
        return this;
    }

    public BulkRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    public BulkRequest waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    public ActiveShardCount waitForActiveShards() {
        return this.waitForActiveShards;
    }

    @Override
    public BulkRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public final BulkRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public final BulkRequest timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout"));
    }

    public TimeValue timeout() {
        return timeout;
    }

    private int findNextMarker(byte marker, int from, BytesReference data, int length) {
        for (int i = from; i < length; i++) {
            if (data.get(i) == marker) {
                return i;
            }
        }
        return -1;
    }

    public boolean hasIndexRequestsWithPipelines() {
        for (DocumentRequest<?> actionRequest : requests) {
            if (actionRequest instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) actionRequest;
                if (Strings.hasText(indexRequest.getPipeline())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        for (DocumentRequest<?> request : requests) {
            if (((WriteRequest<?>) request).getRefreshPolicy() != RefreshPolicy.NONE) {
                validationException = addValidationError("RefreshPolicy is not supported on an item request. Set it on the BulkRequest instead.", validationException);
            }
            ActionRequestValidationException ex = ((WriteRequest<?>) request).validate();
            if (ex != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(ex.validationErrors());
            }
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        waitForActiveShards = ActiveShardCount.readFrom(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            byte type = in.readByte();
            if (type == 0) {
                IndexRequest request = new IndexRequest();
                request.readFrom(in);
                requests.add(request);
            } else if (type == 1) {
                DeleteRequest request = new DeleteRequest();
                request.readFrom(in);
                requests.add(request);
            } else if (type == 2) {
                UpdateRequest request = new UpdateRequest();
                request.readFrom(in);
                requests.add(request);
            }
        }
        refreshPolicy = RefreshPolicy.readFrom(in);
        timeout = new TimeValue(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        waitForActiveShards.writeTo(out);
        out.writeVInt(requests.size());
        for (DocumentRequest<?> request : requests) {
            if (request instanceof IndexRequest) {
                out.writeByte((byte) 0);
                ((IndexRequest) request).writeTo(out);
            } else if (request instanceof DeleteRequest) {
                out.writeByte((byte) 1);
                ((DeleteRequest) request).writeTo(out);
            } else if (request instanceof UpdateRequest) {
                out.writeByte((byte) 2);
                ((UpdateRequest) request).writeTo(out);
            }
        }
        refreshPolicy.writeTo(out);
        timeout.writeTo(out);
    }
}