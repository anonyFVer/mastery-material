package org.elasticsearch.action.update;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.single.instance.InstanceShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.elasticsearch.action.ValidateActions.addValidationError;

public class UpdateRequest extends InstanceShardOperationRequest<UpdateRequest> implements DocumentRequest<UpdateRequest>, WriteRequest<UpdateRequest> {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(UpdateRequest.class));

    private String type;

    private String id;

    @Nullable
    private String routing;

    @Nullable
    private String parent;

    @Nullable
    Script script;

    private String[] fields;

    private FetchSourceContext fetchSourceContext;

    private long version = Versions.MATCH_ANY;

    private VersionType versionType = VersionType.INTERNAL;

    private int retryOnConflict = 0;

    private RefreshPolicy refreshPolicy = RefreshPolicy.NONE;

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    private IndexRequest upsertRequest;

    private boolean scriptedUpsert = false;

    private boolean docAsUpsert = false;

    private boolean detectNoop = true;

    @Nullable
    private IndexRequest doc;

    public UpdateRequest() {
    }

    public UpdateRequest(String index, String type, String id) {
        super(index);
        this.type = type;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (id == null) {
            validationException = addValidationError("id is missing", validationException);
        }
        if (versionType != VersionType.INTERNAL) {
            validationException = addValidationError("version type [" + versionType + "] is not supported by the update API", validationException);
        } else {
            if (version != Versions.MATCH_ANY && retryOnConflict > 0) {
                validationException = addValidationError("can't provide both retry_on_conflict and a specific version", validationException);
            }
            if (!versionType.validateVersionForWrites(version)) {
                validationException = addValidationError("illegal version value [" + version + "] for version type [" + versionType.name() + "]", validationException);
            }
        }
        if (script == null && doc == null) {
            validationException = addValidationError("script or doc is missing", validationException);
        }
        if (script != null && doc != null) {
            validationException = addValidationError("can't provide both script and doc", validationException);
        }
        if (doc == null && docAsUpsert) {
            validationException = addValidationError("doc must be specified if doc_as_upsert is enabled", validationException);
        }
        return validationException;
    }

    @Override
    public String type() {
        return type;
    }

    public UpdateRequest type(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String id() {
        return id;
    }

    public UpdateRequest id(String id) {
        this.id = id;
        return this;
    }

    @Override
    public UpdateRequest routing(String routing) {
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

    public UpdateRequest parent(String parent) {
        this.parent = parent;
        return this;
    }

    public String parent() {
        return parent;
    }

    public ShardId getShardId() {
        return this.shardId;
    }

    public Script script() {
        return this.script;
    }

    public UpdateRequest script(Script script) {
        this.script = script;
        return this;
    }

    @Deprecated
    public String scriptString() {
        return this.script == null ? null : this.script.getScript();
    }

    @Deprecated
    public ScriptService.ScriptType scriptType() {
        return this.script == null ? null : this.script.getType();
    }

    @Deprecated
    public Map<String, Object> scriptParams() {
        return this.script == null ? null : this.script.getParams();
    }

    @Deprecated
    public UpdateRequest script(String script, ScriptService.ScriptType scriptType) {
        updateOrCreateScript(script, scriptType, null, null);
        return this;
    }

    @Deprecated
    public UpdateRequest script(String script) {
        updateOrCreateScript(script, ScriptType.INLINE, null, null);
        return this;
    }

    @Deprecated
    public UpdateRequest scriptLang(String scriptLang) {
        updateOrCreateScript(null, null, scriptLang, null);
        return this;
    }

    @Deprecated
    public String scriptLang() {
        return script == null ? null : script.getLang();
    }

    @Deprecated
    public UpdateRequest addScriptParam(String name, Object value) {
        Script script = script();
        if (script == null) {
            HashMap<String, Object> scriptParams = new HashMap<>();
            scriptParams.put(name, value);
            updateOrCreateScript(null, null, null, scriptParams);
        } else {
            Map<String, Object> scriptParams = script.getParams();
            if (scriptParams == null) {
                scriptParams = new HashMap<>();
                scriptParams.put(name, value);
                updateOrCreateScript(null, null, null, scriptParams);
            } else {
                scriptParams.put(name, value);
            }
        }
        return this;
    }

    @Deprecated
    public UpdateRequest scriptParams(Map<String, Object> scriptParams) {
        updateOrCreateScript(null, null, null, scriptParams);
        return this;
    }

    private void updateOrCreateScript(String scriptContent, ScriptType type, String lang, Map<String, Object> params) {
        Script script = script();
        if (script == null) {
            script = new Script(scriptContent == null ? "" : scriptContent, type == null ? ScriptType.INLINE : type, lang, params);
        } else {
            String newScriptContent = scriptContent == null ? script.getScript() : scriptContent;
            ScriptType newScriptType = type == null ? script.getType() : type;
            String newScriptLang = lang == null ? script.getLang() : lang;
            Map<String, Object> newScriptParams = params == null ? script.getParams() : params;
            script = new Script(newScriptContent, newScriptType, newScriptLang, newScriptParams);
        }
        script(script);
    }

    @Deprecated
    public UpdateRequest script(String script, ScriptService.ScriptType scriptType, @Nullable Map<String, Object> scriptParams) {
        this.script = new Script(script, scriptType, null, scriptParams);
        return this;
    }

    @Deprecated
    public UpdateRequest script(String script, @Nullable String scriptLang, ScriptService.ScriptType scriptType, @Nullable Map<String, Object> scriptParams) {
        this.script = new Script(script, scriptType, scriptLang, scriptParams);
        return this;
    }

    @Deprecated
    public UpdateRequest fields(String... fields) {
        this.fields = fields;
        return this;
    }

    public UpdateRequest fetchSource(@Nullable String include, @Nullable String exclude) {
        this.fetchSourceContext = new FetchSourceContext(include, exclude);
        return this;
    }

    public UpdateRequest fetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        this.fetchSourceContext = new FetchSourceContext(includes, excludes);
        return this;
    }

    public UpdateRequest fetchSource(boolean fetchSource) {
        this.fetchSourceContext = new FetchSourceContext(fetchSource);
        return this;
    }

    public UpdateRequest fetchSource(FetchSourceContext context) {
        this.fetchSourceContext = context;
        return this;
    }

    @Deprecated
    public String[] fields() {
        return fields;
    }

    public FetchSourceContext fetchSource() {
        return fetchSourceContext;
    }

    public UpdateRequest retryOnConflict(int retryOnConflict) {
        this.retryOnConflict = retryOnConflict;
        return this;
    }

    public int retryOnConflict() {
        return this.retryOnConflict;
    }

    public UpdateRequest version(long version) {
        this.version = version;
        return this;
    }

    public long version() {
        return this.version;
    }

    public UpdateRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public VersionType versionType() {
        return this.versionType;
    }

    @Override
    public UpdateRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public ActiveShardCount waitForActiveShards() {
        return this.waitForActiveShards;
    }

    public UpdateRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    public UpdateRequest waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    public UpdateRequest doc(IndexRequest doc) {
        this.doc = doc;
        return this;
    }

    public UpdateRequest doc(XContentBuilder source) {
        safeDoc().source(source);
        return this;
    }

    public UpdateRequest doc(Map source) {
        safeDoc().source(source);
        return this;
    }

    public UpdateRequest doc(Map source, XContentType contentType) {
        safeDoc().source(source, contentType);
        return this;
    }

    public UpdateRequest doc(String source) {
        safeDoc().source(source);
        return this;
    }

    public UpdateRequest doc(byte[] source) {
        safeDoc().source(source);
        return this;
    }

    public UpdateRequest doc(byte[] source, int offset, int length) {
        safeDoc().source(source, offset, length);
        return this;
    }

    public UpdateRequest doc(Object... source) {
        safeDoc().source(source);
        return this;
    }

    public UpdateRequest doc(String field, Object value) {
        safeDoc().source(field, value);
        return this;
    }

    public IndexRequest doc() {
        return this.doc;
    }

    private IndexRequest safeDoc() {
        if (doc == null) {
            doc = new IndexRequest();
        }
        return doc;
    }

    public UpdateRequest upsert(IndexRequest upsertRequest) {
        this.upsertRequest = upsertRequest;
        return this;
    }

    public UpdateRequest upsert(XContentBuilder source) {
        safeUpsertRequest().source(source);
        return this;
    }

    public UpdateRequest upsert(Map source) {
        safeUpsertRequest().source(source);
        return this;
    }

    public UpdateRequest upsert(Map source, XContentType contentType) {
        safeUpsertRequest().source(source, contentType);
        return this;
    }

    public UpdateRequest upsert(String source) {
        safeUpsertRequest().source(source);
        return this;
    }

    public UpdateRequest upsert(byte[] source) {
        safeUpsertRequest().source(source);
        return this;
    }

    public UpdateRequest upsert(byte[] source, int offset, int length) {
        safeUpsertRequest().source(source, offset, length);
        return this;
    }

    public UpdateRequest upsert(Object... source) {
        safeUpsertRequest().source(source);
        return this;
    }

    public IndexRequest upsertRequest() {
        return this.upsertRequest;
    }

    private IndexRequest safeUpsertRequest() {
        if (upsertRequest == null) {
            upsertRequest = new IndexRequest();
        }
        return upsertRequest;
    }

    public UpdateRequest fromXContent(XContentBuilder source) throws Exception {
        return fromXContent(source.bytes());
    }

    public UpdateRequest fromXContent(byte[] source) throws Exception {
        return fromXContent(source, 0, source.length);
    }

    public UpdateRequest fromXContent(byte[] source, int offset, int length) throws Exception {
        return fromXContent(new BytesArray(source, offset, length));
    }

    public UpdateRequest detectNoop(boolean detectNoop) {
        this.detectNoop = detectNoop;
        return this;
    }

    public boolean detectNoop() {
        return detectNoop;
    }

    public UpdateRequest fromXContent(BytesReference source) throws Exception {
        Script script = null;
        try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                return this;
            }
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("script".equals(currentFieldName)) {
                    script = Script.parse(parser, ParseFieldMatcher.EMPTY);
                } else if ("scripted_upsert".equals(currentFieldName)) {
                    scriptedUpsert = parser.booleanValue();
                } else if ("upsert".equals(currentFieldName)) {
                    XContentType xContentType = XContentFactory.xContentType(source);
                    XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
                    builder.copyCurrentStructure(parser);
                    safeUpsertRequest().source(builder);
                } else if ("doc".equals(currentFieldName)) {
                    XContentType xContentType = XContentFactory.xContentType(source);
                    XContentBuilder docBuilder = XContentFactory.contentBuilder(xContentType);
                    docBuilder.copyCurrentStructure(parser);
                    safeDoc().source(docBuilder);
                } else if ("doc_as_upsert".equals(currentFieldName)) {
                    docAsUpsert(parser.booleanValue());
                } else if ("detect_noop".equals(currentFieldName)) {
                    detectNoop(parser.booleanValue());
                } else if ("fields".equals(currentFieldName)) {
                    List<Object> fields = null;
                    if (token == XContentParser.Token.START_ARRAY) {
                        fields = (List) parser.list();
                    } else if (token.isValue()) {
                        fields = Collections.singletonList(parser.text());
                    }
                    if (fields != null) {
                        fields(fields.toArray(new String[fields.size()]));
                    }
                } else if ("_source".equals(currentFieldName)) {
                    fetchSourceContext = FetchSourceContext.parse(parser);
                }
            }
            if (script != null) {
                this.script = script;
            }
        }
        return this;
    }

    public boolean docAsUpsert() {
        return this.docAsUpsert;
    }

    public UpdateRequest docAsUpsert(boolean shouldUpsertDoc) {
        this.docAsUpsert = shouldUpsertDoc;
        return this;
    }

    public boolean scriptedUpsert() {
        return this.scriptedUpsert;
    }

    public UpdateRequest scriptedUpsert(boolean scriptedUpsert) {
        this.scriptedUpsert = scriptedUpsert;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        waitForActiveShards = ActiveShardCount.readFrom(in);
        type = in.readString();
        id = in.readString();
        routing = in.readOptionalString();
        parent = in.readOptionalString();
        if (in.readBoolean()) {
            script = new Script(in);
        }
        retryOnConflict = in.readVInt();
        refreshPolicy = RefreshPolicy.readFrom(in);
        if (in.readBoolean()) {
            doc = new IndexRequest();
            doc.readFrom(in);
        }
        fields = in.readOptionalStringArray();
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
        if (in.readBoolean()) {
            upsertRequest = new IndexRequest();
            upsertRequest.readFrom(in);
        }
        docAsUpsert = in.readBoolean();
        version = in.readLong();
        versionType = VersionType.fromValue(in.readByte());
        detectNoop = in.readBoolean();
        scriptedUpsert = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        waitForActiveShards.writeTo(out);
        out.writeString(type);
        out.writeString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(parent);
        boolean hasScript = script != null;
        out.writeBoolean(hasScript);
        if (hasScript) {
            script.writeTo(out);
        }
        out.writeVInt(retryOnConflict);
        refreshPolicy.writeTo(out);
        if (doc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            doc.index(index);
            doc.type(type);
            doc.id(id);
            doc.writeTo(out);
        }
        out.writeOptionalStringArray(fields);
        out.writeOptionalWriteable(fetchSourceContext);
        if (upsertRequest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            upsertRequest.index(index);
            upsertRequest.type(type);
            upsertRequest.id(id);
            upsertRequest.writeTo(out);
        }
        out.writeBoolean(docAsUpsert);
        out.writeLong(version);
        out.writeByte(versionType.getValue());
        out.writeBoolean(detectNoop);
        out.writeBoolean(scriptedUpsert);
    }
}