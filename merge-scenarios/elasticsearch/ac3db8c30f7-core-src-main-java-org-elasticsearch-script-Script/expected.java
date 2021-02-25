package org.elasticsearch.script;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class Script implements ToXContentObject, Writeable {

    private static final Logger LOGGER = ESLoggerFactory.getLogger(ScriptMetaData.class);

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(LOGGER);

    public static final String DEFAULT_SCRIPT_LANG = "painless";

    public static final String DEFAULT_TEMPLATE_LANG = "mustache";

    public static final ScriptType DEFAULT_SCRIPT_TYPE = ScriptType.INLINE;

    public static final String CONTENT_TYPE_OPTION = "content_type";

    public static final ParseField SCRIPT_PARSE_FIELD = new ParseField("script");

    public static final ParseField SOURCE_PARSE_FIELD = new ParseField("source");

    public static final ParseField LANG_PARSE_FIELD = new ParseField("lang");

    public static final ParseField OPTIONS_PARSE_FIELD = new ParseField("options");

    public static final ParseField PARAMS_PARSE_FIELD = new ParseField("params");

    private static final class Builder {

        private ScriptType type;

        private String lang;

        private String idOrCode;

        private Map<String, String> options;

        private Map<String, Object> params;

        private Builder() {
            this.options = new HashMap<>();
            this.params = Collections.emptyMap();
        }

        private void setInline(XContentParser parser) {
            try {
                if (type != null) {
                    throwOnlyOneOfType();
                }
                type = ScriptType.INLINE;
                if (parser.currentToken() == Token.START_OBJECT) {
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    idOrCode = builder.copyCurrentStructure(parser).string();
                    options.put(CONTENT_TYPE_OPTION, XContentType.JSON.mediaType());
                } else {
                    idOrCode = parser.text();
                }
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        }

        private void setStored(String idOrCode) {
            if (type != null) {
                throwOnlyOneOfType();
            }
            type = ScriptType.STORED;
            this.idOrCode = idOrCode;
        }

        private void throwOnlyOneOfType() {
            throw new IllegalArgumentException("must only use one of [" + ScriptType.INLINE.getParseField().getPreferredName() + ", " + ScriptType.STORED.getParseField().getPreferredName() + "]" + " when specifying a script");
        }

        private void setLang(String lang) {
            this.lang = lang;
        }

        private void setOptions(Map<String, String> options) {
            this.options.putAll(options);
        }

        private void setParams(Map<String, Object> params) {
            this.params = params;
        }

        private Script build(String defaultLang) {
            if (type == null) {
                throw new IllegalArgumentException("must specify either [source] for an inline script or [id] for a stored script");
            }
            if (type == ScriptType.INLINE) {
                if (lang == null) {
                    lang = defaultLang;
                }
                if (idOrCode == null) {
                    throw new IllegalArgumentException("must specify <id> for an [" + ScriptType.INLINE.getParseField().getPreferredName() + "] script");
                }
                if (options.size() > 1 || options.size() == 1 && options.get(CONTENT_TYPE_OPTION) == null) {
                    options.remove(CONTENT_TYPE_OPTION);
                    throw new IllegalArgumentException("illegal compiler options [" + options + "] specified");
                }
            } else if (type == ScriptType.STORED) {
                if (lang != null && lang.equals(DEFAULT_TEMPLATE_LANG) == false) {
                    DEPRECATION_LOGGER.deprecated("specifying the field [" + LANG_PARSE_FIELD.getPreferredName() + "] " + "for executing " + ScriptType.STORED + " scripts is deprecated; use only the field " + "[" + ScriptType.STORED.getParseField().getPreferredName() + "] to specify an <id>");
                }
                if (idOrCode == null) {
                    throw new IllegalArgumentException("must specify <code> for an [" + ScriptType.STORED.getParseField().getPreferredName() + "] script");
                }
                if (options.isEmpty()) {
                    options = null;
                } else {
                    throw new IllegalArgumentException("field [" + OPTIONS_PARSE_FIELD.getPreferredName() + "] " + "cannot be specified using a [" + ScriptType.STORED.getParseField().getPreferredName() + "] script");
                }
            }
            return new Script(type, lang, idOrCode, options, params);
        }
    }

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("script", Builder::new);

    static {
        PARSER.declareField(Builder::setInline, parser -> parser, ScriptType.INLINE.getParseField(), ValueType.OBJECT_OR_STRING);
        PARSER.declareString(Builder::setStored, ScriptType.STORED.getParseField());
        PARSER.declareString(Builder::setLang, LANG_PARSE_FIELD);
        PARSER.declareField(Builder::setOptions, XContentParser::mapStrings, OPTIONS_PARSE_FIELD, ValueType.OBJECT);
        PARSER.declareField(Builder::setParams, XContentParser::map, PARAMS_PARSE_FIELD, ValueType.OBJECT);
    }

    public static Script parse(XContentParser parser) throws IOException {
        return parse(parser, DEFAULT_SCRIPT_LANG);
    }

    public static Script parse(XContentParser parser, String defaultLang) throws IOException {
        Objects.requireNonNull(defaultLang);
        Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == Token.VALUE_STRING) {
            return new Script(ScriptType.INLINE, defaultLang, parser.text(), Collections.emptyMap());
        }
        return PARSER.apply(parser, null).build(defaultLang);
    }

    private final ScriptType type;

    private final String lang;

    private final String idOrCode;

    private final Map<String, String> options;

    private final Map<String, Object> params;

    public Script(String idOrCode) {
        this(DEFAULT_SCRIPT_TYPE, DEFAULT_SCRIPT_LANG, idOrCode, Collections.emptyMap(), Collections.emptyMap());
    }

    public Script(ScriptType type, String lang, String idOrCode, Map<String, Object> params) {
        this(type, lang, idOrCode, type == ScriptType.INLINE ? Collections.emptyMap() : null, params);
    }

    public Script(ScriptType type, String lang, String idOrCode, Map<String, String> options, Map<String, Object> params) {
        this.type = Objects.requireNonNull(type);
        this.idOrCode = Objects.requireNonNull(idOrCode);
        this.params = Collections.unmodifiableMap(Objects.requireNonNull(params));
        if (type == ScriptType.INLINE) {
            this.lang = Objects.requireNonNull(lang);
            this.options = Collections.unmodifiableMap(Objects.requireNonNull(options));
        } else if (type == ScriptType.STORED) {
            this.lang = lang;
            if (options != null) {
                throw new IllegalStateException("options must be null for [" + ScriptType.STORED.getParseField().getPreferredName() + "] scripts");
            }
            this.options = null;
        } else {
            throw new IllegalStateException("unknown script type [" + type.getName() + "]");
        }
    }

    public Script(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_5_3_0)) {
            this.type = ScriptType.readFrom(in);
            this.lang = in.readOptionalString();
            this.idOrCode = in.readString();
            @SuppressWarnings("unchecked")
            Map<String, String> options = (Map) in.readMap();
            this.options = options;
            this.params = in.readMap();
        } else if (in.getVersion().onOrAfter(Version.V_5_1_1)) {
            this.type = ScriptType.readFrom(in);
            this.lang = in.readString();
            this.idOrCode = in.readString();
            @SuppressWarnings("unchecked")
            Map<String, String> options = (Map) in.readMap();
            if (this.type != ScriptType.INLINE && options.isEmpty()) {
                this.options = null;
            } else {
                this.options = options;
            }
            this.params = in.readMap();
        } else {
            this.idOrCode = in.readString();
            if (in.readBoolean()) {
                this.type = ScriptType.readFrom(in);
            } else {
                this.type = DEFAULT_SCRIPT_TYPE;
            }
            String lang = in.readOptionalString();
            if (lang == null) {
                this.lang = DEFAULT_SCRIPT_LANG;
            } else {
                this.lang = lang;
            }
            Map<String, Object> params = in.readMap();
            if (params == null) {
                this.params = new HashMap<>();
            } else {
                this.params = params;
            }
            if (in.readBoolean()) {
                this.options = new HashMap<>();
                XContentType contentType = XContentType.readFrom(in);
                this.options.put(CONTENT_TYPE_OPTION, contentType.mediaType());
            } else if (type == ScriptType.INLINE) {
                options = new HashMap<>();
            } else {
                this.options = null;
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
            type.writeTo(out);
            out.writeOptionalString(lang);
            out.writeString(idOrCode);
            @SuppressWarnings("unchecked")
            Map<String, Object> options = (Map) this.options;
            out.writeMap(options);
            out.writeMap(params);
        } else if (out.getVersion().onOrAfter(Version.V_5_1_1)) {
            type.writeTo(out);
            if (lang == null) {
                out.writeString("");
            } else {
                out.writeString(lang);
            }
            out.writeString(idOrCode);
            @SuppressWarnings("unchecked")
            Map<String, Object> options = (Map) this.options;
            if (options == null) {
                out.writeMap(new HashMap<>());
            } else {
                out.writeMap(options);
            }
            out.writeMap(params);
        } else {
            out.writeString(idOrCode);
            out.writeBoolean(true);
            type.writeTo(out);
            out.writeOptionalString(lang);
            if (params.isEmpty()) {
                out.writeMap(null);
            } else {
                out.writeMap(params);
            }
            if (options != null && options.containsKey(CONTENT_TYPE_OPTION)) {
                XContentType contentType = XContentType.fromMediaTypeOrFormat(options.get(CONTENT_TYPE_OPTION));
                out.writeBoolean(true);
                contentType.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params builderParams) throws IOException {
        builder.startObject();
        String contentType = options == null ? null : options.get(CONTENT_TYPE_OPTION);
        if (type == ScriptType.INLINE) {
            if (contentType != null && builder.contentType().mediaType().equals(contentType)) {
                builder.rawField(SOURCE_PARSE_FIELD.getPreferredName(), new BytesArray(idOrCode));
            } else {
                builder.field(SOURCE_PARSE_FIELD.getPreferredName(), idOrCode);
            }
        } else {
            builder.field("id", idOrCode);
        }
        if (lang != null) {
            builder.field(LANG_PARSE_FIELD.getPreferredName(), lang);
        }
        if (options != null && !options.isEmpty()) {
            builder.field(OPTIONS_PARSE_FIELD.getPreferredName(), options);
        }
        if (!params.isEmpty()) {
            builder.field(PARAMS_PARSE_FIELD.getPreferredName(), params);
        }
        builder.endObject();
        return builder;
    }

    public ScriptType getType() {
        return type;
    }

    public String getLang() {
        return lang;
    }

    public String getIdOrCode() {
        return idOrCode;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Script script = (Script) o;
        if (type != script.type)
            return false;
        if (lang != null ? !lang.equals(script.lang) : script.lang != null)
            return false;
        if (!idOrCode.equals(script.idOrCode))
            return false;
        if (options != null ? !options.equals(script.options) : script.options != null)
            return false;
        return params.equals(script.params);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (lang != null ? lang.hashCode() : 0);
        result = 31 * result + idOrCode.hashCode();
        result = 31 * result + (options != null ? options.hashCode() : 0);
        result = 31 * result + params.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Script{" + "type=" + type + ", lang='" + lang + '\'' + ", idOrCode='" + idOrCode + '\'' + ", options=" + options + ", params=" + params + '}';
    }
}