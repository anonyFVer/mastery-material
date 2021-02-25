package org.elasticsearch.script;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryParseContext;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class Script implements ToXContent, Writeable {

    public static final String DEFAULT_SCRIPT_LANG = "painless";

    public static final String DEFAULT_TEMPLATE_LANG = "mustache";

    public static final ScriptType DEFAULT_SCRIPT_TYPE = ScriptType.INLINE;

    public static final String CONTENT_TYPE_OPTION = "content_type";

    public static final ParseField SCRIPT_PARSE_FIELD = new ParseField("script");

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
                    XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType());
                    idOrCode = builder.copyCurrentStructure(parser).bytes().utf8ToString();
                    options.put(CONTENT_TYPE_OPTION, parser.contentType().mediaType());
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

        private void setFile(String idOrCode) {
            if (type != null) {
                throwOnlyOneOfType();
            }
            type = ScriptType.FILE;
            this.idOrCode = idOrCode;
        }

        private void throwOnlyOneOfType() {
            throw new IllegalArgumentException("must only use one of [" + ScriptType.INLINE.getParseField().getPreferredName() + " + , " + ScriptType.STORED.getParseField().getPreferredName() + " + , " + ScriptType.FILE.getParseField().getPreferredName() + "]" + " when specifying a script");
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
                throw new IllegalArgumentException("must specify either code for an [" + ScriptType.INLINE.getParseField().getPreferredName() + "] script " + "or an id for a [" + ScriptType.STORED.getParseField().getPreferredName() + "] script " + "or [" + ScriptType.FILE.getParseField().getPreferredName() + "] script");
            }
            if (idOrCode == null) {
                throw new IllegalArgumentException("must specify an id or code for a script");
            }
            if (options.size() > 1 || options.size() == 1 && options.get(CONTENT_TYPE_OPTION) == null) {
                throw new IllegalArgumentException("illegal compiler options [" + options + "] specified");
            }
            return new Script(type, this.lang == null ? defaultLang : this.lang, idOrCode, options, params);
        }
    }

    private static final ObjectParser<Builder, ParseFieldMatcherSupplier> PARSER = new ObjectParser<>("script", Builder::new);

    static {
        PARSER.declareField(Builder::setInline, parser -> parser, ScriptType.INLINE.getParseField(), ValueType.OBJECT_OR_STRING);
        PARSER.declareString(Builder::setStored, ScriptType.STORED.getParseField());
        PARSER.declareString(Builder::setFile, ScriptType.FILE.getParseField());
        PARSER.declareString(Builder::setLang, LANG_PARSE_FIELD);
        PARSER.declareField(Builder::setOptions, XContentParser::mapStrings, OPTIONS_PARSE_FIELD, ValueType.OBJECT);
        PARSER.declareField(Builder::setParams, XContentParser::map, PARAMS_PARSE_FIELD, ValueType.OBJECT);
    }

    public static Script parse(XContentParser parser, ParseFieldMatcher matcher) throws IOException {
        return parse(parser, matcher, DEFAULT_SCRIPT_LANG);
    }

    public static Script parse(XContentParser parser, QueryParseContext context) throws IOException {
        return parse(parser, context.getParseFieldMatcher(), context.getDefaultScriptLanguage());
    }

    public static Script parse(XContentParser parser, ParseFieldMatcher matcher, String defaultLang) throws IOException {
        Objects.requireNonNull(defaultLang);
        Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == Token.VALUE_STRING) {
            return new Script(ScriptType.INLINE, defaultLang, parser.text(), Collections.emptyMap());
        }
        return PARSER.apply(parser, () -> matcher).build(defaultLang);
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
        this(type, lang, idOrCode, Collections.emptyMap(), params);
    }

    public Script(ScriptType type, String lang, String idOrCode, Map<String, String> options, Map<String, Object> params) {
        this.idOrCode = Objects.requireNonNull(idOrCode);
        this.type = Objects.requireNonNull(type);
        this.lang = Objects.requireNonNull(lang);
        this.options = Collections.unmodifiableMap(Objects.requireNonNull(options));
        this.params = Collections.unmodifiableMap(Objects.requireNonNull(params));
        if (type != ScriptType.INLINE && !options.isEmpty()) {
            throw new IllegalArgumentException("Compiler options [" + options + "] cannot be specified at runtime for [" + type + "] scripts.");
        }
    }

    public Script(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_5_1_0_UNRELEASED)) {
            this.type = ScriptType.readFrom(in);
            this.lang = in.readString();
            this.idOrCode = in.readString();
            @SuppressWarnings("unchecked")
            Map<String, String> options = (Map) in.readMap();
            this.options = options;
            this.params = in.readMap();
        } else {
            String idOrCode = in.readString();
            ScriptType type;
            if (in.readBoolean()) {
                type = ScriptType.readFrom(in);
            } else {
                type = DEFAULT_SCRIPT_TYPE;
            }
            String lang = in.readOptionalString();
            if (lang == null) {
                lang = DEFAULT_SCRIPT_LANG;
            }
            Map<String, Object> params = in.readMap();
            if (params == null) {
                params = new HashMap<>();
            }
            Map<String, String> options = new HashMap<>();
            if (in.readBoolean()) {
                XContentType contentType = XContentType.readFrom(in);
                options.put(CONTENT_TYPE_OPTION, contentType.mediaType());
            }
            this.type = type;
            this.lang = lang;
            this.idOrCode = idOrCode;
            this.options = options;
            this.params = params;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_5_1_0_UNRELEASED)) {
            type.writeTo(out);
            out.writeString(lang);
            out.writeString(idOrCode);
            @SuppressWarnings("unchecked")
            Map<String, Object> options = (Map) this.options;
            out.writeMap(options);
            out.writeMap(params);
        } else {
            out.writeString(idOrCode);
            out.writeBoolean(true);
            type.writeTo(out);
            out.writeBoolean(true);
            out.writeString(lang);
            out.writeMap(params.isEmpty() ? null : params);
            if (options.containsKey(CONTENT_TYPE_OPTION)) {
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
        String contentType = options.get(CONTENT_TYPE_OPTION);
        if (type == ScriptType.INLINE && contentType != null && builder.contentType().mediaType().equals(contentType)) {
            builder.rawField(type.getParseField().getPreferredName(), new BytesArray(idOrCode));
        } else {
            builder.field(type.getParseField().getPreferredName(), idOrCode);
        }
        builder.field(LANG_PARSE_FIELD.getPreferredName(), lang);
        if (!options.isEmpty()) {
            builder.field(OPTIONS_PARSE_FIELD.getPreferredName(), options);
        }
        if (!params.isEmpty()) {
            builder.field(PARAMS_PARSE_FIELD.getPreferredName(), params);
        }
        builder.endObject();
        return builder;
    }

    public String getIdOrCode() {
        return idOrCode;
    }

    public ScriptType getType() {
        return type;
    }

    public String getLang() {
        return lang;
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
        if (!lang.equals(script.lang))
            return false;
        if (!idOrCode.equals(script.idOrCode))
            return false;
        if (!options.equals(script.options))
            return false;
        return params.equals(script.params);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + lang.hashCode();
        result = 31 * result + idOrCode.hashCode();
        result = 31 * result + options.hashCode();
        result = 31 * result + params.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Script{" + "type=" + type + ", lang='" + lang + '\'' + ", idOrCode='" + idOrCode + '\'' + ", options=" + options + ", params=" + params + '}';
    }
}