package org.elasticsearch.script;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryParseContext;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public final class Script implements ToXContent, Writeable {

    public static final String DEFAULT_SCRIPT_LANG = "painless";

    private String script;

    private ScriptType type;

    @Nullable
    private String lang;

    @Nullable
    private Map<String, Object> params;

    @Nullable
    private XContentType contentType;

    public Script(String script) {
        this(script, ScriptType.INLINE, null, null);
    }

    public Script(String script, ScriptType type, String lang, @Nullable Map<String, ?> params) {
        this(script, type, lang, params, null);
    }

    @SuppressWarnings("unchecked")
    public Script(String script, ScriptType type, String lang, @Nullable Map<String, ?> params, @Nullable XContentType contentType) {
        if (contentType != null && type != ScriptType.INLINE) {
            throw new IllegalArgumentException("The parameter contentType only makes sense for inline scripts");
        }
        this.script = Objects.requireNonNull(script);
        this.type = Objects.requireNonNull(type);
        this.lang = lang == null ? DEFAULT_SCRIPT_LANG : lang;
        this.params = (Map<String, Object>) params;
        this.contentType = contentType;
    }

    public Script(StreamInput in) throws IOException {
        script = in.readString();
        if (in.readBoolean()) {
            type = ScriptType.readFrom(in);
        }
        lang = in.readOptionalString();
        params = in.readMap();
        if (in.readBoolean()) {
            contentType = XContentType.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(script);
        boolean hasType = type != null;
        out.writeBoolean(hasType);
        if (hasType) {
            type.writeTo(out);
        }
        out.writeOptionalString(lang);
        out.writeMap(params);
        boolean hasContentType = contentType != null;
        out.writeBoolean(hasContentType);
        if (hasContentType) {
            XContentType.writeTo(contentType, out);
        }
    }

    public String getScript() {
        return script;
    }

    public ScriptType getType() {
        return type;
    }

    public String getLang() {
        return lang;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public XContentType getContentType() {
        return contentType;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params builderParams) throws IOException {
        if (type == null) {
            return builder.value(script);
        }
        builder.startObject();
        if (type == ScriptType.INLINE && contentType != null && builder.contentType() == contentType) {
            builder.rawField(type.getParseField().getPreferredName(), new BytesArray(script));
        } else {
            builder.field(type.getParseField().getPreferredName(), script);
        }
        if (lang != null) {
            builder.field(ScriptField.LANG.getPreferredName(), lang);
        }
        if (params != null) {
            builder.field(ScriptField.PARAMS.getPreferredName(), params);
        }
        builder.endObject();
        return builder;
    }

    public static Script parse(XContentParser parser, ParseFieldMatcher matcher) {
        return parse(parser, matcher, null);
    }

    public static Script parse(XContentParser parser, QueryParseContext context) {
        return parse(parser, context.getParseFieldMatcher(), null);
    }

    public static Script parse(XContentParser parser, ParseFieldMatcher parseFieldMatcher, @Nullable String lang) {
        try {
            XContentParser.Token token = parser.currentToken();
            if (token == null) {
                token = parser.nextToken();
            }
            if (token == XContentParser.Token.VALUE_STRING) {
                return new Script(parser.text(), ScriptType.INLINE, lang, null);
            }
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("expected a string value or an object, but found [{}] instead", token);
            }
            String script = null;
            ScriptType type = null;
            Map<String, Object> params = null;
            XContentType contentType = null;
            String cfn = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    cfn = parser.currentName();
                } else if (parseFieldMatcher.match(cfn, ScriptType.INLINE.getParseField())) {
                    type = ScriptType.INLINE;
                    if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                        contentType = parser.contentType();
                        XContentBuilder builder = XContentFactory.contentBuilder(contentType);
                        script = builder.copyCurrentStructure(parser).bytes().utf8ToString();
                    } else {
                        script = parser.text();
                    }
                } else if (parseFieldMatcher.match(cfn, ScriptType.FILE.getParseField())) {
                    type = ScriptType.FILE;
                    if (token == XContentParser.Token.VALUE_STRING) {
                        script = parser.text();
                    } else {
                        throw new ElasticsearchParseException("expected a string value for field [{}], but found [{}]", cfn, token);
                    }
                } else if (parseFieldMatcher.match(cfn, ScriptType.STORED.getParseField())) {
                    type = ScriptType.STORED;
                    if (token == XContentParser.Token.VALUE_STRING) {
                        script = parser.text();
                    } else {
                        throw new ElasticsearchParseException("expected a string value for field [{}], but found [{}]", cfn, token);
                    }
                } else if (parseFieldMatcher.match(cfn, ScriptField.LANG)) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        lang = parser.text();
                    } else {
                        throw new ElasticsearchParseException("expected a string value for field [{}], but found [{}]", cfn, token);
                    }
                } else if (parseFieldMatcher.match(cfn, ScriptField.PARAMS)) {
                    if (token == XContentParser.Token.START_OBJECT) {
                        params = parser.map();
                    } else {
                        throw new ElasticsearchParseException("expected an object for field [{}], but found [{}]", cfn, token);
                    }
                } else {
                    throw new ElasticsearchParseException("unexpected field [{}]", cfn);
                }
            }
            if (script == null) {
                throw new ElasticsearchParseException("expected one of [{}], [{}] or [{}] fields, but found none", ScriptType.INLINE.getParseField().getPreferredName(), ScriptType.FILE.getParseField().getPreferredName(), ScriptType.STORED.getParseField().getPreferredName());
            }
            return new Script(script, type, lang, params, contentType);
        } catch (IOException e) {
            throw new ParsingException(parser.getTokenLocation(), "Error parsing [" + ScriptField.SCRIPT.getPreferredName() + "] field", e);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(lang, params, script, type, contentType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Script other = (Script) obj;
        return Objects.equals(lang, other.lang) && Objects.equals(params, other.params) && Objects.equals(script, other.script) && Objects.equals(type, other.type) && Objects.equals(contentType, other.contentType);
    }

    @Override
    public String toString() {
        return "[script: " + script + ", type: " + type.getParseField().getPreferredName() + ", lang: " + lang + ", params: " + params + ", contentType: " + contentType + "]";
    }

    public interface ScriptField {

        ParseField SCRIPT = new ParseField("script");

        ParseField LANG = new ParseField("lang");

        ParseField PARAMS = new ParseField("params");
    }
}