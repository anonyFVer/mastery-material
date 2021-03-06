package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.index.mapper.CompletionFieldMapper2x;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.ContextMappings;
import org.elasticsearch.search.suggest.completion2x.context.CategoryContextMapping;
import org.elasticsearch.search.suggest.completion2x.context.ContextMapping.ContextQuery;
import org.elasticsearch.search.suggest.completion2x.context.GeolocationContextMapping;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CompletionSuggestionBuilder extends SuggestionBuilder<CompletionSuggestionBuilder> {

    static final String SUGGESTION_NAME = "completion";

    static final ParseField CONTEXTS_FIELD = new ParseField("contexts", "context");

    private static ObjectParser<CompletionSuggestionBuilder.InnerBuilder, ParseFieldMatcherSupplier> TLP_PARSER = new ObjectParser<>(SUGGESTION_NAME, null);

    static {
        TLP_PARSER.declareField((parser, completionSuggestionContext, context) -> {
            if (parser.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                if (parser.booleanValue()) {
                    completionSuggestionContext.fuzzyOptions = new FuzzyOptions.Builder().build();
                }
            } else {
                completionSuggestionContext.fuzzyOptions = FuzzyOptions.parse(parser, context);
            }
        }, FuzzyOptions.FUZZY_OPTIONS, ObjectParser.ValueType.OBJECT_OR_BOOLEAN);
        TLP_PARSER.declareField((parser, completionSuggestionContext, context) -> completionSuggestionContext.regexOptions = RegexOptions.parse(parser, context), RegexOptions.REGEX_OPTIONS, ObjectParser.ValueType.OBJECT);
        TLP_PARSER.declareString(CompletionSuggestionBuilder.InnerBuilder::field, FIELDNAME_FIELD);
        TLP_PARSER.declareString(CompletionSuggestionBuilder.InnerBuilder::analyzer, ANALYZER_FIELD);
        TLP_PARSER.declareInt(CompletionSuggestionBuilder.InnerBuilder::size, SIZE_FIELD);
        TLP_PARSER.declareInt(CompletionSuggestionBuilder.InnerBuilder::shardSize, SHARDSIZE_FIELD);
        TLP_PARSER.declareField((p, v, c) -> {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.copyCurrentStructure(p);
            v.contextBytes = builder.bytes();
            p.skipChildren();
        }, CONTEXTS_FIELD, ObjectParser.ValueType.OBJECT);
    }

    protected FuzzyOptions fuzzyOptions;

    protected RegexOptions regexOptions;

    protected BytesReference contextBytes = null;

    public CompletionSuggestionBuilder(String field) {
        super(field);
    }

    private CompletionSuggestionBuilder(String fieldname, CompletionSuggestionBuilder in) {
        super(fieldname, in);
        fuzzyOptions = in.fuzzyOptions;
        regexOptions = in.regexOptions;
        contextBytes = in.contextBytes;
    }

    public CompletionSuggestionBuilder(StreamInput in) throws IOException {
        super(in);
        fuzzyOptions = in.readOptionalWriteable(FuzzyOptions::new);
        regexOptions = in.readOptionalWriteable(RegexOptions::new);
        contextBytes = in.readOptionalBytesReference();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(fuzzyOptions);
        out.writeOptionalWriteable(regexOptions);
        out.writeOptionalBytesReference(contextBytes);
    }

    @Override
    public CompletionSuggestionBuilder prefix(String prefix) {
        super.prefix(prefix);
        return this;
    }

    public CompletionSuggestionBuilder prefix(String prefix, Fuzziness fuzziness) {
        super.prefix(prefix);
        this.fuzzyOptions = new FuzzyOptions.Builder().setFuzziness(fuzziness).build();
        return this;
    }

    public CompletionSuggestionBuilder prefix(String prefix, FuzzyOptions fuzzyOptions) {
        super.prefix(prefix);
        this.fuzzyOptions = fuzzyOptions;
        return this;
    }

    @Override
    public CompletionSuggestionBuilder regex(String regex) {
        super.regex(regex);
        return this;
    }

    public CompletionSuggestionBuilder regex(String regex, RegexOptions regexOptions) {
        this.regex(regex);
        this.regexOptions = regexOptions;
        return this;
    }

    public CompletionSuggestionBuilder contexts(Map<String, List<? extends ToXContent>> queryContexts) {
        Objects.requireNonNull(queryContexts, "contexts must not be null");
        try {
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
            contentBuilder.startObject();
            for (Map.Entry<String, List<? extends ToXContent>> contextEntry : queryContexts.entrySet()) {
                contentBuilder.startArray(contextEntry.getKey());
                for (ToXContent queryContext : contextEntry.getValue()) {
                    queryContext.toXContent(contentBuilder, EMPTY_PARAMS);
                }
                contentBuilder.endArray();
            }
            contentBuilder.endObject();
            return contexts(contentBuilder);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private CompletionSuggestionBuilder contexts(XContentBuilder contextBuilder) {
        contextBytes = contextBuilder.bytes();
        return this;
    }

    public CompletionSuggestionBuilder contexts(Contexts2x contexts2x) {
        Objects.requireNonNull(contexts2x, "contexts must not be null");
        try {
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
            contentBuilder.startObject();
            for (ContextQuery contextQuery : contexts2x.contextQueries) {
                contextQuery.toXContent(contentBuilder, EMPTY_PARAMS);
            }
            contentBuilder.endObject();
            return contexts(contentBuilder);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static class Contexts2x {

        private List<ContextQuery> contextQueries = new ArrayList<>();

        @SuppressWarnings("unchecked")
        private Contexts2x addContextQuery(ContextQuery ctx) {
            this.contextQueries.add(ctx);
            return this;
        }

        @Deprecated
        public Contexts2x addGeoLocation(String name, double lat, double lon, int... precisions) {
            return addContextQuery(GeolocationContextMapping.query(name, lat, lon, precisions));
        }

        @Deprecated
        public Contexts2x addGeoLocationWithPrecision(String name, double lat, double lon, String... precisions) {
            return addContextQuery(GeolocationContextMapping.query(name, lat, lon, precisions));
        }

        @Deprecated
        public Contexts2x addGeoLocation(String name, String geohash) {
            return addContextQuery(GeolocationContextMapping.query(name, geohash));
        }

        @Deprecated
        public Contexts2x addCategory(String name, CharSequence... categories) {
            return addContextQuery(CategoryContextMapping.query(name, categories));
        }

        @Deprecated
        public Contexts2x addCategory(String name, Iterable<? extends CharSequence> categories) {
            return addContextQuery(CategoryContextMapping.query(name, categories));
        }

        @Deprecated
        public Contexts2x addContextField(String name, CharSequence... fieldvalues) {
            return addContextQuery(CategoryContextMapping.query(name, fieldvalues));
        }

        @Deprecated
        public Contexts2x addContextField(String name, Iterable<? extends CharSequence> fieldvalues) {
            return addContextQuery(CategoryContextMapping.query(name, fieldvalues));
        }
    }

    private static class InnerBuilder extends CompletionSuggestionBuilder {

        private String field;

        public InnerBuilder() {
            super("_na_");
        }

        private InnerBuilder field(String field) {
            this.field = field;
            return this;
        }
    }

    @Override
    protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (fuzzyOptions != null) {
            fuzzyOptions.toXContent(builder, params);
        }
        if (regexOptions != null) {
            regexOptions.toXContent(builder, params);
        }
        if (contextBytes != null) {
            try (XContentParser contextParser = XContentFactory.xContent(XContentType.JSON).createParser(contextBytes)) {
                builder.field(CONTEXTS_FIELD.getPreferredName());
                builder.copyCurrentStructure(contextParser);
            }
        }
        return builder;
    }

    static CompletionSuggestionBuilder innerFromXContent(QueryParseContext parseContext) throws IOException {
        CompletionSuggestionBuilder.InnerBuilder builder = new CompletionSuggestionBuilder.InnerBuilder();
        TLP_PARSER.parse(parseContext.parser(), builder, parseContext);
        String field = builder.field;
        if (field == null) {
            throw new ElasticsearchParseException("the required field option [" + FIELDNAME_FIELD.getPreferredName() + "] is missing");
        }
        return new CompletionSuggestionBuilder(field, builder);
    }

    @Override
    public SuggestionContext build(QueryShardContext context) throws IOException {
        CompletionSuggestionContext suggestionContext = new CompletionSuggestionContext(context);
        final MapperService mapperService = context.getMapperService();
        populateCommonFields(mapperService, suggestionContext);
        suggestionContext.setFuzzyOptions(fuzzyOptions);
        suggestionContext.setRegexOptions(regexOptions);
        MappedFieldType mappedFieldType = mapperService.fullName(suggestionContext.getField());
        if (mappedFieldType == null || (mappedFieldType instanceof CompletionFieldMapper.CompletionFieldType == false && mappedFieldType instanceof CompletionFieldMapper2x.CompletionFieldType == false)) {
            throw new IllegalArgumentException("Field [" + suggestionContext.getField() + "] is not a completion suggest field");
        }
        if (mappedFieldType instanceof CompletionFieldMapper.CompletionFieldType) {
            CompletionFieldMapper.CompletionFieldType type = (CompletionFieldMapper.CompletionFieldType) mappedFieldType;
            suggestionContext.setFieldType(type);
            if (type.hasContextMappings() && contextBytes != null) {
                try (XContentParser contextParser = XContentFactory.xContent(contextBytes).createParser(contextBytes)) {
                    if (type.hasContextMappings() && contextParser != null) {
                        ContextMappings contextMappings = type.getContextMappings();
                        contextParser.nextToken();
                        Map<String, List<ContextMapping.InternalQueryContext>> queryContexts = new HashMap<>(contextMappings.size());
                        assert contextParser.currentToken() == XContentParser.Token.START_OBJECT;
                        XContentParser.Token currentToken;
                        String currentFieldName;
                        while ((currentToken = contextParser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (currentToken == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = contextParser.currentName();
                                final ContextMapping mapping = contextMappings.get(currentFieldName);
                                queryContexts.put(currentFieldName, mapping.parseQueryContext(context.newParseContext(contextParser)));
                            }
                        }
                        suggestionContext.setQueryContexts(queryContexts);
                    }
                }
            } else if (contextBytes != null) {
                throw new IllegalArgumentException("suggester [" + type.name() + "] doesn't expect any context");
            }
        } else if (mappedFieldType instanceof CompletionFieldMapper2x.CompletionFieldType) {
            CompletionFieldMapper2x.CompletionFieldType type = ((CompletionFieldMapper2x.CompletionFieldType) mappedFieldType);
            suggestionContext.setFieldType2x(type);
            if (type.requiresContext()) {
                if (contextBytes != null) {
                    try (XContentParser contextParser = XContentFactory.xContent(contextBytes).createParser(contextBytes)) {
                        contextParser.nextToken();
                        suggestionContext.setContextQueries(ContextQuery.parseQueries(type.getContextMapping(), contextParser));
                    }
                } else {
                    throw new IllegalArgumentException("suggester [completion] requires context to be setup");
                }
            } else if (contextBytes != null) {
                throw new IllegalArgumentException("suggester [completion] doesn't expect any context");
            }
        }
        assert suggestionContext.getFieldType() != null || suggestionContext.getFieldType2x() != null : "no completion field type set";
        return suggestionContext;
    }

    @Override
    public String getWriteableName() {
        return SUGGESTION_NAME;
    }

    @Override
    protected boolean doEquals(CompletionSuggestionBuilder other) {
        return Objects.equals(fuzzyOptions, other.fuzzyOptions) && Objects.equals(regexOptions, other.regexOptions) && Objects.equals(contextBytes, other.contextBytes);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fuzzyOptions, regexOptions, contextBytes);
    }
}