package org.elasticsearch.search.builder;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.SearchExtParser;
import org.elasticsearch.search.SearchExtRegistry;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescoreBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.Suggesters;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class SearchSourceBuilder extends ToXContentToBytes implements Writeable {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(SearchSourceBuilder.class));

    public static final ParseField FROM_FIELD = new ParseField("from");

    public static final ParseField SIZE_FIELD = new ParseField("size");

    public static final ParseField TIMEOUT_FIELD = new ParseField("timeout");

    public static final ParseField TERMINATE_AFTER_FIELD = new ParseField("terminate_after");

    public static final ParseField QUERY_FIELD = new ParseField("query");

    public static final ParseField POST_FILTER_FIELD = new ParseField("post_filter");

    public static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");

    public static final ParseField VERSION_FIELD = new ParseField("version");

    public static final ParseField EXPLAIN_FIELD = new ParseField("explain");

    public static final ParseField _SOURCE_FIELD = new ParseField("_source");

    public static final ParseField FIELDS_FIELD = new ParseField("fields");

    public static final ParseField STORED_FIELDS_FIELD = new ParseField("stored_fields");

    public static final ParseField DOCVALUE_FIELDS_FIELD = new ParseField("docvalue_fields", "fielddata_fields");

    public static final ParseField SCRIPT_FIELDS_FIELD = new ParseField("script_fields");

    public static final ParseField SCRIPT_FIELD = new ParseField("script");

    public static final ParseField IGNORE_FAILURE_FIELD = new ParseField("ignore_failure");

    public static final ParseField SORT_FIELD = new ParseField("sort");

    public static final ParseField TRACK_SCORES_FIELD = new ParseField("track_scores");

    public static final ParseField INDICES_BOOST_FIELD = new ParseField("indices_boost");

    public static final ParseField AGGREGATIONS_FIELD = new ParseField("aggregations");

    public static final ParseField AGGS_FIELD = new ParseField("aggs");

    public static final ParseField HIGHLIGHT_FIELD = new ParseField("highlight");

    public static final ParseField SUGGEST_FIELD = new ParseField("suggest");

    public static final ParseField RESCORE_FIELD = new ParseField("rescore");

    public static final ParseField STATS_FIELD = new ParseField("stats");

    public static final ParseField EXT_FIELD = new ParseField("ext");

    public static final ParseField PROFILE_FIELD = new ParseField("profile");

    public static final ParseField SEARCH_AFTER = new ParseField("search_after");

    public static final ParseField SLICE = new ParseField("slice");

    public static final ParseField ALL_FIELDS_FIELDS = new ParseField("all_fields");

    public static SearchSourceBuilder fromXContent(QueryParseContext context, AggregatorParsers aggParsers, Suggesters suggesters, SearchExtRegistry searchExtRegistry) throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.parseXContent(context, aggParsers, suggesters, searchExtRegistry);
        return builder;
    }

    public static SearchSourceBuilder searchSource() {
        return new SearchSourceBuilder();
    }

    public static HighlightBuilder highlight() {
        return new HighlightBuilder();
    }

    private QueryBuilder queryBuilder;

    private QueryBuilder postQueryBuilder;

    private int from = -1;

    private int size = -1;

    private Boolean explain;

    private Boolean version;

    private List<SortBuilder<?>> sorts;

    private boolean trackScores = false;

    private SearchAfterBuilder searchAfterBuilder;

    private SliceBuilder sliceBuilder;

    private Float minScore;

    private TimeValue timeout = null;

    private int terminateAfter = SearchContext.DEFAULT_TERMINATE_AFTER;

    private StoredFieldsContext storedFieldsContext;

    private List<String> docValueFields;

    private List<ScriptField> scriptFields;

    private FetchSourceContext fetchSourceContext;

    private AggregatorFactories.Builder aggregations;

    private HighlightBuilder highlightBuilder;

    private SuggestBuilder suggestBuilder;

    private List<RescoreBuilder> rescoreBuilders;

    private List<IndexBoost> indexBoosts = new ArrayList<>();

    private List<String> stats;

    private List<SearchExtBuilder> extBuilders = Collections.emptyList();

    private boolean profile = false;

    public SearchSourceBuilder() {
    }

    public SearchSourceBuilder(StreamInput in) throws IOException {
        aggregations = in.readOptionalWriteable(AggregatorFactories.Builder::new);
        explain = in.readOptionalBoolean();
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
        docValueFields = (List<String>) in.readGenericValue();
        storedFieldsContext = in.readOptionalWriteable(StoredFieldsContext::new);
        from = in.readVInt();
        highlightBuilder = in.readOptionalWriteable(HighlightBuilder::new);
        indexBoosts = in.readList(IndexBoost::new);
        minScore = in.readOptionalFloat();
        postQueryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        if (in.readBoolean()) {
            rescoreBuilders = in.readNamedWriteableList(RescoreBuilder.class);
        }
        if (in.readBoolean()) {
            scriptFields = in.readList(ScriptField::new);
        }
        size = in.readVInt();
        if (in.readBoolean()) {
            int size = in.readVInt();
            sorts = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                sorts.add(in.readNamedWriteable(SortBuilder.class));
            }
        }
        if (in.readBoolean()) {
            stats = in.readList(StreamInput::readString);
        }
        suggestBuilder = in.readOptionalWriteable(SuggestBuilder::new);
        terminateAfter = in.readVInt();
        timeout = in.readOptionalWriteable(TimeValue::new);
        trackScores = in.readBoolean();
        version = in.readOptionalBoolean();
        extBuilders = in.readNamedWriteableList(SearchExtBuilder.class);
        profile = in.readBoolean();
        searchAfterBuilder = in.readOptionalWriteable(SearchAfterBuilder::new);
        sliceBuilder = in.readOptionalWriteable(SliceBuilder::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(aggregations);
        out.writeOptionalBoolean(explain);
        out.writeOptionalWriteable(fetchSourceContext);
        out.writeGenericValue(docValueFields);
        out.writeOptionalWriteable(storedFieldsContext);
        out.writeVInt(from);
        out.writeOptionalWriteable(highlightBuilder);
        out.writeList(indexBoosts);
        out.writeOptionalFloat(minScore);
        out.writeOptionalNamedWriteable(postQueryBuilder);
        out.writeOptionalNamedWriteable(queryBuilder);
        boolean hasRescoreBuilders = rescoreBuilders != null;
        out.writeBoolean(hasRescoreBuilders);
        if (hasRescoreBuilders) {
            out.writeNamedWriteableList(rescoreBuilders);
        }
        boolean hasScriptFields = scriptFields != null;
        out.writeBoolean(hasScriptFields);
        if (hasScriptFields) {
            out.writeList(scriptFields);
        }
        out.writeVInt(size);
        boolean hasSorts = sorts != null;
        out.writeBoolean(hasSorts);
        if (hasSorts) {
            out.writeVInt(sorts.size());
            for (SortBuilder<?> sort : sorts) {
                out.writeNamedWriteable(sort);
            }
        }
        boolean hasStats = stats != null;
        out.writeBoolean(hasStats);
        if (hasStats) {
            out.writeStringList(stats);
        }
        out.writeOptionalWriteable(suggestBuilder);
        out.writeVInt(terminateAfter);
        out.writeOptionalWriteable(timeout);
        out.writeBoolean(trackScores);
        out.writeOptionalBoolean(version);
        out.writeNamedWriteableList(extBuilders);
        out.writeBoolean(profile);
        out.writeOptionalWriteable(searchAfterBuilder);
        out.writeOptionalWriteable(sliceBuilder);
    }

    public SearchSourceBuilder query(QueryBuilder query) {
        this.queryBuilder = query;
        return this;
    }

    public QueryBuilder query() {
        return queryBuilder;
    }

    public SearchSourceBuilder postFilter(QueryBuilder postFilter) {
        this.postQueryBuilder = postFilter;
        return this;
    }

    public QueryBuilder postFilter() {
        return postQueryBuilder;
    }

    public SearchSourceBuilder from(int from) {
        this.from = from;
        return this;
    }

    public int from() {
        return from;
    }

    public SearchSourceBuilder size(int size) {
        this.size = size;
        return this;
    }

    public int size() {
        return size;
    }

    public SearchSourceBuilder minScore(float minScore) {
        this.minScore = minScore;
        return this;
    }

    public Float minScore() {
        return minScore;
    }

    public SearchSourceBuilder explain(Boolean explain) {
        this.explain = explain;
        return this;
    }

    public Boolean explain() {
        return explain;
    }

    public SearchSourceBuilder version(Boolean version) {
        this.version = version;
        return this;
    }

    public Boolean version() {
        return version;
    }

    public SearchSourceBuilder timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public SearchSourceBuilder terminateAfter(int terminateAfter) {
        if (terminateAfter < 0) {
            throw new IllegalArgumentException("terminateAfter must be > 0");
        }
        this.terminateAfter = terminateAfter;
        return this;
    }

    public int terminateAfter() {
        return terminateAfter;
    }

    public SearchSourceBuilder sort(String name, SortOrder order) {
        if (name.equals(ScoreSortBuilder.NAME)) {
            return sort(SortBuilders.scoreSort().order(order));
        }
        return sort(SortBuilders.fieldSort(name).order(order));
    }

    public SearchSourceBuilder sort(String name) {
        if (name.equals(ScoreSortBuilder.NAME)) {
            return sort(SortBuilders.scoreSort());
        }
        return sort(SortBuilders.fieldSort(name));
    }

    public SearchSourceBuilder sort(SortBuilder<?> sort) {
        if (sorts == null) {
            sorts = new ArrayList<>();
        }
        sorts.add(sort);
        return this;
    }

    public List<SortBuilder<?>> sorts() {
        return sorts;
    }

    public SearchSourceBuilder trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    public boolean trackScores() {
        return trackScores;
    }

    public Object[] searchAfter() {
        if (searchAfterBuilder == null) {
            return null;
        }
        return searchAfterBuilder.getSortValues();
    }

    public SearchSourceBuilder searchAfter(Object[] values) {
        this.searchAfterBuilder = new SearchAfterBuilder().setSortValues(values);
        return this;
    }

    public SearchSourceBuilder slice(SliceBuilder builder) {
        this.sliceBuilder = builder;
        return this;
    }

    public SliceBuilder slice() {
        return sliceBuilder;
    }

    public SearchSourceBuilder aggregation(AggregationBuilder aggregation) {
        if (aggregations == null) {
            aggregations = AggregatorFactories.builder();
        }
        aggregations.addAggregator(aggregation);
        return this;
    }

    public SearchSourceBuilder aggregation(PipelineAggregationBuilder aggregation) {
        if (aggregations == null) {
            aggregations = AggregatorFactories.builder();
        }
        aggregations.addPipelineAggregator(aggregation);
        return this;
    }

    public AggregatorFactories.Builder aggregations() {
        return aggregations;
    }

    public SearchSourceBuilder highlighter(HighlightBuilder highlightBuilder) {
        this.highlightBuilder = highlightBuilder;
        return this;
    }

    public HighlightBuilder highlighter() {
        return highlightBuilder;
    }

    public SearchSourceBuilder suggest(SuggestBuilder suggestBuilder) {
        this.suggestBuilder = suggestBuilder;
        return this;
    }

    public SuggestBuilder suggest() {
        return suggestBuilder;
    }

    public SearchSourceBuilder addRescorer(RescoreBuilder<?> rescoreBuilder) {
        if (rescoreBuilders == null) {
            rescoreBuilders = new ArrayList<>();
        }
        rescoreBuilders.add(rescoreBuilder);
        return this;
    }

    public SearchSourceBuilder clearRescorers() {
        rescoreBuilders = null;
        return this;
    }

    public SearchSourceBuilder profile(boolean profile) {
        this.profile = profile;
        return this;
    }

    public boolean profile() {
        return profile;
    }

    public List<RescoreBuilder> rescores() {
        return rescoreBuilders;
    }

    public SearchSourceBuilder fetchSource(boolean fetch) {
        FetchSourceContext fetchSourceContext = this.fetchSourceContext != null ? this.fetchSourceContext : FetchSourceContext.FETCH_SOURCE;
        this.fetchSourceContext = new FetchSourceContext(fetch, fetchSourceContext.includes(), fetchSourceContext.excludes());
        return this;
    }

    public SearchSourceBuilder fetchSource(@Nullable String include, @Nullable String exclude) {
        return fetchSource(include == null ? Strings.EMPTY_ARRAY : new String[] { include }, exclude == null ? Strings.EMPTY_ARRAY : new String[] { exclude });
    }

    public SearchSourceBuilder fetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        FetchSourceContext fetchSourceContext = this.fetchSourceContext != null ? this.fetchSourceContext : FetchSourceContext.FETCH_SOURCE;
        this.fetchSourceContext = new FetchSourceContext(fetchSourceContext.fetchSource(), includes, excludes);
        return this;
    }

    public SearchSourceBuilder fetchSource(@Nullable FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    public FetchSourceContext fetchSource() {
        return fetchSourceContext;
    }

    public SearchSourceBuilder storedField(String name) {
        return storedFields(Collections.singletonList(name));
    }

    public SearchSourceBuilder storedFields(List<String> fields) {
        if (storedFieldsContext == null) {
            storedFieldsContext = StoredFieldsContext.fromList(fields);
        } else {
            storedFieldsContext.addFieldNames(fields);
        }
        return this;
    }

    public SearchSourceBuilder storedFields(StoredFieldsContext context) {
        storedFieldsContext = context;
        return this;
    }

    public StoredFieldsContext storedFields() {
        return storedFieldsContext;
    }

    @Deprecated
    public SearchSourceBuilder fieldDataField(String name) {
        if (docValueFields == null) {
            docValueFields = new ArrayList<>();
        }
        docValueFields.add(name);
        return this;
    }

    @Deprecated
    public List<String> fieldDataFields() {
        return docValueFields;
    }

    public List<String> docValueFields() {
        return docValueFields;
    }

    public SearchSourceBuilder docValueField(String name) {
        if (docValueFields == null) {
            docValueFields = new ArrayList<>();
        }
        docValueFields.add(name);
        return this;
    }

    public SearchSourceBuilder scriptField(String name, Script script) {
        scriptField(name, script, false);
        return this;
    }

    public SearchSourceBuilder scriptField(String name, Script script, boolean ignoreFailure) {
        if (scriptFields == null) {
            scriptFields = new ArrayList<>();
        }
        scriptFields.add(new ScriptField(name, script, ignoreFailure));
        return this;
    }

    public List<ScriptField> scriptFields() {
        return scriptFields;
    }

    public SearchSourceBuilder indexBoost(String index, float indexBoost) {
        Objects.requireNonNull(index, "index must not be null");
        this.indexBoosts.add(new IndexBoost(index, indexBoost));
        return this;
    }

    public List<IndexBoost> indexBoosts() {
        return indexBoosts;
    }

    public SearchSourceBuilder stats(List<String> statsGroups) {
        this.stats = statsGroups;
        return this;
    }

    public List<String> stats() {
        return stats;
    }

    public SearchSourceBuilder ext(List<SearchExtBuilder> searchExtBuilders) {
        this.extBuilders = Objects.requireNonNull(searchExtBuilders, "searchExtBuilders must not be null");
        return this;
    }

    public List<SearchExtBuilder> ext() {
        return extBuilders;
    }

    public boolean isSuggestOnly() {
        return suggestBuilder != null && queryBuilder == null && aggregations == null;
    }

    public SearchSourceBuilder rewrite(QueryShardContext context) throws IOException {
        assert (this.equals(shallowCopy(queryBuilder, postQueryBuilder, sliceBuilder)));
        QueryBuilder queryBuilder = null;
        if (this.queryBuilder != null) {
            queryBuilder = this.queryBuilder.rewrite(context);
        }
        QueryBuilder postQueryBuilder = null;
        if (this.postQueryBuilder != null) {
            postQueryBuilder = this.postQueryBuilder.rewrite(context);
        }
        boolean rewritten = queryBuilder != this.queryBuilder || postQueryBuilder != this.postQueryBuilder;
        if (rewritten) {
            return shallowCopy(queryBuilder, postQueryBuilder, sliceBuilder);
        }
        return this;
    }

    public SearchSourceBuilder copyWithNewSlice(SliceBuilder slice) {
        return shallowCopy(queryBuilder, postQueryBuilder, slice);
    }

    private SearchSourceBuilder shallowCopy(QueryBuilder queryBuilder, QueryBuilder postQueryBuilder, SliceBuilder slice) {
        SearchSourceBuilder rewrittenBuilder = new SearchSourceBuilder();
        rewrittenBuilder.aggregations = aggregations;
        rewrittenBuilder.explain = explain;
        rewrittenBuilder.extBuilders = extBuilders;
        rewrittenBuilder.fetchSourceContext = fetchSourceContext;
        rewrittenBuilder.docValueFields = docValueFields;
        rewrittenBuilder.storedFieldsContext = storedFieldsContext;
        rewrittenBuilder.from = from;
        rewrittenBuilder.highlightBuilder = highlightBuilder;
        rewrittenBuilder.indexBoosts = indexBoosts;
        rewrittenBuilder.minScore = minScore;
        rewrittenBuilder.postQueryBuilder = postQueryBuilder;
        rewrittenBuilder.profile = profile;
        rewrittenBuilder.queryBuilder = queryBuilder;
        rewrittenBuilder.rescoreBuilders = rescoreBuilders;
        rewrittenBuilder.scriptFields = scriptFields;
        rewrittenBuilder.searchAfterBuilder = searchAfterBuilder;
        rewrittenBuilder.sliceBuilder = slice;
        rewrittenBuilder.size = size;
        rewrittenBuilder.sorts = sorts;
        rewrittenBuilder.stats = stats;
        rewrittenBuilder.suggestBuilder = suggestBuilder;
        rewrittenBuilder.terminateAfter = terminateAfter;
        rewrittenBuilder.timeout = timeout;
        rewrittenBuilder.trackScores = trackScores;
        rewrittenBuilder.version = version;
        return rewrittenBuilder;
    }

    public void parseXContent(QueryParseContext context, AggregatorParsers aggParsers, Suggesters suggesters, SearchExtRegistry searchExtRegistry) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = null;
        if (token != XContentParser.Token.START_OBJECT && (token = parser.nextToken()) != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT + "] but found [" + token + "]", parser.getTokenLocation());
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (context.getParseFieldMatcher().match(currentFieldName, FROM_FIELD)) {
                    from = parser.intValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, SIZE_FIELD)) {
                    size = parser.intValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, TIMEOUT_FIELD)) {
                    timeout = TimeValue.parseTimeValue(parser.text(), null, TIMEOUT_FIELD.getPreferredName());
                } else if (context.getParseFieldMatcher().match(currentFieldName, TERMINATE_AFTER_FIELD)) {
                    terminateAfter = parser.intValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, MIN_SCORE_FIELD)) {
                    minScore = parser.floatValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, VERSION_FIELD)) {
                    version = parser.booleanValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, EXPLAIN_FIELD)) {
                    explain = parser.booleanValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, TRACK_SCORES_FIELD)) {
                    trackScores = parser.booleanValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, _SOURCE_FIELD)) {
                    fetchSourceContext = FetchSourceContext.parse(context.parser());
                } else if (context.getParseFieldMatcher().match(currentFieldName, STORED_FIELDS_FIELD)) {
                    storedFieldsContext = StoredFieldsContext.fromXContent(SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(), context);
                } else if (context.getParseFieldMatcher().match(currentFieldName, SORT_FIELD)) {
                    sort(parser.text());
                } else if (context.getParseFieldMatcher().match(currentFieldName, PROFILE_FIELD)) {
                    profile = parser.booleanValue();
                } else if (context.getParseFieldMatcher().match(currentFieldName, FIELDS_FIELD)) {
                    throw new ParsingException(parser.getTokenLocation(), "Deprecated field [" + SearchSourceBuilder.FIELDS_FIELD + "] used, expected [" + SearchSourceBuilder.STORED_FIELDS_FIELD + "] instead");
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (context.getParseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    queryBuilder = context.parseInnerQueryBuilder();
                } else if (context.getParseFieldMatcher().match(currentFieldName, POST_FILTER_FIELD)) {
                    postQueryBuilder = context.parseInnerQueryBuilder();
                } else if (context.getParseFieldMatcher().match(currentFieldName, _SOURCE_FIELD)) {
                    fetchSourceContext = FetchSourceContext.parse(context.parser());
                } else if (context.getParseFieldMatcher().match(currentFieldName, SCRIPT_FIELDS_FIELD)) {
                    scriptFields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        scriptFields.add(new ScriptField(context));
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, INDICES_BOOST_FIELD)) {
                    DEPRECATION_LOGGER.deprecated("Object format in indices_boost is deprecated, please use array format instead");
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            indexBoosts.add(new IndexBoost(currentFieldName, parser.floatValue()));
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].", parser.getTokenLocation());
                        }
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, AGGREGATIONS_FIELD) || context.getParseFieldMatcher().match(currentFieldName, AGGS_FIELD)) {
                    aggregations = aggParsers.parseAggregators(context);
                } else if (context.getParseFieldMatcher().match(currentFieldName, HIGHLIGHT_FIELD)) {
                    highlightBuilder = HighlightBuilder.fromXContent(context);
                } else if (context.getParseFieldMatcher().match(currentFieldName, SUGGEST_FIELD)) {
                    suggestBuilder = SuggestBuilder.fromXContent(context, suggesters);
                } else if (context.getParseFieldMatcher().match(currentFieldName, SORT_FIELD)) {
                    sorts = new ArrayList<>(SortBuilder.fromXContent(context));
                } else if (context.getParseFieldMatcher().match(currentFieldName, RESCORE_FIELD)) {
                    rescoreBuilders = new ArrayList<>();
                    rescoreBuilders.add(RescoreBuilder.parseFromXContent(context));
                } else if (context.getParseFieldMatcher().match(currentFieldName, EXT_FIELD)) {
                    extBuilders = new ArrayList<>();
                    String extSectionName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            extSectionName = parser.currentName();
                        } else {
                            SearchExtParser searchExtParser = searchExtRegistry.lookup(extSectionName, context.getParseFieldMatcher(), parser.getTokenLocation());
                            SearchExtBuilder searchExtBuilder = searchExtParser.fromXContent(parser);
                            if (searchExtBuilder.getWriteableName().equals(extSectionName) == false) {
                                throw new IllegalStateException("The parsed [" + searchExtBuilder.getClass().getName() + "] object has a " + "different writeable name compared to the name of the section that it was parsed from: found [" + searchExtBuilder.getWriteableName() + "] expected [" + extSectionName + "]");
                            }
                            extBuilders.add(searchExtBuilder);
                        }
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, SLICE)) {
                    sliceBuilder = SliceBuilder.fromXContent(context);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].", parser.getTokenLocation());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (context.getParseFieldMatcher().match(currentFieldName, STORED_FIELDS_FIELD)) {
                    storedFieldsContext = StoredFieldsContext.fromXContent(STORED_FIELDS_FIELD.getPreferredName(), context);
                } else if (context.getParseFieldMatcher().match(currentFieldName, DOCVALUE_FIELDS_FIELD)) {
                    docValueFields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            docValueFields.add(parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING + "] in [" + currentFieldName + "] but found [" + token + "]", parser.getTokenLocation());
                        }
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, INDICES_BOOST_FIELD)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        indexBoosts.add(new IndexBoost(context));
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, SORT_FIELD)) {
                    sorts = new ArrayList<>(SortBuilder.fromXContent(context));
                } else if (context.getParseFieldMatcher().match(currentFieldName, RESCORE_FIELD)) {
                    rescoreBuilders = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        rescoreBuilders.add(RescoreBuilder.parseFromXContent(context));
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, STATS_FIELD)) {
                    stats = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            stats.add(parser.text());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_STRING + "] in [" + currentFieldName + "] but found [" + token + "]", parser.getTokenLocation());
                        }
                    }
                } else if (context.getParseFieldMatcher().match(currentFieldName, _SOURCE_FIELD)) {
                    fetchSourceContext = FetchSourceContext.parse(context.parser());
                } else if (context.getParseFieldMatcher().match(currentFieldName, SEARCH_AFTER)) {
                    searchAfterBuilder = SearchAfterBuilder.fromXContent(parser, context.getParseFieldMatcher());
                } else if (context.getParseFieldMatcher().match(currentFieldName, FIELDS_FIELD)) {
                    throw new ParsingException(parser.getTokenLocation(), "The field [" + SearchSourceBuilder.FIELDS_FIELD + "] is no longer supported, please use [" + SearchSourceBuilder.STORED_FIELDS_FIELD + "] to retrieve stored fields or _source filtering " + "if the field is not stored");
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].", parser.getTokenLocation());
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].", parser.getTokenLocation());
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (from != -1) {
            builder.field(FROM_FIELD.getPreferredName(), from);
        }
        if (size != -1) {
            builder.field(SIZE_FIELD.getPreferredName(), size);
        }
        if (timeout != null && !timeout.equals(TimeValue.MINUS_ONE)) {
            builder.field(TIMEOUT_FIELD.getPreferredName(), timeout.getStringRep());
        }
        if (terminateAfter != SearchContext.DEFAULT_TERMINATE_AFTER) {
            builder.field(TERMINATE_AFTER_FIELD.getPreferredName(), terminateAfter);
        }
        if (queryBuilder != null) {
            builder.field(QUERY_FIELD.getPreferredName(), queryBuilder);
        }
        if (postQueryBuilder != null) {
            builder.field(POST_FILTER_FIELD.getPreferredName(), postQueryBuilder);
        }
        if (minScore != null) {
            builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
        }
        if (version != null) {
            builder.field(VERSION_FIELD.getPreferredName(), version);
        }
        if (explain != null) {
            builder.field(EXPLAIN_FIELD.getPreferredName(), explain);
        }
        if (profile) {
            builder.field("profile", true);
        }
        if (fetchSourceContext != null) {
            builder.field(_SOURCE_FIELD.getPreferredName(), fetchSourceContext);
        }
        if (storedFieldsContext != null) {
            storedFieldsContext.toXContent(STORED_FIELDS_FIELD.getPreferredName(), builder);
        }
        if (docValueFields != null) {
            builder.startArray(DOCVALUE_FIELDS_FIELD.getPreferredName());
            for (String fieldDataField : docValueFields) {
                builder.value(fieldDataField);
            }
            builder.endArray();
        }
        if (scriptFields != null) {
            builder.startObject(SCRIPT_FIELDS_FIELD.getPreferredName());
            for (ScriptField scriptField : scriptFields) {
                scriptField.toXContent(builder, params);
            }
            builder.endObject();
        }
        if (sorts != null) {
            builder.startArray(SORT_FIELD.getPreferredName());
            for (SortBuilder<?> sort : sorts) {
                sort.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (trackScores) {
            builder.field(TRACK_SCORES_FIELD.getPreferredName(), true);
        }
        if (searchAfterBuilder != null) {
            builder.array(SEARCH_AFTER.getPreferredName(), searchAfterBuilder.getSortValues());
        }
        if (sliceBuilder != null) {
            builder.field(SLICE.getPreferredName(), sliceBuilder);
        }
        builder.startArray(INDICES_BOOST_FIELD.getPreferredName());
        for (IndexBoost ib : indexBoosts) {
            builder.startObject();
            builder.field(ib.index, ib.boost);
            builder.endObject();
        }
        builder.endArray();
        if (aggregations != null) {
            builder.field(AGGREGATIONS_FIELD.getPreferredName(), aggregations);
        }
        if (highlightBuilder != null) {
            builder.field(HIGHLIGHT_FIELD.getPreferredName(), highlightBuilder);
        }
        if (suggestBuilder != null) {
            builder.field(SUGGEST_FIELD.getPreferredName(), suggestBuilder);
        }
        if (rescoreBuilders != null) {
            builder.startArray(RESCORE_FIELD.getPreferredName());
            for (RescoreBuilder<?> rescoreBuilder : rescoreBuilders) {
                rescoreBuilder.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (stats != null) {
            builder.field(STATS_FIELD.getPreferredName(), stats);
        }
        if (extBuilders != null && extBuilders.isEmpty() == false) {
            builder.startObject(EXT_FIELD.getPreferredName());
            for (SearchExtBuilder extBuilder : extBuilders) {
                extBuilder.toXContent(builder, params);
            }
            builder.endObject();
        }
    }

    public static class IndexBoost implements Writeable, ToXContent {

        private final String index;

        private final float boost;

        IndexBoost(String index, float boost) {
            this.index = index;
            this.boost = boost;
        }

        IndexBoost(StreamInput in) throws IOException {
            index = in.readString();
            boost = in.readFloat();
        }

        IndexBoost(QueryParseContext context) throws IOException {
            XContentParser parser = context.parser();
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                token = parser.nextToken();
                if (token == XContentParser.Token.FIELD_NAME) {
                    index = parser.currentName();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.FIELD_NAME + "] in [" + INDICES_BOOST_FIELD + "] but found [" + token + "]", parser.getTokenLocation());
                }
                token = parser.nextToken();
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.VALUE_NUMBER + "] in [" + INDICES_BOOST_FIELD + "] but found [" + token + "]", parser.getTokenLocation());
                }
                token = parser.nextToken();
                if (token != XContentParser.Token.END_OBJECT) {
                    throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.END_OBJECT + "] in [" + INDICES_BOOST_FIELD + "] but found [" + token + "]", parser.getTokenLocation());
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT + "] in [" + parser.currentName() + "] but found [" + token + "]", parser.getTokenLocation());
            }
        }

        public String getIndex() {
            return index;
        }

        public float getBoost() {
            return boost;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeFloat(boost);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(index, boost);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, boost);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            IndexBoost other = (IndexBoost) obj;
            return Objects.equals(index, other.index) && Objects.equals(boost, other.boost);
        }
    }

    public static class ScriptField implements Writeable, ToXContent {

        private final boolean ignoreFailure;

        private final String fieldName;

        private final Script script;

        public ScriptField(String fieldName, Script script, boolean ignoreFailure) {
            this.fieldName = fieldName;
            this.script = script;
            this.ignoreFailure = ignoreFailure;
        }

        public ScriptField(StreamInput in) throws IOException {
            fieldName = in.readString();
            script = new Script(in);
            ignoreFailure = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(fieldName);
            script.writeTo(out);
            out.writeBoolean(ignoreFailure);
        }

        public ScriptField(QueryParseContext context) throws IOException {
            boolean ignoreFailure = false;
            XContentParser parser = context.parser();
            String scriptFieldName = parser.currentName();
            Script script = null;
            XContentParser.Token token;
            token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (context.getParseFieldMatcher().match(currentFieldName, SCRIPT_FIELD)) {
                            script = Script.parse(parser, context.getParseFieldMatcher(), context.getDefaultScriptLanguage());
                        } else if (context.getParseFieldMatcher().match(currentFieldName, IGNORE_FAILURE_FIELD)) {
                            ignoreFailure = parser.booleanValue();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].", parser.getTokenLocation());
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (context.getParseFieldMatcher().match(currentFieldName, SCRIPT_FIELD)) {
                            script = Script.parse(parser, context.getParseFieldMatcher(), context.getDefaultScriptLanguage());
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].", parser.getTokenLocation());
                        }
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].", parser.getTokenLocation());
                    }
                }
                this.ignoreFailure = ignoreFailure;
                this.fieldName = scriptFieldName;
                this.script = script;
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT + "] in [" + parser.currentName() + "] but found [" + token + "]", parser.getTokenLocation());
            }
        }

        public String fieldName() {
            return fieldName;
        }

        public Script script() {
            return script;
        }

        public boolean ignoreFailure() {
            return ignoreFailure;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(fieldName);
            builder.field(SCRIPT_FIELD.getPreferredName(), script);
            builder.field(IGNORE_FAILURE_FIELD.getPreferredName(), ignoreFailure);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, script, ignoreFailure);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ScriptField other = (ScriptField) obj;
            return Objects.equals(fieldName, other.fieldName) && Objects.equals(script, other.script) && Objects.equals(ignoreFailure, other.ignoreFailure);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregations, explain, fetchSourceContext, docValueFields, storedFieldsContext, from, highlightBuilder, indexBoosts, minScore, postQueryBuilder, queryBuilder, rescoreBuilders, scriptFields, size, sorts, searchAfterBuilder, sliceBuilder, stats, suggestBuilder, terminateAfter, timeout, trackScores, version, profile, extBuilders);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        SearchSourceBuilder other = (SearchSourceBuilder) obj;
        return Objects.equals(aggregations, other.aggregations) && Objects.equals(explain, other.explain) && Objects.equals(fetchSourceContext, other.fetchSourceContext) && Objects.equals(docValueFields, other.docValueFields) && Objects.equals(storedFieldsContext, other.storedFieldsContext) && Objects.equals(from, other.from) && Objects.equals(highlightBuilder, other.highlightBuilder) && Objects.equals(indexBoosts, other.indexBoosts) && Objects.equals(minScore, other.minScore) && Objects.equals(postQueryBuilder, other.postQueryBuilder) && Objects.equals(queryBuilder, other.queryBuilder) && Objects.equals(rescoreBuilders, other.rescoreBuilders) && Objects.equals(scriptFields, other.scriptFields) && Objects.equals(size, other.size) && Objects.equals(sorts, other.sorts) && Objects.equals(searchAfterBuilder, other.searchAfterBuilder) && Objects.equals(sliceBuilder, other.sliceBuilder) && Objects.equals(stats, other.stats) && Objects.equals(suggestBuilder, other.suggestBuilder) && Objects.equals(terminateAfter, other.terminateAfter) && Objects.equals(timeout, other.timeout) && Objects.equals(trackScores, other.trackScores) && Objects.equals(version, other.version) && Objects.equals(profile, other.profile) && Objects.equals(extBuilders, other.extBuilders);
    }
}