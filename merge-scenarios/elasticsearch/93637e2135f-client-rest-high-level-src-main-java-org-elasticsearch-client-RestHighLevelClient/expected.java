package org.elasticsearch.client;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.rankeval.RankEvalRequest;
import org.elasticsearch.index.rankeval.RankEvalResponse;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.mustache.MultiSearchTemplateRequest;
import org.elasticsearch.script.mustache.MultiSearchTemplateResponse;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.adjacency.ParsedAdjacencyMatrix;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilters;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoHashGrid;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.ParsedGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedAutoDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedHistogram;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.ParsedMissing;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.ParsedBinaryRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedDateRange;
import org.elasticsearch.search.aggregations.bucket.range.ParsedGeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.ParsedSampler;
import org.elasticsearch.search.aggregations.bucket.significant.ParsedSignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.significant.ParsedSignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantLongTerms;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedDoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geobounds.ParsedGeoBounds;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geocentroid.ParsedGeoCentroid;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.ParsedHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.ParsedHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.ParsedTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.ParsedTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.scripted.ParsedScriptedMetric;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ParsedExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.ParsedTopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.ParsedSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.InternalBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.ParsedBucketMetricValue;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.ParsedPercentilesBucket;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.ParsedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ParsedExtendedStatsBucket;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativePipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.derivative.ParsedDerivative;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;

public class RestHighLevelClient implements Closeable {

    private final RestClient client;

    private final NamedXContentRegistry registry;

    private final CheckedConsumer<RestClient, IOException> doClose;

    private final IndicesClient indicesClient = new IndicesClient(this);

    private final ClusterClient clusterClient = new ClusterClient(this);

    private final IngestClient ingestClient = new IngestClient(this);

    private final SnapshotClient snapshotClient = new SnapshotClient(this);

    private final TasksClient tasksClient = new TasksClient(this);

    private final XPackClient xPackClient = new XPackClient(this);

    private final WatcherClient watcherClient = new WatcherClient(this);

    private final LicenseClient licenseClient = new LicenseClient(this);

    private final MachineLearningClient machineLearningClient = new MachineLearningClient(this);

    private final IndexLifecycleClient ilmClient = new IndexLifecycleClient(this);

    public RestHighLevelClient(RestClientBuilder restClientBuilder) {
        this(restClientBuilder, Collections.emptyList());
    }

    protected RestHighLevelClient(RestClientBuilder restClientBuilder, List<NamedXContentRegistry.Entry> namedXContentEntries) {
        this(restClientBuilder.build(), RestClient::close, namedXContentEntries);
    }

    protected RestHighLevelClient(RestClient restClient, CheckedConsumer<RestClient, IOException> doClose, List<NamedXContentRegistry.Entry> namedXContentEntries) {
        this.client = Objects.requireNonNull(restClient, "restClient must not be null");
        this.doClose = Objects.requireNonNull(doClose, "doClose consumer must not be null");
        this.registry = new NamedXContentRegistry(Stream.of(getDefaultNamedXContents().stream(), getProvidedNamedXContents().stream(), namedXContentEntries.stream()).flatMap(Function.identity()).collect(toList()));
    }

    public final RestClient getLowLevelClient() {
        return client;
    }

    @Override
    public final void close() throws IOException {
        doClose.accept(client);
    }

    public final IndicesClient indices() {
        return indicesClient;
    }

    public final ClusterClient cluster() {
        return clusterClient;
    }

    public final IngestClient ingest() {
        return ingestClient;
    }

    public final SnapshotClient snapshot() {
        return snapshotClient;
    }

    public final TasksClient tasks() {
        return tasksClient;
    }

    public final XPackClient xpack() {
        return xPackClient;
    }

    public WatcherClient watcher() {
        return watcherClient;
    }

    public LicenseClient license() {
        return licenseClient;
    }

    public IndexLifecycleClient indexLifecycle() {
        return ilmClient;
    }

    public MachineLearningClient machineLearning() {
        return machineLearningClient;
    }

    public final BulkResponse bulk(BulkRequest bulkRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(bulkRequest, RequestConverters::bulk, options, BulkResponse::fromXContent, emptySet());
    }

    public final void bulkAsync(BulkRequest bulkRequest, RequestOptions options, ActionListener<BulkResponse> listener) {
        performRequestAsyncAndParseEntity(bulkRequest, RequestConverters::bulk, options, BulkResponse::fromXContent, listener, emptySet());
    }

    public final boolean ping(RequestOptions options) throws IOException {
        return performRequest(new MainRequest(), (request) -> RequestConverters.ping(), options, RestHighLevelClient::convertExistsResponse, emptySet());
    }

    public final MainResponse info(RequestOptions options) throws IOException {
        return performRequestAndParseEntity(new MainRequest(), (request) -> RequestConverters.info(), options, MainResponse::fromXContent, emptySet());
    }

    public final GetResponse get(GetRequest getRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(getRequest, RequestConverters::get, options, GetResponse::fromXContent, singleton(404));
    }

    public final void getAsync(GetRequest getRequest, RequestOptions options, ActionListener<GetResponse> listener) {
        performRequestAsyncAndParseEntity(getRequest, RequestConverters::get, options, GetResponse::fromXContent, listener, singleton(404));
    }

    @Deprecated
    public final MultiGetResponse multiGet(MultiGetRequest multiGetRequest, RequestOptions options) throws IOException {
        return mget(multiGetRequest, options);
    }

    public final MultiGetResponse mget(MultiGetRequest multiGetRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(multiGetRequest, RequestConverters::multiGet, options, MultiGetResponse::fromXContent, singleton(404));
    }

    @Deprecated
    public final void multiGetAsync(MultiGetRequest multiGetRequest, RequestOptions options, ActionListener<MultiGetResponse> listener) {
        mgetAsync(multiGetRequest, options, listener);
    }

    public final void mgetAsync(MultiGetRequest multiGetRequest, RequestOptions options, ActionListener<MultiGetResponse> listener) {
        performRequestAsyncAndParseEntity(multiGetRequest, RequestConverters::multiGet, options, MultiGetResponse::fromXContent, listener, singleton(404));
    }

    public final boolean exists(GetRequest getRequest, RequestOptions options) throws IOException {
        return performRequest(getRequest, RequestConverters::exists, options, RestHighLevelClient::convertExistsResponse, emptySet());
    }

    public final void existsAsync(GetRequest getRequest, RequestOptions options, ActionListener<Boolean> listener) {
        performRequestAsync(getRequest, RequestConverters::exists, options, RestHighLevelClient::convertExistsResponse, listener, emptySet());
    }

    public final IndexResponse index(IndexRequest indexRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(indexRequest, RequestConverters::index, options, IndexResponse::fromXContent, emptySet());
    }

    public final void indexAsync(IndexRequest indexRequest, RequestOptions options, ActionListener<IndexResponse> listener) {
        performRequestAsyncAndParseEntity(indexRequest, RequestConverters::index, options, IndexResponse::fromXContent, listener, emptySet());
    }

    public final UpdateResponse update(UpdateRequest updateRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(updateRequest, RequestConverters::update, options, UpdateResponse::fromXContent, emptySet());
    }

    public final void updateAsync(UpdateRequest updateRequest, RequestOptions options, ActionListener<UpdateResponse> listener) {
        performRequestAsyncAndParseEntity(updateRequest, RequestConverters::update, options, UpdateResponse::fromXContent, listener, emptySet());
    }

    public final DeleteResponse delete(DeleteRequest deleteRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(deleteRequest, RequestConverters::delete, options, DeleteResponse::fromXContent, singleton(404));
    }

    public final void deleteAsync(DeleteRequest deleteRequest, RequestOptions options, ActionListener<DeleteResponse> listener) {
        performRequestAsyncAndParseEntity(deleteRequest, RequestConverters::delete, options, DeleteResponse::fromXContent, listener, Collections.singleton(404));
    }

    public final SearchResponse search(SearchRequest searchRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(searchRequest, RequestConverters::search, options, SearchResponse::fromXContent, emptySet());
    }

    public final void searchAsync(SearchRequest searchRequest, RequestOptions options, ActionListener<SearchResponse> listener) {
        performRequestAsyncAndParseEntity(searchRequest, RequestConverters::search, options, SearchResponse::fromXContent, listener, emptySet());
    }

    @Deprecated
    public final MultiSearchResponse multiSearch(MultiSearchRequest multiSearchRequest, RequestOptions options) throws IOException {
        return msearch(multiSearchRequest, options);
    }

    public final MultiSearchResponse msearch(MultiSearchRequest multiSearchRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(multiSearchRequest, RequestConverters::multiSearch, options, MultiSearchResponse::fromXContext, emptySet());
    }

    @Deprecated
    public final void multiSearchAsync(MultiSearchRequest searchRequest, RequestOptions options, ActionListener<MultiSearchResponse> listener) {
        msearchAsync(searchRequest, options, listener);
    }

    public final void msearchAsync(MultiSearchRequest searchRequest, RequestOptions options, ActionListener<MultiSearchResponse> listener) {
        performRequestAsyncAndParseEntity(searchRequest, RequestConverters::multiSearch, options, MultiSearchResponse::fromXContext, listener, emptySet());
    }

    @Deprecated
    public final SearchResponse searchScroll(SearchScrollRequest searchScrollRequest, RequestOptions options) throws IOException {
        return scroll(searchScrollRequest, options);
    }

    public final SearchResponse scroll(SearchScrollRequest searchScrollRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(searchScrollRequest, RequestConverters::searchScroll, options, SearchResponse::fromXContent, emptySet());
    }

    @Deprecated
    public final void searchScrollAsync(SearchScrollRequest searchScrollRequest, RequestOptions options, ActionListener<SearchResponse> listener) {
        scrollAsync(searchScrollRequest, options, listener);
    }

    public final void scrollAsync(SearchScrollRequest searchScrollRequest, RequestOptions options, ActionListener<SearchResponse> listener) {
        performRequestAsyncAndParseEntity(searchScrollRequest, RequestConverters::searchScroll, options, SearchResponse::fromXContent, listener, emptySet());
    }

    public final ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(clearScrollRequest, RequestConverters::clearScroll, options, ClearScrollResponse::fromXContent, emptySet());
    }

    public final void clearScrollAsync(ClearScrollRequest clearScrollRequest, RequestOptions options, ActionListener<ClearScrollResponse> listener) {
        performRequestAsyncAndParseEntity(clearScrollRequest, RequestConverters::clearScroll, options, ClearScrollResponse::fromXContent, listener, emptySet());
    }

    public final SearchTemplateResponse searchTemplate(SearchTemplateRequest searchTemplateRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(searchTemplateRequest, RequestConverters::searchTemplate, options, SearchTemplateResponse::fromXContent, emptySet());
    }

    public final void searchTemplateAsync(SearchTemplateRequest searchTemplateRequest, RequestOptions options, ActionListener<SearchTemplateResponse> listener) {
        performRequestAsyncAndParseEntity(searchTemplateRequest, RequestConverters::searchTemplate, options, SearchTemplateResponse::fromXContent, listener, emptySet());
    }

    public final ExplainResponse explain(ExplainRequest explainRequest, RequestOptions options) throws IOException {
        return performRequest(explainRequest, RequestConverters::explain, options, response -> {
            CheckedFunction<XContentParser, ExplainResponse, IOException> entityParser = parser -> ExplainResponse.fromXContent(parser, convertExistsResponse(response));
            return parseEntity(response.getEntity(), entityParser);
        }, singleton(404));
    }

    public final void explainAsync(ExplainRequest explainRequest, RequestOptions options, ActionListener<ExplainResponse> listener) {
        performRequestAsync(explainRequest, RequestConverters::explain, options, response -> {
            CheckedFunction<XContentParser, ExplainResponse, IOException> entityParser = parser -> ExplainResponse.fromXContent(parser, convertExistsResponse(response));
            return parseEntity(response.getEntity(), entityParser);
        }, listener, singleton(404));
    }

    public final RankEvalResponse rankEval(RankEvalRequest rankEvalRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(rankEvalRequest, RequestConverters::rankEval, options, RankEvalResponse::fromXContent, emptySet());
    }

    public final MultiSearchTemplateResponse msearchTemplate(MultiSearchTemplateRequest multiSearchTemplateRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(multiSearchTemplateRequest, RequestConverters::multiSearchTemplate, options, MultiSearchTemplateResponse::fromXContext, emptySet());
    }

    public final void msearchTemplateAsync(MultiSearchTemplateRequest multiSearchTemplateRequest, RequestOptions options, ActionListener<MultiSearchTemplateResponse> listener) {
        performRequestAsyncAndParseEntity(multiSearchTemplateRequest, RequestConverters::multiSearchTemplate, options, MultiSearchTemplateResponse::fromXContext, listener, emptySet());
    }

    public final void rankEvalAsync(RankEvalRequest rankEvalRequest, RequestOptions options, ActionListener<RankEvalResponse> listener) {
        performRequestAsyncAndParseEntity(rankEvalRequest, RequestConverters::rankEval, options, RankEvalResponse::fromXContent, listener, emptySet());
    }

    public final FieldCapabilitiesResponse fieldCaps(FieldCapabilitiesRequest fieldCapabilitiesRequest, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(fieldCapabilitiesRequest, RequestConverters::fieldCaps, options, FieldCapabilitiesResponse::fromXContent, emptySet());
    }

    public GetStoredScriptResponse getScript(GetStoredScriptRequest request, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(request, RequestConverters::getScript, options, GetStoredScriptResponse::fromXContent, emptySet());
    }

    public void getScriptAsync(GetStoredScriptRequest request, RequestOptions options, ActionListener<GetStoredScriptResponse> listener) {
        performRequestAsyncAndParseEntity(request, RequestConverters::getScript, options, GetStoredScriptResponse::fromXContent, listener, emptySet());
    }

    public DeleteStoredScriptResponse deleteScript(DeleteStoredScriptRequest request, RequestOptions options) throws IOException {
        return performRequestAndParseEntity(request, RequestConverters::deleteScript, options, DeleteStoredScriptResponse::fromXContent, emptySet());
    }

    public void deleteScriptAsync(DeleteStoredScriptRequest request, RequestOptions options, ActionListener<DeleteStoredScriptResponse> listener) {
        performRequestAsyncAndParseEntity(request, RequestConverters::deleteScript, options, DeleteStoredScriptResponse::fromXContent, listener, emptySet());
    }

    public final void fieldCapsAsync(FieldCapabilitiesRequest fieldCapabilitiesRequest, RequestOptions options, ActionListener<FieldCapabilitiesResponse> listener) {
        performRequestAsyncAndParseEntity(fieldCapabilitiesRequest, RequestConverters::fieldCaps, options, FieldCapabilitiesResponse::fromXContent, listener, emptySet());
    }

    protected final <Req extends ActionRequest, Resp> Resp performRequestAndParseEntity(Req request, CheckedFunction<Req, Request, IOException> requestConverter, RequestOptions options, CheckedFunction<XContentParser, Resp, IOException> entityParser, Set<Integer> ignores) throws IOException {
        return performRequest(request, requestConverter, options, response -> parseEntity(response.getEntity(), entityParser), ignores);
    }

    protected final <Req extends ActionRequest, Resp> Resp performRequest(Req request, CheckedFunction<Req, Request, IOException> requestConverter, RequestOptions options, CheckedFunction<Response, Resp, IOException> responseConverter, Set<Integer> ignores) throws IOException {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            throw validationException;
        }
        Request req = requestConverter.apply(request);
        req.setOptions(options);
        Response response;
        try {
            response = client.performRequest(req);
        } catch (ResponseException e) {
            if (ignores.contains(e.getResponse().getStatusLine().getStatusCode())) {
                try {
                    return responseConverter.apply(e.getResponse());
                } catch (Exception innerException) {
                    throw parseResponseException(e);
                }
            }
            throw parseResponseException(e);
        }
        try {
            return responseConverter.apply(response);
        } catch (Exception e) {
            throw new IOException("Unable to parse response body for " + response, e);
        }
    }

    protected final <Req extends ActionRequest, Resp> void performRequestAsyncAndParseEntity(Req request, CheckedFunction<Req, Request, IOException> requestConverter, RequestOptions options, CheckedFunction<XContentParser, Resp, IOException> entityParser, ActionListener<Resp> listener, Set<Integer> ignores) {
        performRequestAsync(request, requestConverter, options, response -> parseEntity(response.getEntity(), entityParser), listener, ignores);
    }

    protected final <Req extends ActionRequest, Resp> void performRequestAsync(Req request, CheckedFunction<Req, Request, IOException> requestConverter, RequestOptions options, CheckedFunction<Response, Resp, IOException> responseConverter, ActionListener<Resp> listener, Set<Integer> ignores) {
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            listener.onFailure(validationException);
            return;
        }
        Request req;
        try {
            req = requestConverter.apply(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        req.setOptions(options);
        ResponseListener responseListener = wrapResponseListener(responseConverter, listener, ignores);
        client.performRequestAsync(req, responseListener);
    }

    final <Resp> ResponseListener wrapResponseListener(CheckedFunction<Response, Resp, IOException> responseConverter, ActionListener<Resp> actionListener, Set<Integer> ignores) {
        return new ResponseListener() {

            @Override
            public void onSuccess(Response response) {
                try {
                    actionListener.onResponse(responseConverter.apply(response));
                } catch (Exception e) {
                    IOException ioe = new IOException("Unable to parse response body for " + response, e);
                    onFailure(ioe);
                }
            }

            @Override
            public void onFailure(Exception exception) {
                if (exception instanceof ResponseException) {
                    ResponseException responseException = (ResponseException) exception;
                    Response response = responseException.getResponse();
                    if (ignores.contains(response.getStatusLine().getStatusCode())) {
                        try {
                            actionListener.onResponse(responseConverter.apply(response));
                        } catch (Exception innerException) {
                            actionListener.onFailure(parseResponseException(responseException));
                        }
                    } else {
                        actionListener.onFailure(parseResponseException(responseException));
                    }
                } else {
                    actionListener.onFailure(exception);
                }
            }
        };
    }

    protected final ElasticsearchStatusException parseResponseException(ResponseException responseException) {
        Response response = responseException.getResponse();
        HttpEntity entity = response.getEntity();
        ElasticsearchStatusException elasticsearchException;
        if (entity == null) {
            elasticsearchException = new ElasticsearchStatusException(responseException.getMessage(), RestStatus.fromCode(response.getStatusLine().getStatusCode()), responseException);
        } else {
            try {
                elasticsearchException = parseEntity(entity, BytesRestResponse::errorFromXContent);
                elasticsearchException.addSuppressed(responseException);
            } catch (Exception e) {
                RestStatus restStatus = RestStatus.fromCode(response.getStatusLine().getStatusCode());
                elasticsearchException = new ElasticsearchStatusException("Unable to parse response body", restStatus, responseException);
                elasticsearchException.addSuppressed(e);
            }
        }
        return elasticsearchException;
    }

    protected final <Resp> Resp parseEntity(final HttpEntity entity, final CheckedFunction<XContentParser, Resp, IOException> entityParser) throws IOException {
        if (entity == null) {
            throw new IllegalStateException("Response body expected but not returned");
        }
        if (entity.getContentType() == null) {
            throw new IllegalStateException("Elasticsearch didn't return the [Content-Type] header, unable to parse response body");
        }
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(entity.getContentType().getValue());
        if (xContentType == null) {
            throw new IllegalStateException("Unsupported Content-Type: " + entity.getContentType().getValue());
        }
        try (XContentParser parser = xContentType.xContent().createParser(registry, DEPRECATION_HANDLER, entity.getContent())) {
            return entityParser.apply(parser);
        }
    }

    private static RequestOptions optionsForHeaders(Header[] headers) {
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        for (Header header : headers) {
            Objects.requireNonNull(header, "header cannot be null");
            options.addHeader(header.getName(), header.getValue());
        }
        return options.build();
    }

    static boolean convertExistsResponse(Response response) {
        return response.getStatusLine().getStatusCode() == 200;
    }

    private static final DeprecationHandler DEPRECATION_HANDLER = new DeprecationHandler() {

        @Override
        public void usedDeprecatedName(String usedName, String modernName) {
        }

        @Override
        public void usedDeprecatedField(String usedName, String replacedWith) {
        }
    };

    static List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        Map<String, ContextParser<Object, ? extends Aggregation>> map = new HashMap<>();
        map.put(CardinalityAggregationBuilder.NAME, (p, c) -> ParsedCardinality.fromXContent(p, (String) c));
        map.put(InternalHDRPercentiles.NAME, (p, c) -> ParsedHDRPercentiles.fromXContent(p, (String) c));
        map.put(InternalHDRPercentileRanks.NAME, (p, c) -> ParsedHDRPercentileRanks.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentiles.NAME, (p, c) -> ParsedTDigestPercentiles.fromXContent(p, (String) c));
        map.put(InternalTDigestPercentileRanks.NAME, (p, c) -> ParsedTDigestPercentileRanks.fromXContent(p, (String) c));
        map.put(PercentilesBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedPercentilesBucket.fromXContent(p, (String) c));
        map.put(MinAggregationBuilder.NAME, (p, c) -> ParsedMin.fromXContent(p, (String) c));
        map.put(MaxAggregationBuilder.NAME, (p, c) -> ParsedMax.fromXContent(p, (String) c));
        map.put(SumAggregationBuilder.NAME, (p, c) -> ParsedSum.fromXContent(p, (String) c));
        map.put(AvgAggregationBuilder.NAME, (p, c) -> ParsedAvg.fromXContent(p, (String) c));
        map.put(ValueCountAggregationBuilder.NAME, (p, c) -> ParsedValueCount.fromXContent(p, (String) c));
        map.put(InternalSimpleValue.NAME, (p, c) -> ParsedSimpleValue.fromXContent(p, (String) c));
        map.put(DerivativePipelineAggregationBuilder.NAME, (p, c) -> ParsedDerivative.fromXContent(p, (String) c));
        map.put(InternalBucketMetricValue.NAME, (p, c) -> ParsedBucketMetricValue.fromXContent(p, (String) c));
        map.put(StatsAggregationBuilder.NAME, (p, c) -> ParsedStats.fromXContent(p, (String) c));
        map.put(StatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedStatsBucket.fromXContent(p, (String) c));
        map.put(ExtendedStatsAggregationBuilder.NAME, (p, c) -> ParsedExtendedStats.fromXContent(p, (String) c));
        map.put(ExtendedStatsBucketPipelineAggregationBuilder.NAME, (p, c) -> ParsedExtendedStatsBucket.fromXContent(p, (String) c));
        map.put(GeoBoundsAggregationBuilder.NAME, (p, c) -> ParsedGeoBounds.fromXContent(p, (String) c));
        map.put(GeoCentroidAggregationBuilder.NAME, (p, c) -> ParsedGeoCentroid.fromXContent(p, (String) c));
        map.put(HistogramAggregationBuilder.NAME, (p, c) -> ParsedHistogram.fromXContent(p, (String) c));
        map.put(DateHistogramAggregationBuilder.NAME, (p, c) -> ParsedDateHistogram.fromXContent(p, (String) c));
        map.put(AutoDateHistogramAggregationBuilder.NAME, (p, c) -> ParsedAutoDateHistogram.fromXContent(p, (String) c));
        map.put(StringTerms.NAME, (p, c) -> ParsedStringTerms.fromXContent(p, (String) c));
        map.put(LongTerms.NAME, (p, c) -> ParsedLongTerms.fromXContent(p, (String) c));
        map.put(DoubleTerms.NAME, (p, c) -> ParsedDoubleTerms.fromXContent(p, (String) c));
        map.put(MissingAggregationBuilder.NAME, (p, c) -> ParsedMissing.fromXContent(p, (String) c));
        map.put(NestedAggregationBuilder.NAME, (p, c) -> ParsedNested.fromXContent(p, (String) c));
        map.put(ReverseNestedAggregationBuilder.NAME, (p, c) -> ParsedReverseNested.fromXContent(p, (String) c));
        map.put(GlobalAggregationBuilder.NAME, (p, c) -> ParsedGlobal.fromXContent(p, (String) c));
        map.put(FilterAggregationBuilder.NAME, (p, c) -> ParsedFilter.fromXContent(p, (String) c));
        map.put(InternalSampler.PARSER_NAME, (p, c) -> ParsedSampler.fromXContent(p, (String) c));
        map.put(GeoGridAggregationBuilder.NAME, (p, c) -> ParsedGeoHashGrid.fromXContent(p, (String) c));
        map.put(RangeAggregationBuilder.NAME, (p, c) -> ParsedRange.fromXContent(p, (String) c));
        map.put(DateRangeAggregationBuilder.NAME, (p, c) -> ParsedDateRange.fromXContent(p, (String) c));
        map.put(GeoDistanceAggregationBuilder.NAME, (p, c) -> ParsedGeoDistance.fromXContent(p, (String) c));
        map.put(FiltersAggregationBuilder.NAME, (p, c) -> ParsedFilters.fromXContent(p, (String) c));
        map.put(AdjacencyMatrixAggregationBuilder.NAME, (p, c) -> ParsedAdjacencyMatrix.fromXContent(p, (String) c));
        map.put(SignificantLongTerms.NAME, (p, c) -> ParsedSignificantLongTerms.fromXContent(p, (String) c));
        map.put(SignificantStringTerms.NAME, (p, c) -> ParsedSignificantStringTerms.fromXContent(p, (String) c));
        map.put(ScriptedMetricAggregationBuilder.NAME, (p, c) -> ParsedScriptedMetric.fromXContent(p, (String) c));
        map.put(IpRangeAggregationBuilder.NAME, (p, c) -> ParsedBinaryRange.fromXContent(p, (String) c));
        map.put(TopHitsAggregationBuilder.NAME, (p, c) -> ParsedTopHits.fromXContent(p, (String) c));
        map.put(CompositeAggregationBuilder.NAME, (p, c) -> ParsedComposite.fromXContent(p, (String) c));
        List<NamedXContentRegistry.Entry> entries = map.entrySet().stream().map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue())).collect(Collectors.toList());
        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(TermSuggestionBuilder.SUGGESTION_NAME), (parser, context) -> TermSuggestion.fromXContent(parser, (String) context)));
        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(PhraseSuggestionBuilder.SUGGESTION_NAME), (parser, context) -> PhraseSuggestion.fromXContent(parser, (String) context)));
        entries.add(new NamedXContentRegistry.Entry(Suggest.Suggestion.class, new ParseField(CompletionSuggestionBuilder.SUGGESTION_NAME), (parser, context) -> CompletionSuggestion.fromXContent(parser, (String) context)));
        return entries;
    }

    static List<NamedXContentRegistry.Entry> getProvidedNamedXContents() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        for (NamedXContentProvider service : ServiceLoader.load(NamedXContentProvider.class)) {
            entries.addAll(service.getNamedXContentParsers());
        }
        return entries;
    }
}