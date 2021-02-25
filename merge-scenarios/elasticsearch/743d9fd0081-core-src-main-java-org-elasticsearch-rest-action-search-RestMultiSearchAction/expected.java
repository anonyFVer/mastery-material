package org.elasticsearch.rest.action.search;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.lenientNodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMultiSearchAction extends BaseRestHandler {

    private final boolean allowExplicitIndex;

    private final SearchRequestParsers searchRequestParsers;

    @Inject
    public RestMultiSearchAction(Settings settings, RestController controller, SearchRequestParsers searchRequestParsers) {
        super(settings);
        this.searchRequestParsers = searchRequestParsers;
        controller.registerHandler(GET, "/_msearch", this);
        controller.registerHandler(POST, "/_msearch", this);
        controller.registerHandler(GET, "/{index}/_msearch", this);
        controller.registerHandler(POST, "/{index}/_msearch", this);
        controller.registerHandler(GET, "/{index}/{type}/_msearch", this);
        controller.registerHandler(POST, "/{index}/{type}/_msearch", this);
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) throws Exception {
        MultiSearchRequest multiSearchRequest = parseRequest(request, allowExplicitIndex, searchRequestParsers, parseFieldMatcher);
        client.multiSearch(multiSearchRequest, new RestToXContentListener<>(channel));
    }

    public static MultiSearchRequest parseRequest(RestRequest restRequest, boolean allowExplicitIndex, SearchRequestParsers searchRequestParsers, ParseFieldMatcher parseFieldMatcher) throws IOException {
        MultiSearchRequest multiRequest = new MultiSearchRequest();
        if (restRequest.hasParam("max_concurrent_searches")) {
            multiRequest.maxConcurrentSearchRequests(restRequest.paramAsInt("max_concurrent_searches", 0));
        }
        parseMultiLineRequest(restRequest, multiRequest.indicesOptions(), allowExplicitIndex, (searchRequest, bytes) -> {
            try (XContentParser requestParser = XContentFactory.xContent(bytes).createParser(bytes)) {
                final QueryParseContext queryParseContext = new QueryParseContext(searchRequestParsers.queryParsers, requestParser, parseFieldMatcher);
                searchRequest.source(SearchSourceBuilder.fromXContent(queryParseContext, searchRequestParsers.aggParsers, searchRequestParsers.suggesters));
                multiRequest.add(searchRequest);
            } catch (IOException e) {
                throw new ElasticsearchParseException("Exception when parsing search request", e);
            }
        });
        return multiRequest;
    }

    public static void parseMultiLineRequest(RestRequest request, IndicesOptions indicesOptions, boolean allowExplicitIndex, BiConsumer<SearchRequest, BytesReference> consumer) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        String searchType = request.param("search_type");
        String routing = request.param("routing");
        final BytesReference data = RestActions.getRestContent(request);
        XContent xContent = XContentFactory.xContent(data);
        int from = 0;
        int length = data.length();
        byte marker = xContent.streamSeparator();
        while (true) {
            int nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }
            if (nextMarker == 0) {
                from = nextMarker + 1;
                continue;
            }
            SearchRequest searchRequest = new SearchRequest();
            if (indices != null) {
                searchRequest.indices(indices);
            }
            if (indicesOptions != null) {
                searchRequest.indicesOptions(indicesOptions);
            }
            if (types != null && types.length > 0) {
                searchRequest.types(types);
            }
            if (routing != null) {
                searchRequest.routing(routing);
            }
            if (searchType != null) {
                searchRequest.searchType(searchType);
            }
            IndicesOptions defaultOptions = IndicesOptions.strictExpandOpenAndForbidClosed();
            if (nextMarker - from > 0) {
                try (XContentParser parser = xContent.createParser(data.slice(from, nextMarker - from))) {
                    Map<String, Object> source = parser.map();
                    for (Map.Entry<String, Object> entry : source.entrySet()) {
                        Object value = entry.getValue();
                        if ("index".equals(entry.getKey()) || "indices".equals(entry.getKey())) {
                            if (!allowExplicitIndex) {
                                throw new IllegalArgumentException("explicit index in multi search is not allowed");
                            }
                            searchRequest.indices(nodeStringArrayValue(value));
                        } else if ("type".equals(entry.getKey()) || "types".equals(entry.getKey())) {
                            searchRequest.types(nodeStringArrayValue(value));
                        } else if ("search_type".equals(entry.getKey()) || "searchType".equals(entry.getKey())) {
                            searchRequest.searchType(nodeStringValue(value, null));
                        } else if ("request_cache".equals(entry.getKey()) || "requestCache".equals(entry.getKey())) {
                            searchRequest.requestCache(lenientNodeBooleanValue(value));
                        } else if ("preference".equals(entry.getKey())) {
                            searchRequest.preference(nodeStringValue(value, null));
                        } else if ("routing".equals(entry.getKey())) {
                            searchRequest.routing(nodeStringValue(value, null));
                        }
                    }
                    defaultOptions = IndicesOptions.fromMap(source, defaultOptions);
                }
            }
            searchRequest.indicesOptions(defaultOptions);
            from = nextMarker + 1;
            nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }
            consumer.accept(searchRequest, data.slice(from, nextMarker - from));
            from = nextMarker + 1;
        }
    }

    private static int findNextMarker(byte marker, int from, BytesReference data, int length) {
        for (int i = from; i < length; i++) {
            if (data.get(i) == marker) {
                return i;
            }
        }
        return -1;
    }
}