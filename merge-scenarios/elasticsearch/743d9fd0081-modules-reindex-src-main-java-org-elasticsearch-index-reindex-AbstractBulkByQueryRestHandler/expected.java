package org.elasticsearch.index.reindex;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchRequestParsers;
import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.SIZE_ALL_MATCHES;

public abstract class AbstractBulkByQueryRestHandler<Request extends AbstractBulkByScrollRequest<Request>, A extends GenericAction<Request, BulkIndexByScrollResponse>> extends AbstractBaseReindexRestHandler<Request, A> {

    protected AbstractBulkByQueryRestHandler(Settings settings, SearchRequestParsers searchRequestParsers, ClusterService clusterService, A action) {
        super(settings, searchRequestParsers, clusterService, action);
    }

    protected void parseInternalRequest(Request internal, RestRequest restRequest, Map<String, Consumer<Object>> consumers) throws IOException {
        assert internal != null : "Request should not be null";
        assert restRequest != null : "RestRequest should not be null";
        SearchRequest searchRequest = internal.getSearchRequest();
        int scrollSize = searchRequest.source().size();
        searchRequest.source().size(SIZE_ALL_MATCHES);
        parseSearchRequest(searchRequest, restRequest, consumers);
        internal.setSize(searchRequest.source().size());
        searchRequest.source().size(restRequest.paramAsInt("scroll_size", scrollSize));
        String conflicts = restRequest.param("conflicts");
        if (conflicts != null) {
            internal.setConflicts(conflicts);
        }
        if (restRequest.hasParam("search_timeout")) {
            searchRequest.source().timeout(restRequest.paramAsTime("search_timeout", null));
        }
    }

    protected void parseSearchRequest(SearchRequest searchRequest, RestRequest restRequest, Map<String, Consumer<Object>> consumers) throws IOException {
        assert searchRequest != null : "SearchRequest should not be null";
        assert restRequest != null : "RestRequest should not be null";
        BytesReference content = RestActions.hasBodyContent(restRequest) ? RestActions.getRestContent(restRequest) : null;
        if ((content != null) && (consumers != null && consumers.size() > 0)) {
            Tuple<XContentType, Map<String, Object>> body = XContentHelper.convertToMap(content, false);
            boolean modified = false;
            for (Map.Entry<String, Consumer<Object>> consumer : consumers.entrySet()) {
                Object value = body.v2().remove(consumer.getKey());
                if (value != null) {
                    consumer.getValue().accept(value);
                    modified = true;
                }
            }
            if (modified) {
                try (XContentBuilder builder = XContentFactory.contentBuilder(body.v1())) {
                    content = builder.map(body.v2()).bytes();
                }
            }
        }
        RestSearchAction.parseSearchRequest(searchRequest, restRequest, searchRequestParsers, parseFieldMatcher, content);
    }
}