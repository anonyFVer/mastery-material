package org.elasticsearch.script.mustache;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;
import java.io.IOException;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSearchTemplateAction extends BaseRestHandler {

    private static ObjectParser<SearchTemplateRequest, ParseFieldMatcherSupplier> PARSER;

    static {
        PARSER = new ObjectParser<>("search_template");
        PARSER.declareField((parser, request, s) -> request.setScriptParams(parser.map()), new ParseField("params"), ObjectParser.ValueType.OBJECT);
        PARSER.declareString((request, s) -> {
            request.setScriptType(ScriptService.ScriptType.FILE);
            request.setScript(s);
        }, new ParseField("file"));
        PARSER.declareString((request, s) -> {
            request.setScriptType(ScriptService.ScriptType.STORED);
            request.setScript(s);
        }, new ParseField("id"));
        PARSER.declareField((parser, request, value) -> {
            request.setScriptType(ScriptService.ScriptType.INLINE);
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                try (XContentBuilder builder = XContentFactory.contentBuilder(parser.contentType())) {
                    request.setScript(builder.copyCurrentStructure(parser).bytes().utf8ToString());
                } catch (IOException e) {
                    throw new ParsingException(parser.getTokenLocation(), "Could not parse inline template", e);
                }
            } else {
                request.setScript(parser.text());
            }
        }, new ParseField("inline", "template"), ObjectParser.ValueType.OBJECT_OR_STRING);
    }

    private final SearchRequestParsers searchRequestParsers;

    @Inject
    public RestSearchTemplateAction(Settings settings, RestController controller, SearchRequestParsers searchRequestParsers) {
        super(settings);
        this.searchRequestParsers = searchRequestParsers;
        controller.registerHandler(GET, "/_search/template", this);
        controller.registerHandler(POST, "/_search/template", this);
        controller.registerHandler(GET, "/{index}/_search/template", this);
        controller.registerHandler(POST, "/{index}/_search/template", this);
        controller.registerHandler(GET, "/{index}/{type}/_search/template", this);
        controller.registerHandler(POST, "/{index}/{type}/_search/template", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        if (RestActions.hasBodyContent(request) == false) {
            throw new ElasticsearchException("request body is required");
        }
        SearchRequest searchRequest = new SearchRequest();
        RestSearchAction.parseSearchRequest(searchRequest, request, searchRequestParsers, parseFieldMatcher, null);
        SearchTemplateRequest searchTemplateRequest = parse(RestActions.getRestContent(request));
        searchTemplateRequest.setRequest(searchRequest);
        client.execute(SearchTemplateAction.INSTANCE, searchTemplateRequest, new RestStatusToXContentListener<>(channel));
    }

    public static SearchTemplateRequest parse(BytesReference bytes) throws IOException {
        try (XContentParser parser = XContentHelper.createParser(bytes)) {
            return PARSER.parse(parser, new SearchTemplateRequest(), () -> ParseFieldMatcher.STRICT);
        }
    }
}