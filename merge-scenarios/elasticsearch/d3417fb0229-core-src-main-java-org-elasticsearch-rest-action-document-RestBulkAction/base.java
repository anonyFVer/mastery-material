package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestBulkAction extends BaseRestHandler {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(RestBulkAction.class));

    private final boolean allowExplicitIndex;

    @Inject
    public RestBulkAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/_bulk", this);
        controller.registerHandler(PUT, "/_bulk", this);
        controller.registerHandler(POST, "/{index}/_bulk", this);
        controller.registerHandler(PUT, "/{index}/_bulk", this);
        controller.registerHandler(POST, "/{index}/{type}/_bulk", this);
        controller.registerHandler(PUT, "/{index}/{type}/_bulk", this);
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) throws Exception {
        BulkRequest bulkRequest = Requests.bulkRequest();
        String defaultIndex = request.param("index");
        String defaultType = request.param("type");
        String defaultRouting = request.param("routing");
        FetchSourceContext defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
        String fieldsParam = request.param("fields");
        if (fieldsParam != null) {
            DEPRECATION_LOGGER.deprecated("Deprecated field [fields] used, expected [_source] instead");
        }
        String[] defaultFields = fieldsParam != null ? Strings.commaDelimitedListToStringArray(fieldsParam) : null;
        String defaultPipeline = request.param("pipeline");
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            bulkRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
        bulkRequest.setRefreshPolicy(request.param("refresh"));
        bulkRequest.add(request.content(), defaultIndex, defaultType, defaultRouting, defaultFields, defaultFetchSourceContext, defaultPipeline, null, allowExplicitIndex);
        client.bulk(bulkRequest, new RestBuilderListener<BulkResponse>(channel) {

            @Override
            public RestResponse buildResponse(BulkResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(Fields.TOOK, response.getTookInMillis());
                if (response.getIngestTookInMillis() != BulkResponse.NO_INGEST_TOOK) {
                    builder.field(Fields.INGEST_TOOK, response.getIngestTookInMillis());
                }
                builder.field(Fields.ERRORS, response.hasFailures());
                builder.startArray(Fields.ITEMS);
                for (BulkItemResponse itemResponse : response) {
                    builder.startObject();
                    itemResponse.toXContent(builder, request);
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }

    static final class Fields {

        static final String ITEMS = "items";

        static final String ERRORS = "errors";

        static final String TOOK = "took";

        static final String INGEST_TOOK = "ingest_took";
    }
}