package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import java.io.IOException;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;

public class RestIndexAction extends BaseRestHandler {

    @Inject
    public RestIndexAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/{index}/{type}", this);
        controller.registerHandler(PUT, "/{index}/{type}/{id}", this);
        controller.registerHandler(POST, "/{index}/{type}/{id}", this);
        CreateHandler createHandler = new CreateHandler(settings);
        controller.registerHandler(PUT, "/{index}/{type}/{id}/_create", createHandler);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_create", createHandler);
    }

    final class CreateHandler extends BaseRestHandler {

        protected CreateHandler(Settings settings) {
            super(settings);
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, final NodeClient client) {
            request.params().put("op_type", "create");
            RestIndexAction.this.handleRequest(request, channel, client);
        }
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) {
        IndexRequest indexRequest = new IndexRequest(request.param("index"), request.param("type"), request.param("id"));
        indexRequest.routing(request.param("routing"));
        indexRequest.parent(request.param("parent"));
        indexRequest.timestamp(request.param("timestamp"));
        if (request.hasParam("ttl")) {
            indexRequest.ttl(request.param("ttl"));
        }
        indexRequest.setPipeline(request.param("pipeline"));
        indexRequest.source(request.content());
        indexRequest.timeout(request.paramAsTime("timeout", IndexRequest.DEFAULT_TIMEOUT));
        indexRequest.setRefreshPolicy(request.param("refresh"));
        indexRequest.version(RestActions.parseVersion(request));
        indexRequest.versionType(VersionType.fromString(request.param("version_type"), indexRequest.versionType()));
        String sOpType = request.param("op_type");
        if (sOpType != null) {
            try {
                indexRequest.opType(sOpType);
            } catch (IllegalArgumentException eia) {
                try {
                    XContentBuilder builder = channel.newErrorBuilder();
                    channel.sendResponse(new BytesRestResponse(BAD_REQUEST, builder.startObject().field("error", eia.getMessage()).endObject()));
                } catch (IOException e1) {
                    logger.warn("Failed to send response", e1);
                    return;
                }
            }
        }
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards != null) {
            indexRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        client.index(indexRequest, new RestStatusToXContentListener<>(channel, r -> r.getLocation(indexRequest.routing())));
    }
}