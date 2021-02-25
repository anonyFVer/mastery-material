package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.protocol.xpack.XPackUsageResponse;
import java.io.IOException;
import static java.util.Collections.emptySet;

public final class XPackClient {

    private final RestHighLevelClient restHighLevelClient;

    XPackClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    public XPackInfoResponse info(XPackInfoRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, RequestConverters::xPackInfo, options, XPackInfoResponse::fromXContent, emptySet());
    }

    public void infoAsync(XPackInfoRequest request, RequestOptions options, ActionListener<XPackInfoResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request, RequestConverters::xPackInfo, options, XPackInfoResponse::fromXContent, listener, emptySet());
    }

    public XPackUsageResponse usage(XPackUsageRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, RequestConverters::xpackUsage, options, XPackUsageResponse::fromXContent, emptySet());
    }

    public void usageAsync(XPackUsageRequest request, RequestOptions options, ActionListener<XPackUsageResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request, RequestConverters::xpackUsage, options, XPackUsageResponse::fromXContent, listener, emptySet());
    }
}