package org.elasticsearch.client.transport.support;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.TransportActionNodeProxy;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.Collections.unmodifiableMap;

public class TransportProxyClient {

    private final TransportClientNodesService nodesService;

    private final Map<Action, TransportActionNodeProxy> proxies;

    public TransportProxyClient(Settings settings, TransportService transportService, TransportClientNodesService nodesService, List<GenericAction> actions) {
        this.nodesService = nodesService;
        Map<Action, TransportActionNodeProxy> proxies = new HashMap<>();
        for (GenericAction action : actions) {
            if (action instanceof Action) {
                proxies.put((Action) action, new TransportActionNodeProxy(settings, action, transportService));
            }
        }
        this.proxies = unmodifiableMap(proxies);
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void execute(final Action<Request, Response, RequestBuilder> action, final Request request, ActionListener<Response> listener) {
        final TransportActionNodeProxy<Request, Response> proxy = proxies.get(action);
        nodesService.execute((n, l) -> proxy.execute(n, request, l), listener);
    }
}