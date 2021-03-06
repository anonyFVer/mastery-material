package org.elasticsearch.xpack.core;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.license.LicensingClient;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.xpack.core.action.XPackInfoAction;
import org.elasticsearch.xpack.core.action.XPackInfoRequestBuilder;
import org.elasticsearch.xpack.core.indexlifecycle.client.ILMClient;
import org.elasticsearch.xpack.core.ml.client.MachineLearningClient;
import org.elasticsearch.xpack.core.monitoring.client.MonitoringClient;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import java.util.Collections;
import java.util.Map;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public class XPackClient {

    private final Client client;

    private final LicensingClient licensingClient;

    private final MonitoringClient monitoringClient;

    private final SecurityClient securityClient;

    private final WatcherClient watcherClient;

    private final MachineLearningClient machineLearning;

    private final ILMClient ilmClient;

    public XPackClient(Client client) {
        this.client = client;
        this.licensingClient = new LicensingClient(client);
        this.monitoringClient = new MonitoringClient(client);
        this.securityClient = new SecurityClient(client);
        this.watcherClient = new WatcherClient(client);
        this.machineLearning = new MachineLearningClient(client);
        this.ilmClient = new ILMClient(client);
    }

    public Client es() {
        return client;
    }

    public LicensingClient licensing() {
        return licensingClient;
    }

    public MonitoringClient monitoring() {
        return monitoringClient;
    }

    public SecurityClient security() {
        return securityClient;
    }

    public WatcherClient watcher() {
        return watcherClient;
    }

    public MachineLearningClient machineLearning() {
        return machineLearning;
    }

    public ILMClient ilmClient() {
        return ilmClient;
    }

    public XPackClient withHeaders(Map<String, String> headers) {
        return new XPackClient(client.filterWithHeader(headers));
    }

    public XPackClient withAuth(String username, char[] passwd) {
        return withHeaders(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue(username, new SecureString(passwd))));
    }

    public XPackInfoRequestBuilder prepareInfo() {
        return new XPackInfoRequestBuilder(client);
    }

    public void info(XPackInfoRequest request, ActionListener<XPackInfoResponse> listener) {
        client.execute(XPackInfoAction.INSTANCE, request, listener);
    }
}