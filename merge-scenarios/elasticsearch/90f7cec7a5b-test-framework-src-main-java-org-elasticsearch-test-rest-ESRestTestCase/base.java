package org.elasticsearch.test.rest;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableList;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public abstract class ESRestTestCase extends ESTestCase {

    public static final String TRUSTSTORE_PATH = "truststore.path";

    public static final String TRUSTSTORE_PASSWORD = "truststore.password";

    public static final String CLIENT_RETRY_TIMEOUT = "client.retry.timeout";

    public static final String CLIENT_SOCKET_TIMEOUT = "client.socket.timeout";

    public static final String CLIENT_PATH_PREFIX = "client.path.prefix";

    public static Map<String, Object> entityAsMap(Response response) throws IOException {
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        try (XContentParser parser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, response.getEntity().getContent())) {
            return parser.map();
        }
    }

    public static boolean hasXPack() throws IOException {
        RestClient client = adminClient();
        if (client == null) {
            throw new IllegalStateException("must be called inside of a rest test case test");
        }
        Map<?, ?> response = entityAsMap(client.performRequest(new Request("GET", "_nodes/plugins")));
        Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
        for (Map.Entry<?, ?> node : nodes.entrySet()) {
            Map<?, ?> nodeInfo = (Map<?, ?>) node.getValue();
            for (Object module : (List<?>) nodeInfo.get("modules")) {
                Map<?, ?> moduleInfo = (Map<?, ?>) module;
                if (moduleInfo.get("name").toString().startsWith("x-pack-")) {
                    return true;
                }
            }
        }
        return false;
    }

    private static List<HttpHost> clusterHosts;

    private static RestClient client;

    private static RestClient adminClient;

    @Before
    public void initClient() throws IOException {
        if (client == null) {
            assert adminClient == null;
            assert clusterHosts == null;
            String cluster = System.getProperty("tests.rest.cluster");
            if (cluster == null) {
                throw new RuntimeException("Must specify [tests.rest.cluster] system property with a comma delimited list of [host:port] " + "to which to send REST requests");
            }
            String[] stringUrls = cluster.split(",");
            List<HttpHost> hosts = new ArrayList<>(stringUrls.length);
            for (String stringUrl : stringUrls) {
                int portSeparator = stringUrl.lastIndexOf(':');
                if (portSeparator < 0) {
                    throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
                }
                String host = stringUrl.substring(0, portSeparator);
                int port = Integer.valueOf(stringUrl.substring(portSeparator + 1));
                hosts.add(buildHttpHost(host, port));
            }
            clusterHosts = unmodifiableList(hosts);
            logger.info("initializing REST clients against {}", clusterHosts);
            client = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[clusterHosts.size()]));
            adminClient = buildClient(restAdminSettings(), clusterHosts.toArray(new HttpHost[clusterHosts.size()]));
        }
        assert client != null;
        assert adminClient != null;
        assert clusterHosts != null;
    }

    protected HttpHost buildHttpHost(String host, int port) {
        return new HttpHost(host, port, getProtocol());
    }

    @After
    public final void cleanUpCluster() throws Exception {
        if (preserveClusterUponCompletion() == false) {
            wipeCluster();
            waitForClusterStateUpdatesToFinish();
            logIfThereAreRunningTasks();
        }
    }

    @AfterClass
    public static void closeClients() throws IOException {
        try {
            IOUtils.close(client, adminClient);
        } finally {
            clusterHosts = null;
            client = null;
            adminClient = null;
        }
    }

    protected static RestClient client() {
        return client;
    }

    protected static RestClient adminClient() {
        return adminClient;
    }

    protected boolean preserveClusterUponCompletion() {
        return false;
    }

    protected boolean preserveIndicesUponCompletion() {
        return false;
    }

    protected boolean preserveTemplatesUponCompletion() {
        return false;
    }

    protected boolean preserveClusterSettings() {
        return false;
    }

    protected boolean preserveReposUponCompletion() {
        return false;
    }

    protected boolean preserveSnapshotsUponCompletion() {
        return false;
    }

    private void wipeCluster() throws IOException {
        if (preserveIndicesUponCompletion() == false) {
            try {
                adminClient().performRequest(new Request("DELETE", "*"));
            } catch (ResponseException e) {
                if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                    throw e;
                }
            }
        }
        if (preserveTemplatesUponCompletion() == false) {
            if (hasXPack()) {
                Request request = new Request("GET", "_cat/templates");
                request.addParameter("h", "name");
                String templates = EntityUtils.toString(adminClient().performRequest(request).getEntity());
                if (false == "".equals(templates)) {
                    for (String template : templates.split("\n")) {
                        if (isXPackTemplate(template))
                            continue;
                        if ("".equals(template)) {
                            throw new IllegalStateException("empty template in templates list:\n" + templates);
                        }
                        logger.debug("Clearing template [{}]", template);
                        adminClient().performRequest(new Request("DELETE", "_template/" + template));
                    }
                }
            } else {
                logger.debug("Clearing all templates");
                adminClient().performRequest(new Request("DELETE", "_template/*"));
            }
        }
        wipeSnapshots();
        if (preserveClusterSettings() == false) {
            wipeClusterSettings();
        }
    }

    private void wipeSnapshots() throws IOException {
        for (Map.Entry<String, ?> repo : entityAsMap(adminClient.performRequest(new Request("GET", "/_snapshot/_all"))).entrySet()) {
            String repoName = repo.getKey();
            Map<?, ?> repoSpec = (Map<?, ?>) repo.getValue();
            String repoType = (String) repoSpec.get("type");
            if (false == preserveSnapshotsUponCompletion() && repoType.equals("fs")) {
                Request listRequest = new Request("GET", "/_snapshot/" + repoName + "/_all");
                listRequest.addParameter("ignore_unavailable", "true");
                List<?> snapshots = (List<?>) entityAsMap(adminClient.performRequest(listRequest)).get("snapshots");
                for (Object snapshot : snapshots) {
                    Map<?, ?> snapshotInfo = (Map<?, ?>) snapshot;
                    String name = (String) snapshotInfo.get("snapshot");
                    logger.debug("wiping snapshot [{}/{}]", repoName, name);
                    adminClient().performRequest(new Request("DELETE", "/_snapshot/" + repoName + "/" + name));
                }
            }
            if (preserveReposUponCompletion() == false) {
                logger.debug("wiping snapshot repository [{}]", repoName);
                adminClient().performRequest(new Request("DELETE", "_snapshot/" + repoName));
            }
        }
    }

    private void wipeClusterSettings() throws IOException {
        Map<?, ?> getResponse = entityAsMap(adminClient().performRequest(new Request("GET", "/_cluster/settings")));
        boolean mustClear = false;
        XContentBuilder clearCommand = JsonXContent.contentBuilder();
        clearCommand.startObject();
        for (Map.Entry<?, ?> entry : getResponse.entrySet()) {
            String type = entry.getKey().toString();
            Map<?, ?> settings = (Map<?, ?>) entry.getValue();
            if (settings.isEmpty()) {
                continue;
            }
            mustClear = true;
            clearCommand.startObject(type);
            for (Object key : settings.keySet()) {
                clearCommand.field(key + ".*").nullValue();
            }
            clearCommand.endObject();
        }
        clearCommand.endObject();
        if (mustClear) {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity(Strings.toString(clearCommand));
            adminClient().performRequest(request);
        }
    }

    private void logIfThereAreRunningTasks() throws InterruptedException, IOException {
        Set<String> runningTasks = runningTasks(adminClient().performRequest(new Request("GET", "/_tasks")));
        runningTasks.remove(ListTasksAction.NAME);
        runningTasks.remove(ListTasksAction.NAME + "[n]");
        if (runningTasks.isEmpty()) {
            return;
        }
        List<String> stillRunning = new ArrayList<>(runningTasks);
        sort(stillRunning);
        logger.info("There are still tasks running after this test that might break subsequent tests {}.", stillRunning);
    }

    private void waitForClusterStateUpdatesToFinish() throws Exception {
        assertBusy(() -> {
            try {
                Response response = adminClient().performRequest(new Request("GET", "/_cluster/pending_tasks"));
                List<?> tasks = (List<?>) entityAsMap(response).get("tasks");
                if (false == tasks.isEmpty()) {
                    StringBuilder message = new StringBuilder("there are still running tasks:");
                    for (Object task : tasks) {
                        message.append('\n').append(task.toString());
                    }
                    fail(message.toString());
                }
            } catch (IOException e) {
                fail("cannot get cluster's pending tasks: " + e.getMessage());
            }
        }, 30, TimeUnit.SECONDS);
    }

    protected Settings restClientSettings() {
        Settings.Builder builder = Settings.builder();
        if (System.getProperty("tests.rest.client_path_prefix") != null) {
            builder.put(CLIENT_PATH_PREFIX, System.getProperty("tests.rest.client_path_prefix"));
        }
        return builder.build();
    }

    protected Settings restAdminSettings() {
        return restClientSettings();
    }

    protected final List<HttpHost> getClusterHosts() {
        return clusterHosts;
    }

    protected String getProtocol() {
        return "http";
    }

    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        return builder.build();
    }

    protected static void configureClient(RestClientBuilder builder, Settings settings) throws IOException {
        String keystorePath = settings.get(TRUSTSTORE_PATH);
        if (keystorePath != null) {
            final String keystorePass = settings.get(TRUSTSTORE_PASSWORD);
            if (keystorePass == null) {
                throw new IllegalStateException(TRUSTSTORE_PATH + " is provided but not " + TRUSTSTORE_PASSWORD);
            }
            Path path = PathUtils.get(keystorePath);
            if (!Files.exists(path)) {
                throw new IllegalStateException(TRUSTSTORE_PATH + " is set but points to a non-existing file");
            }
            try {
                KeyStore keyStore = KeyStore.getInstance("jks");
                try (InputStream is = Files.newInputStream(path)) {
                    keyStore.load(is, keystorePass.toCharArray());
                }
                SSLContext sslcontext = SSLContexts.custom().loadTrustMaterial(keyStore, null).build();
                SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslcontext);
                builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLStrategy(sessionStrategy));
            } catch (KeyStoreException | NoSuchAlgorithmException | KeyManagementException | CertificateException e) {
                throw new RuntimeException("Error setting up ssl", e);
            }
        }
        try (ThreadContext threadContext = new ThreadContext(settings)) {
            Header[] defaultHeaders = new Header[threadContext.getHeaders().size()];
            int i = 0;
            for (Map.Entry<String, String> entry : threadContext.getHeaders().entrySet()) {
                defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
            }
            builder.setDefaultHeaders(defaultHeaders);
        }
        final String requestTimeoutString = settings.get(CLIENT_RETRY_TIMEOUT);
        if (requestTimeoutString != null) {
            final TimeValue maxRetryTimeout = TimeValue.parseTimeValue(requestTimeoutString, CLIENT_RETRY_TIMEOUT);
            builder.setMaxRetryTimeoutMillis(Math.toIntExact(maxRetryTimeout.getMillis()));
        }
        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        if (socketTimeoutString != null) {
            final TimeValue socketTimeout = TimeValue.parseTimeValue(socketTimeoutString, CLIENT_SOCKET_TIMEOUT);
            builder.setRequestConfigCallback(conf -> conf.setSocketTimeout(Math.toIntExact(socketTimeout.getMillis())));
        }
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> runningTasks(Response response) throws IOException {
        Set<String> runningTasks = new HashSet<>();
        Map<String, Object> nodes = (Map<String, Object>) entityAsMap(response).get("nodes");
        for (Map.Entry<String, Object> node : nodes.entrySet()) {
            Map<String, Object> nodeInfo = (Map<String, Object>) node.getValue();
            Map<String, Object> nodeTasks = (Map<String, Object>) nodeInfo.get("tasks");
            for (Map.Entry<String, Object> taskAndName : nodeTasks.entrySet()) {
                Map<String, Object> task = (Map<String, Object>) taskAndName.getValue();
                runningTasks.add(task.get("action").toString());
            }
        }
        return runningTasks;
    }

    protected static void assertOK(Response response) {
        assertThat(response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
    }

    protected static void ensureGreen(String index) throws IOException {
        Request request = new Request("GET", "/_cluster/health/" + index);
        request.addParameter("wait_for_status", "green");
        request.addParameter("wait_for_no_relocating_shards", "true");
        request.addParameter("timeout", "70s");
        request.addParameter("level", "shards");
        client().performRequest(request);
    }

    protected static void ensureNoInitializingShards() throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        request.addParameter("wait_for_no_initializing_shards", "true");
        request.addParameter("timeout", "70s");
        request.addParameter("level", "shards");
        client().performRequest(request);
    }

    protected static void createIndex(String name, Settings settings) throws IOException {
        createIndex(name, settings, "");
    }

    protected static void createIndex(String name, Settings settings, String mapping) throws IOException {
        Request request = new Request("PUT", "/" + name);
        request.setJsonEntity("{\n \"settings\": " + Strings.toString(settings) + ", \"mappings\" : {" + mapping + "} }");
        client().performRequest(request);
    }

    protected static void deleteIndex(String name) throws IOException {
        Request request = new Request("DELETE", "/" + name);
        client().performRequest(request);
    }

    protected static void updateIndexSettings(String index, Settings.Builder settings) throws IOException {
        updateIndexSettings(index, settings.build());
    }

    private static void updateIndexSettings(String index, Settings settings) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_settings");
        request.setJsonEntity(Strings.toString(settings));
        client().performRequest(request);
    }

    protected static Map<String, Object> getIndexSettings(String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("flat_settings", "true");
        Response response = client().performRequest(request);
        try (InputStream is = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }
    }

    protected static boolean indexExists(String index) throws IOException {
        Response response = client().performRequest(new Request("HEAD", "/" + index));
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    protected static void closeIndex(String index) throws IOException {
        Response response = client().performRequest(new Request("POST", "/" + index + "/_close"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    protected static void openIndex(String index) throws IOException {
        Response response = client().performRequest(new Request("POST", "/" + index + "/_open"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    protected static boolean aliasExists(String alias) throws IOException {
        Response response = client().performRequest(new Request("HEAD", "/_alias/" + alias));
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    protected static boolean aliasExists(String index, String alias) throws IOException {
        Response response = client().performRequest(new Request("HEAD", "/" + index + "/_alias/" + alias));
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> getAlias(final String index, final String alias) throws IOException {
        String endpoint = "/_alias";
        if (false == Strings.isEmpty(index)) {
            endpoint = index + endpoint;
        }
        if (false == Strings.isEmpty(alias)) {
            endpoint = endpoint + "/" + alias;
        }
        Map<String, Object> getAliasResponse = getAsMap(endpoint);
        return (Map<String, Object>) XContentMapValues.extractValue(index + ".aliases." + alias, getAliasResponse);
    }

    protected static Map<String, Object> getAsMap(final String endpoint) throws IOException {
        Response response = client().performRequest(new Request("GET", endpoint));
        XContentType entityContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        Map<String, Object> responseEntity = XContentHelper.convertToMap(entityContentType.xContent(), response.getEntity().getContent(), false);
        assertNotNull(responseEntity);
        return responseEntity;
    }

    private static boolean isXPackTemplate(String name) {
        if (name.startsWith(".monitoring-")) {
            return true;
        }
        if (name.startsWith(".watch") || name.startsWith(".triggered_watches")) {
            return true;
        }
        if (name.startsWith(".ml-")) {
            return true;
        }
        switch(name) {
            case ".triggered_watches":
            case ".watches":
            case "logstash-index-template":
            case "security_audit_log":
                return true;
            default:
                return false;
        }
    }
}