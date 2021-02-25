package org.elasticsearch.test.rest;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContexts;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
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
import javax.net.ssl.SSLContext;
import static java.util.Collections.sort;
import static java.util.Collections.unmodifiableList;

public class ESRestTestCase extends ESTestCase {

    public static final String TRUSTSTORE_PATH = "truststore.path";

    public static final String TRUSTSTORE_PASSWORD = "truststore.password";

    public static Map<String, Object> entityAsMap(Response response) throws IOException {
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        try (XContentParser parser = xContentType.xContent().createParser(response.getEntity().getContent())) {
            return parser.map();
        }
    }

    private final List<HttpHost> clusterHosts;

    private final RestClient client;

    private final RestClient adminClient;

    public ESRestTestCase() {
        String cluster = System.getProperty("tests.rest.cluster");
        if (cluster == null) {
            throw new RuntimeException("Must specify [tests.rest.cluster] system property with a comma delimited list of [host:port] " + "to which to send REST requests");
        }
        String[] stringUrls = cluster.split(",");
        List<HttpHost> clusterHosts = new ArrayList<>(stringUrls.length);
        for (String stringUrl : stringUrls) {
            int portSeparator = stringUrl.lastIndexOf(':');
            if (portSeparator < 0) {
                throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
            }
            String host = stringUrl.substring(0, portSeparator);
            int port = Integer.valueOf(stringUrl.substring(portSeparator + 1));
            clusterHosts.add(new HttpHost(host, port, getProtocol()));
        }
        this.clusterHosts = unmodifiableList(clusterHosts);
        try {
            client = buildClient(restClientSettings());
            adminClient = buildClient(restAdminSettings());
        } catch (IOException e) {
            throw new RuntimeException("Error building clients", e);
        }
    }

    @After
    public final void after() throws Exception {
        wipeCluster();
        logIfThereAreRunningTasks();
        closeClients();
    }

    protected final RestClient client() {
        return client;
    }

    protected final RestClient adminClient() {
        return adminClient;
    }

    private void wipeCluster() throws IOException {
        final boolean preserveIndices = Boolean.parseBoolean(System.getProperty("tests.rest.preserve_indices"));
        if (preserveIndices == false) {
            try {
                adminClient().performRequest("DELETE", "*");
            } catch (ResponseException e) {
                if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                    throw e;
                }
            }
        }
        adminClient().performRequest("DELETE", "_template/*");
        adminClient().performRequest("DELETE", "_snapshot/*");
    }

    private void logIfThereAreRunningTasks() throws InterruptedException, IOException {
        Set<String> runningTasks = runningTasks(adminClient().performRequest("GET", "_tasks"));
        runningTasks.remove(ListTasksAction.NAME);
        runningTasks.remove(ListTasksAction.NAME + "[n]");
        if (runningTasks.isEmpty()) {
            return;
        }
        List<String> stillRunning = new ArrayList<>(runningTasks);
        sort(stillRunning);
        logger.info("There are still tasks running after this test that might break subsequent tests {}.", stillRunning);
    }

    private void closeClients() throws IOException {
        IOUtils.close(client, adminClient);
    }

    protected Settings restClientSettings() {
        return Settings.EMPTY;
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

    private RestClient buildClient(Settings settings) throws IOException {
        RestClientBuilder builder = RestClient.builder(clusterHosts.toArray(new HttpHost[0])).setMaxRetryTimeoutMillis(30000).setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setSocketTimeout(30000));
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
        return builder.build();
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
}