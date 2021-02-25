package org.elasticsearch.smoketest;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.MockTcpTransportPlugin;
import org.elasticsearch.transport.Netty3Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiOfLength;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.hamcrest.Matchers.notNullValue;

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public abstract class ESSmokeClientTestCase extends LuceneTestCase {

    public static final String TESTS_CLUSTER = "tests.cluster";

    protected static final ESLogger logger = ESLoggerFactory.getLogger(ESSmokeClientTestCase.class.getName());

    private static final AtomicInteger counter = new AtomicInteger();

    private static Client client;

    private static String clusterAddresses;

    protected String index;

    private static Client startClient(Path tempDir, TransportAddress... transportAddresses) {
        Settings.Builder builder = Settings.builder().put("node.name", "qa_smoke_client_" + counter.getAndIncrement()).put("client.transport.ignore_cluster_name", true).put(Environment.PATH_HOME_SETTING.getKey(), tempDir);
        final Collection<Class<? extends Plugin>> plugins;
        switch(randomIntBetween(0, 2)) {
            case 0:
                builder.put(NetworkModule.TRANSPORT_TYPE_KEY, MockTcpTransportPlugin.MOCK_TCP_TRANSPORT_NAME);
                plugins = Collections.singleton(MockTcpTransportPlugin.class);
                break;
            case 1:
                plugins = Collections.emptyList();
                builder.put(NetworkModule.TRANSPORT_TYPE_KEY, Netty3Plugin.NETTY_TRANSPORT_NAME);
                break;
            case 2:
                plugins = Collections.emptyList();
                break;
            default:
                throw new AssertionError();
        }
        TransportClient client = new PreBuiltTransportClient(builder.build(), plugins).addTransportAddresses(transportAddresses);
        logger.info("--> Elasticsearch Java TransportClient started");
        Exception clientException = null;
        try {
            ClusterHealthResponse health = client.admin().cluster().prepareHealth().get();
            logger.info("--> connected to [{}] cluster which is running [{}] node(s).", health.getClusterName(), health.getNumberOfNodes());
        } catch (Exception e) {
            clientException = e;
        }
        assumeNoException("Sounds like your cluster is not running at " + clusterAddresses, clientException);
        return client;
    }

    private static Client startClient() throws IOException {
        String[] stringAddresses = clusterAddresses.split(",");
        TransportAddress[] transportAddresses = new TransportAddress[stringAddresses.length];
        int i = 0;
        for (String stringAddress : stringAddresses) {
            URL url = new URL("http://" + stringAddress);
            InetAddress inetAddress = InetAddress.getByName(url.getHost());
            transportAddresses[i++] = new InetSocketTransportAddress(new InetSocketAddress(inetAddress, url.getPort()));
        }
        return startClient(createTempDir(), transportAddresses);
    }

    public static Client getClient() {
        if (client == null) {
            try {
                client = startClient();
            } catch (IOException e) {
                logger.error("can not start the client", e);
            }
            assertThat(client, notNullValue());
        }
        return client;
    }

    @BeforeClass
    public static void initializeSettings() {
        clusterAddresses = System.getProperty(TESTS_CLUSTER);
        if (clusterAddresses == null || clusterAddresses.isEmpty()) {
            fail("Must specify " + TESTS_CLUSTER + " for smoke client test");
        }
    }

    @AfterClass
    public static void stopTransportClient() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Before
    public void defineIndexName() {
        doClean();
        index = "qa-smoke-test-client-" + randomAsciiOfLength(10).toLowerCase(Locale.getDefault());
    }

    @After
    public void cleanIndex() {
        doClean();
    }

    private void doClean() {
        if (client != null) {
            try {
                client.admin().indices().prepareDelete(index).get();
            } catch (Exception e) {
            }
        }
    }
}