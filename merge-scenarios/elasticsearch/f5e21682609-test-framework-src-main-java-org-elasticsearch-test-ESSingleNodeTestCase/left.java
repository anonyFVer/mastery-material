package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public abstract class ESSingleNodeTestCase extends ESTestCase {

    private static Node NODE = null;

    protected void startNode(long seed) throws Exception {
        assert NODE == null;
        NODE = RandomizedContext.current().runWithPrivateRandomness(seed, this::newNode);
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertFalse(clusterHealthResponse.isTimedOut());
        client().admin().indices().preparePutTemplate("one_shard_index_template").setPatterns(Collections.singletonList("*")).setOrder(0).setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)).get();
        client().admin().indices().preparePutTemplate("random-soft-deletes-template").setPatterns(Collections.singletonList("*")).setOrder(0).setSettings(Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean()).put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), randomBoolean() ? IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.get(Settings.EMPTY) : between(0, 1000))).get();
    }

    private static void stopNode() throws IOException {
        Node node = NODE;
        NODE = null;
        IOUtils.close(node);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        long seed = random().nextLong();
        if (NODE == null) {
            startNode(seed);
        }
    }

    @Override
    public void tearDown() throws Exception {
        logger.info("[{}#{}]: cleaning up after test", getTestClass().getSimpleName(), getTestName());
        super.tearDown();
        assertAcked(client().admin().indices().prepareDelete("*").get());
        MetaData metaData = client().admin().cluster().prepareState().get().getState().getMetaData();
        assertThat("test leaves persistent cluster metadata behind: " + metaData.persistentSettings().keySet(), metaData.persistentSettings().size(), equalTo(0));
        assertThat("test leaves transient cluster metadata behind: " + metaData.transientSettings().keySet(), metaData.transientSettings().size(), equalTo(0));
        if (resetNodeAfterTest()) {
            assert NODE != null;
            stopNode();
            startNode(random().nextLong());
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        stopNode();
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        stopNode();
    }

    protected boolean resetNodeAfterTest() {
        return false;
    }

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.emptyList();
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    protected final Collection<Class<? extends Plugin>> pluginList(Class<? extends Plugin>... plugins) {
        return Arrays.asList(plugins);
    }

    protected Settings nodeSettings() {
        return Settings.EMPTY;
    }

    protected boolean addMockHttpTransport() {
        return true;
    }

    private Node newNode() {
        final Path tempDir = createTempDir();
        Settings settings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), InternalTestCluster.clusterName("single-node-cluster", random().nextLong())).put(Environment.PATH_HOME_SETTING.getKey(), tempDir).put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo")).put(Environment.PATH_SHARED_DATA_SETTING.getKey(), createTempDir().getParent()).put("node.name", "node_s_0").put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE.getKey(), "1000/1m").put(EsExecutors.PROCESSORS_SETTING.getKey(), 1).put("transport.type", getTestTransportType()).put(Node.NODE_DATA_SETTING.getKey(), true).put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), random().nextLong()).put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b").put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b").put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b").put(nodeSettings()).build();
        Collection<Class<? extends Plugin>> plugins = getPlugins();
        if (plugins.contains(getTestTransportPlugin()) == false) {
            plugins = new ArrayList<>(plugins);
            plugins.add(getTestTransportPlugin());
        }
        if (plugins.contains(TestZenDiscovery.TestPlugin.class) == false) {
            plugins = new ArrayList<>(plugins);
            plugins.add(TestZenDiscovery.TestPlugin.class);
        }
        if (addMockHttpTransport()) {
            plugins.add(MockHttpTransport.TestPlugin.class);
        }
        Node build = new MockNode(settings, plugins);
        try {
            build.start();
        } catch (NodeValidationException e) {
            throw new RuntimeException(e);
        }
        return build;
    }

    public Client client() {
        return NODE.client();
    }

    protected Node node() {
        return NODE;
    }

    protected <T> T getInstanceFromNode(Class<T> clazz) {
        return NODE.injector().getInstance(clazz);
    }

    protected IndexService createIndex(String index) {
        return createIndex(index, Settings.EMPTY);
    }

    protected IndexService createIndex(String index, Settings settings) {
        return createIndex(index, settings, null, (XContentBuilder) null);
    }

    protected IndexService createIndex(String index, Settings settings, String type, XContentBuilder mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (type != null && mappings != null) {
            createIndexRequestBuilder.addMapping(type, mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    protected IndexService createIndex(String index, Settings settings, String type, Object... mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (type != null) {
            createIndexRequestBuilder.addMapping(type, mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    protected IndexService createIndex(String index, CreateIndexRequestBuilder createIndexRequestBuilder) {
        assertAcked(createIndexRequestBuilder.get());
        ClusterHealthResponse health = client().admin().cluster().health(Requests.clusterHealthRequest(index).waitForYellowStatus().waitForEvents(Priority.LANGUID).waitForNoRelocatingShards(true)).actionGet();
        assertThat(health.getStatus(), lessThanOrEqualTo(ClusterHealthStatus.YELLOW));
        assertThat("Cluster must be a single node cluster", health.getNumberOfDataNodes(), equalTo(1));
        IndicesService instanceFromNode = getInstanceFromNode(IndicesService.class);
        return instanceFromNode.indexServiceSafe(resolveIndex(index));
    }

    public Index resolveIndex(String index) {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetaData.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    protected SearchContext createSearchContext(IndexService indexService) {
        BigArrays bigArrays = indexService.getBigArrays();
        ThreadPool threadPool = indexService.getThreadPool();
        return new TestSearchContext(threadPool, bigArrays, indexService);
    }

    public ClusterHealthStatus ensureGreen(String... indices) {
        return ensureGreen(TimeValue.timeValueSeconds(30), indices);
    }

    public ClusterHealthStatus ensureGreen(TimeValue timeout, String... indices) {
        ClusterHealthResponse actionGet = client().admin().cluster().health(Requests.clusterHealthRequest(indices).timeout(timeout).waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForNoRelocatingShards(true)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState(), client().admin().cluster().preparePendingClusterTasks().get());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        logger.debug("indices {} are green", indices.length == 0 ? "[_all]" : indices);
        return actionGet.getStatus();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return getInstanceFromNode(NamedXContentRegistry.class);
    }
}