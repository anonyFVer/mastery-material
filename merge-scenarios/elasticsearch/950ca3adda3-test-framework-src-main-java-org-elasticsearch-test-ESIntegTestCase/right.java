package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.http.HttpHost;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MockFieldFilterPlugin;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.node.NodeMocksPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptMetaData;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.client.RandomizingClient;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.store.MockFSIndexStore;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.elasticsearch.client.Requests.syncedFlushRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.util.CollectionUtils.eagerPartition;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING;
import static org.elasticsearch.discovery.zen.SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.elasticsearch.test.XContentTestUtils.differenceBetweenMapsIgnoringArrayOrder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public abstract class ESIntegTestCase extends ESTestCase {

    public static final String SYSPROP_THIRDPARTY = "tests.thirdparty";

    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @TestGroup(enabled = false, sysProperty = ESIntegTestCase.SYSPROP_THIRDPARTY)
    public @interface ThirdParty {
    }

    public static final String SUITE_CLUSTER_NODE_PREFIX = "node_s";

    public static final String TEST_CLUSTER_NODE_PREFIX = "node_t";

    public static final String TESTS_CLIENT_RATIO = "tests.client.ratio";

    public static final String TESTS_CLUSTER = "tests.cluster";

    public static final Setting<Long> INDEX_TEST_SEED_SETTING = Setting.longSetting("index.tests.seed", 0, Long.MIN_VALUE, Property.IndexScope);

    public static final String TESTS_ENABLE_MOCK_MODULES = "tests.enable_mock_modules";

    private static final boolean MOCK_MODULES_ENABLED = "true".equals(System.getProperty(TESTS_ENABLE_MOCK_MODULES, "true"));

    private static final int FREQUENT_BULK_THRESHOLD = 300;

    private static final int ALWAYS_BULK_THRESHOLD = 3000;

    private static final int MAX_IN_FLIGHT_ASYNC_INDEXES = 150;

    private static final int MAX_BULK_INDEX_REQUEST_SIZE = 1000;

    protected static final int DEFAULT_MIN_NUM_SHARDS = 1;

    protected static final int DEFAULT_MAX_NUM_SHARDS = 10;

    private static TestCluster currentCluster;

    private static RestClient restClient = null;

    private static final double TRANSPORT_CLIENT_RATIO = transportClientRatio();

    private static final Map<Class<?>, TestCluster> clusters = new IdentityHashMap<>();

    private static ESIntegTestCase INSTANCE = null;

    private static Long SUITE_SEED = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        SUITE_SEED = randomLong();
        initializeSuiteScope();
    }

    @Override
    protected final boolean enableWarningsCheck() {
        return false;
    }

    protected final void beforeInternal() throws Exception {
        final Scope currentClusterScope = getCurrentClusterScope();
        switch(currentClusterScope) {
            case SUITE:
                assert SUITE_SEED != null : "Suite seed was not initialized";
                currentCluster = buildAndPutCluster(currentClusterScope, SUITE_SEED);
                break;
            case TEST:
                currentCluster = buildAndPutCluster(currentClusterScope, randomLong());
                break;
            default:
                fail("Unknown Scope: [" + currentClusterScope + "]");
        }
        cluster().beforeTest(random(), getPerTestTransportClientRatio());
        cluster().wipe(excludeTemplates());
        randomIndexTemplate();
    }

    private void printTestMessage(String message) {
        if (isSuiteScopedTest(getClass()) && (getTestName().equals("<unknown>"))) {
            logger.info("[{}]: {} suite", getTestClass().getSimpleName(), message);
        } else {
            logger.info("[{}#{}]: {} test", getTestClass().getSimpleName(), getTestName(), message);
        }
    }

    public void randomIndexTemplate() throws IOException {
        if (cluster().size() > 0) {
            Settings.Builder randomSettingsBuilder = setRandomIndexSettings(random(), Settings.builder());
            if (isInternalCluster()) {
                randomSettingsBuilder.put(INDEX_TEST_SEED_SETTING.getKey(), random().nextLong());
            }
            randomSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards()).put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas());
            SuppressCodecs annotation = getClass().getAnnotation(SuppressCodecs.class);
            if (annotation != null && annotation.value().length == 1 && "*".equals(annotation.value()[0])) {
                randomSettingsBuilder.put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC));
            } else {
                randomSettingsBuilder.put("index.codec", CodecService.LUCENE_DEFAULT_CODEC);
            }
            for (String setting : randomSettingsBuilder.keys()) {
                assertThat("non index. prefix setting set on index template, its a node setting...", setting, startsWith("index."));
            }
            randomSettingsBuilder.put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0);
            if (randomBoolean()) {
                randomSettingsBuilder.put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), randomBoolean());
            }
            PutIndexTemplateRequestBuilder putTemplate = client().admin().indices().preparePutTemplate("random_index_template").setPatterns(Collections.singletonList("*")).setOrder(0).setSettings(randomSettingsBuilder);
            assertAcked(putTemplate.execute().actionGet());
        }
    }

    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        setRandomIndexMergeSettings(random, builder);
        setRandomIndexTranslogSettings(random, builder);
        if (random.nextBoolean()) {
            builder.put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), false);
        }
        if (random.nextBoolean()) {
            builder.put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), random.nextBoolean());
        }
        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), randomFrom("false", "checksum", "true"));
        }
        if (randomBoolean()) {
            builder.put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), RandomNumbers.randomIntBetween(random, 1, 15) + "ms");
        }
        return builder;
    }

    private static Settings.Builder setRandomIndexMergeSettings(Random random, Settings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING.getKey(), (random.nextBoolean() ? random.nextDouble() : random.nextBoolean()).toString());
        }
        switch(random.nextInt(4)) {
            case 3:
                final int maxThreadCount = RandomNumbers.randomIntBetween(random, 1, 4);
                final int maxMergeCount = RandomNumbers.randomIntBetween(random, maxThreadCount, maxThreadCount + 4);
                builder.put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), maxMergeCount);
                builder.put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), maxThreadCount);
                break;
        }
        return builder;
    }

    private static Settings.Builder setRandomIndexTranslogSettings(Random random, Settings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(RandomNumbers.randomIntBetween(random, 1, 300), ByteSizeUnit.MB));
        }
        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.PB));
        }
        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), RandomPicks.randomFrom(random, Translog.Durability.values()));
        }
        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), RandomNumbers.randomIntBetween(random, 100, 5000), TimeUnit.MILLISECONDS);
        }
        return builder;
    }

    private TestCluster buildWithPrivateContext(final Scope scope, final long seed) throws Exception {
        return RandomizedContext.current().runWithPrivateRandomness(seed, new Callable<TestCluster>() {

            @Override
            public TestCluster call() throws Exception {
                return buildTestCluster(scope, seed);
            }
        });
    }

    private TestCluster buildAndPutCluster(Scope currentClusterScope, long seed) throws Exception {
        final Class<?> clazz = this.getClass();
        TestCluster testCluster = clusters.remove(clazz);
        clearClusters();
        switch(currentClusterScope) {
            case SUITE:
                if (testCluster == null) {
                    testCluster = buildWithPrivateContext(currentClusterScope, seed);
                }
                break;
            case TEST:
                IOUtils.closeWhileHandlingException(testCluster);
                testCluster = buildTestCluster(currentClusterScope, seed);
                break;
        }
        clusters.put(clazz, testCluster);
        return testCluster;
    }

    private static void clearClusters() throws IOException {
        if (!clusters.isEmpty()) {
            IOUtils.close(clusters.values());
            clusters.clear();
        }
        if (restClient != null) {
            restClient.close();
            restClient = null;
        }
    }

    protected final void afterInternal(boolean afterClass) throws Exception {
        boolean success = false;
        try {
            final Scope currentClusterScope = getCurrentClusterScope();
            clearDisruptionScheme();
            try {
                if (cluster() != null) {
                    if (currentClusterScope != Scope.TEST) {
                        MetaData metaData = client().admin().cluster().prepareState().execute().actionGet().getState().getMetaData();
                        final Set<String> persistent = metaData.persistentSettings().keySet();
                        assertThat("test leaves persistent cluster metadata behind: " + persistent, persistent.size(), equalTo(0));
                        final Set<String> transientSettings = new HashSet<>(metaData.transientSettings().keySet());
                        if (isInternalCluster() && internalCluster().getAutoManageMinMasterNode()) {
                            transientSettings.remove(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey());
                        }
                        assertThat("test leaves transient cluster metadata behind: " + transientSettings, transientSettings, empty());
                    }
                    ensureClusterSizeConsistency();
                    ensureClusterStateConsistency();
                    if (isInternalCluster()) {
                        for (Discovery discovery : internalCluster().getInstances(Discovery.class)) {
                            if (discovery instanceof ZenDiscovery) {
                                final ZenDiscovery zenDiscovery = (ZenDiscovery) discovery;
                                assertBusy(() -> {
                                    final ClusterState[] states = zenDiscovery.pendingClusterStates();
                                    assertThat(zenDiscovery.clusterState().nodes().getLocalNode().getName() + " still having pending states:\n" + Stream.of(states).map(ClusterState::toString).collect(Collectors.joining("\n")), states, emptyArray());
                                });
                            }
                        }
                    }
                    beforeIndexDeletion();
                    cluster().wipe(excludeTemplates());
                    if (afterClass || currentClusterScope == Scope.TEST) {
                        cluster().close();
                    }
                    cluster().assertAfterTest();
                }
            } finally {
                if (currentClusterScope == Scope.TEST) {
                    clearClusters();
                }
            }
            success = true;
        } finally {
            if (!success) {
            }
        }
    }

    protected Set<String> excludeTemplates() {
        return Collections.emptySet();
    }

    protected void beforeIndexDeletion() throws Exception {
        cluster().beforeIndexDeletion();
    }

    public static TestCluster cluster() {
        return currentCluster;
    }

    public static boolean isInternalCluster() {
        return (currentCluster instanceof InternalTestCluster);
    }

    public static InternalTestCluster internalCluster() {
        if (!isInternalCluster()) {
            throw new UnsupportedOperationException("current test cluster is immutable");
        }
        return (InternalTestCluster) currentCluster;
    }

    public ClusterService clusterService() {
        return internalCluster().clusterService();
    }

    public static Client client() {
        return client(null);
    }

    public static Client client(@Nullable String node) {
        if (node != null) {
            return internalCluster().client(node);
        }
        Client client = cluster().client();
        if (frequently()) {
            client = new RandomizingClient(client, random());
        }
        return client;
    }

    public static Client dataNodeClient() {
        Client client = internalCluster().dataNodeClient();
        if (frequently()) {
            client = new RandomizingClient(client, random());
        }
        return client;
    }

    public static Iterable<Client> clients() {
        return cluster().getClients();
    }

    protected int minimumNumberOfShards() {
        return DEFAULT_MIN_NUM_SHARDS;
    }

    protected int maximumNumberOfShards() {
        return DEFAULT_MAX_NUM_SHARDS;
    }

    protected int numberOfShards() {
        return between(minimumNumberOfShards(), maximumNumberOfShards());
    }

    protected int minimumNumberOfReplicas() {
        return 0;
    }

    protected int maximumNumberOfReplicas() {
        int maxNumReplicas = Math.max(0, cluster().numDataNodes() - 1);
        return frequently() ? Math.min(1, maxNumReplicas) : maxNumReplicas;
    }

    protected int numberOfReplicas() {
        return between(minimumNumberOfReplicas(), maximumNumberOfReplicas());
    }

    public void setDisruptionScheme(ServiceDisruptionScheme scheme) {
        internalCluster().setDisruptionScheme(scheme);
    }

    public void clearDisruptionScheme() {
        if (isInternalCluster()) {
            internalCluster().clearDisruptionScheme();
        }
    }

    public Settings indexSettings() {
        Settings.Builder builder = Settings.builder();
        int numberOfShards = numberOfShards();
        if (numberOfShards > 0) {
            builder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
        }
        int numberOfReplicas = numberOfReplicas();
        if (numberOfReplicas >= 0) {
            builder.put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
        }
        if (randomInt(9) < 3) {
            final String dataPath = randomAlphaOfLength(10);
            logger.info("using custom data_path for index: [{}]", dataPath);
            builder.put(IndexMetaData.SETTING_DATA_PATH, dataPath);
        }
        builder.put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0);
        builder.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean());
        if (randomBoolean()) {
            builder.put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), between(0, 1000));
        }
        return builder.build();
    }

    public final void createIndex(String... names) {
        List<String> created = new ArrayList<>();
        for (String name : names) {
            boolean success = false;
            try {
                assertAcked(prepareCreate(name));
                created.add(name);
                success = true;
            } finally {
                if (!success && !created.isEmpty()) {
                    cluster().wipeIndices(created.toArray(new String[created.size()]));
                }
            }
        }
    }

    public final void createIndex(String name, Settings indexSettings) {
        assertAcked(prepareCreate(name).setSettings(indexSettings));
    }

    public final CreateIndexRequestBuilder prepareCreate(String index) {
        return prepareCreate(index, -1);
    }

    public final CreateIndexRequestBuilder prepareCreate(String index, int numNodes) {
        return prepareCreate(index, numNodes, Settings.builder());
    }

    public CreateIndexRequestBuilder prepareCreate(String index, Settings.Builder settingsBuilder) {
        return prepareCreate(index, -1, settingsBuilder);
    }

    public CreateIndexRequestBuilder prepareCreate(String index, int numNodes, Settings.Builder settingsBuilder) {
        Settings.Builder builder = Settings.builder().put(indexSettings()).put(settingsBuilder.build());
        if (numNodes > 0) {
            internalCluster().ensureAtLeastNumDataNodes(numNodes);
            getExcludeSettings(index, numNodes, builder);
        }
        return client().admin().indices().prepareCreate(index).setSettings(builder.build());
    }

    private Settings.Builder getExcludeSettings(String index, int num, Settings.Builder builder) {
        String exclude = String.join(",", internalCluster().allDataNodesButN(num));
        builder.put("index.routing.allocation.exclude._name", exclude);
        return builder;
    }

    public void waitNoPendingTasksOnAll() throws Exception {
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
        assertBusy(() -> {
            for (Client client : clients()) {
                ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth().setLocal(true).get();
                assertThat("client " + client + " still has in flight fetch", clusterHealth.getNumberOfInFlightFetch(), equalTo(0));
                PendingClusterTasksResponse pendingTasks = client.admin().cluster().preparePendingClusterTasks().setLocal(true).get();
                assertThat("client " + client + " still has pending tasks " + pendingTasks, pendingTasks, Matchers.emptyIterable());
                clusterHealth = client.admin().cluster().prepareHealth().setLocal(true).get();
                assertThat("client " + client + " still has in flight fetch", clusterHealth.getNumberOfInFlightFetch(), equalTo(0));
            }
        });
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
    }

    public void assertConcreteMappingsOnAll(final String index, final String type, final String... fieldNames) throws Exception {
        Set<String> nodes = internalCluster().nodesInclude(index);
        assertThat(nodes, Matchers.not(Matchers.emptyIterable()));
        for (String node : nodes) {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexService = indicesService.indexService(resolveIndex(index));
            assertThat("index service doesn't exists on " + node, indexService, notNullValue());
            MapperService mapperService = indexService.mapperService();
            for (String fieldName : fieldNames) {
                MappedFieldType fieldType = mapperService.fullName(fieldName);
                assertNotNull("field " + fieldName + " doesn't exists on " + node, fieldType);
            }
        }
        assertMappingOnMaster(index, type, fieldNames);
    }

    public void assertMappingOnMaster(final String index, final String type, final String... fieldNames) throws Exception {
        GetMappingsResponse response = client().admin().indices().prepareGetMappings(index).setTypes(type).get();
        ImmutableOpenMap<String, MappingMetaData> mappings = response.getMappings().get(index);
        assertThat(mappings, notNullValue());
        MappingMetaData mappingMetaData = mappings.get(type);
        assertThat(mappingMetaData, notNullValue());
        Map<String, Object> mappingSource = mappingMetaData.getSourceAsMap();
        assertFalse(mappingSource.isEmpty());
        assertTrue(mappingSource.containsKey("properties"));
        for (String fieldName : fieldNames) {
            Map<String, Object> mappingProperties = (Map<String, Object>) mappingSource.get("properties");
            if (fieldName.indexOf('.') != -1) {
                fieldName = fieldName.replace(".", ".properties.");
            }
            assertThat("field " + fieldName + " doesn't exists in mapping " + mappingMetaData.source().string(), XContentMapValues.extractValue(fieldName, mappingProperties), notNullValue());
        }
    }

    public void assertResultsAndLogOnFailure(long expectedResults, SearchResponse searchResponse) {
        if (searchResponse.getHits().getTotalHits() != expectedResults) {
            StringBuilder sb = new StringBuilder("search result contains [");
            sb.append(searchResponse.getHits().getTotalHits()).append("] results. expected [").append(expectedResults).append("]");
            String failMsg = sb.toString();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                sb.append("\n-> _index: [").append(hit.getIndex()).append("] type [").append(hit.getType()).append("] id [").append(hit.getId()).append("]");
            }
            logger.warn("{}", sb);
            fail(failMsg);
        }
    }

    public void allowNodes(String index, int n) {
        assert index != null;
        internalCluster().ensureAtLeastNumDataNodes(n);
        Settings.Builder builder = Settings.builder();
        if (n > 0) {
            getExcludeSettings(index, n, builder);
        }
        Settings build = builder.build();
        if (!build.isEmpty()) {
            logger.debug("allowNodes: updating [{}]'s setting to [{}]", index, build.toDelimitedString(';'));
            client().admin().indices().prepareUpdateSettings(index).setSettings(build).execute().actionGet();
        }
    }

    public ClusterHealthStatus ensureGreen(String... indices) {
        return ensureGreen(TimeValue.timeValueSeconds(30), indices);
    }

    public ClusterHealthStatus ensureGreen(TimeValue timeout, String... indices) {
        return ensureColor(ClusterHealthStatus.GREEN, timeout, false, indices);
    }

    public ClusterHealthStatus ensureYellow(String... indices) {
        return ensureColor(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30), false, indices);
    }

    public ClusterHealthStatus ensureYellowAndNoInitializingShards(String... indices) {
        return ensureColor(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30), true, indices);
    }

    private ClusterHealthStatus ensureColor(ClusterHealthStatus clusterHealthStatus, TimeValue timeout, boolean waitForNoInitializingShards, String... indices) {
        String color = clusterHealthStatus.name().toLowerCase(Locale.ROOT);
        String method = "ensure" + Strings.capitalize(color);
        ClusterHealthRequest healthRequest = Requests.clusterHealthRequest(indices).timeout(timeout).waitForStatus(clusterHealthStatus).waitForEvents(Priority.LANGUID).waitForNoRelocatingShards(true).waitForNoInitializingShards(waitForNoInitializingShards).waitForNodes(Integer.toString(cluster().size()));
        ClusterHealthResponse actionGet = client().admin().cluster().health(healthRequest).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("{} timed out, cluster state:\n{}\n{}", method, client().admin().cluster().prepareState().get().getState(), client().admin().cluster().preparePendingClusterTasks().get());
            fail("timed out waiting for " + color + " state");
        }
        assertThat("Expected at least " + clusterHealthStatus + " but got " + actionGet.getStatus(), actionGet.getStatus().value(), lessThanOrEqualTo(clusterHealthStatus.value()));
        logger.debug("indices {} are {}", indices.length == 0 ? "[_all]" : indices, color);
        return actionGet.getStatus();
    }

    public ClusterHealthStatus waitForRelocation() {
        return waitForRelocation(null);
    }

    public ClusterHealthStatus waitForRelocation(ClusterHealthStatus status) {
        ClusterHealthRequest request = Requests.clusterHealthRequest().waitForNoRelocatingShards(true);
        if (status != null) {
            request.waitForStatus(status);
        }
        ClusterHealthResponse actionGet = client().admin().cluster().health(request).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("waitForRelocation timed out (status={}), cluster state:\n{}\n{}", status, client().admin().cluster().prepareState().get().getState(), client().admin().cluster().preparePendingClusterTasks().get());
            assertThat("timed out waiting for relocation", actionGet.isTimedOut(), equalTo(false));
        }
        if (status != null) {
            assertThat(actionGet.getStatus(), equalTo(status));
        }
        return actionGet.getStatus();
    }

    public long waitForDocs(final long numDocs) throws InterruptedException {
        return waitForDocs(numDocs, null);
    }

    public long waitForDocs(final long numDocs, @Nullable final BackgroundIndexer indexer) throws InterruptedException {
        return waitForDocs(numDocs, 90, TimeUnit.SECONDS, indexer);
    }

    public long waitForDocs(final long numDocs, int maxWaitTime, TimeUnit maxWaitTimeUnit, @Nullable final BackgroundIndexer indexer) throws InterruptedException {
        final AtomicLong lastKnownCount = new AtomicLong(-1);
        long lastStartCount = -1;
        BooleanSupplier testDocs = () -> {
            if (indexer != null) {
                lastKnownCount.set(indexer.totalIndexedDocs());
            }
            if (lastKnownCount.get() >= numDocs) {
                try {
                    long count = client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get().getHits().getTotalHits();
                    if (count == lastKnownCount.get()) {
                        client().admin().indices().prepareRefresh().get();
                    }
                    lastKnownCount.set(count);
                } catch (Exception e) {
                    logger.debug("failed to executed count", e);
                    return false;
                }
                logger.debug("[{}] docs visible for search. waiting for [{}]", lastKnownCount.get(), numDocs);
            } else {
                logger.debug("[{}] docs indexed. waiting for [{}]", lastKnownCount.get(), numDocs);
            }
            return lastKnownCount.get() >= numDocs;
        };
        while (!awaitBusy(testDocs, maxWaitTime, maxWaitTimeUnit)) {
            if (lastStartCount == lastKnownCount.get()) {
                fail("failed to reach " + numDocs + "docs");
            }
            lastStartCount = lastKnownCount.get();
        }
        return lastKnownCount.get();
    }

    public void setMinimumMasterNodes(int n) {
        assertTrue(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), n)).get().isAcknowledged());
    }

    public void logClusterState() {
        logger.debug("cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState(), client().admin().cluster().preparePendingClusterTasks().get());
    }

    public void logSegmentsState(String... indices) throws Exception {
        IndicesSegmentResponse segsRsp = client().admin().indices().prepareSegments(indices).get();
        logger.debug("segments {} state: \n{}", indices.length == 0 ? "[_all]" : indices, Strings.toString(segsRsp.toXContent(JsonXContent.contentBuilder().prettyPrint(), ToXContent.EMPTY_PARAMS)));
    }

    public void logMemoryStats() {
        logger.info("memory: {}", Strings.toString(client().admin().cluster().prepareNodesStats().clear().setJvm(true).get(), true, true));
    }

    protected void ensureClusterSizeConsistency() {
        if (cluster() != null && cluster().size() > 0) {
            logger.trace("Check consistency for [{}] nodes", cluster().size());
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(cluster().size())).get());
        }
    }

    protected void ensureClusterStateConsistency() throws IOException {
        if (cluster() != null && cluster().size() > 0) {
            final NamedWriteableRegistry namedWriteableRegistry = cluster().getNamedWriteableRegistry();
            final Client masterClient = client();
            ClusterState masterClusterState = masterClient.admin().cluster().prepareState().all().get().getState();
            byte[] masterClusterStateBytes = ClusterState.Builder.toBytes(masterClusterState);
            masterClusterState = ClusterState.Builder.fromBytes(masterClusterStateBytes, null, namedWriteableRegistry);
            Map<String, Object> masterStateMap = convertToMap(masterClusterState);
            int masterClusterStateSize = ClusterState.Builder.toBytes(masterClusterState).length;
            String masterId = masterClusterState.nodes().getMasterNodeId();
            for (Client client : cluster().getClients()) {
                ClusterState localClusterState = client.admin().cluster().prepareState().all().setLocal(true).get().getState();
                byte[] localClusterStateBytes = ClusterState.Builder.toBytes(localClusterState);
                localClusterState = ClusterState.Builder.fromBytes(localClusterStateBytes, null, namedWriteableRegistry);
                final Map<String, Object> localStateMap = convertToMap(localClusterState);
                final int localClusterStateSize = ClusterState.Builder.toBytes(localClusterState).length;
                if (masterClusterState.version() == localClusterState.version() && masterId.equals(localClusterState.nodes().getMasterNodeId())) {
                    try {
                        assertEquals("cluster state UUID does not match", masterClusterState.stateUUID(), localClusterState.stateUUID());
                        if (isTransportClient(masterClient) == isTransportClient(client)) {
                            assertEquals("cluster state size does not match", masterClusterStateSize, localClusterStateSize);
                            assertNull("cluster state JSON serialization does not match", differenceBetweenMapsIgnoringArrayOrder(masterStateMap, localStateMap));
                        } else {
                            assertNull("cluster state JSON serialization does not match (after removing some customs)", differenceBetweenMapsIgnoringArrayOrder(convertToMap(removePluginCustoms(masterClusterState)), convertToMap(removePluginCustoms(localClusterState))));
                        }
                    } catch (final AssertionError error) {
                        logger.error("Cluster state from master:\n{}\nLocal cluster state:\n{}", masterClusterState.toString(), localClusterState.toString());
                        throw error;
                    }
                }
            }
        }
    }

    private boolean isTransportClient(final Client client) {
        if (TransportClient.class.isAssignableFrom(client.getClass())) {
            return true;
        } else if (client instanceof RandomizingClient) {
            return isTransportClient(((RandomizingClient) client).in());
        }
        return false;
    }

    private static final Set<String> SAFE_METADATA_CUSTOMS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(IndexGraveyard.TYPE, IngestMetadata.TYPE, RepositoriesMetaData.TYPE, ScriptMetaData.TYPE)));

    private static final Set<String> SAFE_CUSTOMS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(RestoreInProgress.TYPE, SnapshotDeletionsInProgress.TYPE, SnapshotsInProgress.TYPE)));

    private ClusterState removePluginCustoms(final ClusterState clusterState) {
        final ClusterState.Builder builder = ClusterState.builder(clusterState);
        clusterState.customs().keysIt().forEachRemaining(key -> {
            if (SAFE_CUSTOMS.contains(key) == false) {
                builder.removeCustom(key);
            }
        });
        final MetaData.Builder mdBuilder = MetaData.builder(clusterState.metaData());
        clusterState.metaData().customs().keysIt().forEachRemaining(key -> {
            if (SAFE_METADATA_CUSTOMS.contains(key) == false) {
                mdBuilder.removeCustom(key);
            }
        });
        builder.metaData(mdBuilder);
        return builder.build();
    }

    protected ClusterHealthStatus ensureSearchable(String... indices) {
        return ensureGreen(indices);
    }

    protected void ensureStableCluster(int nodeCount) {
        ensureStableCluster(nodeCount, TimeValue.timeValueSeconds(30));
    }

    protected void ensureStableCluster(int nodeCount, TimeValue timeValue) {
        ensureStableCluster(nodeCount, timeValue, false, null);
    }

    protected void ensureStableCluster(int nodeCount, @Nullable String viaNode) {
        ensureStableCluster(nodeCount, TimeValue.timeValueSeconds(30), false, viaNode);
    }

    protected void ensureStableCluster(int nodeCount, TimeValue timeValue, boolean local, @Nullable String viaNode) {
        if (viaNode == null) {
            viaNode = randomFrom(internalCluster().getNodeNames());
        }
        logger.debug("ensuring cluster is stable with [{}] nodes. access node: [{}]. timeout: [{}]", nodeCount, viaNode, timeValue);
        ClusterHealthResponse clusterHealthResponse = client(viaNode).admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes(Integer.toString(nodeCount)).setTimeout(timeValue).setLocal(local).setWaitForNoRelocatingShards(true).get();
        if (clusterHealthResponse.isTimedOut()) {
            ClusterStateResponse stateResponse = client(viaNode).admin().cluster().prepareState().get();
            fail("failed to reach a stable cluster of [" + nodeCount + "] nodes. Tried via [" + viaNode + "]. last cluster state:\n" + stateResponse.getState());
        }
        assertThat(clusterHealthResponse.isTimedOut(), is(false));
        ensureFullyConnectedCluster();
    }

    protected void ensureFullyConnectedCluster() {
        NetworkDisruption.ensureFullyConnectedCluster(internalCluster());
    }

    protected final IndexResponse index(String index, String type, XContentBuilder source) {
        return client().prepareIndex(index, type).setSource(source).execute().actionGet();
    }

    protected final IndexResponse index(String index, String type, String id, Map<String, Object> source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    protected final GetResponse get(String index, String type, String id) {
        return client().prepareGet(index, type, id).execute().actionGet();
    }

    protected final IndexResponse index(String index, String type, String id, XContentBuilder source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    protected final IndexResponse index(String index, String type, String id, Object... source) {
        return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
    }

    protected final IndexResponse index(String index, String type, String id, String source) {
        return client().prepareIndex(index, type, id).setSource(source, XContentType.JSON).execute().actionGet();
    }

    protected final RefreshResponse refresh(String... indices) {
        waitForRelocation();
        RefreshResponse actionGet = client().admin().indices().prepareRefresh(indices).execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected final void flushAndRefresh(String... indices) {
        flush(indices);
        refresh(indices);
    }

    protected final FlushResponse flush(String... indices) {
        waitForRelocation();
        FlushResponse actionGet = client().admin().indices().prepareFlush(indices).execute().actionGet();
        for (DefaultShardOperationFailedException failure : actionGet.getShardFailures()) {
            assertThat("unexpected flush failure " + failure.reason(), failure.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }
        return actionGet;
    }

    protected ForceMergeResponse forceMerge() {
        waitForRelocation();
        ForceMergeResponse actionGet = client().admin().indices().prepareForceMerge().setMaxNumSegments(1).execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected boolean indexExists(String index) {
        IndicesExistsResponse actionGet = client().admin().indices().prepareExists(index).execute().actionGet();
        return actionGet.isExists();
    }

    protected final void enableAllocation(String... indices) {
        client().admin().indices().prepareUpdateSettings(indices).setSettings(Settings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "all")).get();
    }

    protected final void disableAllocation(String... indices) {
        client().admin().indices().prepareUpdateSettings(indices).setSettings(Settings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")).get();
    }

    protected AdminClient admin() {
        return client().admin();
    }

    public void indexRandom(boolean forceRefresh, IndexRequestBuilder... builders) throws InterruptedException, ExecutionException {
        indexRandom(forceRefresh, Arrays.asList(builders));
    }

    public void indexRandom(boolean forceRefresh, boolean dummyDocuments, IndexRequestBuilder... builders) throws InterruptedException, ExecutionException {
        indexRandom(forceRefresh, dummyDocuments, Arrays.asList(builders));
    }

    public void indexRandom(boolean forceRefresh, List<IndexRequestBuilder> builders) throws InterruptedException, ExecutionException {
        indexRandom(forceRefresh, forceRefresh, builders);
    }

    public void indexRandom(boolean forceRefresh, boolean dummyDocuments, List<IndexRequestBuilder> builders) throws InterruptedException, ExecutionException {
        indexRandom(forceRefresh, dummyDocuments, true, builders);
    }

    public void indexRandom(boolean forceRefresh, boolean dummyDocuments, boolean maybeFlush, List<IndexRequestBuilder> builders) throws InterruptedException, ExecutionException {
        Random random = random();
        Map<String, Set<String>> indicesAndTypes = new HashMap<>();
        for (IndexRequestBuilder builder : builders) {
            final Set<String> types = indicesAndTypes.computeIfAbsent(builder.request().index(), index -> new HashSet<>());
            types.add(builder.request().type());
        }
        Set<List<String>> bogusIds = new HashSet<>();
        if (random.nextBoolean() && !builders.isEmpty() && dummyDocuments) {
            builders = new ArrayList<>(builders);
            final int numBogusDocs = scaledRandomIntBetween(1, builders.size() * 2);
            final int unicodeLen = between(1, 10);
            for (int i = 0; i < numBogusDocs; i++) {
                String id = "bogus_doc_" + randomRealisticUnicodeOfLength(unicodeLen) + Integer.toString(dummmyDocIdGenerator.incrementAndGet());
                Map.Entry<String, Set<String>> indexAndTypes = RandomPicks.randomFrom(random, indicesAndTypes.entrySet());
                String index = indexAndTypes.getKey();
                String type = RandomPicks.randomFrom(random, indexAndTypes.getValue());
                bogusIds.add(Arrays.asList(index, type, id));
                builders.add(client().prepareIndex(index, type, id).setSource("{}", XContentType.JSON).setRouting(id));
            }
        }
        Collections.shuffle(builders, random());
        final CopyOnWriteArrayList<Tuple<IndexRequestBuilder, Exception>> errors = new CopyOnWriteArrayList<>();
        List<CountDownLatch> inFlightAsyncOperations = new ArrayList<>();
        final String[] indices = indicesAndTypes.keySet().toArray(new String[0]);
        if (builders.size() < FREQUENT_BULK_THRESHOLD ? frequently() : builders.size() < ALWAYS_BULK_THRESHOLD ? rarely() : false) {
            if (frequently()) {
                logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), true, false);
                for (IndexRequestBuilder indexRequestBuilder : builders) {
                    indexRequestBuilder.execute(new PayloadLatchedActionListener<>(indexRequestBuilder, newLatch(inFlightAsyncOperations), errors));
                    postIndexAsyncActions(indices, inFlightAsyncOperations, maybeFlush);
                }
            } else {
                logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), false, false);
                for (IndexRequestBuilder indexRequestBuilder : builders) {
                    indexRequestBuilder.execute().actionGet();
                    postIndexAsyncActions(indices, inFlightAsyncOperations, maybeFlush);
                }
            }
        } else {
            List<List<IndexRequestBuilder>> partition = eagerPartition(builders, Math.min(MAX_BULK_INDEX_REQUEST_SIZE, Math.max(1, (int) (builders.size() * randomDouble()))));
            logger.info("Index [{}] docs async: [{}] bulk: [{}] partitions [{}]", builders.size(), false, true, partition.size());
            for (List<IndexRequestBuilder> segmented : partition) {
                BulkRequestBuilder bulkBuilder = client().prepareBulk();
                for (IndexRequestBuilder indexRequestBuilder : segmented) {
                    bulkBuilder.add(indexRequestBuilder);
                }
                BulkResponse actionGet = bulkBuilder.execute().actionGet();
                assertThat(actionGet.hasFailures() ? actionGet.buildFailureMessage() : "", actionGet.hasFailures(), equalTo(false));
            }
        }
        for (CountDownLatch operation : inFlightAsyncOperations) {
            operation.await();
        }
        final List<Exception> actualErrors = new ArrayList<>();
        for (Tuple<IndexRequestBuilder, Exception> tuple : errors) {
            if (ExceptionsHelper.unwrapCause(tuple.v2()) instanceof EsRejectedExecutionException) {
                tuple.v1().execute().actionGet();
            } else {
                actualErrors.add(tuple.v2());
            }
        }
        assertThat(actualErrors, emptyIterable());
        if (!bogusIds.isEmpty()) {
            for (List<String> doc : bogusIds) {
                assertEquals("failed to delete a dummy doc [" + doc.get(0) + "][" + doc.get(2) + "]", DocWriteResponse.Result.DELETED, client().prepareDelete(doc.get(0), doc.get(1), doc.get(2)).setRouting(doc.get(2)).get().getResult());
            }
        }
        if (forceRefresh) {
            assertNoFailures(client().admin().indices().prepareRefresh(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen()).get());
        }
    }

    private AtomicInteger dummmyDocIdGenerator = new AtomicInteger();

    public static void disableIndexBlock(String index, String block) {
        Settings settings = Settings.builder().put(block, false).build();
        client().admin().indices().prepareUpdateSettings(index).setSettings(settings).get();
    }

    public static void enableIndexBlock(String index, String block) {
        Settings settings = Settings.builder().put(block, true).build();
        client().admin().indices().prepareUpdateSettings(index).setSettings(settings).get();
    }

    public static void setClusterReadOnly(boolean value) {
        Settings settings = value ? Settings.builder().put(MetaData.SETTING_READ_ONLY_SETTING.getKey(), value).build() : Settings.builder().putNull(MetaData.SETTING_READ_ONLY_SETTING.getKey()).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());
    }

    private static CountDownLatch newLatch(List<CountDownLatch> latches) {
        CountDownLatch l = new CountDownLatch(1);
        latches.add(l);
        return l;
    }

    private void postIndexAsyncActions(String[] indices, List<CountDownLatch> inFlightAsyncOperations, boolean maybeFlush) throws InterruptedException {
        if (rarely()) {
            if (rarely()) {
                client().admin().indices().prepareRefresh(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen()).execute(new LatchedActionListener<>(newLatch(inFlightAsyncOperations)));
            } else if (maybeFlush && rarely()) {
                if (randomBoolean()) {
                    client().admin().indices().prepareFlush(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen()).execute(new LatchedActionListener<>(newLatch(inFlightAsyncOperations)));
                } else {
                    client().admin().indices().syncedFlush(syncedFlushRequest(indices).indicesOptions(IndicesOptions.lenientExpandOpen()), new LatchedActionListener<>(newLatch(inFlightAsyncOperations)));
                }
            } else if (rarely()) {
                client().admin().indices().prepareForceMerge(indices).setIndicesOptions(IndicesOptions.lenientExpandOpen()).setMaxNumSegments(between(1, 10)).setFlush(maybeFlush && randomBoolean()).execute(new LatchedActionListener<>(newLatch(inFlightAsyncOperations)));
            }
        }
        while (inFlightAsyncOperations.size() > MAX_IN_FLIGHT_ASYNC_INDEXES) {
            int waitFor = between(0, inFlightAsyncOperations.size() - 1);
            inFlightAsyncOperations.remove(waitFor).await();
        }
    }

    public enum Scope {

        SUITE, TEST
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.TYPE })
    public @interface ClusterScope {

        Scope scope() default Scope.SUITE;

        int numDataNodes() default -1;

        int minNumDataNodes() default -1;

        int maxNumDataNodes() default -1;

        boolean supportsDedicatedMasters() default true;

        boolean autoMinMasterNodes() default true;

        int numClientNodes() default InternalTestCluster.DEFAULT_NUM_CLIENT_NODES;

        double transportClientRatio() default -1;
    }

    private class LatchedActionListener<Response> implements ActionListener<Response> {

        private final CountDownLatch latch;

        LatchedActionListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public final void onResponse(Response response) {
            latch.countDown();
        }

        @Override
        public final void onFailure(Exception t) {
            try {
                logger.info("Action Failed", t);
                addError(t);
            } finally {
                latch.countDown();
            }
        }

        protected void addError(Exception e) {
        }
    }

    private class PayloadLatchedActionListener<Response, T> extends LatchedActionListener<Response> {

        private final CopyOnWriteArrayList<Tuple<T, Exception>> errors;

        private final T builder;

        PayloadLatchedActionListener(T builder, CountDownLatch latch, CopyOnWriteArrayList<Tuple<T, Exception>> errors) {
            super(latch);
            this.errors = errors;
            this.builder = builder;
        }

        @Override
        protected void addError(Exception e) {
            errors.add(new Tuple<>(builder, e));
        }
    }

    public void clearScroll(String... scrollIds) {
        ClearScrollResponse clearResponse = client().prepareClearScroll().setScrollIds(Arrays.asList(scrollIds)).get();
        assertThat(clearResponse.isSucceeded(), equalTo(true));
    }

    private static <A extends Annotation> A getAnnotation(Class<?> clazz, Class<A> annotationClass) {
        if (clazz == Object.class || clazz == ESIntegTestCase.class) {
            return null;
        }
        A annotation = clazz.getAnnotation(annotationClass);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass(), annotationClass);
    }

    private Scope getCurrentClusterScope() {
        return getCurrentClusterScope(this.getClass());
    }

    private static Scope getCurrentClusterScope(Class<?> clazz) {
        ClusterScope annotation = getAnnotation(clazz, ClusterScope.class);
        return annotation == null ? Scope.SUITE : annotation.scope();
    }

    private boolean getSupportsDedicatedMasters() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? true : annotation.supportsDedicatedMasters();
    }

    private boolean getAutoMinMasterNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? true : annotation.autoMinMasterNodes();
    }

    private int getNumDataNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? -1 : annotation.numDataNodes();
    }

    private int getMinNumDataNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null || annotation.minNumDataNodes() == -1 ? InternalTestCluster.DEFAULT_MIN_NUM_DATA_NODES : annotation.minNumDataNodes();
    }

    private int getMaxNumDataNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null || annotation.maxNumDataNodes() == -1 ? InternalTestCluster.DEFAULT_MAX_NUM_DATA_NODES : annotation.maxNumDataNodes();
    }

    private int getNumClientNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? InternalTestCluster.DEFAULT_NUM_CLIENT_NODES : annotation.numClientNodes();
    }

    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), Integer.MAX_VALUE).put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b").put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b").put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b").put(ScriptService.SCRIPT_MAX_COMPILATIONS_RATE.getKey(), "2048/1m").put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), nodeOrdinal % 2 == 0).put(IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT.getKey(), new TimeValue(1, TimeUnit.SECONDS)).put(SearchService.LOW_LEVEL_CANCELLATION_SETTING.getKey(), randomBoolean()).putList(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey()).putList(DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "file");
        if (rarely()) {
            builder.put("thread_pool.search.min_queue_size", 100);
        }
        return builder.build();
    }

    protected Path nodeConfigPath(int nodeOrdinal) {
        return null;
    }

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.emptyList();
    }

    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.emptyList();
    }

    protected Settings transportClientSettings() {
        return Settings.EMPTY;
    }

    private ExternalTestCluster buildExternalCluster(String clusterAddresses) throws IOException {
        String[] stringAddresses = clusterAddresses.split(",");
        TransportAddress[] transportAddresses = new TransportAddress[stringAddresses.length];
        int i = 0;
        for (String stringAddress : stringAddresses) {
            URL url = new URL("http://" + stringAddress);
            InetAddress inetAddress = InetAddress.getByName(url.getHost());
            transportAddresses[i++] = new TransportAddress(new InetSocketAddress(inetAddress, url.getPort()));
        }
        return new ExternalTestCluster(createTempDir(), externalClusterClientSettings(), transportClientPlugins(), transportAddresses);
    }

    protected Settings externalClusterClientSettings() {
        return Settings.EMPTY;
    }

    protected boolean ignoreExternalCluster() {
        return false;
    }

    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        String clusterAddresses = System.getProperty(TESTS_CLUSTER);
        if (Strings.hasLength(clusterAddresses) && ignoreExternalCluster() == false) {
            if (scope == Scope.TEST) {
                throw new IllegalArgumentException("Cannot run TEST scope test with " + TESTS_CLUSTER);
            }
            return buildExternalCluster(clusterAddresses);
        }
        final String nodePrefix;
        switch(scope) {
            case TEST:
                nodePrefix = TEST_CLUSTER_NODE_PREFIX;
                break;
            case SUITE:
                nodePrefix = SUITE_CLUSTER_NODE_PREFIX;
                break;
            default:
                throw new ElasticsearchException("Scope not supported: " + scope);
        }
        boolean supportsDedicatedMasters = getSupportsDedicatedMasters();
        int numDataNodes = getNumDataNodes();
        int minNumDataNodes;
        int maxNumDataNodes;
        if (numDataNodes >= 0) {
            minNumDataNodes = maxNumDataNodes = numDataNodes;
        } else {
            minNumDataNodes = getMinNumDataNodes();
            maxNumDataNodes = getMaxNumDataNodes();
        }
        Collection<Class<? extends Plugin>> mockPlugins = getMockPlugins();
        final NodeConfigurationSource nodeConfigurationSource = getNodeConfigSource();
        if (addMockTransportService()) {
            ArrayList<Class<? extends Plugin>> mocks = new ArrayList<>(mockPlugins);
            if (mockPlugins.contains(getTestTransportPlugin()) == false) {
                mocks.add(getTestTransportPlugin());
            }
            mockPlugins = mocks;
        }
        return new InternalTestCluster(seed, createTempDir(), supportsDedicatedMasters, getAutoMinMasterNodes(), minNumDataNodes, maxNumDataNodes, InternalTestCluster.clusterName(scope.name(), seed) + "-cluster", nodeConfigurationSource, getNumClientNodes(), nodePrefix, mockPlugins, getClientWrapper(), forbidPrivateIndexSettings());
    }

    protected NodeConfigurationSource getNodeConfigSource() {
        Settings.Builder networkSettings = Settings.builder();
        if (addMockTransportService()) {
            networkSettings.put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
        }
        return new NodeConfigurationSource() {

            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return Settings.builder().put(networkSettings.build()).put(ESIntegTestCase.this.nodeSettings(nodeOrdinal)).build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return ESIntegTestCase.this.nodeConfigPath(nodeOrdinal);
            }

            @Override
            public Collection<Class<? extends Plugin>> nodePlugins() {
                return ESIntegTestCase.this.nodePlugins();
            }

            @Override
            public Settings transportClientSettings() {
                return Settings.builder().put(networkSettings.build()).put(ESIntegTestCase.this.transportClientSettings()).build();
            }

            @Override
            public Collection<Class<? extends Plugin>> transportClientPlugins() {
                Collection<Class<? extends Plugin>> plugins = ESIntegTestCase.this.transportClientPlugins();
                if (plugins.contains(getTestTransportPlugin()) == false) {
                    plugins = new ArrayList<>(plugins);
                    plugins.add(getTestTransportPlugin());
                }
                return Collections.unmodifiableCollection(plugins);
            }
        };
    }

    protected boolean addMockTransportService() {
        return true;
    }

    protected boolean addTestZenDiscovery() {
        return true;
    }

    protected boolean addMockHttpTransport() {
        return true;
    }

    protected Function<Client, Client> getClientWrapper() {
        return Function.identity();
    }

    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final ArrayList<Class<? extends Plugin>> mocks = new ArrayList<>();
        if (MOCK_MODULES_ENABLED && randomBoolean()) {
            if (randomBoolean() && addMockTransportService()) {
                mocks.add(MockTransportService.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockFSIndexStore.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(NodeMocksPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockEngineFactoryPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockSearchService.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockFieldFilterPlugin.class);
            }
        }
        if (addMockTransportService()) {
            mocks.add(getTestTransportPlugin());
        }
        if (addTestZenDiscovery()) {
            mocks.add(TestZenDiscovery.TestPlugin.class);
        }
        if (addMockHttpTransport()) {
            mocks.add(MockHttpTransport.TestPlugin.class);
        }
        mocks.add(TestSeedPlugin.class);
        mocks.add(AssertActionNamePlugin.class);
        return Collections.unmodifiableList(mocks);
    }

    public static final class TestSeedPlugin extends Plugin {

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(INDEX_TEST_SEED_SETTING);
        }
    }

    public static final class AssertActionNamePlugin extends Plugin implements NetworkPlugin {

        @Override
        public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
            return Arrays.asList(new TransportInterceptor() {

                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, String executor, boolean forceExecution, TransportRequestHandler<T> actualHandler) {
                    if (TransportService.isValidActionName(action) == false) {
                        throw new IllegalArgumentException("invalid action name [" + action + "] must start with one of: " + TransportService.VALID_ACTION_PREFIXES);
                    }
                    return actualHandler;
                }
            });
        }
    }

    private static double transportClientRatio() {
        String property = System.getProperty(TESTS_CLIENT_RATIO);
        if (property == null || property.isEmpty()) {
            return Double.NaN;
        }
        return Double.parseDouble(property);
    }

    protected double getPerTestTransportClientRatio() {
        final ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        double perTestRatio = -1;
        if (annotation != null) {
            perTestRatio = annotation.transportClientRatio();
        }
        if (perTestRatio == -1) {
            return Double.isNaN(TRANSPORT_CLIENT_RATIO) ? randomDouble() : TRANSPORT_CLIENT_RATIO;
        }
        assert perTestRatio >= 0.0 && perTestRatio <= 1.0;
        return perTestRatio;
    }

    public Path randomRepoPath() {
        if (currentCluster instanceof InternalTestCluster) {
            return randomRepoPath(((InternalTestCluster) currentCluster).getDefaultSettings());
        }
        throw new UnsupportedOperationException("unsupported cluster type");
    }

    public static Path randomRepoPath(Settings settings) {
        Environment environment = TestEnvironment.newEnvironment(settings);
        Path[] repoFiles = environment.repoFiles();
        assert repoFiles.length > 0;
        Path path;
        do {
            path = repoFiles[0].resolve(randomAlphaOfLength(10));
        } while (Files.exists(path));
        return path;
    }

    protected NumShards getNumShards(String index) {
        MetaData metaData = client().admin().cluster().prepareState().get().getState().metaData();
        assertThat(metaData.hasIndex(index), equalTo(true));
        int numShards = Integer.valueOf(metaData.index(index).getSettings().get(SETTING_NUMBER_OF_SHARDS));
        int numReplicas = Integer.valueOf(metaData.index(index).getSettings().get(SETTING_NUMBER_OF_REPLICAS));
        return new NumShards(numShards, numReplicas);
    }

    public Set<String> assertAllShardsOnNodes(String index, String... pattern) {
        Set<String> nodes = new HashSet<>();
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    if (shardRouting.currentNodeId() != null && index.equals(shardRouting.getIndexName())) {
                        String name = clusterState.nodes().get(shardRouting.currentNodeId()).getName();
                        nodes.add(name);
                        assertThat("Allocated on new node: " + name, Regex.simpleMatch(pattern, name), is(true));
                    }
                }
            }
        }
        return nodes;
    }

    public void assertSortedSegments(String indexName, Sort expectedIndexSort) {
        IndicesSegmentResponse segmentResponse = client().admin().indices().prepareSegments(indexName).execute().actionGet();
        IndexSegments indexSegments = segmentResponse.getIndices().get(indexName);
        for (IndexShardSegments indexShardSegments : indexSegments.getShards().values()) {
            for (ShardSegments shardSegments : indexShardSegments.getShards()) {
                for (Segment segment : shardSegments) {
                    assertThat(expectedIndexSort, equalTo(segment.getSegmentSort()));
                }
            }
        }
    }

    protected static class NumShards {

        public final int numPrimaries;

        public final int numReplicas;

        public final int totalNumShards;

        public final int dataCopies;

        private NumShards(int numPrimaries, int numReplicas) {
            this.numPrimaries = numPrimaries;
            this.numReplicas = numReplicas;
            this.dataCopies = numReplicas + 1;
            this.totalNumShards = numPrimaries * dataCopies;
        }
    }

    private static boolean runTestScopeLifecycle() {
        return INSTANCE == null;
    }

    @Before
    public final void setupTestCluster() throws Exception {
        if (runTestScopeLifecycle()) {
            printTestMessage("setting up");
            beforeInternal();
            printTestMessage("all set up");
        }
    }

    @After
    public final void cleanUpCluster() throws Exception {
        super.ensureAllSearchContextsReleased();
        if (runTestScopeLifecycle()) {
            printTestMessage("cleaning up after");
            afterInternal(false);
            printTestMessage("cleaned up after");
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (!runTestScopeLifecycle()) {
            try {
                INSTANCE.printTestMessage("cleaning up after");
                INSTANCE.afterInternal(true);
                checkStaticState(true);
            } finally {
                INSTANCE = null;
            }
        } else {
            clearClusters();
        }
        SUITE_SEED = null;
        currentCluster = null;
    }

    private static void initializeSuiteScope() throws Exception {
        Class<?> targetClass = getTestClass();
        assert INSTANCE == null;
        if (isSuiteScopedTest(targetClass)) {
            INSTANCE = (ESIntegTestCase) targetClass.getConstructor().newInstance();
            boolean success = false;
            try {
                INSTANCE.printTestMessage("setup");
                INSTANCE.beforeInternal();
                INSTANCE.setupSuiteScopeCluster();
                success = true;
            } finally {
                if (!success) {
                    afterClass();
                }
            }
        } else {
            INSTANCE = null;
        }
    }

    protected String routingKeyForShard(String index, int shard) {
        return internalCluster().routingKeyForShard(resolveIndex(index), shard, random());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        if (isInternalCluster() && cluster().size() > 0) {
            return internalCluster().getInstance(NamedXContentRegistry.class);
        } else {
            return new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
        }
    }

    protected boolean forbidPrivateIndexSettings() {
        return true;
    }

    protected static synchronized RestClient getRestClient() {
        if (restClient == null) {
            restClient = createRestClient(null);
        }
        return restClient;
    }

    protected static RestClient createRestClient(RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback) {
        return createRestClient(httpClientConfigCallback, "http");
    }

    protected static RestClient createRestClient(RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback, String protocol) {
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        return createRestClient(nodesInfoResponse.getNodes(), httpClientConfigCallback, protocol);
    }

    protected static RestClient createRestClient(final List<NodeInfo> nodes, RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback, String protocol) {
        List<HttpHost> hosts = new ArrayList<>();
        for (NodeInfo node : nodes) {
            if (node.getHttp() != null) {
                TransportAddress publishAddress = node.getHttp().address().publishAddress();
                InetSocketAddress address = publishAddress.address();
                hosts.add(new HttpHost(NetworkAddress.format(address.getAddress()), address.getPort(), protocol));
            }
        }
        RestClientBuilder builder = RestClient.builder(hosts.toArray(new HttpHost[hosts.size()]));
        if (httpClientConfigCallback != null) {
            builder.setHttpClientConfigCallback(httpClientConfigCallback);
        }
        return builder.build();
    }

    protected void setupSuiteScopeCluster() throws Exception {
    }

    private static boolean isSuiteScopedTest(Class<?> clazz) {
        return clazz.getAnnotation(SuiteScopeTestCase.class) != null;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Inherited
    @Target(ElementType.TYPE)
    public @interface SuiteScopeTestCase {
    }

    public static Index resolveIndex(String index) {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetaData.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    public static boolean inFipsJvm() {
        return Security.getProviders()[0].getName().toLowerCase(Locale.ROOT).contains("fips");
    }
}