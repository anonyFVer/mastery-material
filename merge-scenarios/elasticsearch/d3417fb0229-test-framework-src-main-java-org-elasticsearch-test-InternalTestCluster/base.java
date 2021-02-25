package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.SeedUtils;
import com.carrotsearch.randomizedtesting.SysGlobals;
import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.StoreRateLimiting;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStoreConfig;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.MockTransportClient;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.junit.Assert;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.apache.lucene.util.LuceneTestCase.TEST_NIGHTLY;
import static org.apache.lucene.util.LuceneTestCase.rarely;
import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public final class InternalTestCluster extends TestCluster {

    private final Logger logger = Loggers.getLogger(getClass());

    public static final int PORTS_PER_JVM = 100;

    public static final int PORTS_PER_CLUSTER = 20;

    private static final int GLOBAL_TRANSPORT_BASE_PORT = 9300;

    private static final int GLOBAL_HTTP_BASE_PORT = 19200;

    private static final int JVM_ORDINAL = Integer.parseInt(System.getProperty(SysGlobals.CHILDVM_SYSPROP_JVM_ID, "0"));

    public static final int JVM_BASE_PORT_OFFSET = PORTS_PER_JVM * (JVM_ORDINAL + 1);

    private static final AtomicInteger clusterOrdinal = new AtomicInteger();

    private final int CLUSTER_BASE_PORT_OFFSET = JVM_BASE_PORT_OFFSET + (clusterOrdinal.getAndIncrement() * PORTS_PER_CLUSTER) % PORTS_PER_JVM;

    public final int TRANSPORT_BASE_PORT = GLOBAL_TRANSPORT_BASE_PORT + CLUSTER_BASE_PORT_OFFSET;

    public final int HTTP_BASE_PORT = GLOBAL_HTTP_BASE_PORT + CLUSTER_BASE_PORT_OFFSET;

    public static final int DEFAULT_LOW_NUM_MASTER_NODES = 1;

    public static final int DEFAULT_HIGH_NUM_MASTER_NODES = 3;

    static final int DEFAULT_MIN_NUM_DATA_NODES = 1;

    static final int DEFAULT_MAX_NUM_DATA_NODES = TEST_NIGHTLY ? 6 : 3;

    static final int DEFAULT_NUM_CLIENT_NODES = -1;

    static final int DEFAULT_MIN_NUM_CLIENT_NODES = 0;

    static final int DEFAULT_MAX_NUM_CLIENT_NODES = 1;

    static final boolean DEFAULT_ENABLE_HTTP_PIPELINING = true;

    private final NavigableMap<String, NodeAndClient> nodes = new TreeMap<>();

    private final Set<Path> dataDirToClean = new HashSet<>();

    private final String clusterName;

    private final AtomicBoolean open = new AtomicBoolean(true);

    private final Settings defaultSettings;

    private AtomicInteger nextNodeId = new AtomicInteger(0);

    private final long[] sharedNodesSeeds;

    private final int numSharedDedicatedMasterNodes;

    private final int numSharedDataNodes;

    private final int numSharedCoordOnlyNodes;

    private final NodeConfigurationSource nodeConfigurationSource;

    private final ExecutorService executor;

    private final Collection<Class<? extends Plugin>> mockPlugins;

    private final String nodePrefix;

    private final Path baseDir;

    private ServiceDisruptionScheme activeDisruptionScheme;

    private Function<Client, Client> clientWrapper;

    public InternalTestCluster(long clusterSeed, Path baseDir, boolean randomlyAddDedicatedMasters, int minNumDataNodes, int maxNumDataNodes, String clusterName, NodeConfigurationSource nodeConfigurationSource, int numClientNodes, boolean enableHttpPipelining, String nodePrefix, Collection<Class<? extends Plugin>> mockPlugins, Function<Client, Client> clientWrapper) {
        super(clusterSeed);
        this.clientWrapper = clientWrapper;
        this.baseDir = baseDir;
        this.clusterName = clusterName;
        if (minNumDataNodes < 0 || maxNumDataNodes < 0) {
            throw new IllegalArgumentException("minimum and maximum number of data nodes must be >= 0");
        }
        if (maxNumDataNodes < minNumDataNodes) {
            throw new IllegalArgumentException("maximum number of data nodes must be >= minimum number of  data nodes");
        }
        Random random = new Random(clusterSeed);
        boolean useDedicatedMasterNodes = randomlyAddDedicatedMasters ? random.nextBoolean() : false;
        this.numSharedDataNodes = RandomInts.randomIntBetween(random, minNumDataNodes, maxNumDataNodes);
        assert this.numSharedDataNodes >= 0;
        if (numSharedDataNodes == 0) {
            this.numSharedCoordOnlyNodes = 0;
            this.numSharedDedicatedMasterNodes = 0;
        } else {
            if (useDedicatedMasterNodes) {
                if (random.nextBoolean()) {
                    this.numSharedDedicatedMasterNodes = DEFAULT_LOW_NUM_MASTER_NODES;
                } else {
                    this.numSharedDedicatedMasterNodes = DEFAULT_HIGH_NUM_MASTER_NODES;
                }
            } else {
                this.numSharedDedicatedMasterNodes = 0;
            }
            if (numClientNodes < 0) {
                this.numSharedCoordOnlyNodes = RandomInts.randomIntBetween(random, DEFAULT_MIN_NUM_CLIENT_NODES, DEFAULT_MAX_NUM_CLIENT_NODES);
            } else {
                this.numSharedCoordOnlyNodes = numClientNodes;
            }
        }
        assert this.numSharedCoordOnlyNodes >= 0;
        this.nodePrefix = nodePrefix;
        assert nodePrefix != null;
        this.mockPlugins = mockPlugins;
        sharedNodesSeeds = new long[numSharedDedicatedMasterNodes + numSharedDataNodes + numSharedCoordOnlyNodes];
        for (int i = 0; i < sharedNodesSeeds.length; i++) {
            sharedNodesSeeds[i] = random.nextLong();
        }
        logger.info("Setup InternalTestCluster [{}] with seed [{}] using [{}] dedicated masters, " + "[{}] (data) nodes and [{}] coord only nodes", clusterName, SeedUtils.formatSeed(clusterSeed), numSharedDedicatedMasterNodes, numSharedDataNodes, numSharedCoordOnlyNodes);
        this.nodeConfigurationSource = nodeConfigurationSource;
        Builder builder = Settings.builder();
        if (random.nextInt(5) == 0) {
            final int numOfDataPaths = random.nextInt(5);
            if (numOfDataPaths > 0) {
                StringBuilder dataPath = new StringBuilder();
                for (int i = 0; i < numOfDataPaths; i++) {
                    dataPath.append(baseDir.resolve("d" + i).toAbsolutePath()).append(',');
                }
                builder.put(Environment.PATH_DATA_SETTING.getKey(), dataPath.toString());
            }
        }
        builder.put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), Integer.MAX_VALUE);
        builder.put(Environment.PATH_SHARED_DATA_SETTING.getKey(), baseDir.resolve("custom"));
        builder.put(Environment.PATH_HOME_SETTING.getKey(), baseDir);
        builder.put(Environment.PATH_REPO_SETTING.getKey(), baseDir.resolve("repos"));
        builder.put(TransportSettings.PORT.getKey(), TRANSPORT_BASE_PORT + "-" + (TRANSPORT_BASE_PORT + PORTS_PER_CLUSTER));
        builder.put("http.port", HTTP_BASE_PORT + "-" + (HTTP_BASE_PORT + PORTS_PER_CLUSTER));
        builder.put("http.pipelining", enableHttpPipelining);
        if (Strings.hasLength(System.getProperty("tests.es.logger.level"))) {
            builder.put("logger.level", System.getProperty("tests.es.logger.level"));
        }
        if (Strings.hasLength(System.getProperty("es.logger.prefix"))) {
            builder.put("logger.prefix", System.getProperty("es.logger.prefix"));
        }
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b");
        builder.put(ScriptService.SCRIPT_MAX_COMPILATIONS_PER_MINUTE.getKey(), 1000);
        if (TEST_NIGHTLY) {
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), RandomInts.randomIntBetween(random, 5, 10));
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), RandomInts.randomIntBetween(random, 5, 10));
        } else if (random.nextInt(100) <= 90) {
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), RandomInts.randomIntBetween(random, 2, 5));
            builder.put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), RandomInts.randomIntBetween(random, 2, 5));
        }
        builder.put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING.getKey(), TimeValue.timeValueMillis(RandomInts.randomIntBetween(random, 20, 50)));
        defaultSettings = builder.build();
        executor = EsExecutors.newScaling("test runner", 0, Integer.MAX_VALUE, 0, TimeUnit.SECONDS, EsExecutors.daemonThreadFactory("test_" + clusterName), new ThreadContext(Settings.EMPTY));
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    public String[] getNodeNames() {
        return nodes.keySet().toArray(Strings.EMPTY_ARRAY);
    }

    private Settings getSettings(int nodeOrdinal, long nodeSeed, Settings others) {
        Builder builder = Settings.builder().put(defaultSettings).put(getRandomNodeSettings(nodeSeed));
        Settings settings = nodeConfigurationSource.nodeSettings(nodeOrdinal);
        if (settings != null) {
            if (settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey()) != null) {
                throw new IllegalStateException("Tests must not set a '" + ClusterName.CLUSTER_NAME_SETTING.getKey() + "' as a node setting set '" + ClusterName.CLUSTER_NAME_SETTING.getKey() + "': [" + settings.get(ClusterName.CLUSTER_NAME_SETTING.getKey()) + "]");
            }
            builder.put(settings);
        }
        if (others != null) {
            builder.put(others);
        }
        builder.put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName);
        return builder.build();
    }

    private Collection<Class<? extends Plugin>> getPlugins() {
        Set<Class<? extends Plugin>> plugins = new HashSet<>(nodeConfigurationSource.nodePlugins());
        plugins.addAll(mockPlugins);
        return plugins;
    }

    private Settings getRandomNodeSettings(long seed) {
        Random random = new Random(seed);
        Builder builder = Settings.builder();
        builder.put(Transport.TRANSPORT_TCP_COMPRESS.getKey(), rarely(random));
        if (random.nextBoolean()) {
            builder.put("cache.recycler.page.type", RandomPicks.randomFrom(random, PageCacheRecycler.Type.values()));
        }
        if (random.nextInt(10) == 0) {
            builder.put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(10 + random.nextInt(2000)).getStringRep());
        } else if (random.nextInt(10) != 0) {
            builder.put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(10 + random.nextInt(5 * 60)).getStringRep());
        }
        if (random.nextBoolean()) {
            builder.put(SearchService.DEFAULT_KEEPALIVE_SETTING.getKey(), TimeValue.timeValueSeconds(100 + random.nextInt(5 * 60)).getStringRep());
        }
        builder.put(EsExecutors.PROCESSORS_SETTING.getKey(), 1 + random.nextInt(3));
        if (random.nextBoolean()) {
            if (random.nextBoolean()) {
                builder.put("indices.fielddata.cache.size", 1 + random.nextInt(1000), ByteSizeUnit.MB);
            }
        }
        if (random.nextBoolean()) {
            builder.put(TcpTransport.CONNECTIONS_PER_NODE_RECOVERY.getKey(), random.nextInt(2) + 1);
            builder.put(TcpTransport.CONNECTIONS_PER_NODE_BULK.getKey(), random.nextInt(3) + 1);
            builder.put(TcpTransport.CONNECTIONS_PER_NODE_REG.getKey(), random.nextInt(6) + 1);
        }
        if (random.nextBoolean()) {
            builder.put(MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING.getKey(), new TimeValue(RandomInts.randomIntBetween(random, 10, 30), TimeUnit.SECONDS));
        }
        if (random.nextInt(10) == 0) {
            builder.put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "noop");
            builder.put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "noop");
        }
        if (random.nextBoolean()) {
            if (random.nextInt(10) == 0) {
                builder.put(IndexStoreConfig.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING.getKey(), new ByteSizeValue(RandomInts.randomIntBetween(random, 1, 10), ByteSizeUnit.MB));
            } else {
                builder.put(IndexStoreConfig.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING.getKey(), new ByteSizeValue(RandomInts.randomIntBetween(random, 10, 200), ByteSizeUnit.MB));
            }
        }
        if (random.nextBoolean()) {
            builder.put(IndexStoreConfig.INDICES_STORE_THROTTLE_TYPE_SETTING.getKey(), RandomPicks.randomFrom(random, StoreRateLimiting.Type.values()));
        }
        if (random.nextBoolean()) {
            if (random.nextInt(10) == 0) {
                builder.put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), new ByteSizeValue(RandomInts.randomIntBetween(random, 1, 10), ByteSizeUnit.MB));
            } else {
                builder.put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), new ByteSizeValue(RandomInts.randomIntBetween(random, 10, 200), ByteSizeUnit.MB));
            }
        }
        if (random.nextBoolean()) {
            builder.put(TcpTransport.PING_SCHEDULE.getKey(), RandomInts.randomIntBetween(random, 100, 2000) + "ms");
        }
        if (random.nextBoolean()) {
            builder.put(ScriptService.SCRIPT_CACHE_SIZE_SETTING.getKey(), RandomInts.randomIntBetween(random, 0, 2000));
        }
        if (random.nextBoolean()) {
            builder.put(ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.getKey(), TimeValue.timeValueMillis(RandomInts.randomIntBetween(random, 750, 10000000)).getStringRep());
        }
        return builder.build();
    }

    public static String clusterName(String prefix, long clusterSeed) {
        StringBuilder builder = new StringBuilder(prefix);
        final int childVM = RandomizedTest.systemPropertyAsInt(SysGlobals.CHILDVM_SYSPROP_JVM_ID, 0);
        builder.append("-CHILD_VM=[").append(childVM).append(']');
        builder.append("-CLUSTER_SEED=[").append(clusterSeed).append(']');
        builder.append("-HASH=[").append(SeedUtils.formatSeed(System.nanoTime())).append(']');
        return builder.toString();
    }

    private void ensureOpen() {
        if (!open.get()) {
            throw new RuntimeException("Cluster is already closed");
        }
    }

    private synchronized NodeAndClient getOrBuildRandomNode() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient();
        if (randomNodeAndClient != null) {
            return randomNodeAndClient;
        }
        NodeAndClient buildNode = buildNode();
        buildNode.startNode();
        publishNode(buildNode);
        return buildNode;
    }

    private synchronized NodeAndClient getRandomNodeAndClient() {
        return getRandomNodeAndClient(nc -> true);
    }

    private synchronized NodeAndClient getRandomNodeAndClient(Predicate<NodeAndClient> predicate) {
        ensureOpen();
        Collection<NodeAndClient> values = nodes.values().stream().filter(predicate).collect(Collectors.toCollection(ArrayList::new));
        if (!values.isEmpty()) {
            int whichOne = random.nextInt(values.size());
            for (NodeAndClient nodeAndClient : values) {
                if (whichOne-- == 0) {
                    return nodeAndClient;
                }
            }
        }
        return null;
    }

    public void ensureAtLeastNumDataNodes(int n) {
        final List<Async<String>> asyncs = new ArrayList<>();
        synchronized (this) {
            int size = numDataNodes();
            for (int i = size; i < n; i++) {
                logger.info("increasing cluster size from {} to {}", size, n);
                if (numSharedDedicatedMasterNodes > 0) {
                    asyncs.add(startDataOnlyNodeAsync());
                } else {
                    asyncs.add(startNodeAsync());
                }
            }
        }
        try {
            for (Async<String> async : asyncs) {
                async.get();
            }
        } catch (Exception e) {
            throw new ElasticsearchException("failed to start nodes", e);
        }
        if (!asyncs.isEmpty()) {
            synchronized (this) {
                assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nodes.size())).get());
            }
        }
    }

    public synchronized void ensureAtMostNumDataNodes(int n) throws IOException {
        int size = numDataNodes();
        if (size <= n) {
            return;
        }
        final Stream<NodeAndClient> collection = n == 0 ? nodes.values().stream() : nodes.values().stream().filter(new DataNodePredicate().and(new MasterNodePredicate(getMasterName()).negate()));
        final Iterator<NodeAndClient> values = collection.iterator();
        logger.info("changing cluster size from {} data nodes to {}", size, n);
        Set<NodeAndClient> nodesToRemove = new HashSet<>();
        int numNodesAndClients = 0;
        while (values.hasNext() && numNodesAndClients++ < size - n) {
            NodeAndClient next = values.next();
            nodesToRemove.add(next);
            removeDisruptionSchemeFromNode(next);
            next.close();
        }
        for (NodeAndClient toRemove : nodesToRemove) {
            nodes.remove(toRemove.name);
        }
        if (!nodesToRemove.isEmpty() && size() > 0) {
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(nodes.size())).get());
        }
    }

    private NodeAndClient buildNode(Settings settings) {
        int ord = nextNodeId.getAndIncrement();
        return buildNode(ord, random.nextLong(), settings, false);
    }

    private NodeAndClient buildNode() {
        int ord = nextNodeId.getAndIncrement();
        return buildNode(ord, random.nextLong(), null, false);
    }

    private NodeAndClient buildNode(int nodeId, long seed, Settings settings, boolean reuseExisting) {
        assert Thread.holdsLock(this);
        ensureOpen();
        settings = getSettings(nodeId, seed, settings);
        Collection<Class<? extends Plugin>> plugins = getPlugins();
        String name = buildNodeName(nodeId, settings);
        if (reuseExisting && nodes.containsKey(name)) {
            return nodes.get(name);
        } else {
            assert reuseExisting == true || nodes.containsKey(name) == false : "node name [" + name + "] already exists but not allowed to use it";
        }
        Settings finalSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), baseDir).put(settings).put("node.name", name).put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), seed).build();
        MockNode node = new MockNode(finalSettings, plugins);
        return new NodeAndClient(name, node, nodeId);
    }

    private String buildNodeName(int id, Settings settings) {
        String prefix = nodePrefix;
        prefix = prefix + getRoleSuffix(settings);
        return prefix + id;
    }

    private String getRoleSuffix(Settings settings) {
        String suffix = "";
        if (Node.NODE_MASTER_SETTING.exists(settings) && Node.NODE_MASTER_SETTING.get(settings)) {
            suffix = suffix + Role.MASTER.getAbbreviation();
        }
        if (Node.NODE_DATA_SETTING.exists(settings) && Node.NODE_DATA_SETTING.get(settings)) {
            suffix = suffix + Role.DATA.getAbbreviation();
        }
        if (Node.NODE_MASTER_SETTING.exists(settings) && Node.NODE_MASTER_SETTING.get(settings) == false && Node.NODE_DATA_SETTING.exists(settings) && Node.NODE_DATA_SETTING.get(settings) == false) {
            suffix = suffix + "c";
        }
        return suffix;
    }

    public String nodePrefix() {
        return nodePrefix;
    }

    @Override
    public synchronized Client client() {
        ensureOpen();
        return getOrBuildRandomNode().client(random);
    }

    public synchronized Client dataNodeClient() {
        ensureOpen();
        return getRandomNodeAndClient(new DataNodePredicate()).client(random);
    }

    public synchronized Client masterClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new MasterNodePredicate(getMasterName()));
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient();
        }
        Assert.fail("No master client found");
        return null;
    }

    public synchronized Client nonMasterClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new MasterNodePredicate(getMasterName()).negate());
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient();
        }
        Assert.fail("No non-master client found");
        return null;
    }

    public synchronized Client coordOnlyNodeClient() {
        ensureOpen();
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(new NoDataNoMasterNodePredicate());
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.client(random);
        }
        int nodeId = nextNodeId.getAndIncrement();
        Settings settings = getSettings(nodeId, random.nextLong(), Settings.EMPTY);
        startCoordinatingOnlyNode(settings);
        return getRandomNodeAndClient(new NoDataNoMasterNodePredicate()).client(random);
    }

    public synchronized String startCoordinatingOnlyNode(Settings settings) {
        ensureOpen();
        Builder builder = Settings.builder().put(settings).put(Node.NODE_MASTER_SETTING.getKey(), false).put(Node.NODE_DATA_SETTING.getKey(), false).put(Node.NODE_INGEST_SETTING.getKey(), false);
        if (size() == 0) {
            builder.put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), 0);
        }
        return startNode(builder);
    }

    public synchronized Client transportClient() {
        ensureOpen();
        return getOrBuildRandomNode().transportClient();
    }

    public synchronized Client client(String nodeName) {
        ensureOpen();
        NodeAndClient nodeAndClient = nodes.get(nodeName);
        if (nodeAndClient != null) {
            return nodeAndClient.client(random);
        }
        Assert.fail("No node found with name: [" + nodeName + "]");
        return null;
    }

    public synchronized Client smartClient() {
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient();
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.nodeClient();
        }
        Assert.fail("No smart client found");
        return null;
    }

    public synchronized Client client(final Predicate<Settings> filterPredicate) {
        ensureOpen();
        final NodeAndClient randomNodeAndClient = getRandomNodeAndClient(nodeAndClient -> filterPredicate.test(nodeAndClient.node.settings()));
        if (randomNodeAndClient != null) {
            return randomNodeAndClient.client(random);
        }
        return null;
    }

    @Override
    public synchronized void close() {
        if (this.open.compareAndSet(true, false)) {
            if (activeDisruptionScheme != null) {
                activeDisruptionScheme.testClusterClosed();
                activeDisruptionScheme = null;
            }
            IOUtils.closeWhileHandlingException(nodes.values());
            nodes.clear();
            executor.shutdownNow();
        }
    }

    private final class NodeAndClient implements Closeable {

        private MockNode node;

        private Client nodeClient;

        private Client transportClient;

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private final String name;

        private final int nodeAndClientId;

        NodeAndClient(String name, MockNode node, int nodeAndClientId) {
            this.node = node;
            this.name = name;
            this.nodeAndClientId = nodeAndClientId;
            markNodeDataDirsAsNotEligableForWipe(node);
        }

        Node node() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            return node;
        }

        public int nodeAndClientId() {
            return nodeAndClientId;
        }

        Client client(Random random) {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            double nextDouble = random.nextDouble();
            if (nextDouble < transportClientRatio) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Using transport client for node [{}] sniff: [{}]", node.settings().get("node.name"), false);
                }
                return getOrBuildTransportClient();
            } else {
                return getOrBuildNodeClient();
            }
        }

        Client nodeClient() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            return getOrBuildNodeClient();
        }

        Client transportClient() {
            if (closed.get()) {
                throw new RuntimeException("already closed");
            }
            return getOrBuildTransportClient();
        }

        private Client getOrBuildNodeClient() {
            if (nodeClient == null) {
                nodeClient = node.client();
            }
            return clientWrapper.apply(nodeClient);
        }

        private Client getOrBuildTransportClient() {
            if (transportClient == null) {
                transportClient = new TransportClientFactory(false, nodeConfigurationSource.transportClientSettings(), baseDir, nodeConfigurationSource.transportClientPlugins()).client(node, clusterName);
            }
            return clientWrapper.apply(transportClient);
        }

        void resetClient() throws IOException {
            if (closed.get() == false) {
                Releasables.close(nodeClient, transportClient);
                nodeClient = null;
                transportClient = null;
            }
        }

        void startNode() {
            try {
                node.start();
            } catch (NodeValidationException e) {
                throw new RuntimeException(e);
            }
        }

        void closeNode() throws IOException {
            markNodeDataDirsAsPendingForWipe(node);
            node.close();
        }

        void restart(RestartCallback callback, boolean clearDataIfNeeded) throws Exception {
            assert callback != null;
            resetClient();
            if (!node.isClosed()) {
                closeNode();
            }
            Settings newSettings = callback.onNodeStopped(name);
            if (newSettings == null) {
                newSettings = Settings.EMPTY;
            }
            if (clearDataIfNeeded) {
                clearDataIfNeeded(callback);
            }
            createNewNode(newSettings);
            startNode();
        }

        private void clearDataIfNeeded(RestartCallback callback) throws IOException {
            if (callback.clearData(name)) {
                NodeEnvironment nodeEnv = node.getNodeEnvironment();
                if (nodeEnv.hasNodeFile()) {
                    final Path[] locations = nodeEnv.nodeDataPaths();
                    logger.debug("removing node data paths: [{}]", Arrays.toString(locations));
                    IOUtils.rm(locations);
                }
            }
        }

        private void createNewNode(final Settings newSettings) {
            final long newIdSeed = NodeEnvironment.NODE_ID_SEED_SETTING.get(node.settings()) + 1;
            Settings finalSettings = Settings.builder().put(node.settings()).put(newSettings).put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), newIdSeed).build();
            Collection<Class<? extends Plugin>> plugins = node.getClasspathPlugins();
            node = new MockNode(finalSettings, plugins);
            markNodeDataDirsAsNotEligableForWipe(node);
        }

        @Override
        public void close() throws IOException {
            try {
                resetClient();
            } finally {
                closed.set(true);
                closeNode();
            }
        }
    }

    public static final String TRANSPORT_CLIENT_PREFIX = "transport_client_";

    static class TransportClientFactory {

        private final boolean sniff;

        private final Settings settings;

        private final Path baseDir;

        private final Collection<Class<? extends Plugin>> plugins;

        TransportClientFactory(boolean sniff, Settings settings, Path baseDir, Collection<Class<? extends Plugin>> plugins) {
            this.sniff = sniff;
            this.settings = settings != null ? settings : Settings.EMPTY;
            this.baseDir = baseDir;
            this.plugins = plugins;
        }

        public Client client(Node node, String clusterName) {
            TransportAddress addr = node.injector().getInstance(TransportService.class).boundAddress().publishAddress();
            Settings nodeSettings = node.settings();
            Builder builder = Settings.builder().put("client.transport.nodes_sampler_interval", "1s").put(Environment.PATH_HOME_SETTING.getKey(), baseDir).put("node.name", TRANSPORT_CLIENT_PREFIX + node.settings().get("node.name")).put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName).put("client.transport.sniff", sniff).put("logger.prefix", nodeSettings.get("logger.prefix", "")).put("logger.level", nodeSettings.get("logger.level", "INFO")).put(settings);
            if (NetworkModule.TRANSPORT_TYPE_SETTING.exists(settings)) {
                builder.put(NetworkModule.TRANSPORT_TYPE_KEY, NetworkModule.TRANSPORT_TYPE_SETTING.get(settings));
            }
            TransportClient client = new MockTransportClient(builder.build(), plugins);
            client.addTransportAddress(addr);
            return client;
        }
    }

    @Override
    public synchronized void beforeTest(Random random, double transportClientRatio) throws IOException, InterruptedException {
        super.beforeTest(random, transportClientRatio);
        reset(true);
    }

    private synchronized void reset(boolean wipeData) throws IOException {
        for (NodeAndClient nodeAndClient : nodes.values()) {
            TransportService transportService = nodeAndClient.node.injector().getInstance(TransportService.class);
            if (transportService instanceof MockTransportService) {
                final MockTransportService mockTransportService = (MockTransportService) transportService;
                mockTransportService.clearAllRules();
                mockTransportService.clearTracers();
            }
        }
        randomlyResetClients();
        final int newSize = sharedNodesSeeds.length;
        if (nextNodeId.get() == newSize && nodes.size() == newSize) {
            if (wipeData) {
                wipePendingDataDirectories();
            }
            logger.debug("Cluster hasn't changed - moving out - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), newSize);
            return;
        }
        logger.debug("Cluster is NOT consistent - restarting shared nodes - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), newSize);
        for (Iterator<NodeAndClient> iterator = nodes.values().iterator(); iterator.hasNext(); ) {
            NodeAndClient nodeAndClient = iterator.next();
            if (nodeAndClient.nodeAndClientId() >= sharedNodesSeeds.length) {
                logger.debug("Close Node [{}] not shared", nodeAndClient.name);
                nodeAndClient.close();
                iterator.remove();
            }
        }
        if (wipeData) {
            wipePendingDataDirectories();
        }
        assert newSize == numSharedDedicatedMasterNodes + numSharedDataNodes + numSharedCoordOnlyNodes;
        for (int i = 0; i < numSharedDedicatedMasterNodes; i++) {
            final Settings.Builder settings = Settings.builder();
            settings.put(Node.NODE_MASTER_SETTING.getKey(), true).build();
            settings.put(Node.NODE_DATA_SETTING.getKey(), false).build();
            NodeAndClient nodeAndClient = buildNode(i, sharedNodesSeeds[i], settings.build(), true);
            nodeAndClient.startNode();
            publishNode(nodeAndClient);
        }
        for (int i = numSharedDedicatedMasterNodes; i < numSharedDedicatedMasterNodes + numSharedDataNodes; i++) {
            final Settings.Builder settings = Settings.builder();
            if (numSharedDedicatedMasterNodes > 0) {
                settings.put(Node.NODE_MASTER_SETTING.getKey(), false).build();
                settings.put(Node.NODE_DATA_SETTING.getKey(), true).build();
            }
            NodeAndClient nodeAndClient = buildNode(i, sharedNodesSeeds[i], settings.build(), true);
            nodeAndClient.startNode();
            publishNode(nodeAndClient);
        }
        for (int i = numSharedDedicatedMasterNodes + numSharedDataNodes; i < numSharedDedicatedMasterNodes + numSharedDataNodes + numSharedCoordOnlyNodes; i++) {
            final Builder settings = Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false).put(Node.NODE_DATA_SETTING.getKey(), false).put(Node.NODE_INGEST_SETTING.getKey(), false);
            NodeAndClient nodeAndClient = buildNode(i, sharedNodesSeeds[i], settings.build(), true);
            nodeAndClient.startNode();
            publishNode(nodeAndClient);
        }
        nextNodeId.set(newSize);
        assert size() == newSize;
        if (newSize > 0) {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(newSize)).get();
            if (response.isTimedOut()) {
                logger.warn("failed to wait for a cluster of size [{}], got [{}]", newSize, response);
                throw new IllegalStateException("cluster failed to reach the expected size of [" + newSize + "]");
            }
        }
        logger.debug("Cluster is consistent again - nodes: [{}] nextNodeId: [{}] numSharedNodes: [{}]", nodes.keySet(), nextNodeId.get(), newSize);
    }

    @Override
    public synchronized void afterTest() throws IOException {
        wipePendingDataDirectories();
        randomlyResetClients();
    }

    @Override
    public void beforeIndexDeletion() {
        assertShardIndexCounter();
        assertSameSyncIdSameDocs();
    }

    private void assertSameSyncIdSameDocs() {
        Map<String, Long> docsOnShards = new HashMap<>();
        final Collection<NodeAndClient> nodesAndClients = nodes.values();
        for (NodeAndClient nodeAndClient : nodesAndClients) {
            IndicesService indexServices = getInstance(IndicesService.class, nodeAndClient.name);
            for (IndexService indexService : indexServices) {
                for (IndexShard indexShard : indexService) {
                    CommitStats commitStats = indexShard.commitStats();
                    if (commitStats != null) {
                        String syncId = commitStats.getUserData().get(Engine.SYNC_COMMIT_ID);
                        if (syncId != null) {
                            long liveDocsOnShard = commitStats.getNumDocs();
                            if (docsOnShards.get(syncId) != null) {
                                assertThat("sync id is equal but number of docs does not match on node " + nodeAndClient.name + ". expected " + docsOnShards.get(syncId) + " but got " + liveDocsOnShard, docsOnShards.get(syncId), equalTo(liveDocsOnShard));
                            } else {
                                docsOnShards.put(syncId, liveDocsOnShard);
                            }
                        }
                    }
                }
            }
        }
    }

    private void assertShardIndexCounter() {
        final Collection<NodeAndClient> nodesAndClients = nodes.values();
        for (NodeAndClient nodeAndClient : nodesAndClients) {
            IndicesService indexServices = getInstance(IndicesService.class, nodeAndClient.name);
            for (IndexService indexService : indexServices) {
                for (IndexShard indexShard : indexService) {
                    assertThat("index shard counter on shard " + indexShard.shardId() + " on node " + nodeAndClient.name + " not 0", indexShard.getActiveOperationsCount(), equalTo(0));
                }
            }
        }
    }

    private void randomlyResetClients() throws IOException {
        if (RandomizedTest.isNightly() && rarely(random)) {
            final Collection<NodeAndClient> nodesAndClients = nodes.values();
            for (NodeAndClient nodeAndClient : nodesAndClients) {
                nodeAndClient.resetClient();
            }
        }
    }

    private void wipePendingDataDirectories() {
        assert Thread.holdsLock(this);
        if (!dataDirToClean.isEmpty()) {
            try {
                for (Path path : dataDirToClean) {
                    try {
                        FileSystemUtils.deleteSubDirectories(path);
                        logger.info("Successfully wiped data directory for node location: {}", path);
                    } catch (IOException e) {
                        logger.info("Failed to wipe data directory for node location: {}", path);
                    }
                }
            } finally {
                dataDirToClean.clear();
            }
        }
    }

    private void markNodeDataDirsAsPendingForWipe(Node node) {
        assert Thread.holdsLock(this);
        NodeEnvironment nodeEnv = node.getNodeEnvironment();
        if (nodeEnv.hasNodeFile()) {
            dataDirToClean.addAll(Arrays.asList(nodeEnv.nodeDataPaths()));
        }
    }

    private void markNodeDataDirsAsNotEligableForWipe(Node node) {
        assert Thread.holdsLock(this);
        NodeEnvironment nodeEnv = node.getNodeEnvironment();
        if (nodeEnv.hasNodeFile()) {
            dataDirToClean.removeAll(Arrays.asList(nodeEnv.nodeDataPaths()));
        }
    }

    public ClusterService clusterService() {
        return clusterService(null);
    }

    public synchronized ClusterService clusterService(@Nullable String node) {
        return getInstance(ClusterService.class, node);
    }

    public synchronized <T> Iterable<T> getInstances(Class<T> clazz) {
        List<T> instances = new ArrayList<>(nodes.size());
        for (NodeAndClient nodeAndClient : nodes.values()) {
            instances.add(getInstanceFromNode(clazz, nodeAndClient.node));
        }
        return instances;
    }

    public synchronized <T> Iterable<T> getDataNodeInstances(Class<T> clazz) {
        return getInstances(clazz, new DataNodePredicate());
    }

    public synchronized <T> Iterable<T> getDataOrMasterNodeInstances(Class<T> clazz) {
        return getInstances(clazz, new DataOrMasterNodePredicate());
    }

    private synchronized <T> Iterable<T> getInstances(Class<T> clazz, Predicate<NodeAndClient> predicate) {
        Iterable<NodeAndClient> filteredNodes = nodes.values().stream().filter(predicate)::iterator;
        List<T> instances = new ArrayList<>();
        for (NodeAndClient nodeAndClient : filteredNodes) {
            instances.add(getInstanceFromNode(clazz, nodeAndClient.node));
        }
        return instances;
    }

    public synchronized <T> T getInstance(Class<T> clazz, final String node) {
        return getInstance(clazz, nc -> node == null || node.equals(nc.name));
    }

    public synchronized <T> T getDataNodeInstance(Class<T> clazz) {
        return getInstance(clazz, new DataNodePredicate());
    }

    private synchronized <T> T getInstance(Class<T> clazz, Predicate<NodeAndClient> predicate) {
        NodeAndClient randomNodeAndClient = getRandomNodeAndClient(predicate);
        assert randomNodeAndClient != null;
        return getInstanceFromNode(clazz, randomNodeAndClient.node);
    }

    public synchronized <T> T getInstance(Class<T> clazz) {
        return getInstance(clazz, nc -> true);
    }

    private synchronized <T> T getInstanceFromNode(Class<T> clazz, Node node) {
        return node.injector().getInstance(clazz);
    }

    @Override
    public synchronized int size() {
        return this.nodes.size();
    }

    @Override
    public InetSocketAddress[] httpAddresses() {
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (HttpServerTransport httpServerTransport : getInstances(HttpServerTransport.class)) {
            addresses.add(((InetSocketTransportAddress) httpServerTransport.boundAddress().publishAddress()).address());
        }
        return addresses.toArray(new InetSocketAddress[addresses.size()]);
    }

    public synchronized boolean stopRandomDataNode() throws IOException {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient(new DataNodePredicate());
        if (nodeAndClient != null) {
            logger.info("Closing random node [{}] ", nodeAndClient.name);
            removeDisruptionSchemeFromNode(nodeAndClient);
            nodes.remove(nodeAndClient.name);
            nodeAndClient.close();
            return true;
        }
        return false;
    }

    public synchronized void stopRandomNode(final Predicate<Settings> filter) throws IOException {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient(nc -> filter.test(nc.node.settings()));
        if (nodeAndClient != null) {
            logger.info("Closing filtered random node [{}] ", nodeAndClient.name);
            removeDisruptionSchemeFromNode(nodeAndClient);
            nodes.remove(nodeAndClient.name);
            nodeAndClient.close();
        }
    }

    public synchronized void stopCurrentMasterNode() throws IOException {
        ensureOpen();
        assert size() > 0;
        String masterNodeName = getMasterName();
        assert nodes.containsKey(masterNodeName);
        logger.info("Closing master node [{}] ", masterNodeName);
        removeDisruptionSchemeFromNode(nodes.get(masterNodeName));
        NodeAndClient remove = nodes.remove(masterNodeName);
        remove.close();
    }

    public synchronized void stopRandomNonMasterNode() throws IOException {
        NodeAndClient nodeAndClient = getRandomNodeAndClient(new MasterNodePredicate(getMasterName()).negate());
        if (nodeAndClient != null) {
            logger.info("Closing random non master node [{}] current master [{}] ", nodeAndClient.name, getMasterName());
            removeDisruptionSchemeFromNode(nodeAndClient);
            nodes.remove(nodeAndClient.name);
            nodeAndClient.close();
        }
    }

    public void restartRandomNode() throws Exception {
        restartRandomNode(EMPTY_CALLBACK);
    }

    public void restartRandomNode(RestartCallback callback) throws Exception {
        restartRandomNode(nc -> true, callback);
    }

    public void restartRandomDataNode() throws Exception {
        restartRandomDataNode(EMPTY_CALLBACK);
    }

    public void restartRandomDataNode(RestartCallback callback) throws Exception {
        restartRandomNode(new DataNodePredicate(), callback);
    }

    private synchronized void restartRandomNode(Predicate<NodeAndClient> predicate, RestartCallback callback) throws Exception {
        ensureOpen();
        NodeAndClient nodeAndClient = getRandomNodeAndClient(predicate);
        if (nodeAndClient != null) {
            logger.info("Restarting random node [{}] ", nodeAndClient.name);
            nodeAndClient.restart(callback, true);
        }
    }

    public synchronized void restartNode(String nodeName, RestartCallback callback) throws Exception {
        ensureOpen();
        NodeAndClient nodeAndClient = nodes.get(nodeName);
        if (nodeAndClient != null) {
            logger.info("Restarting node [{}] ", nodeAndClient.name);
            nodeAndClient.restart(callback, true);
        }
    }

    private synchronized void restartAllNodes(boolean rollingRestart, RestartCallback callback) throws Exception {
        ensureOpen();
        List<NodeAndClient> toRemove = new ArrayList<>();
        try {
            for (NodeAndClient nodeAndClient : nodes.values()) {
                if (!callback.doRestart(nodeAndClient.name)) {
                    logger.info("Closing node [{}] during restart", nodeAndClient.name);
                    toRemove.add(nodeAndClient);
                    if (activeDisruptionScheme != null) {
                        activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
                    }
                    nodeAndClient.close();
                }
            }
        } finally {
            for (NodeAndClient nodeAndClient : toRemove) {
                nodes.remove(nodeAndClient.name);
            }
        }
        logger.info("Restarting remaining nodes rollingRestart [{}]", rollingRestart);
        if (rollingRestart) {
            int numNodesRestarted = 0;
            for (NodeAndClient nodeAndClient : nodes.values()) {
                callback.doAfterNodes(numNodesRestarted++, nodeAndClient.nodeClient());
                logger.info("Restarting node [{}] ", nodeAndClient.name);
                if (activeDisruptionScheme != null) {
                    activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
                }
                nodeAndClient.restart(callback, true);
                if (activeDisruptionScheme != null) {
                    activeDisruptionScheme.applyToNode(nodeAndClient.name, this);
                }
            }
        } else {
            int numNodesRestarted = 0;
            Set[] nodesRoleOrder = new Set[nextNodeId.get()];
            Map<Set<Role>, List<NodeAndClient>> nodesByRoles = new HashMap<>();
            for (NodeAndClient nodeAndClient : nodes.values()) {
                callback.doAfterNodes(numNodesRestarted++, nodeAndClient.nodeClient());
                logger.info("Stopping node [{}] ", nodeAndClient.name);
                if (activeDisruptionScheme != null) {
                    activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
                }
                nodeAndClient.closeNode();
                nodeAndClient.clearDataIfNeeded(callback);
                DiscoveryNode discoveryNode = getInstanceFromNode(ClusterService.class, nodeAndClient.node()).localNode();
                nodesRoleOrder[nodeAndClient.nodeAndClientId()] = discoveryNode.getRoles();
                nodesByRoles.computeIfAbsent(discoveryNode.getRoles(), k -> new ArrayList<>()).add(nodeAndClient);
            }
            assert nodesByRoles.values().stream().collect(Collectors.summingInt(List::size)) == nodes.size();
            for (List<NodeAndClient> sameRoleNodes : nodesByRoles.values()) {
                Collections.shuffle(sameRoleNodes, random);
            }
            for (Set roles : nodesRoleOrder) {
                if (roles == null) {
                    continue;
                }
                NodeAndClient nodeAndClient = nodesByRoles.get(roles).remove(0);
                logger.info("Starting node [{}] ", nodeAndClient.name);
                if (activeDisruptionScheme != null) {
                    activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
                }
                nodeAndClient.restart(callback, false);
                if (activeDisruptionScheme != null) {
                    activeDisruptionScheme.applyToNode(nodeAndClient.name, this);
                }
            }
        }
    }

    public static final RestartCallback EMPTY_CALLBACK = new RestartCallback() {

        @Override
        public Settings onNodeStopped(String node) {
            return null;
        }
    };

    public void fullRestart() throws Exception {
        fullRestart(EMPTY_CALLBACK);
    }

    public void rollingRestart() throws Exception {
        rollingRestart(EMPTY_CALLBACK);
    }

    public void rollingRestart(RestartCallback function) throws Exception {
        restartAllNodes(true, function);
    }

    public void fullRestart(RestartCallback function) throws Exception {
        restartAllNodes(false, function);
    }

    public String getMasterName() {
        return getMasterName(null);
    }

    public String getMasterName(@Nullable String viaNode) {
        try {
            Client client = viaNode != null ? client(viaNode) : client();
            ClusterState state = client.admin().cluster().prepareState().execute().actionGet().getState();
            return state.nodes().getMasterNode().getName();
        } catch (Exception e) {
            logger.warn("Can't fetch cluster state", e);
            throw new RuntimeException("Can't get master node " + e.getMessage(), e);
        }
    }

    synchronized Set<String> allDataNodesButN(int numNodes) {
        return nRandomDataNodes(numDataNodes() - numNodes);
    }

    private synchronized Set<String> nRandomDataNodes(int numNodes) {
        assert size() >= numNodes;
        Map<String, NodeAndClient> dataNodes = nodes.entrySet().stream().filter(new EntryNodePredicate(new DataNodePredicate())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        final HashSet<String> set = new HashSet<>();
        final Iterator<String> iterator = dataNodes.keySet().iterator();
        for (int i = 0; i < numNodes; i++) {
            assert iterator.hasNext();
            set.add(iterator.next());
        }
        return set;
    }

    public synchronized Set<String> nodesInclude(String index) {
        if (clusterService().state().routingTable().hasIndex(index)) {
            List<ShardRouting> allShards = clusterService().state().routingTable().allShards(index);
            DiscoveryNodes discoveryNodes = clusterService().state().getNodes();
            Set<String> nodes = new HashSet<>();
            for (ShardRouting shardRouting : allShards) {
                if (shardRouting.assignedToNode()) {
                    DiscoveryNode discoveryNode = discoveryNodes.get(shardRouting.currentNodeId());
                    nodes.add(discoveryNode.getName());
                }
            }
            return nodes;
        }
        return Collections.emptySet();
    }

    public synchronized String startNode() {
        return startNode(Settings.EMPTY);
    }

    public synchronized String startNode(Settings.Builder settings) {
        return startNode(settings.build());
    }

    public synchronized String startNode(Settings settings) {
        NodeAndClient buildNode = buildNode(settings);
        buildNode.startNode();
        publishNode(buildNode);
        return buildNode.name;
    }

    public synchronized Async<List<String>> startMasterOnlyNodesAsync(int numNodes) {
        return startMasterOnlyNodesAsync(numNodes, Settings.EMPTY);
    }

    public synchronized Async<List<String>> startMasterOnlyNodesAsync(int numNodes, Settings settings) {
        Settings settings1 = Settings.builder().put(settings).put(Node.NODE_MASTER_SETTING.getKey(), true).put(Node.NODE_DATA_SETTING.getKey(), false).build();
        return startNodesAsync(numNodes, settings1, Version.CURRENT);
    }

    public synchronized Async<List<String>> startDataOnlyNodesAsync(int numNodes) {
        return startDataOnlyNodesAsync(numNodes, Settings.EMPTY);
    }

    public synchronized Async<List<String>> startDataOnlyNodesAsync(int numNodes, Settings settings) {
        Settings settings1 = Settings.builder().put(settings).put(Node.NODE_MASTER_SETTING.getKey(), false).put(Node.NODE_DATA_SETTING.getKey(), true).build();
        return startNodesAsync(numNodes, settings1, Version.CURRENT);
    }

    public synchronized Async<String> startMasterOnlyNodeAsync() {
        return startMasterOnlyNodeAsync(Settings.EMPTY);
    }

    public synchronized Async<String> startMasterOnlyNodeAsync(Settings settings) {
        Settings settings1 = Settings.builder().put(settings).put(Node.NODE_MASTER_SETTING.getKey(), true).put(Node.NODE_DATA_SETTING.getKey(), false).build();
        return startNodeAsync(settings1, Version.CURRENT);
    }

    public synchronized String startMasterOnlyNode(Settings settings) {
        Settings settings1 = Settings.builder().put(settings).put(Node.NODE_MASTER_SETTING.getKey(), true).put(Node.NODE_DATA_SETTING.getKey(), false).build();
        return startNode(settings1);
    }

    public synchronized Async<String> startDataOnlyNodeAsync() {
        return startDataOnlyNodeAsync(Settings.EMPTY);
    }

    public synchronized Async<String> startDataOnlyNodeAsync(Settings settings) {
        Settings settings1 = Settings.builder().put(settings).put(Node.NODE_MASTER_SETTING.getKey(), false).put(Node.NODE_DATA_SETTING.getKey(), true).build();
        return startNodeAsync(settings1, Version.CURRENT);
    }

    public synchronized String startDataOnlyNode(Settings settings) {
        Settings settings1 = Settings.builder().put(settings).put(Node.NODE_MASTER_SETTING.getKey(), false).put(Node.NODE_DATA_SETTING.getKey(), true).build();
        return startNode(settings1);
    }

    public synchronized Async<String> startNodeAsync() {
        return startNodeAsync(Settings.EMPTY, Version.CURRENT);
    }

    public synchronized Async<String> startNodeAsync(final Settings settings) {
        return startNodeAsync(settings, Version.CURRENT);
    }

    public synchronized Async<String> startNodeAsync(final Settings settings, final Version version) {
        final NodeAndClient buildNode = buildNode(settings);
        final Future<String> submit = executor.submit(() -> {
            buildNode.startNode();
            publishNode(buildNode);
            return buildNode.name;
        });
        return () -> submit.get();
    }

    public synchronized Async<List<String>> startNodesAsync(final int numNodes) {
        return startNodesAsync(numNodes, Settings.EMPTY, Version.CURRENT);
    }

    public synchronized Async<List<String>> startNodesAsync(final int numNodes, final Settings settings) {
        return startNodesAsync(numNodes, settings, Version.CURRENT);
    }

    public synchronized Async<List<String>> startNodesAsync(final int numNodes, final Settings settings, final Version version) {
        final List<Async<String>> asyncs = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            asyncs.add(startNodeAsync(settings, version));
        }
        return () -> {
            List<String> ids = new ArrayList<>();
            for (Async<String> async : asyncs) {
                ids.add(async.get());
            }
            return ids;
        };
    }

    public synchronized Async<List<String>> startNodesAsync(final Settings... settings) {
        List<Async<String>> asyncs = new ArrayList<>();
        for (Settings setting : settings) {
            asyncs.add(startNodeAsync(setting, Version.CURRENT));
        }
        return () -> {
            List<String> ids = new ArrayList<>();
            for (Async<String> async : asyncs) {
                ids.add(async.get());
            }
            return ids;
        };
    }

    private synchronized void publishNode(NodeAndClient nodeAndClient) {
        assert !nodeAndClient.node().isClosed();
        nodes.put(nodeAndClient.name, nodeAndClient);
        applyDisruptionSchemeToNode(nodeAndClient);
    }

    public void closeNonSharedNodes(boolean wipeData) throws IOException {
        reset(wipeData);
    }

    @Override
    public int numDataNodes() {
        return dataNodeAndClients().size();
    }

    @Override
    public int numDataAndMasterNodes() {
        return dataAndMasterNodes().size();
    }

    public void setDisruptionScheme(ServiceDisruptionScheme scheme) {
        clearDisruptionScheme();
        scheme.applyToCluster(this);
        activeDisruptionScheme = scheme;
    }

    public void clearDisruptionScheme() {
        clearDisruptionScheme(true);
    }

    public void clearDisruptionScheme(boolean ensureHealthyCluster) {
        if (activeDisruptionScheme != null) {
            TimeValue expectedHealingTime = activeDisruptionScheme.expectedTimeToHeal();
            logger.info("Clearing active scheme {}, expected healing time {}", activeDisruptionScheme, expectedHealingTime);
            if (ensureHealthyCluster) {
                activeDisruptionScheme.removeAndEnsureHealthy(this);
            } else {
                activeDisruptionScheme.removeFromCluster(this);
            }
        }
        activeDisruptionScheme = null;
    }

    private void applyDisruptionSchemeToNode(NodeAndClient nodeAndClient) {
        if (activeDisruptionScheme != null) {
            assert nodes.containsKey(nodeAndClient.name);
            activeDisruptionScheme.applyToNode(nodeAndClient.name, this);
        }
    }

    private void removeDisruptionSchemeFromNode(NodeAndClient nodeAndClient) {
        if (activeDisruptionScheme != null) {
            assert nodes.containsKey(nodeAndClient.name);
            activeDisruptionScheme.removeFromNode(nodeAndClient.name, this);
        }
    }

    private synchronized Collection<NodeAndClient> dataNodeAndClients() {
        return filterNodes(nodes, new DataNodePredicate());
    }

    private synchronized Collection<NodeAndClient> dataAndMasterNodes() {
        return filterNodes(nodes, new DataOrMasterNodePredicate());
    }

    private synchronized Collection<NodeAndClient> filterNodes(Map<String, InternalTestCluster.NodeAndClient> map, Predicate<NodeAndClient> predicate) {
        return map.values().stream().filter(predicate).collect(Collectors.toCollection(ArrayList::new));
    }

    private static final class DataNodePredicate implements Predicate<NodeAndClient> {

        @Override
        public boolean test(NodeAndClient nodeAndClient) {
            return DiscoveryNode.isDataNode(nodeAndClient.node.settings());
        }
    }

    private static final class DataOrMasterNodePredicate implements Predicate<NodeAndClient> {

        @Override
        public boolean test(NodeAndClient nodeAndClient) {
            return DiscoveryNode.isDataNode(nodeAndClient.node.settings()) || DiscoveryNode.isMasterNode(nodeAndClient.node.settings());
        }
    }

    private static final class MasterNodePredicate implements Predicate<NodeAndClient> {

        private final String masterNodeName;

        public MasterNodePredicate(String masterNodeName) {
            this.masterNodeName = masterNodeName;
        }

        @Override
        public boolean test(NodeAndClient nodeAndClient) {
            return masterNodeName.equals(nodeAndClient.name);
        }
    }

    private static final class NoDataNoMasterNodePredicate implements Predicate<NodeAndClient> {

        @Override
        public boolean test(NodeAndClient nodeAndClient) {
            return DiscoveryNode.isMasterNode(nodeAndClient.node.settings()) == false && DiscoveryNode.isDataNode(nodeAndClient.node.settings()) == false;
        }
    }

    private static final class EntryNodePredicate implements Predicate<Map.Entry<String, NodeAndClient>> {

        private final Predicate<NodeAndClient> delegateNodePredicate;

        EntryNodePredicate(Predicate<NodeAndClient> delegateNodePredicate) {
            this.delegateNodePredicate = delegateNodePredicate;
        }

        @Override
        public boolean test(Map.Entry<String, NodeAndClient> entry) {
            return delegateNodePredicate.test(entry.getValue());
        }
    }

    synchronized String routingKeyForShard(Index index, int shard, Random random) {
        assertThat(shard, greaterThanOrEqualTo(0));
        assertThat(shard, greaterThanOrEqualTo(0));
        for (NodeAndClient n : nodes.values()) {
            Node node = n.node;
            IndicesService indicesService = getInstanceFromNode(IndicesService.class, node);
            ClusterService clusterService = getInstanceFromNode(ClusterService.class, node);
            IndexService indexService = indicesService.indexService(index);
            if (indexService != null) {
                assertThat(indexService.getIndexSettings().getSettings().getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, -1), greaterThan(shard));
                OperationRouting operationRouting = clusterService.operationRouting();
                while (true) {
                    String routing = RandomStrings.randomAsciiOfLength(random, 10);
                    final int targetShard = operationRouting.indexShards(clusterService.state(), index.getName(), null, routing).shardId().getId();
                    if (shard == targetShard) {
                        return routing;
                    }
                }
            }
        }
        fail("Could not find a node that holds " + index);
        return null;
    }

    public synchronized Iterable<Client> getClients() {
        ensureOpen();
        return () -> {
            ensureOpen();
            final Iterator<NodeAndClient> iterator = nodes.values().iterator();
            return new Iterator<Client>() {

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Client next() {
                    return iterator.next().client(random);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("");
                }
            };
        };
    }

    public static Predicate<Settings> nameFilter(String... nodeName) {
        return new NodeNamePredicate(new HashSet<>(Arrays.asList(nodeName)));
    }

    private static final class NodeNamePredicate implements Predicate<Settings> {

        private final HashSet<String> nodeNames;

        public NodeNamePredicate(HashSet<String> nodeNames) {
            this.nodeNames = nodeNames;
        }

        @Override
        public boolean test(Settings settings) {
            return nodeNames.contains(settings.get("node.name"));
        }
    }

    public static class RestartCallback {

        public Settings onNodeStopped(String nodeName) throws Exception {
            return Settings.EMPTY;
        }

        public void doAfterNodes(int n, Client client) throws Exception {
        }

        public boolean clearData(String nodeName) {
            return false;
        }

        public boolean doRestart(String nodeName) {
            return true;
        }
    }

    public Settings getDefaultSettings() {
        return defaultSettings;
    }

    @Override
    public void ensureEstimatedStats() {
        if (size() > 0) {
            for (NodeAndClient nodeAndClient : nodes.values()) {
                final IndicesFieldDataCache fdCache = getInstanceFromNode(IndicesService.class, nodeAndClient.node).getIndicesFieldDataCache();
                fdCache.getCache().refresh();
                final String name = nodeAndClient.name;
                final CircuitBreakerService breakerService = getInstanceFromNode(CircuitBreakerService.class, nodeAndClient.node);
                CircuitBreaker fdBreaker = breakerService.getBreaker(CircuitBreaker.FIELDDATA);
                assertThat("Fielddata breaker not reset to 0 on node: " + name, fdBreaker.getUsed(), equalTo(0L));
                try {
                    assertBusy(new Runnable() {

                        @Override
                        public void run() {
                            CircuitBreaker reqBreaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
                            assertThat("Request breaker not reset to 0 on node: " + name, reqBreaker.getUsed(), equalTo(0L));
                        }
                    });
                } catch (Exception e) {
                    fail("Exception during check for request breaker reset to 0: " + e);
                }
                NodeService nodeService = getInstanceFromNode(NodeService.class, nodeAndClient.node);
                CommonStatsFlags flags = new CommonStatsFlags(Flag.FieldData, Flag.QueryCache, Flag.Segments);
                NodeStats stats = nodeService.stats(flags, false, false, false, false, false, false, false, false, false, false, false);
                assertThat("Fielddata size must be 0 on node: " + stats.getNode(), stats.getIndices().getFieldData().getMemorySizeInBytes(), equalTo(0L));
                assertThat("Query cache size must be 0 on node: " + stats.getNode(), stats.getIndices().getQueryCache().getMemorySizeInBytes(), equalTo(0L));
                assertThat("FixedBitSet cache size must be 0 on node: " + stats.getNode(), stats.getIndices().getSegments().getBitsetMemoryInBytes(), equalTo(0L));
            }
        }
    }

    @Override
    public void assertAfterTest() throws IOException {
        super.assertAfterTest();
        assertRequestsFinished();
        for (NodeAndClient nodeAndClient : nodes.values()) {
            NodeEnvironment env = nodeAndClient.node().getNodeEnvironment();
            Set<ShardId> shardIds = env.lockedShards();
            for (ShardId id : shardIds) {
                try {
                    env.shardLock(id, TimeUnit.SECONDS.toMillis(5)).close();
                } catch (ShardLockObtainFailedException ex) {
                    fail("Shard " + id + " is still locked after 5 sec waiting");
                }
            }
        }
    }

    private void assertRequestsFinished() {
        if (size() > 0) {
            for (NodeAndClient nodeAndClient : nodes.values()) {
                CircuitBreaker inFlightRequestsBreaker = getInstance(CircuitBreakerService.class, nodeAndClient.name).getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
                try {
                    assertBusy(() -> {
                        long bytesUsed = inFlightRequestsBreaker.getUsed();
                        assertThat("All incoming requests on node [" + nodeAndClient.name + "] should have finished. Expected 0 but got " + bytesUsed, bytesUsed, equalTo(0L));
                    });
                } catch (Exception e) {
                    logger.error("Could not assert finished requests within timeout", e);
                    fail("Could not assert finished requests within timeout on node [" + nodeAndClient.name + "]");
                }
            }
        }
    }

    public interface Async<T> {

        T get() throws ExecutionException, InterruptedException;
    }
}