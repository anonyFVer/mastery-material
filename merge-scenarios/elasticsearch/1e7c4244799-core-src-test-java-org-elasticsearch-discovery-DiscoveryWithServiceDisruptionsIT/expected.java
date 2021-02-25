package org.elasticsearch.discovery;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.ClusterStateStatus;
import org.elasticsearch.cluster.service.ClusterServiceState;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.FaultDetection;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.discovery.zen.PublishClusterStateAction;
import org.elasticsearch.discovery.zen.UnicastZenPing;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.ZenPing;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.store.IndicesStoreIntegrationIT;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.disruption.IntermittentLongGCDisruption;
import org.elasticsearch.test.disruption.LongGCDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.Bridge;
import org.elasticsearch.test.disruption.NetworkDisruption.DisruptedLinks;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkDelay;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkDisconnect;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkLinkDisruptionType;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkUnresponsive;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.disruption.SingleNodeDisruption;
import org.elasticsearch.test.disruption.SlowClusterStateProcessing;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
@TestLogging("_root:DEBUG,org.elasticsearch.cluster.service:TRACE")
public class DiscoveryWithServiceDisruptionsIT extends ESIntegTestCase {

    private static final TimeValue DISRUPTION_HEALING_OVERHEAD = TimeValue.timeValueSeconds(40);

    private ClusterDiscoveryConfiguration discoveryConfig;

    @Override
    protected boolean addMockZenPings() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return discoveryConfig.nodeSettings(nodeOrdinal);
    }

    @Before
    public void clearConfig() {
        discoveryConfig = null;
    }

    @Override
    protected int numberOfShards() {
        return 3;
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    private boolean disableBeforeIndexDeletion;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        disableBeforeIndexDeletion = false;
    }

    @Override
    public void setDisruptionScheme(ServiceDisruptionScheme scheme) {
        if (scheme instanceof NetworkDisruption && ((NetworkDisruption) scheme).getNetworkLinkDisruptionType() instanceof NetworkUnresponsive) {
            disableBeforeIndexDeletion = true;
        }
        super.setDisruptionScheme(scheme);
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        if (disableBeforeIndexDeletion == false) {
            super.beforeIndexDeletion();
        }
    }

    private List<String> startCluster(int numberOfNodes) throws ExecutionException, InterruptedException {
        return startCluster(numberOfNodes, -1);
    }

    private List<String> startCluster(int numberOfNodes, int minimumMasterNode) throws ExecutionException, InterruptedException {
        return startCluster(numberOfNodes, minimumMasterNode, null);
    }

    private List<String> startCluster(int numberOfNodes, int minimumMasterNode, @Nullable int[] unicastHostsOrdinals) throws ExecutionException, InterruptedException {
        configureCluster(numberOfNodes, unicastHostsOrdinals, minimumMasterNode);
        List<String> nodes = internalCluster().startNodesAsync(numberOfNodes).get();
        ensureStableCluster(numberOfNodes);
        ZenPing zenPing = internalCluster().getInstance(ZenPing.class);
        if (zenPing instanceof UnicastZenPing) {
            ((UnicastZenPing) zenPing).clearTemporalResponses();
        }
        return nodes;
    }

    static final Settings DEFAULT_SETTINGS = Settings.builder().put(FaultDetection.PING_TIMEOUT_SETTING.getKey(), "1s").put(FaultDetection.PING_RETRIES_SETTING.getKey(), "1").put("discovery.zen.join_timeout", "10s").put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "1s").put(TcpTransport.TCP_CONNECT_TIMEOUT.getKey(), "10s").build();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    private void configureCluster(int numberOfNodes, @Nullable int[] unicastHostsOrdinals, int minimumMasterNode) throws ExecutionException, InterruptedException {
        configureCluster(DEFAULT_SETTINGS, numberOfNodes, unicastHostsOrdinals, minimumMasterNode);
    }

    private void configureCluster(Settings settings, int numberOfNodes, @Nullable int[] unicastHostsOrdinals, int minimumMasterNode) throws ExecutionException, InterruptedException {
        if (minimumMasterNode < 0) {
            minimumMasterNode = numberOfNodes / 2 + 1;
        }
        logger.info("---> configured unicast");
        Settings nodeSettings = Settings.builder().put(settings).put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), numberOfNodes).put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), minimumMasterNode).build();
        if (discoveryConfig == null) {
            if (unicastHostsOrdinals == null) {
                discoveryConfig = new ClusterDiscoveryConfiguration.UnicastZen(numberOfNodes, nodeSettings);
            } else {
                discoveryConfig = new ClusterDiscoveryConfiguration.UnicastZen(numberOfNodes, nodeSettings, unicastHostsOrdinals);
            }
        }
    }

    public void testFailWithMinimumMasterNodesConfigured() throws Exception {
        List<String> nodes = startCluster(3);
        final String masterNode = internalCluster().getMasterName();
        logger.info("---> legit elected master node={}", masterNode);
        Set<String> nonMasters = new HashSet<>(nodes);
        nonMasters.remove(masterNode);
        final String unluckyNode = randomFrom(nonMasters.toArray(Strings.EMPTY_ARRAY));
        NetworkDisruption networkDisconnect = new NetworkDisruption(new TwoPartitions(masterNode, unluckyNode), new NetworkDisconnect());
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();
        ensureStableCluster(2, masterNode);
        assertNoMaster(unluckyNode);
        networkDisconnect.stopDisrupting();
        ensureStableCluster(3);
        assertMaster(masterNode, nodes);
    }

    public void testNodesFDAfterMasterReelection() throws Exception {
        startCluster(4);
        logger.info("--> stopping current master");
        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);
        logger.info("--> reducing min master nodes to 2");
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 2)).get());
        String master = internalCluster().getMasterName();
        String nonMaster = null;
        for (String node : internalCluster().getNodeNames()) {
            if (!node.equals(master)) {
                nonMaster = node;
            }
        }
        logger.info("--> isolating [{}]", nonMaster);
        TwoPartitions partitions = isolateNode(nonMaster);
        NetworkDisruption networkDisruption = addRandomDisruptionType(partitions);
        networkDisruption.startDisrupting();
        logger.info("--> waiting for master to remove it");
        ensureStableCluster(2, master);
    }

    public void testVerifyApiBlocksDuringPartition() throws Exception {
        startCluster(3);
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)));
        ensureGreen("test");
        TwoPartitions partitions = TwoPartitions.random(random(), internalCluster().getNodeNames());
        NetworkDisruption networkDisruption = addRandomDisruptionType(partitions);
        assertEquals(1, partitions.getMinoritySide().size());
        final String isolatedNode = partitions.getMinoritySide().iterator().next();
        assertEquals(2, partitions.getMajoritySide().size());
        final String nonIsolatedNode = partitions.getMajoritySide().iterator().next();
        networkDisruption.startDisrupting();
        logger.info("waiting for isolated node [{}] to have no master", isolatedNode);
        assertNoMaster(isolatedNode, DiscoverySettings.NO_MASTER_BLOCK_WRITES, TimeValue.timeValueSeconds(10));
        logger.info("wait until elected master has been removed and a new 2 node cluster was from (via [{}])", isolatedNode);
        ensureStableCluster(2, nonIsolatedNode);
        for (String node : partitions.getMajoritySide()) {
            ClusterState nodeState = getNodeClusterState(node);
            boolean success = true;
            if (nodeState.nodes().getMasterNode() == null) {
                success = false;
            }
            if (!nodeState.blocks().global().isEmpty()) {
                success = false;
            }
            if (!success) {
                fail("node [" + node + "] has no master or has blocks, despite of being on the right side of the partition. State dump:\n" + nodeState);
            }
        }
        networkDisruption.stopDisrupting();
        ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + networkDisruption.expectedTimeToHeal().millis()));
        logger.info("Verify no master block with {} set to {}", DiscoverySettings.NO_MASTER_BLOCK_SETTING.getKey(), "all");
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(DiscoverySettings.NO_MASTER_BLOCK_SETTING.getKey(), "all")).get();
        networkDisruption.startDisrupting();
        logger.info("waiting for isolated node [{}] to have no master", isolatedNode);
        assertNoMaster(isolatedNode, DiscoverySettings.NO_MASTER_BLOCK_ALL, TimeValue.timeValueSeconds(10));
        ensureStableCluster(2, nonIsolatedNode);
    }

    @TestLogging("_root:DEBUG,org.elasticsearch.cluster.service:TRACE,org.elasticsearch.gateway:TRACE,org.elasticsearch.indices.store:TRACE")
    public void testIsolateMasterAndVerifyClusterStateConsensus() throws Exception {
        final List<String> nodes = startCluster(3);
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1 + randomInt(2)).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomInt(2))));
        ensureGreen();
        String isolatedNode = internalCluster().getMasterName();
        TwoPartitions partitions = isolateNode(isolatedNode);
        NetworkDisruption networkDisruption = addRandomDisruptionType(partitions);
        networkDisruption.startDisrupting();
        String nonIsolatedNode = partitions.getMajoritySide().iterator().next();
        ensureStableCluster(2, nonIsolatedNode);
        assertNoMaster(isolatedNode, TimeValue.timeValueSeconds(40));
        networkDisruption.stopDisrupting();
        for (String node : nodes) {
            ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + networkDisruption.expectedTimeToHeal().millis()), true, node);
        }
        logger.info("issue a reroute");
        assertAcked(client().admin().cluster().prepareReroute());
        ensureGreen("test");
        ClusterState state = null;
        for (String node : nodes) {
            ClusterState nodeState = getNodeClusterState(node);
            if (state == null) {
                state = nodeState;
                continue;
            }
            try {
                assertEquals("unequal versions", state.version(), nodeState.version());
                assertEquals("unequal node count", state.nodes().getSize(), nodeState.nodes().getSize());
                assertEquals("different masters ", state.nodes().getMasterNodeId(), nodeState.nodes().getMasterNodeId());
                assertEquals("different meta data version", state.metaData().version(), nodeState.metaData().version());
                if (!state.routingTable().toString().equals(nodeState.routingTable().toString())) {
                    fail("different routing");
                }
            } catch (AssertionError t) {
                fail("failed comparing cluster state: " + t.getMessage() + "\n" + "--- cluster state of node [" + nodes.get(0) + "]: ---\n" + state + "\n--- cluster state [" + node + "]: ---\n" + nodeState);
            }
        }
    }

    @TestLogging("_root:DEBUG,org.elasticsearch.action.index:TRACE,org.elasticsearch.action.get:TRACE,discovery:TRACE,org.elasticsearch.cluster.service:TRACE," + "org.elasticsearch.indices.recovery:TRACE,org.elasticsearch.indices.cluster:TRACE")
    public void testAckedIndexing() throws Exception {
        final int seconds = !(TEST_NIGHTLY && rarely()) ? 1 : 5;
        final String timeout = seconds + "s";
        final List<String> nodes = startCluster(rarely() ? 5 : 3);
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1 + randomInt(2)).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomInt(2))));
        ensureGreen();
        ServiceDisruptionScheme disruptionScheme = addRandomDisruptionScheme();
        logger.info("disruption scheme [{}] added", disruptionScheme);
        final ConcurrentHashMap<String, String> ackedDocs = new ConcurrentHashMap<>();
        final AtomicBoolean stop = new AtomicBoolean(false);
        List<Thread> indexers = new ArrayList<>(nodes.size());
        List<Semaphore> semaphores = new ArrayList<>(nodes.size());
        final AtomicInteger idGenerator = new AtomicInteger(0);
        final AtomicReference<CountDownLatch> countDownLatchRef = new AtomicReference<>();
        final List<Exception> exceptedExceptions = Collections.synchronizedList(new ArrayList<Exception>());
        logger.info("starting indexers");
        try {
            for (final String node : nodes) {
                final Semaphore semaphore = new Semaphore(0);
                semaphores.add(semaphore);
                final Client client = client(node);
                final String name = "indexer_" + indexers.size();
                final int numPrimaries = getNumShards("test").numPrimaries;
                Thread thread = new Thread(() -> {
                    while (!stop.get()) {
                        String id = null;
                        try {
                            if (!semaphore.tryAcquire(10, TimeUnit.SECONDS)) {
                                continue;
                            }
                            logger.info("[{}] Acquired semaphore and it has {} permits left", name, semaphore.availablePermits());
                            try {
                                id = Integer.toString(idGenerator.incrementAndGet());
                                int shard = Math.floorMod(Murmur3HashFunction.hash(id), numPrimaries);
                                logger.trace("[{}] indexing id [{}] through node [{}] targeting shard [{}]", name, id, node, shard);
                                IndexResponse response = client.prepareIndex("test", "type", id).setSource("{}").setTimeout(timeout).get(timeout);
                                assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
                                ackedDocs.put(id, node);
                                logger.trace("[{}] indexed id [{}] through node [{}]", name, id, node);
                            } catch (ElasticsearchException e) {
                                exceptedExceptions.add(e);
                                final String docId = id;
                                logger.trace((Supplier<?>) () -> new ParameterizedMessage("[{}] failed id [{}] through node [{}]", name, docId, node), e);
                            } finally {
                                countDownLatchRef.get().countDown();
                                logger.trace("[{}] decreased counter : {}", name, countDownLatchRef.get().getCount());
                            }
                        } catch (InterruptedException e) {
                        } catch (AssertionError | Exception e) {
                            logger.info((Supplier<?>) () -> new ParameterizedMessage("unexpected exception in background thread of [{}]", node), e);
                        }
                    }
                });
                thread.setName(name);
                thread.start();
                indexers.add(thread);
            }
            int docsPerIndexer = randomInt(3);
            logger.info("indexing {} docs per indexer before partition", docsPerIndexer);
            countDownLatchRef.set(new CountDownLatch(docsPerIndexer * indexers.size()));
            for (Semaphore semaphore : semaphores) {
                semaphore.release(docsPerIndexer);
            }
            assertTrue(countDownLatchRef.get().await(1, TimeUnit.MINUTES));
            for (int iter = 1 + randomInt(2); iter > 0; iter--) {
                logger.info("starting disruptions & indexing (iteration [{}])", iter);
                disruptionScheme.startDisrupting();
                docsPerIndexer = 1 + randomInt(5);
                logger.info("indexing {} docs per indexer during partition", docsPerIndexer);
                countDownLatchRef.set(new CountDownLatch(docsPerIndexer * indexers.size()));
                Collections.shuffle(semaphores, random());
                for (Semaphore semaphore : semaphores) {
                    assertThat(semaphore.availablePermits(), equalTo(0));
                    semaphore.release(docsPerIndexer);
                }
                logger.info("waiting for indexing requests to complete");
                assertTrue(countDownLatchRef.get().await(docsPerIndexer * seconds * 1000 + 2000, TimeUnit.MILLISECONDS));
                logger.info("stopping disruption");
                disruptionScheme.stopDisrupting();
                for (String node : internalCluster().getNodeNames()) {
                    ensureStableCluster(nodes.size(), TimeValue.timeValueMillis(disruptionScheme.expectedTimeToHeal().millis() + DISRUPTION_HEALING_OVERHEAD.millis()), true, node);
                }
                ensureGreen("test");
                logger.info("validating successful docs");
                for (String node : nodes) {
                    try {
                        logger.debug("validating through node [{}] ([{}] acked docs)", node, ackedDocs.size());
                        for (String id : ackedDocs.keySet()) {
                            assertTrue("doc [" + id + "] indexed via node [" + ackedDocs.get(id) + "] not found", client(node).prepareGet("test", "type", id).setPreference("_local").get().isExists());
                        }
                    } catch (AssertionError e) {
                        throw new AssertionError(e.getMessage() + " (checked via node [" + node + "]", e);
                    }
                }
                logger.info("done validating (iteration [{}])", iter);
            }
        } finally {
            if (exceptedExceptions.size() > 0) {
                StringBuilder sb = new StringBuilder();
                for (Exception e : exceptedExceptions) {
                    sb.append("\n").append(e.getMessage());
                }
                logger.debug("Indexing exceptions during disruption: {}", sb);
            }
            logger.info("shutting down indexers");
            stop.set(true);
            for (Thread indexer : indexers) {
                indexer.interrupt();
                indexer.join(60000);
            }
        }
    }

    public void testMasterNodeGCs() throws Exception {
        List<String> nodes = startCluster(3, -1);
        String oldMasterNode = internalCluster().getMasterName();
        SingleNodeDisruption masterNodeDisruption = new IntermittentLongGCDisruption(oldMasterNode, random(), 100, 200, 30000, 60000);
        internalCluster().setDisruptionScheme(masterNodeDisruption);
        masterNodeDisruption.startDisrupting();
        Set<String> oldNonMasterNodesSet = new HashSet<>(nodes);
        oldNonMasterNodesSet.remove(oldMasterNode);
        List<String> oldNonMasterNodes = new ArrayList<>(oldNonMasterNodesSet);
        logger.info("waiting for nodes to de-elect master [{}]", oldMasterNode);
        for (String node : oldNonMasterNodesSet) {
            assertDifferentMaster(node, oldMasterNode);
        }
        logger.info("waiting for nodes to elect a new master");
        ensureStableCluster(2, oldNonMasterNodes.get(0));
        logger.info("waiting for any pinging to stop");
        assertDiscoveryCompleted(oldNonMasterNodes);
        masterNodeDisruption.stopDisrupting();
        ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + masterNodeDisruption.expectedTimeToHeal().millis()), false, oldNonMasterNodes.get(0));
        String newMaster = internalCluster().getMasterName();
        assertThat(newMaster, not(equalTo(oldMasterNode)));
        assertMaster(newMaster, nodes);
    }

    @TestLogging("_root:DEBUG,org.elasticsearch.cluster.service:TRACE,org.elasticsearch.test.disruption:TRACE")
    public void testStaleMasterNotHijackingMajority() throws Exception {
        final List<String> nodes = startCluster(3, 2);
        final String oldMasterNode = internalCluster().getMasterName();
        for (String node : nodes) {
            ensureStableCluster(3, node);
        }
        assertMaster(oldMasterNode, nodes);
        SingleNodeDisruption masterNodeDisruption = new LongGCDisruption(random(), oldMasterNode);
        final List<String> majoritySide = new ArrayList<>(nodes);
        majoritySide.remove(oldMasterNode);
        final Map<String, List<Tuple<String, String>>> masters = Collections.synchronizedMap(new HashMap<String, List<Tuple<String, String>>>());
        for (final String node : majoritySide) {
            masters.put(node, new ArrayList<Tuple<String, String>>());
            internalCluster().getInstance(ClusterService.class, node).add(new ClusterStateListener() {

                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
                    DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
                    if (!Objects.equals(previousMaster, currentMaster)) {
                        logger.info("node {} received new cluster state: {} \n and had previous cluster state: {}", node, event.state(), event.previousState());
                        String previousMasterNodeName = previousMaster != null ? previousMaster.getName() : null;
                        String currentMasterNodeName = currentMaster != null ? currentMaster.getName() : null;
                        masters.get(node).add(new Tuple<>(previousMasterNodeName, currentMasterNodeName));
                    }
                }
            });
        }
        final CountDownLatch oldMasterNodeSteppedDown = new CountDownLatch(1);
        internalCluster().getInstance(ClusterService.class, oldMasterNode).add(new ClusterStateListener() {

            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.state().nodes().getMasterNodeId() == null) {
                    oldMasterNodeSteppedDown.countDown();
                }
            }
        });
        internalCluster().setDisruptionScheme(masterNodeDisruption);
        logger.info("freezing node [{}]", oldMasterNode);
        masterNodeDisruption.startDisrupting();
        assertDifferentMaster(majoritySide.get(0), oldMasterNode);
        assertDifferentMaster(majoritySide.get(1), oldMasterNode);
        boolean failed = true;
        try {
            assertDiscoveryCompleted(majoritySide);
            failed = false;
        } finally {
            if (failed) {
                logger.error("discovery failed to complete, probably caused by a blocked thread: {}", new HotThreads().busiestThreads(Integer.MAX_VALUE).ignoreIdleThreads(false).detect());
            }
        }
        internalCluster().getInstance(ClusterService.class, oldMasterNode).submitStateUpdateTask("sneaky-update", new ClusterStateUpdateTask(Priority.IMMEDIATE) {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ClusterState.builder(currentState).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failure [{}]", source), e);
            }
        });
        final String newMasterNode = internalCluster().getMasterName(majoritySide.get(0));
        logger.info("new detected master node [{}]", newMasterNode);
        logger.info("Unfreeze node [{}]", oldMasterNode);
        masterNodeDisruption.stopDisrupting();
        oldMasterNodeSteppedDown.await(30, TimeUnit.SECONDS);
        assertDiscoveryCompleted(nodes);
        assertMaster(newMasterNode, nodes);
        assertThat(masters.size(), equalTo(2));
        for (Map.Entry<String, List<Tuple<String, String>>> entry : masters.entrySet()) {
            String nodeName = entry.getKey();
            List<Tuple<String, String>> recordedMasterTransition = entry.getValue();
            assertThat("[" + nodeName + "] Each node should only record two master node transitions", recordedMasterTransition.size(), equalTo(2));
            assertThat("[" + nodeName + "] First transition's previous master should be [null]", recordedMasterTransition.get(0).v1(), equalTo(oldMasterNode));
            assertThat("[" + nodeName + "] First transition's current master should be [" + newMasterNode + "]", recordedMasterTransition.get(0).v2(), nullValue());
            assertThat("[" + nodeName + "] Second transition's previous master should be [null]", recordedMasterTransition.get(1).v1(), nullValue());
            assertThat("[" + nodeName + "] Second transition's current master should be [" + newMasterNode + "]", recordedMasterTransition.get(1).v2(), equalTo(newMasterNode));
        }
    }

    public void testRejoinDocumentExistsInAllShardCopies() throws Exception {
        List<String> nodes = startCluster(3);
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)).get());
        ensureGreen("test");
        nodes = new ArrayList<>(nodes);
        Collections.shuffle(nodes, random());
        String isolatedNode = nodes.get(0);
        String notIsolatedNode = nodes.get(1);
        TwoPartitions partitions = isolateNode(isolatedNode);
        NetworkDisruption scheme = addRandomDisruptionType(partitions);
        scheme.startDisrupting();
        ensureStableCluster(2, notIsolatedNode);
        assertFalse(client(notIsolatedNode).admin().cluster().prepareHealth("test").setWaitForYellowStatus().get().isTimedOut());
        IndexResponse indexResponse = internalCluster().client(notIsolatedNode).prepareIndex("test", "type").setSource("field", "value").get();
        assertThat(indexResponse.getVersion(), equalTo(1L));
        logger.info("Verifying if document exists via node[{}]", notIsolatedNode);
        GetResponse getResponse = internalCluster().client(notIsolatedNode).prepareGet("test", "type", indexResponse.getId()).setPreference("_local").get();
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getVersion(), equalTo(1L));
        assertThat(getResponse.getId(), equalTo(indexResponse.getId()));
        scheme.stopDisrupting();
        ensureStableCluster(3);
        ensureGreen("test");
        for (String node : nodes) {
            logger.info("Verifying if document exists after isolating node[{}] via node[{}]", isolatedNode, node);
            getResponse = internalCluster().client(node).prepareGet("test", "type", indexResponse.getId()).setPreference("_local").get();
            assertThat(getResponse.isExists(), is(true));
            assertThat(getResponse.getVersion(), equalTo(1L));
            assertThat(getResponse.getId(), equalTo(indexResponse.getId()));
        }
    }

    public void testUnicastSinglePingResponseContainsMaster() throws Exception {
        List<String> nodes = startCluster(4, -1, new int[] { 0 });
        final String masterNode = internalCluster().getMasterName();
        logger.info("---> legit elected master node={}", masterNode);
        List<String> otherNodes = new ArrayList<>(nodes);
        otherNodes.remove(masterNode);
        otherNodes.remove(nodes.get(0));
        final String isolatedNode = otherNodes.get(0);
        ZenPing zenPing = internalCluster().getInstance(ZenPing.class);
        if (zenPing instanceof UnicastZenPing) {
            ((UnicastZenPing) zenPing).clearTemporalResponses();
        }
        NetworkDisruption networkDisconnect = new NetworkDisruption(new TwoPartitions(masterNode, isolatedNode), new NetworkDisconnect());
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();
        ensureStableCluster(3, masterNode);
        assertNoMaster(isolatedNode);
        networkDisconnect.stopDisrupting();
        ensureStableCluster(4);
        assertMaster(masterNode, nodes);
    }

    public void testIsolatedUnicastNodes() throws Exception {
        List<String> nodes = startCluster(4, -1, new int[] { 0 });
        final String unicastTarget = nodes.get(0);
        Set<String> unicastTargetSide = new HashSet<>();
        unicastTargetSide.add(unicastTarget);
        Set<String> restOfClusterSide = new HashSet<>();
        restOfClusterSide.addAll(nodes);
        restOfClusterSide.remove(unicastTarget);
        ZenPing zenPing = internalCluster().getInstance(ZenPing.class);
        if (zenPing instanceof UnicastZenPing) {
            ((UnicastZenPing) zenPing).clearTemporalResponses();
        }
        NetworkDisruption networkDisconnect = new NetworkDisruption(new TwoPartitions(unicastTargetSide, restOfClusterSide), new NetworkDisconnect());
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();
        ensureStableCluster(3, nodes.get(1));
        assertNoMaster(unicastTarget);
        networkDisconnect.stopDisrupting();
        ensureStableCluster(4);
    }

    public void testClusterJoinDespiteOfPublishingIssues() throws Exception {
        List<String> nodes = startCluster(2, 1);
        String masterNode = internalCluster().getMasterName();
        String nonMasterNode;
        if (masterNode.equals(nodes.get(0))) {
            nonMasterNode = nodes.get(1);
        } else {
            nonMasterNode = nodes.get(0);
        }
        DiscoveryNodes discoveryNodes = internalCluster().getInstance(ClusterService.class, nonMasterNode).state().nodes();
        TransportService masterTranspotService = internalCluster().getInstance(TransportService.class, discoveryNodes.getMasterNode().getName());
        logger.info("blocking requests from non master [{}] to master [{}]", nonMasterNode, masterNode);
        MockTransportService nonMasterTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nonMasterNode);
        nonMasterTransportService.addFailToSendNoConnectRule(masterTranspotService);
        assertNoMaster(nonMasterNode);
        logger.info("blocking cluster state publishing from master [{}] to non master [{}]", masterNode, nonMasterNode);
        MockTransportService masterTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, masterNode);
        TransportService localTransportService = internalCluster().getInstance(TransportService.class, discoveryNodes.getLocalNode().getName());
        if (randomBoolean()) {
            masterTransportService.addFailToSendNoConnectRule(localTransportService, PublishClusterStateAction.SEND_ACTION_NAME);
        } else {
            masterTransportService.addFailToSendNoConnectRule(localTransportService, PublishClusterStateAction.COMMIT_ACTION_NAME);
        }
        logger.info("allowing requests from non master [{}] to master [{}], waiting for two join request", nonMasterNode, masterNode);
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        nonMasterTransportService.addDelegate(masterTranspotService, new MockTransportService.DelegateTransport(nonMasterTransportService.original()) {

            @Override
            public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                if (action.equals(MembershipAction.DISCOVERY_JOIN_ACTION_NAME)) {
                    countDownLatch.countDown();
                }
                super.sendRequest(node, requestId, action, request, options);
            }
        });
        countDownLatch.await();
        logger.info("waiting for cluster to reform");
        masterTransportService.clearRule(localTransportService);
        nonMasterTransportService.clearRule(localTransportService);
        ensureStableCluster(2);
        internalCluster().stopRandomNonMasterNode();
    }

    public void testSendingShardFailure() throws Exception {
        List<String> nodes = startCluster(3, 2);
        String masterNode = internalCluster().getMasterName();
        List<String> nonMasterNodes = nodes.stream().filter(node -> !node.equals(masterNode)).collect(Collectors.toList());
        String nonMasterNode = randomFrom(nonMasterNodes);
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)));
        ensureGreen();
        String nonMasterNodeId = internalCluster().clusterService(nonMasterNode).localNode().getId();
        ShardRouting failedShard = randomFrom(clusterService().state().getRoutingNodes().node(nonMasterNodeId).shardsWithState(ShardRoutingState.STARTED));
        ShardStateAction service = internalCluster().getInstance(ShardStateAction.class, nonMasterNode);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean();
        String isolatedNode = randomBoolean() ? masterNode : nonMasterNode;
        TwoPartitions partitions = isolateNode(isolatedNode);
        NetworkLinkDisruptionType disruptionType = new NetworkDisconnect();
        NetworkDisruption networkDisruption = new NetworkDisruption(partitions, disruptionType);
        setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        service.localShardFailed(failedShard, "simulated", new CorruptIndexException("simulated", (String) null), new ShardStateAction.Listener() {

            @Override
            public void onSuccess() {
                success.set(true);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                success.set(false);
                latch.countDown();
                assert false;
            }
        });
        if (isolatedNode.equals(nonMasterNode)) {
            assertNoMaster(nonMasterNode);
        } else {
            ensureStableCluster(2, nonMasterNode);
        }
        networkDisruption.removeAndEnsureHealthy(internalCluster());
        ensureStableCluster(3);
        latch.await();
        assertTrue(success.get());
        List<ShardRouting> shards = clusterService().state().getRoutingTable().allShards("test");
        for (ShardRouting shard : shards) {
            assertThat(shard.allocationId(), not(equalTo(failedShard.allocationId())));
        }
    }

    public void testClusterFormingWithASlowNode() throws Exception {
        configureCluster(3, null, 2);
        SlowClusterStateProcessing disruption = new SlowClusterStateProcessing(random(), 0, 0, 1000, 2000);
        internalCluster().startNodesAsync(3, Settings.builder().put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "1ms").put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "3s").build()).get();
        logger.info("applying disruption while cluster is forming ...");
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();
        ensureStableCluster(3);
    }

    public void testNodeNotReachableFromMaster() throws Exception {
        startCluster(3);
        String masterNode = internalCluster().getMasterName();
        String nonMasterNode = null;
        while (nonMasterNode == null) {
            nonMasterNode = randomFrom(internalCluster().getNodeNames());
            if (nonMasterNode.equals(masterNode)) {
                nonMasterNode = null;
            }
        }
        logger.info("blocking request from master [{}] to [{}]", masterNode, nonMasterNode);
        MockTransportService masterTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, masterNode);
        if (randomBoolean()) {
            masterTransportService.addUnresponsiveRule(internalCluster().getInstance(TransportService.class, nonMasterNode));
        } else {
            masterTransportService.addFailToSendNoConnectRule(internalCluster().getInstance(TransportService.class, nonMasterNode));
        }
        logger.info("waiting for [{}] to be removed from cluster", nonMasterNode);
        ensureStableCluster(2, masterNode);
        logger.info("waiting for [{}] to have no master", nonMasterNode);
        assertNoMaster(nonMasterNode);
        logger.info("healing partition and checking cluster reforms");
        masterTransportService.clearAllRules();
        ensureStableCluster(3);
    }

    public void testSearchWithRelocationAndSlowClusterStateProcessing() throws Exception {
        configureCluster(Settings.EMPTY, 3, null, 1);
        InternalTestCluster.Async<String> masterNodeFuture = internalCluster().startMasterOnlyNodeAsync();
        InternalTestCluster.Async<String> node_1Future = internalCluster().startDataOnlyNodeAsync();
        final String node_1 = node_1Future.get();
        final String masterNode = masterNodeFuture.get();
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen("test");
        InternalTestCluster.Async<String> node_2Future = internalCluster().startDataOnlyNodeAsync();
        final String node_2 = node_2Future.get();
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            indexRequestBuilderList.add(client().prepareIndex().setIndex("test").setType("doc").setSource("{\"int_field\":1}"));
        }
        indexRandom(true, indexRequestBuilderList);
        SingleNodeDisruption disruption = new BlockClusterStateProcessing(node_2, random());
        internalCluster().setDisruptionScheme(disruption);
        MockTransportService transportServiceNode2 = (MockTransportService) internalCluster().getInstance(TransportService.class, node_2);
        CountDownLatch beginRelocationLatch = new CountDownLatch(1);
        CountDownLatch endRelocationLatch = new CountDownLatch(1);
        transportServiceNode2.addTracer(new IndicesStoreIntegrationIT.ReclocationStartEndTracer(logger, beginRelocationLatch, endRelocationLatch));
        internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, node_1, node_2)).get();
        beginRelocationLatch.await();
        disruption.startDisrupting();
        endRelocationLatch.await();
        assertThat(client().prepareSearch().setSize(0).get().getHits().totalHits(), equalTo(100L));
    }

    public void testIndexImportedFromDataOnlyNodesIfMasterLostDataFolder() throws Exception {
        configureCluster(2, null, 1);
        String masterNode = internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNode(Settings.EMPTY);
        ensureStableCluster(2);
        assertAcked(prepareCreate("index").setSettings(Settings.builder().put("index.number_of_replicas", 0)));
        index("index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        ensureGreen();
        internalCluster().restartNode(masterNode, new InternalTestCluster.RestartCallback() {

            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });
        ensureGreen("index");
        assertTrue(client().prepareGet("index", "doc", "1").get().isExists());
    }

    public void testIndicesDeleted() throws Exception {
        final Settings settings = Settings.builder().put(DEFAULT_SETTINGS).put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "0s").put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), "30s").build();
        final String idxName = "test";
        configureCluster(settings, 3, null, 2);
        InternalTestCluster.Async<List<String>> masterNodes = internalCluster().startMasterOnlyNodesAsync(2);
        InternalTestCluster.Async<String> dataNode = internalCluster().startDataOnlyNodeAsync();
        dataNode.get();
        final List<String> allMasterEligibleNodes = masterNodes.get();
        ensureStableCluster(3);
        assertAcked(prepareCreate("test"));
        final String masterNode1 = internalCluster().getMasterName();
        NetworkDisruption networkDisruption = new NetworkDisruption(new TwoPartitions(masterNode1, dataNode.get()), new NetworkUnresponsive());
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        internalCluster().client(masterNode1).admin().indices().prepareDelete(idxName).setTimeout("0s").get();
        assertBusy(() -> {
            for (String masterNode : allMasterEligibleNodes) {
                final ClusterServiceState masterState = internalCluster().clusterService(masterNode).clusterServiceState();
                assertTrue("index not deleted on " + masterNode, masterState.getClusterState().metaData().hasIndex(idxName) == false && masterState.getClusterStateStatus() == ClusterStateStatus.APPLIED);
            }
        });
        internalCluster().restartNode(masterNode1, InternalTestCluster.EMPTY_CALLBACK);
        ensureYellow();
        assertFalse(client().admin().indices().prepareExists(idxName).get().isExists());
    }

    public void testElectMasterWithLatestVersion() throws Exception {
        configureCluster(3, null, 2);
        final Set<String> nodes = new HashSet<>(internalCluster().startNodesAsync(3).get());
        ensureStableCluster(3);
        ServiceDisruptionScheme isolateAllNodes = new NetworkDisruption(new NetworkDisruption.IsolateAllNodes(nodes), new NetworkDisconnect());
        internalCluster().setDisruptionScheme(isolateAllNodes);
        logger.info("--> forcing a complete election to make sure \"preferred\" master is elected");
        isolateAllNodes.startDisrupting();
        for (String node : nodes) {
            assertNoMaster(node);
        }
        isolateAllNodes.stopDisrupting();
        ensureStableCluster(3);
        final String preferredMasterName = internalCluster().getMasterName();
        final DiscoveryNode preferredMaster = internalCluster().clusterService(preferredMasterName).localNode();
        for (String node : nodes) {
            DiscoveryNode discoveryNode = internalCluster().clusterService(node).localNode();
            assertThat(discoveryNode.getId(), greaterThanOrEqualTo(preferredMaster.getId()));
        }
        logger.info("--> preferred master is {}", preferredMaster);
        final Set<String> nonPreferredNodes = new HashSet<>(nodes);
        nonPreferredNodes.remove(preferredMasterName);
        final ServiceDisruptionScheme isolatePreferredMaster = new NetworkDisruption(new NetworkDisruption.TwoPartitions(Collections.singleton(preferredMasterName), nonPreferredNodes), new NetworkDisconnect());
        internalCluster().setDisruptionScheme(isolatePreferredMaster);
        isolatePreferredMaster.startDisrupting();
        assertAcked(client(randomFrom(nonPreferredNodes)).admin().indices().prepareCreate("test").setSettings(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1, INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0));
        internalCluster().clearDisruptionScheme(false);
        internalCluster().setDisruptionScheme(isolateAllNodes);
        logger.info("--> forcing a complete election again");
        isolateAllNodes.startDisrupting();
        for (String node : nodes) {
            assertNoMaster(node);
        }
        isolateAllNodes.stopDisrupting();
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        if (state.metaData().hasIndex("test") == false) {
            fail("index 'test' was lost. current cluster state: " + state);
        }
    }

    protected NetworkDisruption addRandomDisruptionType(TwoPartitions partitions) {
        final NetworkLinkDisruptionType disruptionType;
        if (randomBoolean()) {
            disruptionType = new NetworkUnresponsive();
        } else {
            disruptionType = new NetworkDisconnect();
        }
        NetworkDisruption partition = new NetworkDisruption(partitions, disruptionType);
        setDisruptionScheme(partition);
        return partition;
    }

    protected TwoPartitions isolateNode(String isolatedNode) {
        Set<String> side1 = new HashSet<>();
        Set<String> side2 = new HashSet<>(Arrays.asList(internalCluster().getNodeNames()));
        side1.add(isolatedNode);
        side2.remove(isolatedNode);
        return new TwoPartitions(side1, side2);
    }

    private ServiceDisruptionScheme addRandomDisruptionScheme() {
        final DisruptedLinks disruptedLinks;
        if (randomBoolean()) {
            disruptedLinks = TwoPartitions.random(random(), internalCluster().getNodeNames());
        } else {
            disruptedLinks = Bridge.random(random(), internalCluster().getNodeNames());
        }
        final NetworkLinkDisruptionType disruptionType;
        switch(randomInt(2)) {
            case 0:
                disruptionType = new NetworkUnresponsive();
                break;
            case 1:
                disruptionType = new NetworkDisconnect();
                break;
            case 2:
                disruptionType = NetworkDelay.random(random());
                break;
            default:
                throw new IllegalArgumentException();
        }
        final ServiceDisruptionScheme scheme;
        if (rarely()) {
            scheme = new SlowClusterStateProcessing(random());
        } else {
            scheme = new NetworkDisruption(disruptedLinks, disruptionType);
        }
        setDisruptionScheme(scheme);
        return scheme;
    }

    private ClusterState getNodeClusterState(String node) {
        return client(node).admin().cluster().prepareState().setLocal(true).get().getState();
    }

    private void assertNoMaster(final String node) throws Exception {
        assertNoMaster(node, null, TimeValue.timeValueSeconds(10));
    }

    private void assertNoMaster(final String node, TimeValue maxWaitTime) throws Exception {
        assertNoMaster(node, null, maxWaitTime);
    }

    private void assertNoMaster(final String node, @Nullable final ClusterBlock expectedBlocks, TimeValue maxWaitTime) throws Exception {
        assertBusy(new Runnable() {

            @Override
            public void run() {
                ClusterState state = getNodeClusterState(node);
                assertNull("node [" + node + "] still has [" + state.nodes().getMasterNode() + "] as master", state.nodes().getMasterNode());
                if (expectedBlocks != null) {
                    for (ClusterBlockLevel level : expectedBlocks.levels()) {
                        assertTrue("node [" + node + "] does have level [" + level + "] in it's blocks", state.getBlocks().hasGlobalBlock(level));
                    }
                }
            }
        }, maxWaitTime.getMillis(), TimeUnit.MILLISECONDS);
    }

    private void assertDifferentMaster(final String node, final String oldMasterNode) throws Exception {
        assertBusy(new Runnable() {

            @Override
            public void run() {
                ClusterState state = getNodeClusterState(node);
                String masterNode = null;
                if (state.nodes().getMasterNode() != null) {
                    masterNode = state.nodes().getMasterNode().getName();
                }
                logger.trace("[{}] master is [{}]", node, state.nodes().getMasterNode());
                assertThat("node [" + node + "] still has [" + masterNode + "] as master", oldMasterNode, not(equalTo(masterNode)));
            }
        }, 10, TimeUnit.SECONDS);
    }

    private void assertMaster(String masterNode, List<String> nodes) throws Exception {
        assertBusy(() -> {
            for (String node : nodes) {
                ClusterState state = getNodeClusterState(node);
                String failMsgSuffix = "cluster_state:\n" + state;
                assertThat("wrong node count on [" + node + "]. " + failMsgSuffix, state.nodes().getSize(), equalTo(nodes.size()));
                String otherMasterNodeName = state.nodes().getMasterNode() != null ? state.nodes().getMasterNode().getName() : null;
                assertThat("wrong master on node [" + node + "]. " + failMsgSuffix, otherMasterNodeName, equalTo(masterNode));
            }
        });
    }

    private void assertDiscoveryCompleted(List<String> nodes) throws InterruptedException {
        for (final String node : nodes) {
            assertTrue("node [" + node + "] is still joining master", awaitBusy(() -> !((ZenDiscovery) internalCluster().getInstance(Discovery.class, node)).joiningCluster(), 30, TimeUnit.SECONDS));
        }
    }
}