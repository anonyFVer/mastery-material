package org.elasticsearch.indices.store;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.disruption.SingleNodeDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static java.lang.Thread.sleep;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndicesStoreIntegrationIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(Environment.PATH_DATA_SETTING.getKey(), "").put(IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT.getKey(), new TimeValue(30, TimeUnit.SECONDS)).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    @Override
    protected void ensureClusterStateConsistency() throws IOException {
    }

    public void testIndexCleanup() throws Exception {
        final String masterNode = internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false));
        final String node_1 = internalCluster().startNode(Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false));
        final String node_2 = internalCluster().startNode(Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false));
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)));
        ensureGreen("test");
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        Index index = state.metaData().index("test").getIndex();
        logger.info("--> making sure that shard and its replica are allocated on node_1 and node_2");
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_1, index)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, index)), equalTo(true));
        logger.info("--> starting node server3");
        final String node_3 = internalCluster().startNode(Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false));
        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForNodes("4").setWaitForRelocatingShards(0).get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_1, index)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, index)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_3, index, 0)), equalTo(false));
        assertThat(Files.exists(indexDirectory(node_3, index)), equalTo(false));
        logger.info("--> move shard from node_1 to node_3, and wait for relocation to finish");
        if (randomBoolean()) {
            SingleNodeDisruption disruption = new BlockClusterStateProcessing(node_3, random());
            internalCluster().setDisruptionScheme(disruption);
            MockTransportService transportServiceNode3 = (MockTransportService) internalCluster().getInstance(TransportService.class, node_3);
            CountDownLatch beginRelocationLatch = new CountDownLatch(1);
            CountDownLatch endRelocationLatch = new CountDownLatch(1);
            transportServiceNode3.addTracer(new ReclocationStartEndTracer(logger, beginRelocationLatch, endRelocationLatch));
            internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, node_1, node_3)).get();
            beginRelocationLatch.await();
            disruption.startDisrupting();
            endRelocationLatch.await();
            sleep(50);
            disruption.stopDisrupting();
        } else {
            internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, node_1, node_3)).get();
        }
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(waitForShardDeletion(node_1, index, 0), equalTo(false));
        assertThat(waitForIndexDeletion(node_1, index), equalTo(false));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_2, index)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_3, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_3, index)), equalTo(true));
    }

    public void testShardCleanupIfShardDeletionAfterRelocationFailedAndIndexDeleted() throws Exception {
        final String node_1 = internalCluster().startNode();
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen("test");
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        Index index = state.metaData().index("test").getIndex();
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_1, index)), equalTo(true));
        final String node_2 = internalCluster().startDataOnlyNode(Settings.builder().build());
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("2").get().isTimedOut());
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(indexDirectory(node_1, index)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(false));
        assertThat(Files.exists(indexDirectory(node_2, index)), equalTo(false));
        MockTransportService transportServiceNode_1 = (MockTransportService) internalCluster().getInstance(TransportService.class, node_1);
        TransportService transportServiceNode_2 = internalCluster().getInstance(TransportService.class, node_2);
        final CountDownLatch shardActiveRequestSent = new CountDownLatch(1);
        transportServiceNode_1.addDelegate(transportServiceNode_2, new MockTransportService.DelegateTransport(transportServiceNode_1.original()) {

            @Override
            public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                if (action.equals("internal:index/shard/exists") && shardActiveRequestSent.getCount() > 0) {
                    shardActiveRequestSent.countDown();
                    logger.info("prevent shard active request from being sent");
                    throw new ConnectTransportException(node, "DISCONNECT: simulated");
                }
                super.sendRequest(node, requestId, action, request, options);
            }
        });
        logger.info("--> move shard from {} to {}, and wait for relocation to finish", node_1, node_2);
        internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, node_1, node_2)).get();
        shardActiveRequestSent.await();
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        logClusterState();
        client().admin().indices().prepareDelete("test").get();
        assertThat(waitForShardDeletion(node_1, index, 0), equalTo(false));
        assertThat(waitForIndexDeletion(node_1, index), equalTo(false));
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(false));
        assertThat(Files.exists(indexDirectory(node_1, index)), equalTo(false));
        assertThat(waitForShardDeletion(node_2, index, 0), equalTo(false));
        assertThat(waitForIndexDeletion(node_2, index), equalTo(false));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(false));
        assertThat(Files.exists(indexDirectory(node_2, index)), equalTo(false));
    }

    public void testShardsCleanup() throws Exception {
        final String node_1 = internalCluster().startNode();
        final String node_2 = internalCluster().startNode();
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)));
        ensureGreen("test");
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        Index index = state.metaData().index("test").getIndex();
        logger.info("--> making sure that shard and its replica are allocated on node_1 and node_2");
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_2, index, 0)), equalTo(true));
        logger.info("--> starting node server3");
        String node_3 = internalCluster().startNode();
        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForNodes("3").setWaitForRelocatingShards(0).get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        logger.info("--> making sure that shard is not allocated on server3");
        assertThat(waitForShardDeletion(node_3, index, 0), equalTo(false));
        Path server2Shard = shardDirectory(node_2, index, 0);
        logger.info("--> stopping node {}", node_2);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node_2));
        logger.info("--> running cluster_health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").setWaitForRelocatingShards(0).get();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        logger.info("--> done cluster_health, status {}", clusterHealth.getStatus());
        assertThat(Files.exists(server2Shard), equalTo(true));
        logger.info("--> making sure that shard and its replica exist on server1, server2 and server3");
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(server2Shard), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_3, index, 0)), equalTo(true));
        logger.info("--> starting node node_4");
        final String node_4 = internalCluster().startNode();
        logger.info("--> running cluster_health");
        ensureGreen();
        logger.info("--> making sure that shard and its replica are allocated on server1 and server3 but not on server2");
        assertThat(Files.exists(shardDirectory(node_1, index, 0)), equalTo(true));
        assertThat(Files.exists(shardDirectory(node_3, index, 0)), equalTo(true));
        assertThat(waitForShardDeletion(node_4, index, 0), equalTo(false));
    }

    public void testShardActiveElsewhereDoesNotDeleteAnother() throws Exception {
        InternalTestCluster.Async<String> masterFuture = internalCluster().startNodeAsync(Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), true, Node.NODE_DATA_SETTING.getKey(), false).build());
        InternalTestCluster.Async<List<String>> nodesFutures = internalCluster().startNodesAsync(4, Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false, Node.NODE_DATA_SETTING.getKey(), true).build());
        final String masterNode = masterFuture.get();
        final String node1 = nodesFutures.get().get(0);
        final String node2 = nodesFutures.get().get(1);
        final String node3 = nodesFutures.get().get(2);
        final String node4 = nodesFutures.get().get(3);
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", node4)));
        assertFalse(client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).setWaitForGreenStatus().setWaitForNodes("5").get().isTimedOut());
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")));
        logger.debug("--> shutting down two random nodes");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node1, node2, node3));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node1, node2, node3));
        logger.debug("--> verifying index is red");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        if (health.getStatus() != ClusterHealthStatus.RED) {
            logClusterState();
            fail("cluster didn't become red, despite of shutting 2 of 3 nodes");
        }
        logger.debug("--> allowing index to be assigned to node [{}]", node4);
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", "NONE")));
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "all")));
        logger.debug("--> waiting for shards to recover on [{}]", node4);
        Index index = resolveIndex("test");
        assertBusy(new Runnable() {

            @Override
            public void run() {
                assertTrue(internalCluster().getInstance(IndicesService.class, node4).hasIndex(index));
            }
        });
        assertFalse(client().admin().cluster().prepareHealth().setWaitForActiveShards(4).get().isTimedOut());
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")));
        logger.debug("--> starting the two old nodes back");
        internalCluster().startNodesAsync(2, Settings.builder().put(Node.NODE_MASTER_SETTING.getKey(), false, Node.NODE_DATA_SETTING.getKey(), true).build());
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("5").get().isTimedOut());
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "all")));
        logger.debug("--> waiting for the lost shard to be recovered");
        ensureGreen("test");
    }

    public void testShardActiveElseWhere() throws Exception {
        List<String> nodes = internalCluster().startNodesAsync(2).get();
        final String masterNode = internalCluster().getMasterName();
        final String nonMasterNode = nodes.get(0).equals(masterNode) ? nodes.get(1) : nodes.get(0);
        final String masterId = internalCluster().clusterService(masterNode).localNode().getId();
        final String nonMasterId = internalCluster().clusterService(nonMasterNode).localNode().getId();
        final int numShards = scaledRandomIntBetween(2, 10);
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards)));
        ensureGreen("test");
        waitNoPendingTasksOnAll();
        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        final Index index = stateResponse.getState().metaData().index("test").getIndex();
        RoutingNode routingNode = stateResponse.getState().getRoutingNodes().node(nonMasterId);
        final int[] node2Shards = new int[routingNode.numberOfOwningShards()];
        int i = 0;
        for (ShardRouting shardRouting : routingNode) {
            node2Shards[i] = shardRouting.shardId().id();
            i++;
        }
        logger.info("Node [{}] has shards: {}", nonMasterNode, Arrays.toString(node2Shards));
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)).get();
        internalCluster().getInstance(ClusterService.class, nonMasterNode).submitStateUpdateTask("test", new ClusterStateUpdateTask(Priority.IMMEDIATE) {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
                for (int i = 0; i < numShards; i++) {
                    indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(new ShardId(index, i)).addShard(TestShardRouting.newShardRouting("test", i, masterId, true, ShardRoutingState.STARTED)).build());
                }
                return ClusterState.builder(currentState).routingTable(RoutingTable.builder().add(indexRoutingTableBuilder).build()).build();
            }

            @Override
            public boolean runOnlyOnMaster() {
                return false;
            }

            @Override
            public void onFailure(String source, Exception e) {
            }
        });
        waitNoPendingTasksOnAll();
        logger.info("Checking if shards aren't removed");
        for (int shard : node2Shards) {
            assertTrue(waitForShardDeletion(nonMasterNode, index, shard));
        }
    }

    private Path indexDirectory(String server, Index index) {
        NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, server);
        final Path[] paths = env.indexPaths(index);
        assert paths.length == 1;
        return paths[0];
    }

    private Path shardDirectory(String server, Index index, int shard) {
        NodeEnvironment env = internalCluster().getInstance(NodeEnvironment.class, server);
        final Path[] paths = env.availableShardPaths(new ShardId(index, shard));
        assert paths.length == 1;
        return paths[0];
    }

    private boolean waitForShardDeletion(final String server, final Index index, final int shard) throws InterruptedException {
        awaitBusy(() -> !Files.exists(shardDirectory(server, index, shard)));
        return Files.exists(shardDirectory(server, index, shard));
    }

    private boolean waitForIndexDeletion(final String server, final Index index) throws InterruptedException {
        awaitBusy(() -> !Files.exists(indexDirectory(server, index)));
        return Files.exists(indexDirectory(server, index));
    }

    public static class ReclocationStartEndTracer extends MockTransportService.Tracer {

        private final Logger logger;

        private final CountDownLatch beginRelocationLatch;

        private final CountDownLatch receivedShardExistsRequestLatch;

        public ReclocationStartEndTracer(Logger logger, CountDownLatch beginRelocationLatch, CountDownLatch receivedShardExistsRequestLatch) {
            this.logger = logger;
            this.beginRelocationLatch = beginRelocationLatch;
            this.receivedShardExistsRequestLatch = receivedShardExistsRequestLatch;
        }

        @Override
        public void receivedRequest(long requestId, String action) {
            if (action.equals(IndicesStore.ACTION_SHARD_EXISTS)) {
                receivedShardExistsRequestLatch.countDown();
                logger.info("received: {}, relocation done", action);
            }
        }

        @Override
        public void requestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
            if (action.equals(PeerRecoverySourceService.Actions.START_RECOVERY)) {
                logger.info("sent: {}, relocation starts", action);
                beginRelocationLatch.countDown();
            }
        }
    }
}