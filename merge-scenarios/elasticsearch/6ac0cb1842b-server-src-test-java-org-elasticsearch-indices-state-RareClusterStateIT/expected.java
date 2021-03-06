package org.elasticsearch.indices.state;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.junit.annotations.TestLogging;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0)
@TestLogging("_root:DEBUG")
public class RareClusterStateIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(TestZenDiscovery.USE_MOCK_PINGS.getKey(), false).build();
    }

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testAssignmentWithJustAddedNodes() throws Exception {
        internalCluster().startNode();
        final String index = "index";
        prepareCreate(index).setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)).get();
        ensureGreen(index);
        client().admin().indices().prepareClose(index).get();
        final String masterName = internalCluster().getMasterName();
        final ClusterService clusterService = internalCluster().clusterService(masterName);
        final AllocationService allocationService = internalCluster().getInstance(AllocationService.class, masterName);
        clusterService.submitStateUpdateTask("test-inject-node-and-reroute", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                ClusterState.Builder builder = ClusterState.builder(currentState);
                builder.nodes(DiscoveryNodes.builder(currentState.nodes()).add(new DiscoveryNode("_non_existent", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT)));
                final IndexMetaData indexMetaData = IndexMetaData.builder(currentState.metaData().index(index)).state(IndexMetaData.State.OPEN).build();
                builder.metaData(MetaData.builder(currentState.metaData()).put(indexMetaData, true));
                builder.blocks(ClusterBlocks.builder().blocks(currentState.blocks()).removeIndexBlocks(index));
                ClusterState updatedState = builder.build();
                RoutingTable.Builder routingTable = RoutingTable.builder(updatedState.routingTable());
                routingTable.addAsRecovery(updatedState.metaData().index(index));
                updatedState = ClusterState.builder(updatedState).routingTable(routingTable.build()).build();
                return allocationService.reroute(updatedState, "reroute");
            }

            @Override
            public void onFailure(String source, Exception e) {
            }
        });
        ensureGreen(index);
        clusterService.submitStateUpdateTask("test-remove-injected-node", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                ClusterState.Builder builder = ClusterState.builder(currentState);
                builder.nodes(DiscoveryNodes.builder(currentState.nodes()).remove("_non_existent"));
                currentState = builder.build();
                return allocationService.deassociateDeadNodes(currentState, true, "reroute");
            }

            @Override
            public void onFailure(String source, Exception e) {
            }
        });
    }

    public void testDeleteCreateInOneBulk() throws Exception {
        internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("2").get().isTimedOut());
        prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)).addMapping("type").get();
        ensureGreen("test");
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "0").put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), "30s")));
        BlockClusterStateProcessing disruption = new BlockClusterStateProcessing(dataNode, random());
        internalCluster().setDisruptionScheme(disruption);
        logger.info("--> indexing a doc");
        index("test", "type", "1");
        refresh();
        disruption.startDisrupting();
        logger.info("--> delete index and recreate it");
        assertFalse(client().admin().indices().prepareDelete("test").setTimeout("200ms").get().isAcknowledged());
        assertFalse(prepareCreate("test").setTimeout("200ms").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), "0")).get().isAcknowledged());
        logger.info("--> letting cluster proceed");
        disruption.stopDisrupting();
        ensureGreen(TimeValue.timeValueMinutes(30), "test");
        assertBusy(() -> {
            long masterClusterStateVersion = internalCluster().clusterService(internalCluster().getMasterName()).state().version();
            long dataClusterStateVersion = internalCluster().clusterService(dataNode).state().version();
            assertThat(masterClusterStateVersion, equalTo(dataClusterStateVersion));
        });
        assertHitCount(client().prepareSearch("test").get(), 0);
    }

    public void testDelayedMappingPropagationOnPrimary() throws Exception {
        Settings settings = Settings.builder().put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), "30s").put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "0s").put(TestZenDiscovery.USE_ZEN2.getKey(), false).build();
        final List<String> nodeNames = internalCluster().startNodes(2, settings);
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("2").get().isTimedOut());
        final String master = internalCluster().getMasterName();
        assertThat(nodeNames, hasItem(master));
        String otherNode = null;
        for (String node : nodeNames) {
            if (node.equals(master) == false) {
                otherNode = node;
                break;
            }
        }
        assertNotNull(otherNode);
        assertAcked(prepareCreate("index").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put("index.routing.allocation.exclude._name", master)).get());
        ensureGreen();
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertEquals(master, state.nodes().getMasterNode().getName());
        List<ShardRouting> shards = state.routingTable().allShards("index");
        assertThat(shards, hasSize(1));
        for (ShardRouting shard : shards) {
            if (shard.primary()) {
                assertFalse(state.nodes().getMasterNodeId().equals(shard.currentNodeId()));
            } else {
                fail();
            }
        }
        BlockClusterStateProcessing disruption = new BlockClusterStateProcessing(otherNode, random());
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();
        final AtomicReference<Object> putMappingResponse = new AtomicReference<>();
        client().admin().indices().preparePutMapping("index").setType("type").setSource("field", "type=long").execute(new ActionListener<AcknowledgedResponse>() {

            @Override
            public void onResponse(AcknowledgedResponse response) {
                putMappingResponse.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                putMappingResponse.set(e);
            }
        });
        assertBusy(() -> {
            ImmutableOpenMap<String, MappingMetaData> indexMappings = client().admin().indices().prepareGetMappings("index").get().getMappings().get("index");
            assertNotNull(indexMappings);
            MappingMetaData typeMappings = indexMappings.get("type");
            assertNotNull(typeMappings);
            Object properties;
            try {
                properties = typeMappings.getSourceAsMap().get("properties");
            } catch (ElasticsearchParseException e) {
                throw new AssertionError(e);
            }
            assertNotNull(properties);
            Object fieldMapping = ((Map<String, Object>) properties).get("field");
            assertNotNull(fieldMapping);
        });
        final AtomicReference<Object> docIndexResponse = new AtomicReference<>();
        client().prepareIndex("index", "type", "1").setSource("field", 42).execute(new ActionListener<IndexResponse>() {

            @Override
            public void onResponse(IndexResponse response) {
                docIndexResponse.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                docIndexResponse.set(e);
            }
        });
        Thread.sleep(100);
        assertThat(putMappingResponse.get(), equalTo(null));
        assertThat(docIndexResponse.get(), equalTo(null));
        disruption.stopDisrupting();
        assertBusy(() -> {
            assertThat(putMappingResponse.get(), instanceOf(AcknowledgedResponse.class));
            AcknowledgedResponse resp = (AcknowledgedResponse) putMappingResponse.get();
            assertTrue(resp.isAcknowledged());
            assertThat(docIndexResponse.get(), instanceOf(IndexResponse.class));
            IndexResponse docResp = (IndexResponse) docIndexResponse.get();
            assertEquals(Arrays.toString(docResp.getShardInfo().getFailures()), 1, docResp.getShardInfo().getTotal());
        });
    }

    public void testDelayedMappingPropagationOnReplica() throws Exception {
        final List<String> nodeNames = internalCluster().startNodes(2, Settings.builder().put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), "30s").put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "0s").put(TestZenDiscovery.USE_ZEN2.getKey(), false).build());
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("2").get().isTimedOut());
        final String master = internalCluster().getMasterName();
        assertThat(nodeNames, hasItem(master));
        String otherNode = null;
        for (String node : nodeNames) {
            if (node.equals(master) == false) {
                otherNode = node;
                break;
            }
        }
        assertNotNull(otherNode);
        assertAcked(prepareCreate("index").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).put("index.routing.allocation.include._name", master)).get());
        assertAcked(client().admin().indices().prepareUpdateSettings("index").setSettings(Settings.builder().put("index.routing.allocation.include._name", "")).get());
        ensureGreen();
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertEquals(master, state.nodes().getMasterNode().getName());
        List<ShardRouting> shards = state.routingTable().allShards("index");
        assertThat(shards, hasSize(2));
        for (ShardRouting shard : shards) {
            if (shard.primary()) {
                assertEquals(state.nodes().getMasterNodeId(), shard.currentNodeId());
            } else {
                assertTrue(shard.active());
            }
        }
        BlockClusterStateProcessing disruption = new BlockClusterStateProcessing(otherNode, random());
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();
        final AtomicReference<Object> putMappingResponse = new AtomicReference<>();
        client().admin().indices().preparePutMapping("index").setType("type").setSource("field", "type=long").execute(new ActionListener<AcknowledgedResponse>() {

            @Override
            public void onResponse(AcknowledgedResponse response) {
                putMappingResponse.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                putMappingResponse.set(e);
            }
        });
        final Index index = resolveIndex("index");
        assertBusy(() -> {
            final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, master);
            final IndexService indexService = indicesService.indexServiceSafe(index);
            assertNotNull(indexService);
            final MapperService mapperService = indexService.mapperService();
            DocumentMapper mapper = mapperService.documentMapper("type");
            assertNotNull(mapper);
            assertNotNull(mapper.mappers().getMapper("field"));
        });
        final AtomicReference<Object> docIndexResponse = new AtomicReference<>();
        client().prepareIndex("index", "type", "1").setSource("field", 42).execute(new ActionListener<IndexResponse>() {

            @Override
            public void onResponse(IndexResponse response) {
                docIndexResponse.set(response);
            }

            @Override
            public void onFailure(Exception e) {
                docIndexResponse.set(e);
            }
        });
        assertBusy(() -> assertTrue(client().prepareGet("index", "type", "1").get().isExists()));
        Thread.sleep(100);
        assertThat(putMappingResponse.get(), equalTo(null));
        assertThat(docIndexResponse.get(), equalTo(null));
        disruption.stopDisrupting();
        assertBusy(() -> {
            assertThat(putMappingResponse.get(), instanceOf(AcknowledgedResponse.class));
            AcknowledgedResponse resp = (AcknowledgedResponse) putMappingResponse.get();
            assertTrue(resp.isAcknowledged());
            assertThat(docIndexResponse.get(), instanceOf(IndexResponse.class));
            IndexResponse docResp = (IndexResponse) docIndexResponse.get();
            assertEquals(Arrays.toString(docResp.getShardInfo().getFailures()), 2, docResp.getShardInfo().getTotal());
        });
    }
}