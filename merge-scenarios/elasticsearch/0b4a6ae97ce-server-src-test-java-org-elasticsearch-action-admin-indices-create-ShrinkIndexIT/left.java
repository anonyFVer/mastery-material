package org.elasticsearch.action.admin.indices.create;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ShrinkIndexIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testCreateShrinkIndexToN() {
        int[][] possibleShardSplits = new int[][] { { 8, 4, 2 }, { 9, 3, 1 }, { 4, 2, 1 }, { 15, 5, 1 } };
        int[] shardSplits = randomFrom(possibleShardSplits);
        assertEquals(shardSplits[0], (shardSplits[0] / shardSplits[1]) * shardSplits[1]);
        assertEquals(shardSplits[1], (shardSplits[1] / shardSplits[2]) * shardSplits[2]);
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings()).put("number_of_shards", shardSplits[0])).get();
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("source", "t1", Integer.toString(i)).setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin().cluster().prepareState().get().getState().nodes().getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode.class);
        String mergeNode = discoveryNodes[0].getName();
        ensureGreen();
        client().admin().indices().prepareUpdateSettings("source").setSettings(Settings.builder().put("index.routing.allocation.require._name", mergeNode).put("index.blocks.write", true)).get();
        ensureGreen();
        assertAcked(client().admin().indices().prepareResizeIndex("source", "first_shrink").setSettings(Settings.builder().put("index.number_of_replicas", 0).put("index.number_of_shards", shardSplits[1]).putNull("index.blocks.write").build()).get());
        ensureGreen();
        assertHitCount(client().prepareSearch("first_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("first_shrink", "t1", Integer.toString(i)).setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        flushAndRefresh();
        assertHitCount(client().prepareSearch("first_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        assertHitCount(client().prepareSearch("source").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        client().admin().indices().prepareUpdateSettings("first_shrink").setSettings(Settings.builder().put("index.routing.allocation.require._name", mergeNode).put("index.blocks.write", true)).get();
        ensureGreen();
        assertAcked(client().admin().indices().prepareResizeIndex("first_shrink", "second_shrink").setSettings(Settings.builder().put("index.number_of_replicas", 0).put("index.number_of_shards", shardSplits[2]).putNull("index.blocks.write").putNull("index.routing.allocation.require._name").build()).get());
        ensureGreen();
        assertHitCount(client().prepareSearch("second_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        client().admin().indices().prepareUpdateSettings("second_shrink").setSettings(Settings.builder().putNull("index.routing.allocation.include._id").put("index.number_of_replicas", 1)).get();
        ensureGreen();
        assertHitCount(client().prepareSearch("second_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("second_shrink", "t1", Integer.toString(i)).setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        flushAndRefresh();
        assertHitCount(client().prepareSearch("second_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        assertHitCount(client().prepareSearch("first_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        assertHitCount(client().prepareSearch("source").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
    }

    public void testShrinkIndexPrimaryTerm() throws Exception {
        final List<Integer> factors = Arrays.asList(2, 3, 5, 7);
        final List<Integer> numberOfShardsFactors = randomSubsetOf(scaledRandomIntBetween(1, factors.size() - 1), factors);
        final int numberOfShards = numberOfShardsFactors.stream().reduce(1, (x, y) -> x * y);
        final int numberOfTargetShards = randomSubsetOf(randomInt(numberOfShardsFactors.size() - 1), numberOfShardsFactors).stream().reduce(1, (x, y) -> x * y);
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings()).put("number_of_shards", numberOfShards)).get();
        final ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin().cluster().prepareState().get().getState().nodes().getDataNodes();
        assertThat(dataNodes.size(), greaterThanOrEqualTo(2));
        final DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode.class);
        final String mergeNode = discoveryNodes[0].getName();
        ensureGreen(TimeValue.timeValueSeconds(120));
        final Index source = resolveIndex("source");
        final int iterations = scaledRandomIntBetween(0, 16);
        for (int i = 0; i < iterations; i++) {
            final String node = randomSubsetOf(1, internalCluster().nodesInclude("source")).get(0);
            final IndicesService indexServices = internalCluster().getInstance(IndicesService.class, node);
            final IndexService indexShards = indexServices.indexServiceSafe(source);
            for (final Integer shardId : indexShards.shardIds()) {
                final IndexShard shard = indexShards.getShard(shardId);
                if (shard.routingEntry().primary() && randomBoolean()) {
                    disableAllocation("source");
                    shard.failShard("test", new Exception("test"));
                    int id = 0;
                    while (true) {
                        final String s = Integer.toString(id);
                        final int hash = Math.floorMod(Murmur3HashFunction.hash(s), numberOfShards);
                        if (hash == shardId) {
                            final IndexRequest request = new IndexRequest("source", "type", s).source("{ \"f\": \"" + s + "\"}", XContentType.JSON);
                            client().index(request).get();
                            break;
                        } else {
                            id++;
                        }
                    }
                    enableAllocation("source");
                    ensureGreen();
                }
            }
        }
        final Settings.Builder prepareShrinkSettings = Settings.builder().put("index.routing.allocation.require._name", mergeNode).put("index.blocks.write", true);
        client().admin().indices().prepareUpdateSettings("source").setSettings(prepareShrinkSettings).get();
        ensureGreen();
        final IndexMetaData indexMetaData = indexMetaData(client(), "source");
        final long beforeShrinkPrimaryTerm = IntStream.range(0, numberOfShards).mapToLong(indexMetaData::primaryTerm).max().getAsLong();
        final Settings shrinkSettings = Settings.builder().put("index.number_of_replicas", 0).put("index.number_of_shards", numberOfTargetShards).build();
        assertAcked(client().admin().indices().prepareResizeIndex("source", "target").setSettings(shrinkSettings).get());
        ensureGreen();
        final IndexMetaData afterShrinkIndexMetaData = indexMetaData(client(), "target");
        for (int shardId = 0; shardId < numberOfTargetShards; shardId++) {
            assertThat(afterShrinkIndexMetaData.primaryTerm(shardId), equalTo(beforeShrinkPrimaryTerm + 1));
        }
    }

    private static IndexMetaData indexMetaData(final Client client, final String index) {
        final ClusterStateResponse clusterStateResponse = client.admin().cluster().state(new ClusterStateRequest()).actionGet();
        return clusterStateResponse.getState().metaData().index(index);
    }

    public void testCreateShrinkIndex() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        Version version = VersionUtils.randomVersion(random());
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings()).put("number_of_shards", randomIntBetween(2, 7)).put("index.version.created", version)).get();
        final int docs = randomIntBetween(0, 128);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("source", "type").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin().cluster().prepareState().get().getState().nodes().getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode.class);
        ensureGreen();
        client().admin().indices().prepareUpdateSettings("source").setSettings(Settings.builder().put("index.routing.allocation.require._name", discoveryNodes[0].getName()).put("index.blocks.write", true)).get();
        ensureGreen();
        final IndicesStatsResponse sourceStats = client().admin().indices().prepareStats("source").setSegments(true).get();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")).get();
        final boolean createWithReplicas = randomBoolean();
        assertAcked(client().admin().indices().prepareResizeIndex("source", "target").setSettings(Settings.builder().put("index.number_of_replicas", createWithReplicas ? 1 : 0).putNull("index.blocks.write").putNull("index.routing.allocation.require._name").build()).get());
        ensureGreen();
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        DiscoveryNode mergeNode = state.nodes().get(state.getRoutingTable().index("target").shard(0).primaryShard().currentNodeId());
        logger.info("merge node {}", mergeNode);
        final long maxSeqNo = Arrays.stream(sourceStats.getShards()).filter(shard -> shard.getShardRouting().currentNodeId().equals(mergeNode.getId())).map(ShardStats::getSeqNoStats).mapToLong(SeqNoStats::getMaxSeqNo).max().getAsLong();
        final long maxUnsafeAutoIdTimestamp = Arrays.stream(sourceStats.getShards()).filter(shard -> shard.getShardRouting().currentNodeId().equals(mergeNode.getId())).map(ShardStats::getStats).map(CommonStats::getSegments).mapToLong(SegmentsStats::getMaxUnsafeAutoIdTimestamp).max().getAsLong();
        final IndicesStatsResponse targetStats = client().admin().indices().prepareStats("target").get();
        for (final ShardStats shardStats : targetStats.getShards()) {
            final SeqNoStats seqNoStats = shardStats.getSeqNoStats();
            final ShardRouting shardRouting = shardStats.getShardRouting();
            assertThat("failed on " + shardRouting, seqNoStats.getMaxSeqNo(), equalTo(maxSeqNo));
            assertThat("failed on " + shardRouting, seqNoStats.getLocalCheckpoint(), equalTo(maxSeqNo));
            assertThat("failed on " + shardRouting, shardStats.getStats().getSegments().getMaxUnsafeAutoIdTimestamp(), equalTo(maxUnsafeAutoIdTimestamp));
        }
        final int size = docs > 0 ? 2 * docs : 1;
        assertHitCount(client().prepareSearch("target").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);
        if (createWithReplicas == false) {
            client().admin().indices().prepareUpdateSettings("target").setSettings(Settings.builder().put("index.number_of_replicas", 1)).get();
            ensureGreen();
            assertHitCount(client().prepareSearch("target").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);
        }
        for (int i = docs; i < 2 * docs; i++) {
            client().prepareIndex("target", "type").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        flushAndRefresh();
        assertHitCount(client().prepareSearch("target").setSize(2 * size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 2 * docs);
        assertHitCount(client().prepareSearch("source").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);
        GetSettingsResponse target = client().admin().indices().prepareGetSettings("target").get();
        assertEquals(version, target.getIndexToSettings().get("target").getAsVersion("index.version.created", null));
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), (String) null)).get();
    }

    public void testCreateShrinkIndexFails() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings()).put("number_of_shards", randomIntBetween(2, 7)).put("number_of_replicas", 0)).get();
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("source", "type").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin().cluster().prepareState().get().getState().nodes().getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode.class);
        String spareNode = discoveryNodes[0].getName();
        String mergeNode = discoveryNodes[1].getName();
        ensureGreen();
        client().admin().indices().prepareUpdateSettings("source").setSettings(Settings.builder().put("index.routing.allocation.require._name", mergeNode).put("index.blocks.write", true)).get();
        ensureGreen();
        client().admin().indices().prepareResizeIndex("source", "target").setWaitForActiveShards(ActiveShardCount.NONE).setSettings(Settings.builder().put("index.routing.allocation.exclude._name", mergeNode).put("index.number_of_replicas", 0).put("index.allocation.max_retries", 1).build()).get();
        client().admin().cluster().prepareHealth("target").setWaitForEvents(Priority.LANGUID).get();
        client().admin().indices().prepareUpdateSettings("source").setSettings(Settings.builder().put("index.routing.allocation.require._name", spareNode).put("index.blocks.write", true)).get();
        ensureGreen("source");
        client().admin().indices().prepareUpdateSettings("target").setSettings(Settings.builder().putNull("index.routing.allocation.exclude._name")).get();
        assertBusy(() -> {
            ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
            RoutingTable routingTables = clusterStateResponse.getState().routingTable();
            assertTrue(routingTables.index("target").shard(0).getShards().get(0).unassigned());
            assertEquals(UnassignedInfo.Reason.ALLOCATION_FAILED, routingTables.index("target").shard(0).getShards().get(0).unassignedInfo().getReason());
            assertEquals(1, routingTables.index("target").shard(0).getShards().get(0).unassignedInfo().getNumFailedAllocations());
        });
        client().admin().indices().prepareUpdateSettings("source").setSettings(Settings.builder().put("index.routing.allocation.require._name", mergeNode)).get();
        ensureGreen("source");
        final InternalClusterInfoService infoService = (InternalClusterInfoService) internalCluster().getInstance(ClusterInfoService.class, internalCluster().getMasterName());
        infoService.refresh();
        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        long expectedShardSize = clusterRerouteResponse.getState().routingTable().index("target").shard(0).getShards().get(0).getExpectedShardSize();
        assertTrue("expected shard size must be set but wasn't: " + expectedShardSize, expectedShardSize > 0);
        ensureGreen();
        assertHitCount(client().prepareSearch("target").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
    }

    public void testCreateShrinkWithIndexSort() throws Exception {
        SortField expectedSortField = new SortedSetSortField("id", true, SortedSetSelector.Type.MAX);
        expectedSortField.setMissingValue(SortedSetSortField.STRING_FIRST);
        Sort expectedIndexSort = new Sort(expectedSortField);
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings()).put("sort.field", "id").put("sort.order", "desc").put("number_of_shards", 8).put("number_of_replicas", 0)).addMapping("type", "id", "type=keyword,doc_values=true").get();
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("source", "type", Integer.toString(i)).setSource("{\"foo\" : \"bar\", \"id\" : " + i + "}", XContentType.JSON).get();
        }
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin().cluster().prepareState().get().getState().nodes().getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode.class);
        String mergeNode = discoveryNodes[0].getName();
        ensureGreen();
        flushAndRefresh();
        assertSortedSegments("source", expectedIndexSort);
        client().admin().indices().prepareUpdateSettings("source").setSettings(Settings.builder().put("index.routing.allocation.require._name", mergeNode).put("index.blocks.write", true)).get();
        ensureGreen();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> client().admin().indices().prepareResizeIndex("source", "target").setSettings(Settings.builder().put("index.number_of_replicas", 0).put("index.number_of_shards", "2").put("index.sort.field", "foo").build()).get());
        assertThat(exc.getMessage(), containsString("can't override index sort when resizing an index"));
        assertAcked(client().admin().indices().prepareResizeIndex("source", "target").setSettings(Settings.builder().put("index.number_of_replicas", 0).put("index.number_of_shards", "2").putNull("index.blocks.write").build()).get());
        ensureGreen();
        flushAndRefresh();
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings("target").execute().actionGet();
        assertEquals(settingsResponse.getSetting("target", "index.sort.field"), "id");
        assertEquals(settingsResponse.getSetting("target", "index.sort.order"), "desc");
        assertSortedSegments("target", expectedIndexSort);
        for (int i = 20; i < 40; i++) {
            client().prepareIndex("target", "type").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        flushAndRefresh();
        assertSortedSegments("target", expectedIndexSort);
    }

    public void testShrinkCommitsMergeOnIdle() throws Exception {
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings()).put("index.number_of_replicas", 0).put("number_of_shards", 5)).get();
        for (int i = 0; i < 30; i++) {
            client().prepareIndex("source", "type").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        client().admin().indices().prepareFlush("source").get();
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin().cluster().prepareState().get().getState().nodes().getDataNodes();
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode.class);
        ensureGreen();
        client().admin().indices().prepareUpdateSettings("source").setSettings(Settings.builder().put("index.routing.allocation.require._name", discoveryNodes[0].getName()).put("index.blocks.write", true)).get();
        ensureGreen();
        IndicesSegmentResponse sourceStats = client().admin().indices().prepareSegments("source").get();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")).get();
        assertAcked(client().admin().indices().prepareResizeIndex("source", "target").setSettings(Settings.builder().put("index.number_of_replicas", 0).build()).get());
        ensureGreen();
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        IndexMetaData target = clusterStateResponse.getState().getMetaData().index("target");
        client().admin().indices().prepareForceMerge("target").setMaxNumSegments(1).setFlush(false).get();
        IndicesSegmentResponse targetSegStats = client().admin().indices().prepareSegments("target").get();
        ShardSegments segmentsStats = targetSegStats.getIndices().get("target").getShards().get(0).getShards()[0];
        assertTrue(segmentsStats.getNumberOfCommitted() > 0);
        assertNotEquals(segmentsStats.getSegments(), segmentsStats.getNumberOfCommitted());
        Iterable<IndicesService> dataNodeInstances = internalCluster().getDataNodeInstances(IndicesService.class);
        for (IndicesService service : dataNodeInstances) {
            if (service.hasIndex(target.getIndex())) {
                IndexService indexShards = service.indexService(target.getIndex());
                IndexShard shard = indexShards.getShard(0);
                assertTrue(shard.isActive());
                shard.checkIdle(0);
                assertFalse(shard.isActive());
            }
        }
        assertBusy(() -> {
            IndicesSegmentResponse targetStats = client().admin().indices().prepareSegments("target").get();
            ShardSegments targetShardSegments = targetStats.getIndices().get("target").getShards().get(0).getShards()[0];
            Map<Integer, IndexShardSegments> source = sourceStats.getIndices().get("source").getShards();
            int numSourceSegments = 0;
            for (IndexShardSegments s : source.values()) {
                numSourceSegments += s.getAt(0).getNumberOfCommitted();
            }
            assertTrue(targetShardSegments.getSegments().size() < numSourceSegments);
            assertEquals(targetShardSegments.getNumberOfCommitted(), targetShardSegments.getNumberOfSearch());
            assertEquals(targetShardSegments.getNumberOfCommitted(), targetShardSegments.getSegments().size());
            assertEquals(1, targetShardSegments.getSegments().size());
        });
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), (String) null)).get();
    }
}