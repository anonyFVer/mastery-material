package org.elasticsearch.action.admin.indices.create;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.IsNull.notNullValue;

@ClusterScope(scope = Scope.TEST)
public class CreateIndexIT extends ESIntegTestCase {

    public void testCreationDateGivenFails() {
        try {
            prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_CREATION_DATE, 4L)).get();
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("unknown setting [index.creation_date] please check that any required plugins are installed, or check the " + "breaking changes documentation for removed settings", ex.getMessage());
        }
    }

    public void testCreationDateGenerated() {
        long timeBeforeRequest = System.currentTimeMillis();
        prepareCreate("test").get();
        long timeAfterRequest = System.currentTimeMillis();
        ClusterStateResponse response = client().admin().cluster().prepareState().get();
        ClusterState state = response.getState();
        assertThat(state, notNullValue());
        MetaData metadata = state.getMetaData();
        assertThat(metadata, notNullValue());
        ImmutableOpenMap<String, IndexMetaData> indices = metadata.getIndices();
        assertThat(indices, notNullValue());
        assertThat(indices.size(), equalTo(1));
        IndexMetaData index = indices.get("test");
        assertThat(index, notNullValue());
        assertThat(index.getCreationDate(), allOf(lessThanOrEqualTo(timeAfterRequest), greaterThanOrEqualTo(timeBeforeRequest)));
    }

    public void testDoubleAddMapping() throws Exception {
        try {
            prepareCreate("test").addMapping("type1", "date", "type=date").addMapping("type1", "num", "type=integer");
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
        }
        try {
            prepareCreate("test").addMapping("type1", new HashMap<String, Object>()).addMapping("type1", new HashMap<String, Object>());
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
        }
        try {
            prepareCreate("test").addMapping("type1", jsonBuilder()).addMapping("type1", jsonBuilder());
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
        }
    }

    public void testInvalidShardCountSettings() throws Exception {
        int value = randomIntBetween(-10, 0);
        try {
            prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, value).build()).get();
            fail("should have thrown an exception about the primary shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_shards] must be >= 1", e.getMessage());
        }
        value = randomIntBetween(-10, -1);
        try {
            prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, value).build()).get();
            fail("should have thrown an exception about the replica shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_replicas] must be >= 0", e.getMessage());
        }
    }

    public void testCreateIndexWithBlocks() {
        try {
            setClusterReadOnly(true);
            assertBlocked(prepareCreate("test"));
        } finally {
            setClusterReadOnly(false);
        }
    }

    public void testCreateIndexWithMetadataBlocks() {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_BLOCKS_METADATA, true)));
        assertBlocked(client().admin().indices().prepareGetSettings("test"), IndexMetaData.INDEX_METADATA_BLOCK);
        disableIndexBlock("test", IndexMetaData.SETTING_BLOCKS_METADATA);
    }

    public void testUnknownSettingFails() {
        try {
            prepareCreate("test").setSettings(Settings.builder().put("index.unknown.value", "this must fail").build()).get();
            fail("should have thrown an exception about the shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("unknown setting [index.unknown.value] please check that any required plugins are installed, or check the" + " breaking changes documentation for removed settings", e.getMessage());
        }
    }

    public void testInvalidShardCountSettingsWithoutPrefix() throws Exception {
        int value = randomIntBetween(-10, 0);
        try {
            prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS.substring(IndexMetaData.INDEX_SETTING_PREFIX.length()), value).build()).get();
            fail("should have thrown an exception about the shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_shards] must be >= 1", e.getMessage());
        }
        value = randomIntBetween(-10, -1);
        try {
            prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS.substring(IndexMetaData.INDEX_SETTING_PREFIX.length()), value).build()).get();
            fail("should have thrown an exception about the shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_replicas] must be >= 0", e.getMessage());
        }
    }

    public void testCreateAndDeleteIndexConcurrently() throws InterruptedException {
        createIndex("test");
        final AtomicInteger indexVersion = new AtomicInteger(0);
        final Object indexVersionLock = new Object();
        final CountDownLatch latch = new CountDownLatch(1);
        int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "test").setSource("index_version", indexVersion.get()).get();
        }
        synchronized (indexVersionLock) {
            indexVersion.incrementAndGet();
        }
        client().admin().indices().prepareDelete("test").execute(new ActionListener<AcknowledgedResponse>() {

            @Override
            public void onResponse(AcknowledgedResponse deleteIndexResponse) {
                Thread thread = new Thread() {

                    @Override
                    public void run() {
                        try {
                            client().prepareIndex("test", "test").setSource("index_version", indexVersion.get()).get();
                            synchronized (indexVersionLock) {
                                indexVersion.incrementAndGet();
                            }
                            assertAcked(client().admin().indices().prepareDelete("test").get());
                        } finally {
                            latch.countDown();
                        }
                    }
                };
                thread.start();
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        });
        numDocs = randomIntBetween(100, 200);
        for (int i = 0; i < numDocs; i++) {
            try {
                synchronized (indexVersionLock) {
                    client().prepareIndex("test", "test").setSource("index_version", indexVersion.get()).setTimeout(TimeValue.timeValueSeconds(10)).get();
                }
            } catch (IndexNotFoundException inf) {
            } catch (UnavailableShardsException ex) {
                assertEquals(ex.getCause().getClass(), IndexNotFoundException.class);
            }
        }
        latch.await();
        refresh();
        SearchResponse expected = client().prepareSearch("test").setIndicesOptions(IndicesOptions.lenientExpandOpen()).setQuery(new RangeQueryBuilder("index_version").from(indexVersion.get(), true)).get();
        SearchResponse all = client().prepareSearch("test").setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
        assertEquals(expected + " vs. " + all, expected.getHits().getTotalHits(), all.getHits().getTotalHits());
        logger.info("total: {}", expected.getHits().getTotalHits());
    }

    public void testRestartIndexCreationAfterFullClusterRestart() throws Exception {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put("cluster.routing.allocation.enable", "none")).get();
        client().admin().indices().prepareCreate("test").setWaitForActiveShards(ActiveShardCount.NONE).setSettings(indexSettings()).get();
        internalCluster().fullRestart();
        ensureGreen("test");
    }

    public void testDefaultWaitForActiveShardsUsesIndexSetting() throws Exception {
        final int numReplicas = internalCluster().numDataNodes();
        Settings settings = Settings.builder().put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), Integer.toString(numReplicas)).put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1).put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), numReplicas).build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());
        settings = Settings.builder().put(settings).put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), "all").build();
        assertFalse(client().admin().indices().prepareCreate("test-idx-2").setSettings(settings).setTimeout("100ms").get().isShardsAcknowledged());
        settings = Settings.builder().put(settings).put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), Integer.toString(numReplicas + 1)).build();
        assertFalse(client().admin().indices().prepareCreate("test-idx-3").setSettings(settings).setTimeout("100ms").get().isShardsAcknowledged());
    }

    public void testInvalidPartitionSize() {
        BiFunction<Integer, Integer, Boolean> createPartitionedIndex = (shards, partitionSize) -> {
            CreateIndexResponse response;
            try {
                response = prepareCreate("test_" + shards + "_" + partitionSize).setSettings(Settings.builder().put("index.number_of_shards", shards).put("index.number_of_routing_shards", shards).put("index.routing_partition_size", partitionSize)).execute().actionGet();
            } catch (IllegalStateException | IllegalArgumentException e) {
                return false;
            }
            return response.isAcknowledged();
        };
        assertFalse(createPartitionedIndex.apply(3, 6));
        assertFalse(createPartitionedIndex.apply(3, 0));
        assertFalse(createPartitionedIndex.apply(3, 3));
        assertTrue(createPartitionedIndex.apply(3, 1));
        assertTrue(createPartitionedIndex.apply(3, 2));
        assertTrue(createPartitionedIndex.apply(1, 1));
    }

    public void testIndexNameInResponse() {
        CreateIndexResponse response = prepareCreate("foo").setSettings(Settings.builder().build()).get();
        assertEquals("Should have index name in response", "foo", response.index());
    }

    public void testIndexWithUnknownSetting() throws Exception {
        final int replicas = internalCluster().numDataNodes() - 1;
        final Settings settings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", replicas).build();
        client().admin().indices().prepareCreate("test").setSettings(settings).get();
        ensureGreen("test");
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final Set<String> dataOrMasterNodeNames = new HashSet<>();
        for (final ObjectCursor<DiscoveryNode> node : state.nodes().getMasterAndDataNodes().values()) {
            assertTrue(dataOrMasterNodeNames.add(node.value.getName()));
        }
        final IndexMetaData metaData = state.getMetaData().index("test");
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                if (dataOrMasterNodeNames.contains(nodeName)) {
                    final MetaStateService metaStateService = internalCluster().getInstance(MetaStateService.class, nodeName);
                    final IndexMetaData brokenMetaData = IndexMetaData.builder(metaData).settings(Settings.builder().put(metaData.getSettings()).put("index.foo", true)).build();
                    metaStateService.writeIndexAndUpdateManifest("broken metadata", brokenMetaData);
                }
                return super.onNodeStopped(nodeName);
            }
        });
        ensureGreen(metaData.getIndex().getName());
        final ClusterState stateAfterRestart = client().admin().cluster().prepareState().get().getState();
        assertThat(stateAfterRestart.getMetaData().index(metaData.getIndex()).getState(), equalTo(IndexMetaData.State.CLOSE));
        final ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> client().admin().indices().prepareOpen("test").get());
        assertThat(e, hasToString(containsString("Failed to verify index " + metaData.getIndex())));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e, hasToString(containsString("unknown setting [index.foo]")));
    }
}