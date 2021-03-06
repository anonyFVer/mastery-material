package org.elasticsearch.indices;

import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.gateway.LocalAllocateDangledIndices;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.IndicesService.ShardDeletionCheckResult;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesServiceTests extends ESSingleNodeTestCase {

    public IndicesService getIndicesService() {
        return getInstanceFromNode(IndicesService.class);
    }

    public NodeEnvironment getNodeEnvironment() {
        return getInstanceFromNode(NodeEnvironment.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(TestPlugin.class);
        return plugins;
    }

    public static class TestPlugin extends Plugin implements MapperPlugin {

        public TestPlugin() {
        }

        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Collections.singletonMap("fake-mapper", new KeywordFieldMapper.TypeParser());
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            super.onIndexModule(indexModule);
            indexModule.addSimilarity("fake-similarity", (settings, indexCreatedVersion, scriptService) -> new BM25Similarity());
        }
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testCanDeleteShardContent() {
        IndicesService indicesService = getIndicesService();
        IndexMetaData meta = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        ShardId shardId = new ShardId(meta.getIndex(), 0);
        assertEquals("no shard location", indicesService.canDeleteShardContent(shardId, indexSettings), ShardDeletionCheckResult.NO_FOLDER_FOUND);
        IndexService test = createIndex("test");
        shardId = new ShardId(test.index(), 0);
        assertTrue(test.hasShard(0));
        assertEquals("shard is allocated", indicesService.canDeleteShardContent(shardId, test.getIndexSettings()), ShardDeletionCheckResult.STILL_ALLOCATED);
        test.removeShard(0, "boom");
        assertEquals("shard is removed", indicesService.canDeleteShardContent(shardId, test.getIndexSettings()), ShardDeletionCheckResult.FOLDER_FOUND_CAN_DELETE);
        ShardId notAllocated = new ShardId(test.index(), 100);
        assertEquals("shard that was never on this node should NOT be deletable", indicesService.canDeleteShardContent(notAllocated, test.getIndexSettings()), ShardDeletionCheckResult.NO_FOLDER_FOUND);
    }

    public void testDeleteIndexStore() throws Exception {
        IndicesService indicesService = getIndicesService();
        IndexService test = createIndex("test");
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        IndexMetaData firstMetaData = clusterService.state().metaData().index("test");
        assertTrue(test.hasShard(0));
        try {
            indicesService.deleteIndexStore("boom", firstMetaData, clusterService.state());
            fail();
        } catch (IllegalStateException ex) {
        }
        GatewayMetaState gwMetaState = getInstanceFromNode(GatewayMetaState.class);
        MetaData meta = gwMetaState.loadMetaState();
        assertNotNull(meta);
        assertNotNull(meta.index("test"));
        assertAcked(client().admin().indices().prepareDelete("test"));
        meta = gwMetaState.loadMetaState();
        assertNotNull(meta);
        assertNull(meta.index("test"));
        test = createIndex("test");
        client().prepareIndex("test", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().admin().indices().prepareFlush("test").get();
        assertHitCount(client().prepareSearch("test").get(), 1);
        IndexMetaData secondMetaData = clusterService.state().metaData().index("test");
        assertAcked(client().admin().indices().prepareClose("test"));
        ShardPath path = ShardPath.loadShardPath(logger, getNodeEnvironment(), new ShardId(test.index(), 0), test.getIndexSettings());
        assertTrue(path.exists());
        try {
            indicesService.deleteIndexStore("boom", secondMetaData, clusterService.state());
            fail();
        } catch (IllegalStateException ex) {
        }
        assertTrue(path.exists());
        try {
            indicesService.deleteIndexStore("boom", firstMetaData, clusterService.state());
            fail();
        } catch (IllegalStateException ex) {
        }
        assertAcked(client().admin().indices().prepareOpen("test"));
        ensureGreen("test");
    }

    public void testPendingTasks() throws Exception {
        IndicesService indicesService = getIndicesService();
        IndexService test = createIndex("test");
        assertTrue(test.hasShard(0));
        ShardPath path = test.getShardOrNull(0).shardPath();
        assertTrue(test.getShardOrNull(0).routingEntry().started());
        ShardPath shardPath = ShardPath.loadShardPath(logger, getNodeEnvironment(), new ShardId(test.index(), 0), test.getIndexSettings());
        assertEquals(shardPath, path);
        try {
            indicesService.processPendingDeletes(test.index(), test.getIndexSettings(), new TimeValue(0, TimeUnit.MILLISECONDS));
            fail("can't get lock");
        } catch (ShardLockObtainFailedException ex) {
        }
        assertTrue(path.exists());
        int numPending = 1;
        if (randomBoolean()) {
            indicesService.addPendingDelete(new ShardId(test.index(), 0), test.getIndexSettings());
        } else {
            if (randomBoolean()) {
                numPending++;
                indicesService.addPendingDelete(new ShardId(test.index(), 0), test.getIndexSettings());
            }
            indicesService.addPendingDelete(test.index(), test.getIndexSettings());
        }
        assertAcked(client().admin().indices().prepareClose("test"));
        assertTrue(path.exists());
        assertEquals(indicesService.numPendingDeletes(test.index()), numPending);
        assertTrue(indicesService.hasUncompletedPendingDeletes());
        indicesService.processPendingDeletes(test.index(), test.getIndexSettings(), new TimeValue(0, TimeUnit.MILLISECONDS));
        assertEquals(indicesService.numPendingDeletes(test.index()), 0);
        assertFalse(indicesService.hasUncompletedPendingDeletes());
        assertFalse(path.exists());
        if (randomBoolean()) {
            indicesService.addPendingDelete(new ShardId(test.index(), 0), test.getIndexSettings());
            indicesService.addPendingDelete(new ShardId(test.index(), 1), test.getIndexSettings());
            indicesService.addPendingDelete(new ShardId("bogus", "_na_", 1), test.getIndexSettings());
            assertEquals(indicesService.numPendingDeletes(test.index()), 2);
            assertTrue(indicesService.hasUncompletedPendingDeletes());
            indicesService.processPendingDeletes(test.index(), test.getIndexSettings(), new TimeValue(0, TimeUnit.MILLISECONDS));
            assertEquals(indicesService.numPendingDeletes(test.index()), 0);
            assertTrue(indicesService.hasUncompletedPendingDeletes());
        }
        assertAcked(client().admin().indices().prepareOpen("test").setTimeout(TimeValue.timeValueSeconds(1)));
    }

    public void testVerifyIfIndexContentDeleted() throws Exception {
        final Index index = new Index("test", UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final NodeEnvironment nodeEnv = getNodeEnvironment();
        final MetaStateService metaStateService = getInstanceFromNode(MetaStateService.class);
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        final Settings idxSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID()).build();
        final IndexMetaData indexMetaData = new IndexMetaData.Builder(index.getName()).settings(idxSettings).numberOfShards(1).numberOfReplicas(0).build();
        metaStateService.writeIndex("test index being created", indexMetaData);
        final MetaData metaData = MetaData.builder(clusterService.state().metaData()).put(indexMetaData, true).build();
        final ClusterState csWithIndex = new ClusterState.Builder(clusterService.state()).metaData(metaData).build();
        try {
            indicesService.verifyIndexIsDeleted(index, csWithIndex);
            fail("Should not be able to delete index contents when the index is part of the cluster state.");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("Cannot delete index"));
        }
        final ClusterState withoutIndex = new ClusterState.Builder(csWithIndex).metaData(MetaData.builder(csWithIndex.metaData()).remove(index.getName())).build();
        indicesService.verifyIndexIsDeleted(index, withoutIndex);
        assertFalse("index files should be deleted", FileSystemUtils.exists(nodeEnv.indexPaths(index)));
    }

    public void testDanglingIndicesWithAliasConflict() throws Exception {
        final String indexName = "test-idx1";
        final String alias = "test-alias";
        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        createIndex(indexName);
        client().admin().indices().prepareAliases().addAlias(indexName, alias).get();
        final ClusterState originalState = clusterService.state();
        final LocalAllocateDangledIndices dangling = getInstanceFromNode(LocalAllocateDangledIndices.class);
        final Settings idxSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()).build();
        final IndexMetaData indexMetaData = new IndexMetaData.Builder(alias).settings(idxSettings).numberOfShards(1).numberOfReplicas(0).build();
        DanglingListener listener = new DanglingListener();
        dangling.allocateDangled(Arrays.asList(indexMetaData), listener);
        listener.latch.await();
        assertThat(clusterService.state(), equalTo(originalState));
        client().admin().indices().prepareAliases().removeAlias(indexName, alias).get();
        listener = new DanglingListener();
        dangling.allocateDangled(Arrays.asList(indexMetaData), listener);
        listener.latch.await();
        assertThat(clusterService.state(), not(originalState));
        assertNotNull(clusterService.state().getMetaData().index(alias));
    }

    public void testIndexAndTombstoneWithSameNameOnStartup() throws Exception {
        final String indexName = "test";
        final Index index = new Index(indexName, UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final Settings idxSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID()).build();
        final IndexMetaData indexMetaData = new IndexMetaData.Builder(index.getName()).settings(idxSettings).numberOfShards(1).numberOfReplicas(0).build();
        final Index tombstonedIndex = new Index(indexName, UUIDs.randomBase64UUID());
        final IndexGraveyard graveyard = IndexGraveyard.builder().addTombstone(tombstonedIndex).build();
        final MetaData metaData = MetaData.builder().put(indexMetaData, true).indexGraveyard(graveyard).build();
        final ClusterState clusterState = new ClusterState.Builder(new ClusterName("testCluster")).metaData(metaData).build();
        indicesService.verifyIndexIsDeleted(tombstonedIndex, clusterState);
    }

    private static class DanglingListener implements LocalAllocateDangledIndices.Listener {

        final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onResponse(LocalAllocateDangledIndices.AllocateDangledResponse response) {
            latch.countDown();
        }

        @Override
        public void onFailure(Throwable e) {
            latch.countDown();
        }
    }

    public void testStandAloneMapperServiceWithPlugins() throws IOException {
        final String indexName = "test";
        final Index index = new Index(indexName, UUIDs.randomBase64UUID());
        final IndicesService indicesService = getIndicesService();
        final Settings idxSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetaData.SETTING_INDEX_UUID, index.getUUID()).put(IndexModule.SIMILARITY_SETTINGS_PREFIX + ".test.type", "fake-similarity").build();
        final IndexMetaData indexMetaData = new IndexMetaData.Builder(index.getName()).settings(idxSettings).numberOfShards(1).numberOfReplicas(0).build();
        MapperService mapperService = indicesService.createIndexMapperService(indexMetaData);
        assertNotNull(mapperService.documentMapperParser().parserContext("type").typeParser("fake-mapper"));
        assertThat(mapperService.documentMapperParser().parserContext("type").getSimilarity("test").get(), instanceOf(BM25Similarity.class));
    }

    public void testStatsByShardDoesNotDieFromExpectedExceptions() {
        final int shardCount = randomIntBetween(2, 5);
        final int failedShardId = randomIntBetween(0, shardCount - 1);
        final Index index = new Index("test-index", "abc123");
        final ShardId shardId = new ShardId(index, failedShardId);
        final List<IndexShard> shards = new ArrayList<>(shardCount);
        final List<IndexShardStats> shardStats = new ArrayList<>(shardCount - 1);
        final IndexShardState state = randomFrom(IndexShardState.values());
        final String message = "TEST - expected";
        final RuntimeException expectedException = randomFrom(new IllegalIndexShardStateException(shardId, state, message), new AlreadyClosedException(message));
        final IndicesService mockIndicesService = mock(IndicesService.class);
        final IndexService indexService = mock(IndexService.class);
        for (int i = 0; i < shardCount; ++i) {
            final IndexShard shard = mock(IndexShard.class);
            shards.add(shard);
            if (failedShardId != i) {
                final IndexShardStats successfulShardStats = mock(IndexShardStats.class);
                shardStats.add(successfulShardStats);
                when(mockIndicesService.indexShardStats(mockIndicesService, shard, CommonStatsFlags.ALL)).thenReturn(successfulShardStats);
            } else {
                when(mockIndicesService.indexShardStats(mockIndicesService, shard, CommonStatsFlags.ALL)).thenThrow(expectedException);
            }
        }
        when(mockIndicesService.iterator()).thenReturn(Collections.singleton(indexService).iterator());
        when(indexService.iterator()).thenReturn(shards.iterator());
        when(indexService.index()).thenReturn(index);
        final IndicesService indicesService = getIndicesService();
        final Map<Index, List<IndexShardStats>> indexStats = indicesService.statsByShard(mockIndicesService, CommonStatsFlags.ALL);
        assertThat(indexStats.isEmpty(), equalTo(false));
        assertThat("index not defined", indexStats.containsKey(index), equalTo(true));
        assertThat("unexpected shard stats", indexStats.get(index), equalTo(shardStats));
    }

    public void testIsMetaDataField() {
        IndicesService indicesService = getIndicesService();
        assertFalse(indicesService.isMetaDataField(randomAlphaOfLengthBetween(10, 15)));
        for (String builtIn : IndicesModule.getBuiltInMetaDataFields()) {
            assertTrue(indicesService.isMetaDataField(builtIn));
        }
    }
}