package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.test.store.MockFSIndexStore;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(numDataNodes = 0, scope = Scope.TEST)
public class RecoveryFromGatewayIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockFSIndexStore.TestPlugin.class, InternalSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(TestZenDiscovery.USE_ZEN2.getKey(), false).build();
    }

    public void testOneNodeRecoverFromGateway() throws Exception {
        internalCluster().startNode();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("appAccountIds").field("type", "text").endObject().endObject().endObject().endObject());
        assertAcked(prepareCreate("test").addMapping("type1", mapping, XContentType.JSON));
        client().prepareIndex("test", "type1", "10990239").setSource(jsonBuilder().startObject().startArray("appAccountIds").value(14).value(179).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "10990473").setSource(jsonBuilder().startObject().startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "10990513").setSource(jsonBuilder().startObject().startArray("appAccountIds").value(14).value(179).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "10990695").setSource(jsonBuilder().startObject().startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();
        client().prepareIndex("test", "type1", "11026351").setSource(jsonBuilder().startObject().startArray("appAccountIds").value(14).endArray().endObject()).execute().actionGet();
        refresh();
        assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);
        ensureYellow("test");
        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);
        internalCluster().fullRestart();
        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);
        client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);
        internalCluster().fullRestart();
        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);
        client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);
    }

    private Map<String, long[]> assertAndCapturePrimaryTerms(Map<String, long[]> previousTerms) {
        if (previousTerms == null) {
            previousTerms = new HashMap<>();
        }
        final Map<String, long[]> result = new HashMap<>();
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        for (ObjectCursor<IndexMetaData> cursor : state.metaData().indices().values()) {
            final IndexMetaData indexMetaData = cursor.value;
            final String index = indexMetaData.getIndex().getName();
            final long[] previous = previousTerms.get(index);
            final long[] current = IntStream.range(0, indexMetaData.getNumberOfShards()).mapToLong(indexMetaData::primaryTerm).toArray();
            if (previous == null) {
                result.put(index, current);
            } else {
                assertThat("number of terms changed for index [" + index + "]", current.length, equalTo(previous.length));
                for (int shard = 0; shard < current.length; shard++) {
                    assertThat("primary term didn't increase for [" + index + "][" + shard + "]", current[shard], greaterThan(previous[shard]));
                }
                result.put(index, current);
            }
        }
        return result;
    }

    public void testSingleNodeNoFlush() throws Exception {
        internalCluster().startNode();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field").field("type", "text").endObject().startObject("num").field("type", "integer").endObject().endObject().endObject().endObject());
        int numberOfShards = numberOfShards();
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numberOfShards()).put(SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 1))).addMapping("type1", mapping, XContentType.JSON));
        int value1Docs;
        int value2Docs;
        boolean indexToAllShards = randomBoolean();
        if (indexToAllShards) {
            value1Docs = randomIntBetween(numberOfShards * 10, numberOfShards * 20);
            value2Docs = randomIntBetween(numberOfShards * 10, numberOfShards * 20);
        } else {
            value1Docs = 1;
            value2Docs = 1;
        }
        for (int i = 0; i < 1 + randomInt(100); i++) {
            for (int id = 0; id < Math.max(value1Docs, value2Docs); id++) {
                if (id < value1Docs) {
                    index("test", "type1", "1_" + id, jsonBuilder().startObject().field("field", "value1").startArray("num").value(14).value(179).endArray().endObject());
                }
                if (id < value2Docs) {
                    index("test", "type1", "2_" + id, jsonBuilder().startObject().field("field", "value2").startArray("num").value(14).endArray().endObject());
                }
            }
        }
        refresh();
        for (int i = 0; i <= randomInt(10); i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), value1Docs + value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value1")).get(), value1Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value2")).get(), value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("num", 179)).get(), value1Docs);
        }
        if (!indexToAllShards) {
            logger.info("Ensure all primaries have been started");
            ensureYellow();
        }
        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);
        internalCluster().fullRestart();
        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);
        for (int i = 0; i <= randomInt(10); i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), value1Docs + value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value1")).get(), value1Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value2")).get(), value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("num", 179)).get(), value1Docs);
        }
        internalCluster().fullRestart();
        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);
        for (int i = 0; i <= randomInt(10); i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), value1Docs + value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value1")).get(), value1Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value2")).get(), value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("num", 179)).get(), value1Docs);
        }
    }

    public void testSingleNodeWithFlush() throws Exception {
        internalCluster().startNode();
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute().actionGet();
        flush();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute().actionGet();
        refresh();
        assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        ensureYellow("test");
        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);
        internalCluster().fullRestart();
        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }
        internalCluster().fullRestart();
        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }
    }

    public void testTwoNodeFirstNodeCleared() throws Exception {
        final String firstNode = internalCluster().startNode();
        internalCluster().startNode();
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute().actionGet();
        flush();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute().actionGet();
        refresh();
        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }
        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);
        internalCluster().fullRestart(new RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return Settings.builder().put("gateway.recover_after_nodes", 2).build();
            }

            @Override
            public boolean clearData(String nodeName) {
                return firstNode.equals(nodeName);
            }
        });
        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureGreen();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }
    }

    public void testLatestVersionLoaded() throws Exception {
        internalCluster().startNodes(2, Settings.builder().put("gateway.recover_after_nodes", 2).build());
        assertAcked(client().admin().indices().prepareCreate("test"));
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).execute().actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> running cluster_health (wait for the shards to startup)");
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }
        String metaDataUuid = client().admin().cluster().prepareState().execute().get().getState().getMetaData().clusterUUID();
        assertThat(metaDataUuid, not(equalTo("_na_")));
        logger.info("--> closing first node, and indexing more data to the second node");
        internalCluster().stopRandomDataNode();
        logger.info("--> one node is closed - start indexing data into the second one");
        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject().field("field", "value3").endObject()).execute().actionGet();
        logger.info("--> refreshing all indices after indexing is complete");
        client().admin().indices().prepareRefresh().execute().actionGet();
        logger.info("--> checking if documents exist, there should be 3");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 3);
        }
        logger.info("--> add some metadata and additional template");
        client().admin().indices().preparePutTemplate("template_1").setTemplate("te*").setOrder(0).addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties").startObject("field1").field("type", "text").field("store", true).endObject().startObject("field2").field("type", "keyword").field("store", true).endObject().endObject().endObject().endObject()).execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test", "test_alias", QueryBuilders.termQuery("field", "value")).execute().actionGet();
        logger.info("--> stopping the second node");
        internalCluster().stopRandomDataNode();
        logger.info("--> starting the two nodes back");
        internalCluster().startNodes(2, Settings.builder().put("gateway.recover_after_nodes", 2).build());
        logger.info("--> running cluster_health (wait for the shards to startup)");
        ensureGreen();
        assertThat(client().admin().cluster().prepareState().execute().get().getState().getMetaData().clusterUUID(), equalTo(metaDataUuid));
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 3);
        }
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metaData().templates().get("template_1").patterns(), equalTo(Collections.singletonList("te*")));
        assertThat(state.metaData().index("test").getAliases().get("test_alias"), notNullValue());
        assertThat(state.metaData().index("test").getAliases().get("test_alias").filter(), notNullValue());
    }

    public void testReuseInFileBasedPeerRecovery() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String primaryNode = internalCluster().startDataOnlyNode(nodeSettings(0));
        client(primaryNode).admin().indices().prepareCreate("test").setSettings(Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 1).put(MergePolicyConfig.INDEX_MERGE_ENABLED, "false")).get();
        logger.info("--> indexing docs");
        int numDocs = randomIntBetween(1, 1024);
        for (int i = 0; i < numDocs; i++) {
            client(primaryNode).prepareIndex("test", "type").setSource("field", "value").execute().actionGet();
        }
        client(primaryNode).admin().indices().prepareFlush("test").setForce(true).get();
        final String replicaNode = internalCluster().startDataOnlyNode(nodeSettings(1));
        ensureGreen();
        final RecoveryResponse initialRecoveryReponse = client().admin().indices().prepareRecoveries("test").get();
        final Set<String> files = new HashSet<>();
        for (final RecoveryState recoveryState : initialRecoveryReponse.shardRecoveryStates().get("test")) {
            if (recoveryState.getTargetNode().getName().equals(replicaNode)) {
                for (final RecoveryState.File file : recoveryState.getIndex().fileDetails()) {
                    files.add(file.name());
                }
                break;
            }
        }
        logger.info("--> restart replica node");
        boolean softDeleteEnabled = internalCluster().getInstance(IndicesService.class, primaryNode).indexServiceSafe(resolveIndex("test")).getShard(0).indexSettings().isSoftDeleteEnabled();
        int moreDocs = randomIntBetween(1, 1024);
        internalCluster().restartNode(replicaNode, new RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                for (int i = 0; i < moreDocs; i++) {
                    client(primaryNode).prepareIndex("test", "type").setSource("field", "value").execute().actionGet();
                }
                client(primaryNode).admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), "-1").put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), "-1").put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 0)).get();
                client(primaryNode).admin().indices().prepareFlush("test").setForce(true).get();
                if (softDeleteEnabled) {
                    client(primaryNode).admin().indices().prepareFlush("test").setForce(true).get();
                }
                return super.onNodeStopped(nodeName);
            }
        });
        ensureGreen();
        final RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries("test").get();
        for (final RecoveryState recoveryState : recoveryResponse.shardRecoveryStates().get("test")) {
            long recovered = 0;
            long reused = 0;
            int filesRecovered = 0;
            int filesReused = 0;
            for (final RecoveryState.File file : recoveryState.getIndex().fileDetails()) {
                if (files.contains(file.name()) == false) {
                    recovered += file.length();
                    filesRecovered++;
                } else {
                    reused += file.length();
                    filesReused++;
                }
            }
            if (recoveryState.getPrimary()) {
                assertThat(recoveryState.getIndex().recoveredBytes(), equalTo(0L));
                assertThat(recoveryState.getIndex().reusedBytes(), equalTo(recoveryState.getIndex().totalBytes()));
                assertThat(recoveryState.getIndex().recoveredFileCount(), equalTo(0));
                assertThat(recoveryState.getIndex().reusedFileCount(), equalTo(recoveryState.getIndex().totalFileCount()));
            } else {
                logger.info("--> replica shard {} recovered from {} to {}, recovered {}, reuse {}", recoveryState.getShardId().getId(), recoveryState.getSourceNode().getName(), recoveryState.getTargetNode().getName(), recoveryState.getIndex().recoveredBytes(), recoveryState.getIndex().reusedBytes());
                assertThat("bytes should have been recovered", recoveryState.getIndex().recoveredBytes(), equalTo(recovered));
                assertThat("data should have been reused", recoveryState.getIndex().reusedBytes(), greaterThan(0L));
                assertThat("all existing files should be reused, byte count mismatch", recoveryState.getIndex().reusedBytes(), equalTo(reused));
                assertThat(recoveryState.getIndex().reusedBytes(), equalTo(recoveryState.getIndex().totalBytes() - recovered));
                assertThat("the segment from the last round of indexing should be recovered", recoveryState.getIndex().recoveredFileCount(), equalTo(filesRecovered));
                assertThat("all existing files should be reused, file count mismatch", recoveryState.getIndex().reusedFileCount(), equalTo(filesReused));
                assertThat(recoveryState.getIndex().reusedFileCount(), equalTo(recoveryState.getIndex().totalFileCount() - filesRecovered));
                assertThat("> 0 files should be reused", recoveryState.getIndex().reusedFileCount(), greaterThan(0));
                assertThat("no translog ops should be recovered", recoveryState.getTranslog().recoveredOperations(), equalTo(0));
            }
        }
    }

    public void assertSyncIdsNotNull() {
        IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNotNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }
    }

    public void testRecoveryDifferentNodeOrderStartup() throws Exception {
        final Path pathNode1 = createTempDir();
        final String node_1 = internalCluster().startNode(Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), pathNode1).build());
        client().prepareIndex("test", "type1", "1").setSource("field", "value").execute().actionGet();
        final Path pathNode2 = createTempDir();
        final String node_2 = internalCluster().startNode(Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), pathNode2).build());
        ensureGreen();
        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);
        if (randomBoolean()) {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node_1));
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node_2));
        } else {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node_2));
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node_1));
        }
        internalCluster().startNode(Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), pathNode2).build());
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);
        assertThat(client().admin().indices().prepareExists("test").execute().actionGet().isExists(), equalTo(true));
        assertHitCount(client().prepareSearch("test").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet(), 1);
    }

    public void testStartedShardFoundIfStateNotYetProcessed() throws Exception {
        final String nodeName = internalCluster().startNode();
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1)));
        final Index index = resolveIndex("test");
        final ShardId shardId = new ShardId(index, 0);
        index("test", "type", "1");
        flush("test");
        final boolean corrupt = randomBoolean();
        internalCluster().fullRestart(new RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return Settings.builder().put(GatewayService.RECOVER_AFTER_NODES_SETTING.getKey(), 2).build();
            }
        });
        if (corrupt) {
            for (Path path : internalCluster().getInstance(NodeEnvironment.class, nodeName).availableShardPaths(shardId)) {
                final Path indexPath = path.resolve(ShardPath.INDEX_FOLDER_NAME);
                if (Files.exists(indexPath)) {
                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexPath)) {
                        for (Path item : stream) {
                            if (item.getFileName().toString().startsWith("segments_")) {
                                logger.debug("--> deleting [{}]", item);
                                Files.delete(item);
                            }
                        }
                    }
                }
            }
        }
        DiscoveryNode node = internalCluster().getInstance(ClusterService.class, nodeName).localNode();
        TransportNodesListGatewayStartedShards.NodesGatewayStartedShards response;
        response = ActionTestUtils.executeBlocking(internalCluster().getInstance(TransportNodesListGatewayStartedShards.class), new TransportNodesListGatewayStartedShards.Request(shardId, new DiscoveryNode[] { node }));
        assertThat(response.getNodes(), hasSize(1));
        assertThat(response.getNodes().get(0).allocationId(), notNullValue());
        if (corrupt) {
            assertThat(response.getNodes().get(0).storeException(), notNullValue());
        } else {
            assertThat(response.getNodes().get(0).storeException(), nullValue());
        }
        internalCluster().startNode();
    }
}