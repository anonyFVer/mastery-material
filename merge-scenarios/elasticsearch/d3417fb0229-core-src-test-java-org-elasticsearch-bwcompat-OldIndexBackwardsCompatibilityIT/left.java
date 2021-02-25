package org.elasticsearch.bwcompat;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.StringFieldMapperPositionIncrementGapTests;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.OldIndexUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Before;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import static org.elasticsearch.test.OldIndexUtils.assertUpgradeWorks;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
@LuceneTestCase.AwaitsFix(bugUrl = "needs a solution for translog operations that are recovered from the translog, don't have a seq no " + "and trigger assertions in Engine.Operation")
public class OldIndexBackwardsCompatibilityIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class);
    }

    List<String> indexes;

    List<String> unsupportedIndexes;

    static String singleDataPathNodeName;

    static String multiDataPathNodeName;

    static Path singleDataPath;

    static Path[] multiDataPath;

    @Before
    public void initIndexesList() throws Exception {
        indexes = OldIndexUtils.loadDataFilesList("index", getBwcIndicesPath());
        unsupportedIndexes = OldIndexUtils.loadDataFilesList("unsupported", getBwcIndicesPath());
    }

    @AfterClass
    public static void tearDownStatics() {
        singleDataPathNodeName = null;
        multiDataPathNodeName = null;
        singleDataPath = null;
        multiDataPath = null;
    }

    @Override
    public Settings nodeSettings(int ord) {
        return OldIndexUtils.getSettings();
    }

    void setupCluster() throws Exception {
        InternalTestCluster.Async<List<String>> replicas = internalCluster().startNodesAsync(1);
        Path baseTempDir = createTempDir();
        Settings.Builder nodeSettings = Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), baseTempDir.resolve("single-path").toAbsolutePath()).put(Node.NODE_MASTER_SETTING.getKey(), false);
        InternalTestCluster.Async<String> singleDataPathNode = internalCluster().startNodeAsync(nodeSettings.build());
        nodeSettings = Settings.builder().put(Environment.PATH_DATA_SETTING.getKey(), baseTempDir.resolve("multi-path1").toAbsolutePath() + "," + baseTempDir.resolve("multi-path2").toAbsolutePath()).put(Node.NODE_MASTER_SETTING.getKey(), false);
        InternalTestCluster.Async<String> multiDataPathNode = internalCluster().startNodeAsync(nodeSettings.build());
        singleDataPathNodeName = singleDataPathNode.get();
        Path[] nodePaths = internalCluster().getInstance(NodeEnvironment.class, singleDataPathNodeName).nodeDataPaths();
        assertEquals(1, nodePaths.length);
        singleDataPath = nodePaths[0].resolve(NodeEnvironment.INDICES_FOLDER);
        assertFalse(Files.exists(singleDataPath));
        Files.createDirectories(singleDataPath);
        logger.info("--> Single data path: {}", singleDataPath);
        multiDataPathNodeName = multiDataPathNode.get();
        nodePaths = internalCluster().getInstance(NodeEnvironment.class, multiDataPathNodeName).nodeDataPaths();
        assertEquals(2, nodePaths.length);
        multiDataPath = new Path[] { nodePaths[0].resolve(NodeEnvironment.INDICES_FOLDER), nodePaths[1].resolve(NodeEnvironment.INDICES_FOLDER) };
        assertFalse(Files.exists(multiDataPath[0]));
        assertFalse(Files.exists(multiDataPath[1]));
        Files.createDirectories(multiDataPath[0]);
        Files.createDirectories(multiDataPath[1]);
        logger.info("--> Multi data paths: {}, {}", multiDataPath[0], multiDataPath[1]);
        replicas.get();
    }

    void upgradeIndexFolder() throws Exception {
        OldIndexUtils.upgradeIndexFolder(internalCluster(), singleDataPathNodeName);
        OldIndexUtils.upgradeIndexFolder(internalCluster(), multiDataPathNodeName);
    }

    void importIndex(String indexName) throws IOException {
        client().admin().cluster().prepareReroute().get();
        ensureGreen(indexName);
    }

    void unloadIndex(String indexName) throws Exception {
        assertAcked(client().admin().indices().prepareDelete(indexName).get());
    }

    public void testAllVersionsTested() throws Exception {
        SortedSet<String> expectedVersions = new TreeSet<>();
        for (Version v : VersionUtils.allVersions()) {
            if (VersionUtils.isSnapshot(v))
                continue;
            if (v.isAlpha())
                continue;
            if (v.onOrBefore(Version.V_2_0_0_beta1))
                continue;
            if (v.equals(Version.CURRENT))
                continue;
            expectedVersions.add("index-" + v.toString() + ".zip");
        }
        for (String index : indexes) {
            if (expectedVersions.remove(index) == false) {
                logger.warn("Old indexes tests contain extra index: {}", index);
            }
        }
        if (expectedVersions.isEmpty() == false) {
            StringBuilder msg = new StringBuilder("Old index tests are missing indexes:");
            for (String expected : expectedVersions) {
                msg.append("\n" + expected);
            }
            fail(msg.toString());
        }
    }

    public void testOldIndexes() throws Exception {
        setupCluster();
        Collections.shuffle(indexes, random());
        for (String index : indexes) {
            long startTime = System.currentTimeMillis();
            logger.info("--> Testing old index {}", index);
            assertOldIndexWorks(index);
            logger.info("--> Done testing {}, took {} seconds", index, (System.currentTimeMillis() - startTime) / 1000.0);
        }
    }

    void assertOldIndexWorks(String index) throws Exception {
        Version version = OldIndexUtils.extractVersion(index);
        Path[] paths;
        if (randomBoolean()) {
            logger.info("--> injecting index [{}] into single data path", index);
            paths = new Path[] { singleDataPath };
        } else {
            logger.info("--> injecting index [{}] into multi data path", index);
            paths = multiDataPath;
        }
        String indexName = index.replace(".zip", "").toLowerCase(Locale.ROOT).replace("unsupported-", "index-");
        OldIndexUtils.loadIndex(indexName, index, createTempDir(), getBwcIndicesPath(), logger, paths);
        upgradeIndexFolder();
        importIndex(indexName);
        assertIndexSanity(indexName, version);
        assertBasicSearchWorks(indexName);
        assertAllSearchWorks(indexName);
        assertBasicAggregationWorks(indexName);
        assertRealtimeGetWorks(indexName);
        assertNewReplicasWork(indexName);
        assertUpgradeWorks(client(), indexName, version);
        assertDeleteByQueryWorked(indexName, version);
        assertPositionIncrementGapDefaults(indexName, version);
        unloadIndex(indexName);
    }

    void assertIndexSanity(String indexName, Version indexCreated) {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().addIndices(indexName).get();
        assertEquals(1, getIndexResponse.indices().length);
        assertEquals(indexName, getIndexResponse.indices()[0]);
        Version actualVersionCreated = Version.indexCreated(getIndexResponse.getSettings().get(indexName));
        assertEquals(indexCreated, actualVersionCreated);
        ensureYellow(indexName);
        RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(indexName).setDetailed(true).setActiveOnly(false).get();
        boolean foundTranslog = false;
        for (List<RecoveryState> states : recoveryResponse.shardRecoveryStates().values()) {
            for (RecoveryState state : states) {
                if (state.getStage() == RecoveryState.Stage.DONE && state.getPrimary() && state.getRecoverySource().getType() == RecoverySource.Type.EXISTING_STORE) {
                    assertFalse("more than one primary recoverd?", foundTranslog);
                    assertNotEquals(0, state.getTranslog().recoveredOperations());
                    foundTranslog = true;
                }
            }
        }
        assertTrue("expected translog but nothing was recovered", foundTranslog);
        IndicesSegmentResponse segmentsResponse = client().admin().indices().prepareSegments(indexName).get();
        IndexSegments segments = segmentsResponse.getIndices().get(indexName);
        int numCurrent = 0;
        int numBWC = 0;
        for (IndexShardSegments indexShardSegments : segments) {
            for (ShardSegments shardSegments : indexShardSegments) {
                for (Segment segment : shardSegments) {
                    if (indexCreated.luceneVersion.equals(segment.version)) {
                        numBWC++;
                        if (Version.CURRENT.luceneVersion.equals(segment.version)) {
                            numCurrent++;
                        }
                    } else if (Version.CURRENT.luceneVersion.equals(segment.version)) {
                        numCurrent++;
                    } else {
                        fail("unexpected version " + segment.version);
                    }
                }
            }
        }
        assertNotEquals("expected at least 1 current segment after translog recovery", 0, numCurrent);
        assertNotEquals("expected at least 1 old segment", 0, numBWC);
        SearchResponse test = client().prepareSearch(indexName).get();
        assertThat(test.getHits().getTotalHits(), greaterThanOrEqualTo(1L));
    }

    void assertBasicSearchWorks(String indexName) {
        logger.info("--> testing basic search");
        SearchRequestBuilder searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery());
        SearchResponse searchRsp = searchReq.get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        long numDocs = searchRsp.getHits().getTotalHits();
        logger.info("Found {} in old index", numDocs);
        logger.info("--> testing basic search with sort");
        searchReq.addSort("long_sort", SortOrder.ASC);
        ElasticsearchAssertions.assertNoFailures(searchReq.get());
        logger.info("--> testing exists filter");
        searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.existsQuery("string"));
        searchRsp = searchReq.get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        assertEquals(numDocs, searchRsp.getHits().getTotalHits());
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings(indexName).get();
        Version versionCreated = Version.fromId(Integer.parseInt(getSettingsResponse.getSetting(indexName, "index.version.created")));
        if (versionCreated.onOrAfter(Version.V_2_4_0)) {
            searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.existsQuery("field.with.dots"));
            searchRsp = searchReq.get();
            ElasticsearchAssertions.assertNoFailures(searchRsp);
            assertEquals(numDocs, searchRsp.getHits().getTotalHits());
        }
    }

    boolean findPayloadBoostInExplanation(Explanation expl) {
        if (expl.getDescription().startsWith("payloadBoost=") && expl.getValue() != 1f) {
            return true;
        } else {
            boolean found = false;
            for (Explanation sub : expl.getDetails()) {
                found |= findPayloadBoostInExplanation(sub);
            }
            return found;
        }
    }

    void assertAllSearchWorks(String indexName) {
        logger.info("--> testing _all search");
        SearchResponse searchRsp = client().prepareSearch(indexName).get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        assertThat(searchRsp.getHits().getTotalHits(), greaterThanOrEqualTo(1L));
        SearchHit bestHit = searchRsp.getHits().getAt(0);
        String stringValue = (String) bestHit.sourceAsMap().get("string");
        assertNotNull(stringValue);
        Explanation explanation = client().prepareExplain(indexName, bestHit.getType(), bestHit.getId()).setQuery(QueryBuilders.matchQuery("_all", stringValue)).get().getExplanation();
        assertTrue("Could not find payload boost in explanation\n" + explanation, findPayloadBoostInExplanation(explanation));
        searchRsp = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("_all", stringValue)).setExplain(true).get();
        ElasticsearchAssertions.assertNoFailures(searchRsp);
        assertThat(searchRsp.getHits().getTotalHits(), greaterThanOrEqualTo(1L));
    }

    void assertBasicAggregationWorks(String indexName) {
        SearchResponse searchRsp = client().prepareSearch(indexName).addAggregation(AggregationBuilders.histogram("histo").field("long_sort").interval(10)).get();
        ElasticsearchAssertions.assertSearchResponse(searchRsp);
        Histogram histo = searchRsp.getAggregations().get("histo");
        assertNotNull(histo);
        long totalCount = 0;
        for (Histogram.Bucket bucket : histo.getBuckets()) {
            totalCount += bucket.getDocCount();
        }
        assertEquals(totalCount, searchRsp.getHits().getTotalHits());
        searchRsp = client().prepareSearch(indexName).addAggregation(AggregationBuilders.terms("bool_terms").field("bool")).get();
        Terms terms = searchRsp.getAggregations().get("bool_terms");
        totalCount = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            totalCount += bucket.getDocCount();
        }
        assertEquals(totalCount, searchRsp.getHits().getTotalHits());
    }

    void assertRealtimeGetWorks(String indexName) {
        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder().put("refresh_interval", -1).build()));
        SearchRequestBuilder searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery());
        SearchHit hit = searchReq.get().getHits().getAt(0);
        String docId = hit.getId();
        client().prepareUpdate(indexName, "doc", docId).setDoc("foo", "bar").get();
        GetResponse getRsp = client().prepareGet(indexName, "doc", docId).get();
        Map<String, Object> source = getRsp.getSourceAsMap();
        assertThat(source, Matchers.hasKey("foo"));
        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder().put("refresh_interval", IndexSettings.DEFAULT_REFRESH_INTERVAL).build()));
    }

    void assertNewReplicasWork(String indexName) throws Exception {
        final int numReplicas = 1;
        final long startTime = System.currentTimeMillis();
        logger.debug("--> creating [{}] replicas for index [{}]", numReplicas, indexName);
        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder().put("number_of_replicas", numReplicas)).execute().actionGet());
        ensureGreen(TimeValue.timeValueMinutes(2), indexName);
        logger.debug("--> index [{}] is green, took [{}]", indexName, TimeValue.timeValueMillis(System.currentTimeMillis() - startTime));
        logger.debug("--> recovery status:\n{}", XContentHelper.toString(client().admin().indices().prepareRecoveries(indexName).get()));
    }

    void assertDeleteByQueryWorked(String indexName, Version version) throws Exception {
        if (version.onOrAfter(Version.V_2_0_0_beta1)) {
            return;
        }
        SearchRequestBuilder searchReq = client().prepareSearch(indexName).setQuery(QueryBuilders.queryStringQuery("long_sort:[10 TO 20]"));
        assertEquals(0, searchReq.get().getHits().getTotalHits());
    }

    void assertPositionIncrementGapDefaults(String indexName, Version version) throws Exception {
        if (version.before(Version.V_2_0_0_beta1)) {
            StringFieldMapperPositionIncrementGapTests.assertGapIsZero(client(), indexName, "doc");
        } else {
            StringFieldMapperPositionIncrementGapTests.assertGapIsOneHundred(client(), indexName, "doc");
        }
    }

    private Path getNodeDir(String indexFile) throws IOException {
        Path unzipDir = createTempDir();
        Path unzipDataDir = unzipDir.resolve("data");
        Path backwardsIndex = getBwcIndicesPath().resolve(indexFile);
        try (InputStream stream = Files.newInputStream(backwardsIndex)) {
            TestUtil.unzip(stream, unzipDir);
        }
        assertTrue(Files.exists(unzipDataDir));
        Path[] list = FileSystemUtils.files(unzipDataDir);
        if (list.length != 1) {
            throw new IllegalStateException("Backwards index must contain exactly one cluster");
        }
        return list[0].resolve("nodes/0/");
    }

    public void testOldClusterStates() throws Exception {
        MetaDataStateFormat<MetaData> globalFormat = new MetaDataStateFormat<MetaData>(XContentType.JSON, "global-") {

            @Override
            public void toXContent(XContentBuilder builder, MetaData state) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public MetaData fromXContent(XContentParser parser) throws IOException {
                return MetaData.Builder.fromXContent(parser);
            }
        };
        MetaDataStateFormat<IndexMetaData> indexFormat = new MetaDataStateFormat<IndexMetaData>(XContentType.JSON, "state-") {

            @Override
            public void toXContent(XContentBuilder builder, IndexMetaData state) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public IndexMetaData fromXContent(XContentParser parser) throws IOException {
                return IndexMetaData.Builder.fromXContent(parser);
            }
        };
        Collections.shuffle(indexes, random());
        for (String indexFile : indexes) {
            String indexName = indexFile.replace(".zip", "").toLowerCase(Locale.ROOT).replace("unsupported-", "index-");
            Path nodeDir = getNodeDir(indexFile);
            logger.info("Parsing cluster state files from index [{}]", indexName);
            assertNotNull(globalFormat.loadLatestState(logger, nodeDir));
            Path indexDir = nodeDir.resolve("indices").resolve(indexName);
            assertNotNull(indexFormat.loadLatestState(logger, indexDir));
        }
    }
}