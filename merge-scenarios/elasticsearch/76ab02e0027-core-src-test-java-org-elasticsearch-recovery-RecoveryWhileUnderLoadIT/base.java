package org.elasticsearch.recovery;

import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;

@TestLogging("_root:DEBUG,index.shard:TRACE")
public class RecoveryWhileUnderLoadIT extends ESIntegTestCase {

    private final ESLogger logger = Loggers.getLogger(RecoveryWhileUnderLoadIT.class);

    public void testRecoverWhileUnderLoadAllocateReplicasTest() throws Exception {
        logger.info("--> creating test index ...");
        int numberOfShards = numberOfShards();
        assertAcked(prepareCreate("test", 1, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numberOfShards).put(SETTING_NUMBER_OF_REPLICAS, 1).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)));
        final int totalNumDocs = scaledRandomIntBetween(200, 10000);
        int waitFor = totalNumDocs / 10;
        int extraDocs = waitFor;
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", "type", client(), extraDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);
            extraDocs = totalNumDocs / 10;
            waitFor += extraDocs;
            indexer.continueIndexing(extraDocs);
            logger.info("--> flushing the index ....");
            client().admin().indices().prepareFlush().execute().actionGet();
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);
            extraDocs = totalNumDocs - waitFor;
            indexer.continueIndexing(extraDocs);
            logger.info("--> allow 2 nodes for index [test] ...");
            allowNodes("test", 2);
            logger.info("--> waiting for GREEN health status ...");
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForGreenStatus());
            logger.info("--> waiting for {} docs to be indexed ...", totalNumDocs);
            waitForDocs(totalNumDocs, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", totalNumDocs);
            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stop();
            logger.info("--> indexing threads stopped");
            logger.info("--> refreshing the index");
            refreshAndAssert();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numberOfShards, indexer.totalIndexedDocs(), 10);
        }
    }

    public void testRecoverWhileUnderLoadAllocateReplicasRelocatePrimariesTest() throws Exception {
        logger.info("--> creating test index ...");
        int numberOfShards = numberOfShards();
        assertAcked(prepareCreate("test", 1, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numberOfShards).put(SETTING_NUMBER_OF_REPLICAS, 1).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)));
        final int totalNumDocs = scaledRandomIntBetween(200, 10000);
        int waitFor = totalNumDocs / 10;
        int extraDocs = waitFor;
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", "type", client(), extraDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);
            extraDocs = totalNumDocs / 10;
            waitFor += extraDocs;
            indexer.continueIndexing(extraDocs);
            logger.info("--> flushing the index ....");
            client().admin().indices().prepareFlush().execute().actionGet();
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);
            extraDocs = totalNumDocs - waitFor;
            indexer.continueIndexing(extraDocs);
            logger.info("--> allow 4 nodes for index [test] ...");
            allowNodes("test", 4);
            logger.info("--> waiting for GREEN health status ...");
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForGreenStatus());
            logger.info("--> waiting for {} docs to be indexed ...", totalNumDocs);
            waitForDocs(totalNumDocs, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", totalNumDocs);
            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stop();
            logger.info("--> indexing threads stopped");
            logger.info("--> refreshing the index");
            refreshAndAssert();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numberOfShards, indexer.totalIndexedDocs(), 10);
        }
    }

    public void testRecoverWhileUnderLoadWithReducedAllowedNodes() throws Exception {
        logger.info("--> creating test index ...");
        int numberOfShards = numberOfShards();
        assertAcked(prepareCreate("test", 2, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numberOfShards).put(SETTING_NUMBER_OF_REPLICAS, 1).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)));
        final int totalNumDocs = scaledRandomIntBetween(200, 10000);
        int waitFor = totalNumDocs / 10;
        int extraDocs = waitFor;
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", "type", client(), extraDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);
            extraDocs = totalNumDocs / 10;
            waitFor += extraDocs;
            indexer.continueIndexing(extraDocs);
            logger.info("--> flushing the index ....");
            client().admin().indices().prepareFlush().execute().actionGet();
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);
            extraDocs = totalNumDocs - waitFor;
            indexer.continueIndexing(extraDocs);
            logger.info("--> allow 4 nodes for index [test] ...");
            allowNodes("test", 4);
            logger.info("--> waiting for GREEN health status ...");
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForGreenStatus().setWaitForRelocatingShards(0));
            logger.info("--> waiting for {} docs to be indexed ...", totalNumDocs);
            waitForDocs(totalNumDocs, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", totalNumDocs);
            logger.info("--> allow 3 nodes for index [test] ...");
            allowNodes("test", 3);
            logger.info("--> waiting for relocations ...");
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForRelocatingShards(0));
            logger.info("--> allow 2 nodes for index [test] ...");
            allowNodes("test", 2);
            logger.info("--> waiting for relocations ...");
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForRelocatingShards(0));
            logger.info("--> allow 1 nodes for index [test] ...");
            allowNodes("test", 1);
            logger.info("--> waiting for relocations ...");
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForRelocatingShards(0));
            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stop();
            logger.info("--> indexing threads stopped");
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForRelocatingShards(0));
            logger.info("--> refreshing the index");
            refreshAndAssert();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numberOfShards, indexer.totalIndexedDocs(), 10);
        }
    }

    public void testRecoverWhileRelocating() throws Exception {
        final int numShards = between(2, 5);
        final int numReplicas = 0;
        logger.info("--> creating test index ...");
        int allowNodes = 2;
        assertAcked(prepareCreate("test", 3, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numShards).put(SETTING_NUMBER_OF_REPLICAS, numReplicas).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)));
        final int numDocs = scaledRandomIntBetween(200, 9999);
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", "type", client(), numDocs)) {
            for (int i = 0; i < numDocs; i += scaledRandomIntBetween(100, Math.min(1000, numDocs))) {
                indexer.assertNoFailures();
                logger.info("--> waiting for {} docs to be indexed ...", i);
                waitForDocs(i, indexer);
                logger.info("--> {} docs indexed", i);
                allowNodes = 2 / allowNodes;
                allowNodes("test", allowNodes);
                logger.info("--> waiting for GREEN health status ...");
                ensureGreen(TimeValue.timeValueMinutes(5));
            }
            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stop();
            logger.info("--> indexing threads stopped");
            logger.info("--> bump up number of replicas to 1 and allow all nodes to hold the index");
            allowNodes("test", 3);
            assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put("number_of_replicas", 1)).get());
            ensureGreen(TimeValue.timeValueMinutes(5));
            logger.info("--> refreshing the index");
            refreshAndAssert();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numShards, indexer.totalIndexedDocs(), 10);
        }
    }

    private void iterateAssertCount(final int numberOfShards, final long numberOfDocs, final int iterations) throws Exception {
        SearchResponse[] iterationResults = new SearchResponse[iterations];
        boolean error = false;
        for (int i = 0; i < iterations; i++) {
            SearchResponse searchResponse = client().prepareSearch().setSize((int) numberOfDocs).setQuery(matchAllQuery()).addSort("id", SortOrder.ASC).get();
            logSearchResponse(numberOfShards, numberOfDocs, i, searchResponse);
            iterationResults[i] = searchResponse;
            if (searchResponse.getHits().totalHits() != numberOfDocs) {
                error = true;
            }
        }
        if (error) {
            IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats().get();
            for (ShardStats shardStats : indicesStatsResponse.getShards()) {
                DocsStats docsStats = shardStats.getStats().docs;
                logger.info("shard [{}] - count {}, primary {}", shardStats.getShardRouting().id(), docsStats.getCount(), shardStats.getShardRouting().primary());
            }
            ClusterService clusterService = clusterService();
            final ClusterState state = clusterService.state();
            for (int shard = 0; shard < numberOfShards; shard++) {
                for (int id = 1; id <= numberOfDocs; id++) {
                    ShardId docShard = clusterService.operationRouting().shardId(state, "test", Long.toString(id), null);
                    if (docShard.id() == shard) {
                        for (ShardRouting shardRouting : state.routingTable().shardRoutingTable("test", shard)) {
                            GetResponse response = client().prepareGet("test", "type", Long.toString(id)).setPreference("_only_nodes:" + shardRouting.currentNodeId()).get();
                            if (response.isExists()) {
                                logger.info("missing id [{}] on shard {}", id, shardRouting);
                            }
                        }
                    }
                }
            }
            logger.info("--> trying to wait");
            assertTrue(awaitBusy(() -> {
                boolean errorOccurred = false;
                for (int i = 0; i < iterations; i++) {
                    SearchResponse searchResponse = client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get();
                    if (searchResponse.getHits().totalHits() != numberOfDocs) {
                        errorOccurred = true;
                    }
                }
                return !errorOccurred;
            }, 5, TimeUnit.MINUTES));
        }
        for (int i = 0; i < iterations; i++) {
            assertHitCount(iterationResults[i], numberOfDocs);
        }
    }

    private void logSearchResponse(int numberOfShards, long numberOfDocs, int iteration, SearchResponse searchResponse) {
        logger.info("iteration [{}] - successful shards: {} (expected {})", iteration, searchResponse.getSuccessfulShards(), numberOfShards);
        logger.info("iteration [{}] - failed shards: {} (expected 0)", iteration, searchResponse.getFailedShards());
        if (searchResponse.getShardFailures() != null && searchResponse.getShardFailures().length > 0) {
            logger.info("iteration [{}] - shard failures: {}", iteration, Arrays.toString(searchResponse.getShardFailures()));
        }
        logger.info("iteration [{}] - returned documents: {} (expected {})", iteration, searchResponse.getHits().totalHits(), numberOfDocs);
    }

    private void refreshAndAssert() throws Exception {
        assertBusy(new Runnable() {

            @Override
            public void run() {
                RefreshResponse actionGet = client().admin().indices().prepareRefresh().get();
                assertAllSuccessful(actionGet);
            }
        }, 5, TimeUnit.MINUTES);
    }
}