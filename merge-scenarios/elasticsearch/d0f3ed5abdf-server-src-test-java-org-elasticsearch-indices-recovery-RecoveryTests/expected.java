package org.elasticsearch.indices.recovery;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.replication.ESIndexLevelReplicationTestCase;
import org.elasticsearch.index.replication.RecoveryDuringReplicationTests;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.SnapshotMatchers;
import org.elasticsearch.index.translog.Translog;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class RecoveryTests extends ESIndexLevelReplicationTestCase {

    public void testTranslogHistoryTransferred() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startPrimary();
            int docs = shards.indexDocs(10);
            getTranslog(shards.getPrimary()).rollGeneration();
            shards.flush();
            if (randomBoolean()) {
                docs += shards.indexDocs(10);
            }
            shards.addReplica();
            shards.startAll();
            final IndexShard replica = shards.getReplicas().get(0);
            assertThat(getTranslog(replica).totalOperations(), equalTo(docs));
        }
    }

    public void testRetentionPolicyChangeDuringRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startPrimary();
            shards.indexDocs(10);
            getTranslog(shards.getPrimary()).rollGeneration();
            shards.flush();
            shards.indexDocs(10);
            final IndexShard replica = shards.addReplica();
            final CountDownLatch recoveryBlocked = new CountDownLatch(1);
            final CountDownLatch releaseRecovery = new CountDownLatch(1);
            Future<Void> future = shards.asyncRecoverReplica(replica, (indexShard, node) -> new RecoveryDuringReplicationTests.BlockingTarget(RecoveryState.Stage.TRANSLOG, recoveryBlocked, releaseRecovery, indexShard, node, recoveryListener, logger));
            recoveryBlocked.await();
            IndexMetaData.Builder builder = IndexMetaData.builder(replica.indexSettings().getIndexMetaData());
            builder.settings(Settings.builder().put(replica.indexSettings().getSettings()).put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), "-1").put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), "-1").put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "100b"));
            replica.indexSettings().updateIndexMetaData(builder.build());
            replica.onSettingsChanged();
            releaseRecovery.countDown();
            future.get();
            assertBusy(() -> assertThat(getTranslog(replica).totalOperations(), equalTo(0)));
        }
    }

    public void testRecoveryWithOutOfOrderDelete() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 10).build();
        try (ReplicationGroup shards = createGroup(1, settings)) {
            shards.startAll();
            final IndexShard orgReplica = shards.getReplicas().get(0);
            final String indexName = orgReplica.shardId().getIndexName();
            orgReplica.applyDeleteOperationOnReplica(1, 2, "type", "id");
            orgReplica.flush(new FlushRequest().force(true));
            orgReplica.applyIndexOperationOnReplica(0, 1, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false, SourceToParse.source(indexName, "type", "id", new BytesArray("{}"), XContentType.JSON));
            orgReplica.applyIndexOperationOnReplica(3, 1, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false, SourceToParse.source(indexName, "type", "id-3", new BytesArray("{}"), XContentType.JSON));
            orgReplica.flush(new FlushRequest().force(true).waitIfOngoing(true));
            orgReplica.applyIndexOperationOnReplica(2, 1, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false, SourceToParse.source(indexName, "type", "id-2", new BytesArray("{}"), XContentType.JSON));
            orgReplica.updateGlobalCheckpointOnReplica(3L, "test");
            orgReplica.applyIndexOperationOnReplica(5, 1, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false, SourceToParse.source(indexName, "type", "id-5", new BytesArray("{}"), XContentType.JSON));
            final int translogOps;
            if (randomBoolean()) {
                if (randomBoolean()) {
                    logger.info("--> flushing shard (translog/soft-deletes will be trimmed)");
                    IndexMetaData.Builder builder = IndexMetaData.builder(orgReplica.indexSettings().getIndexMetaData());
                    builder.settings(Settings.builder().put(orgReplica.indexSettings().getSettings()).put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), "-1").put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), "-1").put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 0));
                    orgReplica.indexSettings().updateIndexMetaData(builder.build());
                    orgReplica.onSettingsChanged();
                    translogOps = 5;
                } else {
                    logger.info("--> flushing shard (translog/soft-deletes will be retained)");
                    translogOps = 6;
                }
                flushShard(orgReplica);
            } else {
                translogOps = 6;
            }
            final IndexShard orgPrimary = shards.getPrimary();
            shards.promoteReplicaToPrimary(orgReplica).get();
            IndexShard newReplica = shards.addReplicaWithExistingPath(orgPrimary.shardPath(), orgPrimary.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);
            shards.assertAllEqual(3);
            assertThat(getTranslog(newReplica).totalOperations(), equalTo(translogOps));
        }
    }

    public void testDifferentHistoryUUIDDisablesOPsRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(1)) {
            shards.startAll();
            final int flushedDocs = 10;
            final int nonFlushedDocs = randomIntBetween(0, 10);
            final int numDocs = flushedDocs + nonFlushedDocs;
            shards.indexDocs(flushedDocs);
            shards.flush();
            shards.indexDocs(nonFlushedDocs);
            IndexShard replica = shards.getReplicas().get(0);
            final String historyUUID = replica.getHistoryUUID();
            Translog.TranslogGeneration translogGeneration = getTranslog(replica).getGeneration();
            shards.removeReplica(replica);
            replica.close("test", false);
            IndexWriterConfig iwc = new IndexWriterConfig(null).setCommitOnClose(false).setMergePolicy(NoMergePolicy.INSTANCE).setOpenMode(IndexWriterConfig.OpenMode.APPEND);
            Map<String, String> userData = new HashMap<>(replica.store().readLastCommittedSegmentsInfo().getUserData());
            final String translogUUIDtoUse;
            final long translogGenToUse;
            final String historyUUIDtoUse = UUIDs.randomBase64UUID(random());
            if (randomBoolean()) {
                translogUUIDtoUse = Translog.createEmptyTranslog(replica.shardPath().resolveTranslog(), flushedDocs, replica.shardId(), replica.getPrimaryTerm());
                translogGenToUse = 1;
            } else {
                translogUUIDtoUse = translogGeneration.translogUUID;
                translogGenToUse = translogGeneration.translogFileGeneration;
            }
            try (IndexWriter writer = new IndexWriter(replica.store().directory(), iwc)) {
                userData.put(Engine.HISTORY_UUID_KEY, historyUUIDtoUse);
                userData.put(Translog.TRANSLOG_UUID_KEY, translogUUIDtoUse);
                userData.put(Translog.TRANSLOG_GENERATION_KEY, Long.toString(translogGenToUse));
                writer.setLiveCommitData(userData.entrySet());
                writer.commit();
            }
            replica.store().close();
            IndexShard newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);
            assertThat(newReplica.recoveryState().getIndex().fileDetails(), not(empty()));
            assertThat(getTranslog(newReplica).totalOperations(), equalTo(numDocs));
            assertThat(newReplica.getHistoryUUID(), equalTo(historyUUID));
            assertThat(newReplica.commitStats().getUserData().get(Engine.HISTORY_UUID_KEY), equalTo(historyUUID));
            shards.assertAllEqual(numDocs);
        }
    }

    public void testPeerRecoveryPersistGlobalCheckpoint() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startPrimary();
            final long numDocs = shards.indexDocs(between(1, 100));
            if (randomBoolean()) {
                shards.flush();
            }
            final IndexShard replica = shards.addReplica();
            shards.recoverReplica(replica);
            assertThat(replica.getLastSyncedGlobalCheckpoint(), equalTo(numDocs - 1));
        }
    }

    public void testPeerRecoverySendSafeCommitInFileBased() throws Exception {
        IndexShard primaryShard = newStartedShard(true);
        int numDocs = between(1, 100);
        long globalCheckpoint = 0;
        for (int i = 0; i < numDocs; i++) {
            Engine.IndexResult result = primaryShard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL, SourceToParse.source(primaryShard.shardId().getIndexName(), "_doc", Integer.toString(i), new BytesArray("{}"), XContentType.JSON), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false);
            assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            if (randomBoolean()) {
                globalCheckpoint = randomLongBetween(globalCheckpoint, i);
                primaryShard.updateLocalCheckpointForShard(primaryShard.routingEntry().allocationId().getId(), globalCheckpoint);
                primaryShard.updateGlobalCheckpointForShard(primaryShard.routingEntry().allocationId().getId(), globalCheckpoint);
                primaryShard.flush(new FlushRequest());
            }
        }
        IndexShard replicaShard = newShard(primaryShard.shardId(), false);
        updateMappings(replicaShard, primaryShard.indexSettings().getIndexMetaData());
        recoverReplica(replicaShard, primaryShard, true);
        List<IndexCommit> commits = DirectoryReader.listCommits(replicaShard.store().directory());
        long maxSeqNo = Long.parseLong(commits.get(0).getUserData().get(SequenceNumbers.MAX_SEQ_NO));
        assertThat(maxSeqNo, lessThanOrEqualTo(globalCheckpoint));
        closeShards(primaryShard, replicaShard);
    }

    public void testSequenceBasedRecoveryKeepsTranslog() throws Exception {
        try (ReplicationGroup shards = createGroup(1)) {
            shards.startAll();
            final IndexShard replica = shards.getReplicas().get(0);
            final int initDocs = scaledRandomIntBetween(0, 20);
            int uncommittedDocs = 0;
            for (int i = 0; i < initDocs; i++) {
                shards.indexDocs(1);
                uncommittedDocs++;
                if (randomBoolean()) {
                    shards.syncGlobalCheckpoint();
                    shards.flush();
                    uncommittedDocs = 0;
                }
            }
            shards.removeReplica(replica);
            final int moreDocs = shards.indexDocs(scaledRandomIntBetween(0, 20));
            if (randomBoolean()) {
                shards.flush();
            }
            replica.close("test", randomBoolean());
            replica.store().close();
            final IndexShard newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);
            try (Translog.Snapshot snapshot = getTranslog(newReplica).newSnapshot()) {
                assertThat("Sequence based recovery should keep existing translog", snapshot, SnapshotMatchers.size(initDocs + moreDocs));
            }
            assertThat(newReplica.recoveryState().getTranslog().recoveredOperations(), equalTo(uncommittedDocs + moreDocs));
            assertThat(newReplica.recoveryState().getIndex().fileDetails(), empty());
        }
    }

    public void testShouldFlushAfterPeerRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startAll();
            int numDocs = shards.indexDocs(between(10, 100));
            final long translogSizeOnPrimary = shards.getPrimary().translogStats().getUncommittedSizeInBytes();
            shards.flush();
            final IndexShard replica = shards.addReplica();
            IndexMetaData.Builder builder = IndexMetaData.builder(replica.indexSettings().getIndexMetaData());
            long flushThreshold = RandomNumbers.randomLongBetween(random(), 100, translogSizeOnPrimary);
            builder.settings(Settings.builder().put(replica.indexSettings().getSettings()).put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), flushThreshold + "b"));
            replica.indexSettings().updateIndexMetaData(builder.build());
            replica.onSettingsChanged();
            shards.recoverReplica(replica);
            assertBusy(() -> assertThat(getEngine(replica).shouldPeriodicallyFlush(), equalTo(false)));
            assertThat(getTranslog(replica).totalOperations(), equalTo(numDocs));
            shards.assertAllEqual(numDocs);
        }
    }
}