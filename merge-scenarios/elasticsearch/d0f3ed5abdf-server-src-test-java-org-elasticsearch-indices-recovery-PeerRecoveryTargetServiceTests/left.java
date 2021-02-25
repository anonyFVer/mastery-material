package org.elasticsearch.indices.recovery;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.translog.Translog;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.hamcrest.Matchers.equalTo;

public class PeerRecoveryTargetServiceTests extends IndexShardTestCase {

    public void testGetStartingSeqNo() throws Exception {
        final IndexShard replica = newShard(false);
        try {
            {
                recoveryEmptyReplica(replica);
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(0L));
                recoveryTarget.decRef();
            }
            final long initDocs = scaledRandomIntBetween(1, 10);
            {
                for (int i = 0; i < initDocs; i++) {
                    indexDoc(replica, "_doc", Integer.toString(i));
                    if (randomBoolean()) {
                        flushShard(replica);
                    }
                }
                flushShard(replica);
                replica.updateGlobalCheckpointOnReplica(initDocs - 1, "test");
                replica.sync();
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(initDocs));
                recoveryTarget.decRef();
            }
            final int moreDocs = randomIntBetween(1, 10);
            {
                for (int i = 0; i < moreDocs; i++) {
                    indexDoc(replica, "_doc", Long.toString(i));
                    if (randomBoolean()) {
                        flushShard(replica);
                    }
                }
                flushShard(replica);
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(initDocs));
                recoveryTarget.decRef();
            }
            {
                replica.updateGlobalCheckpointOnReplica(initDocs + moreDocs - 1, "test");
                replica.sync();
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(initDocs + moreDocs));
                recoveryTarget.decRef();
            }
            {
                replica.close("test", false);
                final List<IndexCommit> commits = DirectoryReader.listCommits(replica.store().directory());
                IndexWriterConfig iwc = new IndexWriterConfig(null).setSoftDeletesField(Lucene.SOFT_DELETE_FIELD).setCommitOnClose(false).setMergePolicy(NoMergePolicy.INSTANCE).setOpenMode(IndexWriterConfig.OpenMode.APPEND);
                try (IndexWriter writer = new IndexWriter(replica.store().directory(), iwc)) {
                    final Map<String, String> userData = new HashMap<>(commits.get(commits.size() - 1).getUserData());
                    userData.put(Translog.TRANSLOG_UUID_KEY, UUIDs.randomBase64UUID());
                    writer.setLiveCommitData(userData.entrySet());
                    writer.commit();
                }
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
                recoveryTarget.decRef();
            }
        } finally {
            closeShards(replica);
        }
    }
}