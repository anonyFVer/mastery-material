package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import static org.elasticsearch.cluster.routing.RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE;

public class BlobStoreRepositoryRestoreTests extends IndexShardTestCase {

    public void testRestoreSnapshotWithExistingFiles() throws IOException {
        final IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        final ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 0);
        IndexShard shard = newShard(shardId, true);
        try {
            final int numDocs = scaledRandomIntBetween(1, 500);
            recoverShardFromStore(shard);
            for (int i = 0; i < numDocs; i++) {
                indexDoc(shard, "doc", Integer.toString(i));
                if (rarely()) {
                    flushShard(shard, false);
                }
            }
            assertDocCount(shard, numDocs);
            final Repository repository = createRepository();
            final Snapshot snapshot = new Snapshot(repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "_uuid"));
            snapshotShard(shard, snapshot, repository);
            final Store.MetadataSnapshot storeFiles = shard.snapshotStoreMetadata();
            assertFalse(storeFiles.asMap().isEmpty());
            closeShards(shard);
            List<String> deletedFiles = randomSubsetOf(randomIntBetween(1, storeFiles.size() - 1), storeFiles.asMap().keySet());
            for (String deletedFile : deletedFiles) {
                Files.delete(shard.shardPath().resolveIndex().resolve(deletedFile));
            }
            ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(shard.routingEntry(), EXISTING_STORE_INSTANCE);
            shard = newShard(shardRouting, shard.shardPath(), shard.indexSettings().getIndexMetaData(), null, new InternalEngineFactory(), () -> {
            });
            recoverShardFromSnapshot(shard, snapshot, repository);
            TestUtil.checkIndex(shard.store().directory());
            final Directory directory = shard.store().directory();
            final List<String> directoryFiles = Arrays.asList(directory.listAll());
            for (StoreFileMetaData storeFile : storeFiles) {
                String fileName = storeFile.name();
                assertTrue("File [" + fileName + "] does not exist in store directory", directoryFiles.contains(fileName));
                assertEquals(storeFile.length(), shard.store().directory().fileLength(fileName));
            }
        } finally {
            if (shard != null && shard.state() != IndexShardState.CLOSED) {
                try {
                    shard.close("test", false);
                } finally {
                    IOUtils.close(shard.store());
                }
            }
        }
    }

    private Repository createRepository() throws IOException {
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        return new FsRepository(repositoryMetaData, createEnvironment(), xContentRegistry());
    }

    private Environment createEnvironment() {
        Path home = createTempDir();
        return TestEnvironment.newEnvironment(Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home.toAbsolutePath()).put(Environment.PATH_REPO_SETTING.getKey(), home.resolve("repo").toAbsolutePath()).build());
    }
}