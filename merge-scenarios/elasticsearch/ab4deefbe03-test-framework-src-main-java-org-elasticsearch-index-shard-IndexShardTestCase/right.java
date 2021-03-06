package org.elasticsearch.index.shard;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.query.DisabledQueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoverySourceHandler;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.indices.recovery.StartRecoveryRequest;
import org.elasticsearch.node.Node;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

public abstract class IndexShardTestCase extends ESTestCase {

    public static final IndexEventListener EMPTY_EVENT_LISTENER = new IndexEventListener() {
    };

    private static final AtomicBoolean failOnShardFailures = new AtomicBoolean(true);

    private static final Consumer<IndexShard.ShardFailure> DEFAULT_SHARD_FAILURE_HANDLER = failure -> {
        if (failOnShardFailures.get()) {
            throw new AssertionError(failure.reason, failure.cause);
        }
    };

    protected static final PeerRecoveryTargetService.RecoveryListener recoveryListener = new PeerRecoveryTargetService.RecoveryListener() {

        @Override
        public void onRecoveryDone(RecoveryState state) {
        }

        @Override
        public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
            throw new AssertionError(e);
        }
    };

    protected ThreadPool threadPool;

    private long primaryTerm;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName(), threadPoolSettings());
        primaryTerm = randomIntBetween(1, 100);
        failOnShardFailures.set(true);
    }

    @Override
    public void tearDown() throws Exception {
        try {
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        } finally {
            super.tearDown();
        }
    }

    protected void allowShardFailures() {
        failOnShardFailures.set(false);
    }

    public Settings threadPoolSettings() {
        return Settings.EMPTY;
    }

    protected Store createStore(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        return createStore(shardPath.getShardId(), indexSettings, newFSDirectory(shardPath.resolveIndex()));
    }

    protected Store createStore(ShardId shardId, IndexSettings indexSettings, Directory directory) throws IOException {
        final DirectoryService directoryService = new DirectoryService(shardId, indexSettings) {

            @Override
            public Directory newDirectory() throws IOException {
                return directory;
            }
        };
        return new Store(shardId, indexSettings, directoryService, new DummyShardLock(shardId));
    }

    protected IndexShard newShard(boolean primary) throws IOException {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(new ShardId("index", "_na_", 0), randomAlphaOfLength(10), primary, ShardRoutingState.INITIALIZING, primary ? RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE);
        return newShard(shardRouting);
    }

    protected IndexShard newShard(final ShardRouting shardRouting, final IndexingOperationListener... listeners) throws IOException {
        assert shardRouting.initializing() : shardRouting;
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).build();
        IndexMetaData.Builder metaData = IndexMetaData.builder(shardRouting.getIndexName()).settings(settings).primaryTerm(0, primaryTerm).putMapping("_doc", "{ \"properties\": {} }");
        return newShard(shardRouting, metaData.build(), listeners);
    }

    protected IndexShard newShard(ShardId shardId, boolean primary, IndexingOperationListener... listeners) throws IOException {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(5), primary, ShardRoutingState.INITIALIZING, primary ? RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE);
        return newShard(shardRouting, listeners);
    }

    protected IndexShard newShard(ShardId shardId, boolean primary, String nodeId, IndexMetaData indexMetaData, @Nullable IndexSearcherWrapper searcherWrapper) throws IOException {
        return newShard(shardId, primary, nodeId, indexMetaData, searcherWrapper, () -> {
        });
    }

    protected IndexShard newShard(ShardId shardId, boolean primary, String nodeId, IndexMetaData indexMetaData, @Nullable IndexSearcherWrapper searcherWrapper, Runnable globalCheckpointSyncer) throws IOException {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, nodeId, primary, ShardRoutingState.INITIALIZING, primary ? RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE);
        return newShard(shardRouting, indexMetaData, searcherWrapper, new InternalEngineFactory(), globalCheckpointSyncer);
    }

    protected IndexShard newShard(ShardRouting routing, IndexMetaData indexMetaData, IndexingOperationListener... listeners) throws IOException {
        return newShard(routing, indexMetaData, null, new InternalEngineFactory(), () -> {
        }, listeners);
    }

    protected IndexShard newShard(ShardRouting routing, IndexMetaData indexMetaData, @Nullable IndexSearcherWrapper indexSearcherWrapper, @Nullable EngineFactory engineFactory, Runnable globalCheckpointSyncer, IndexingOperationListener... listeners) throws IOException {
        final ShardId shardId = routing.shardId();
        final NodeEnvironment.NodePath nodePath = new NodeEnvironment.NodePath(createTempDir());
        ShardPath shardPath = new ShardPath(false, nodePath.resolve(shardId), nodePath.resolve(shardId), shardId);
        return newShard(routing, shardPath, indexMetaData, null, indexSearcherWrapper, engineFactory, globalCheckpointSyncer, EMPTY_EVENT_LISTENER, listeners);
    }

    protected IndexShard newShard(ShardRouting routing, ShardPath shardPath, IndexMetaData indexMetaData, @Nullable Store store, @Nullable IndexSearcherWrapper indexSearcherWrapper, @Nullable EngineFactory engineFactory, Runnable globalCheckpointSyncer, IndexEventListener indexEventListener, IndexingOperationListener... listeners) throws IOException {
        final Settings nodeSettings = Settings.builder().put("node.name", routing.currentNodeId()).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetaData, nodeSettings);
        final IndexShard indexShard;
        if (store == null) {
            store = createStore(indexSettings, shardPath);
        }
        boolean success = false;
        try {
            IndexCache indexCache = new IndexCache(indexSettings, new DisabledQueryCache(indexSettings), null);
            MapperService mapperService = MapperTestUtils.newMapperService(xContentRegistry(), createTempDir(), indexSettings.getSettings(), "index");
            mapperService.merge(indexMetaData, MapperService.MergeReason.MAPPING_RECOVERY);
            SimilarityService similarityService = new SimilarityService(indexSettings, null, Collections.emptyMap());
            final Engine.Warmer warmer = searcher -> {
            };
            ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            CircuitBreakerService breakerService = new HierarchyCircuitBreakerService(nodeSettings, clusterSettings);
            indexShard = new IndexShard(routing, indexSettings, shardPath, store, () -> null, indexCache, mapperService, similarityService, engineFactory, indexEventListener, indexSearcherWrapper, threadPool, BigArrays.NON_RECYCLING_INSTANCE, warmer, Collections.emptyList(), Arrays.asList(listeners), globalCheckpointSyncer, breakerService);
            indexShard.addShardFailureCallback(DEFAULT_SHARD_FAILURE_HANDLER);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.close(store);
            }
        }
        return indexShard;
    }

    protected IndexShard reinitShard(IndexShard current, IndexingOperationListener... listeners) throws IOException {
        final ShardRouting shardRouting = current.routingEntry();
        return reinitShard(current, ShardRoutingHelper.initWithSameId(shardRouting, shardRouting.primary() ? RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE), listeners);
    }

    protected IndexShard reinitShard(IndexShard current, ShardRouting routing, IndexingOperationListener... listeners) throws IOException {
        closeShards(current);
        return newShard(routing, current.shardPath(), current.indexSettings().getIndexMetaData(), null, null, current.engineFactory, current.getGlobalCheckpointSyncer(), EMPTY_EVENT_LISTENER, listeners);
    }

    protected IndexShard newStartedShard() throws IOException {
        return newStartedShard(randomBoolean());
    }

    protected IndexShard newStartedShard(boolean primary) throws IOException {
        IndexShard shard = newShard(primary);
        if (primary) {
            recoverShardFromStore(shard);
        } else {
            recoveryEmptyReplica(shard, true);
        }
        return shard;
    }

    protected void closeShards(IndexShard... shards) throws IOException {
        closeShards(Arrays.asList(shards));
    }

    protected void closeShards(Iterable<IndexShard> shards) throws IOException {
        for (IndexShard shard : shards) {
            if (shard != null) {
                try {
                    shard.close("test", false);
                } finally {
                    IOUtils.close(shard.store());
                }
            }
        }
    }

    protected void recoverShardFromStore(IndexShard primary) throws IOException {
        primary.markAsRecovering("store", new RecoveryState(primary.routingEntry(), getFakeDiscoNode(primary.routingEntry().currentNodeId()), null));
        primary.recoverFromStore();
        updateRoutingEntry(primary, ShardRoutingHelper.moveToStarted(primary.routingEntry()));
    }

    protected static AtomicLong currentClusterStateVersion = new AtomicLong();

    public static void updateRoutingEntry(IndexShard shard, ShardRouting shardRouting) throws IOException {
        Set<String> inSyncIds = shardRouting.active() ? Collections.singleton(shardRouting.allocationId().getId()) : Collections.emptySet();
        IndexShardRoutingTable newRoutingTable = new IndexShardRoutingTable.Builder(shardRouting.shardId()).addShard(shardRouting).build();
        shard.updateShardState(shardRouting, shard.getPrimaryTerm(), null, currentClusterStateVersion.incrementAndGet(), inSyncIds, newRoutingTable, Collections.emptySet());
    }

    protected void recoveryEmptyReplica(IndexShard replica, boolean startReplica) throws IOException {
        IndexShard primary = null;
        try {
            primary = newStartedShard(true);
            recoverReplica(replica, primary, startReplica);
        } finally {
            closeShards(primary);
        }
    }

    protected DiscoveryNode getFakeDiscoNode(String id) {
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), Collections.emptyMap(), EnumSet.allOf(DiscoveryNode.Role.class), Version.CURRENT);
    }

    protected void recoverReplica(IndexShard replica, IndexShard primary, boolean startReplica) throws IOException {
        recoverReplica(replica, primary, (r, sourceNode) -> new RecoveryTarget(r, sourceNode, recoveryListener, version -> {
        }), true, true);
    }

    protected void recoverReplica(final IndexShard replica, final IndexShard primary, final BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier, final boolean markAsRecovering, final boolean markAsStarted) throws IOException {
        IndexShardRoutingTable.Builder newRoutingTable = new IndexShardRoutingTable.Builder(replica.shardId());
        newRoutingTable.addShard(primary.routingEntry());
        if (replica.routingEntry().isRelocationTarget() == false) {
            newRoutingTable.addShard(replica.routingEntry());
        }
        final Set<String> inSyncIds = Collections.singleton(primary.routingEntry().allocationId().getId());
        final IndexShardRoutingTable routingTable = newRoutingTable.build();
        recoverUnstartedReplica(replica, primary, targetSupplier, markAsRecovering, inSyncIds, routingTable);
        if (markAsStarted) {
            startReplicaAfterRecovery(replica, primary, inSyncIds, routingTable);
        }
    }

    protected final void recoverUnstartedReplica(final IndexShard replica, final IndexShard primary, final BiFunction<IndexShard, DiscoveryNode, RecoveryTarget> targetSupplier, final boolean markAsRecovering, final Set<String> inSyncIds, final IndexShardRoutingTable routingTable) throws IOException {
        final DiscoveryNode pNode = getFakeDiscoNode(primary.routingEntry().currentNodeId());
        final DiscoveryNode rNode = getFakeDiscoNode(replica.routingEntry().currentNodeId());
        if (markAsRecovering) {
            replica.markAsRecovering("remote", new RecoveryState(replica.routingEntry(), pNode, rNode));
        } else {
            assertEquals(replica.state(), IndexShardState.RECOVERING);
        }
        replica.prepareForIndexRecovery();
        final RecoveryTarget recoveryTarget = targetSupplier.apply(replica, pNode);
        final String targetAllocationId = recoveryTarget.indexShard().routingEntry().allocationId().getId();
        final Store.MetadataSnapshot snapshot = getMetadataSnapshotOrEmpty(replica);
        final long startingSeqNo;
        if (snapshot.size() > 0) {
            startingSeqNo = PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget);
        } else {
            startingSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        }
        final StartRecoveryRequest request = new StartRecoveryRequest(replica.shardId(), targetAllocationId, pNode, rNode, snapshot, replica.routingEntry().primary(), 0, startingSeqNo);
        final RecoverySourceHandler recovery = new RecoverySourceHandler(primary, recoveryTarget, request, (int) ByteSizeUnit.MB.toBytes(1), Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), pNode.getName()).build());
        primary.updateShardState(primary.routingEntry(), primary.getPrimaryTerm(), null, currentClusterStateVersion.incrementAndGet(), inSyncIds, routingTable, Collections.emptySet());
        recovery.recoverToTarget();
        recoveryTarget.markAsDone();
    }

    protected void startReplicaAfterRecovery(IndexShard replica, IndexShard primary, Set<String> inSyncIds, IndexShardRoutingTable routingTable) throws IOException {
        ShardRouting initializingReplicaRouting = replica.routingEntry();
        IndexShardRoutingTable newRoutingTable = initializingReplicaRouting.isRelocationTarget() ? new IndexShardRoutingTable.Builder(routingTable).removeShard(primary.routingEntry()).addShard(replica.routingEntry()).build() : new IndexShardRoutingTable.Builder(routingTable).removeShard(initializingReplicaRouting).addShard(replica.routingEntry()).build();
        Set<String> inSyncIdsWithReplica = new HashSet<>(inSyncIds);
        inSyncIdsWithReplica.add(replica.routingEntry().allocationId().getId());
        primary.updateShardState(primary.routingEntry(), primary.getPrimaryTerm(), null, currentClusterStateVersion.incrementAndGet(), inSyncIdsWithReplica, newRoutingTable, Collections.emptySet());
        replica.updateShardState(replica.routingEntry().moveToStarted(), replica.getPrimaryTerm(), null, currentClusterStateVersion.get(), inSyncIdsWithReplica, newRoutingTable, Collections.emptySet());
    }

    protected void promoteReplica(IndexShard replica, Set<String> inSyncIds, IndexShardRoutingTable routingTable) throws IOException {
        assertThat(inSyncIds, contains(replica.routingEntry().allocationId().getId()));
        final ShardRouting routingEntry = newShardRouting(replica.routingEntry().shardId(), replica.routingEntry().currentNodeId(), null, true, ShardRoutingState.STARTED, replica.routingEntry().allocationId());
        final IndexShardRoutingTable newRoutingTable = new IndexShardRoutingTable.Builder(routingTable).removeShard(replica.routingEntry()).addShard(routingEntry).build();
        replica.updateShardState(routingEntry, replica.getPrimaryTerm() + 1, (is, listener) -> listener.onResponse(new PrimaryReplicaSyncer.ResyncTask(1, "type", "action", "desc", null, Collections.emptyMap())), currentClusterStateVersion.incrementAndGet(), inSyncIds, newRoutingTable, Collections.emptySet());
    }

    private Store.MetadataSnapshot getMetadataSnapshotOrEmpty(IndexShard replica) throws IOException {
        Store.MetadataSnapshot result;
        try {
            result = replica.snapshotStoreMetadata();
        } catch (IndexNotFoundException e) {
            result = Store.MetadataSnapshot.EMPTY;
        } catch (IOException e) {
            logger.warn("failed read store, treating as empty", e);
            result = Store.MetadataSnapshot.EMPTY;
        }
        return result;
    }

    protected Set<String> getShardDocUIDs(final IndexShard shard) throws IOException {
        shard.refresh("get_uids");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            Set<String> ids = new HashSet<>();
            for (LeafReaderContext leafContext : searcher.reader().leaves()) {
                LeafReader reader = leafContext.reader();
                Bits liveDocs = reader.getLiveDocs();
                for (int i = 0; i < reader.maxDoc(); i++) {
                    if (liveDocs == null || liveDocs.get(i)) {
                        Document uuid = reader.document(i, Collections.singleton(IdFieldMapper.NAME));
                        BytesRef binaryID = uuid.getBinaryValue(IdFieldMapper.NAME);
                        ids.add(Uid.decodeId(Arrays.copyOfRange(binaryID.bytes, binaryID.offset, binaryID.offset + binaryID.length)));
                    }
                }
            }
            return ids;
        }
    }

    protected void assertDocCount(IndexShard shard, int docDount) throws IOException {
        assertThat(getShardDocUIDs(shard), hasSize(docDount));
    }

    protected void assertDocs(IndexShard shard, String... ids) throws IOException {
        final Set<String> shardDocUIDs = getShardDocUIDs(shard);
        assertThat(shardDocUIDs, contains(ids));
        assertThat(shardDocUIDs, hasSize(ids.length));
    }

    protected Engine.IndexResult indexDoc(IndexShard shard, String type, String id) throws IOException {
        return indexDoc(shard, type, id, "{}");
    }

    protected Engine.IndexResult indexDoc(IndexShard shard, String type, String id, String source) throws IOException {
        return indexDoc(shard, type, id, source, XContentType.JSON, null);
    }

    protected Engine.IndexResult indexDoc(IndexShard shard, String type, String id, String source, XContentType xContentType, String routing) throws IOException {
        SourceToParse sourceToParse = SourceToParse.source(shard.shardId().getIndexName(), type, id, new BytesArray(source), xContentType);
        sourceToParse.routing(routing);
        Engine.IndexResult result;
        if (shard.routingEntry().primary()) {
            result = shard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL, sourceToParse, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false);
            if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                updateMappings(shard, IndexMetaData.builder(shard.indexSettings().getIndexMetaData()).putMapping(type, result.getRequiredMappingUpdate().toString()).build());
                result = shard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL, sourceToParse, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false);
            }
            shard.updateLocalCheckpointForShard(shard.routingEntry().allocationId().getId(), shard.getLocalCheckpoint());
        } else {
            result = shard.applyIndexOperationOnReplica(shard.seqNoStats().getMaxSeqNo() + 1, 0, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false, sourceToParse);
            if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                throw new TransportReplicationAction.RetryOnReplicaException(shard.shardId, "Mappings are not available on the replica yet, triggered update: " + result.getRequiredMappingUpdate());
            }
        }
        return result;
    }

    protected void updateMappings(IndexShard shard, IndexMetaData indexMetadata) {
        shard.indexSettings().updateIndexMetaData(indexMetadata);
        shard.mapperService().merge(indexMetadata, MapperService.MergeReason.MAPPING_UPDATE);
    }

    protected Engine.DeleteResult deleteDoc(IndexShard shard, String type, String id) throws IOException {
        if (shard.routingEntry().primary()) {
            return shard.applyDeleteOperationOnPrimary(Versions.MATCH_ANY, type, id, VersionType.INTERNAL);
        } else {
            return shard.applyDeleteOperationOnReplica(shard.seqNoStats().getMaxSeqNo() + 1, 0L, type, id);
        }
    }

    protected void flushShard(IndexShard shard) {
        flushShard(shard, false);
    }

    protected void flushShard(IndexShard shard, boolean force) {
        shard.flush(new FlushRequest(shard.shardId().getIndexName()).force(force));
    }

    protected void recoverShardFromSnapshot(final IndexShard shard, final Snapshot snapshot, final Repository repository) throws IOException {
        final Version version = Version.CURRENT;
        final ShardId shardId = shard.shardId();
        final String index = shardId.getIndexName();
        final IndexId indexId = new IndexId(shardId.getIndex().getName(), shardId.getIndex().getUUID());
        final DiscoveryNode node = getFakeDiscoNode(shard.routingEntry().currentNodeId());
        final RecoverySource.SnapshotRecoverySource recoverySource = new RecoverySource.SnapshotRecoverySource(snapshot, version, index);
        final ShardRouting shardRouting = newShardRouting(shardId, node.getId(), true, recoverySource, ShardRoutingState.INITIALIZING);
        shard.markAsRecovering("from snapshot", new RecoveryState(shardRouting, node, null));
        repository.restoreShard(shard, snapshot.getSnapshotId(), version, indexId, shard.shardId(), shard.recoveryState());
    }

    protected void snapshotShard(final IndexShard shard, final Snapshot snapshot, final Repository repository) throws IOException {
        final IndexShardSnapshotStatus snapshotStatus = IndexShardSnapshotStatus.newInitializing();
        try (Engine.IndexCommitRef indexCommitRef = shard.acquireLastIndexCommit(true)) {
            Index index = shard.shardId().getIndex();
            IndexId indexId = new IndexId(index.getName(), index.getUUID());
            repository.snapshotShard(shard, snapshot.getSnapshotId(), indexId, indexCommitRef.getIndexCommit(), snapshotStatus);
        }
        final IndexShardSnapshotStatus.Copy lastSnapshotStatus = snapshotStatus.asCopy();
        assertEquals(IndexShardSnapshotStatus.Stage.DONE, lastSnapshotStatus.getStage());
        assertEquals(shard.snapshotStoreMetadata().size(), lastSnapshotStatus.getTotalFileCount());
        assertNull(lastSnapshotStatus.getFailure());
    }

    public static Engine getEngine(IndexShard indexShard) {
        return indexShard.getEngine();
    }

    public static Translog getTranslog(IndexShard shard) {
        return EngineTestCase.getTranslog(getEngine(shard));
    }

    public static ReplicationTracker getReplicationTracker(IndexShard indexShard) {
        return indexShard.getReplicationTracker();
    }
}