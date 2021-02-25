package org.elasticsearch.index.shard;

import com.carrotsearch.hppc.ObjectLongMap;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AsyncIOProcessor;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.ShardBitsetFilterCache;
import org.elasticsearch.index.cache.request.ShardRequestCache;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.RefreshFailedEngineException;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.fielddata.ShardFieldData;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.get.ShardGetService;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.seqno.GlobalCheckpointTracker;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.Store.MetadataSnapshot;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.warmer.ShardIndexWarmerService;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.suggest.completion.CompletionFieldStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import static org.elasticsearch.index.mapper.SourceToParse.source;

public class IndexShard extends AbstractIndexShardComponent implements IndicesClusterStateService.Shard {

    private final ThreadPool threadPool;

    private final MapperService mapperService;

    private final IndexCache indexCache;

    private final Store store;

    private final InternalIndexingStats internalIndexingStats;

    private final ShardSearchStats searchStats = new ShardSearchStats();

    private final ShardGetService getService;

    private final ShardIndexWarmerService shardWarmerService;

    private final ShardRequestCache requestCacheStats;

    private final ShardFieldData shardFieldData;

    private final ShardBitsetFilterCache shardBitsetFilterCache;

    private final Object mutex = new Object();

    private final String checkIndexOnStartup;

    private final CodecService codecService;

    private final Engine.Warmer warmer;

    private final SimilarityService similarityService;

    private final TranslogConfig translogConfig;

    private final IndexEventListener indexEventListener;

    private final QueryCachingPolicy cachingPolicy;

    private final Supplier<Sort> indexSortSupplier;

    private final SearchOperationListener searchOperationListener;

    protected volatile ShardRouting shardRouting;

    protected volatile IndexShardState state;

    protected volatile long primaryTerm;

    protected final AtomicReference<Engine> currentEngineReference = new AtomicReference<>();

    final EngineFactory engineFactory;

    private final IndexingOperationListener indexingOperationListeners;

    private final Runnable globalCheckpointSyncer;

    Runnable getGlobalCheckpointSyncer() {
        return globalCheckpointSyncer;
    }

    @Nullable
    private RecoveryState recoveryState;

    private final RecoveryStats recoveryStats = new RecoveryStats();

    private final MeanMetric refreshMetric = new MeanMetric();

    private final MeanMetric flushMetric = new MeanMetric();

    private final ShardEventListener shardEventListener = new ShardEventListener();

    private final ShardPath path;

    private final IndexShardOperationPermits indexShardOperationPermits;

    private static final EnumSet<IndexShardState> readAllowedStates = EnumSet.of(IndexShardState.STARTED, IndexShardState.RELOCATED, IndexShardState.POST_RECOVERY);

    public static final EnumSet<IndexShardState> writeAllowedStatesForPrimary = EnumSet.of(IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED);

    private static final EnumSet<IndexShardState> writeAllowedStatesForReplica = EnumSet.of(IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED, IndexShardState.RELOCATED);

    private final IndexSearcherWrapper searcherWrapper;

    private final AtomicBoolean active = new AtomicBoolean();

    private final RefreshListeners refreshListeners;

    private final AtomicLong lastSearcherAccess = new AtomicLong();

    private final AtomicReference<Translog.Location> pendingRefreshLocation = new AtomicReference<>();

    public IndexShard(ShardRouting shardRouting, IndexSettings indexSettings, ShardPath path, Store store, Supplier<Sort> indexSortSupplier, IndexCache indexCache, MapperService mapperService, SimilarityService similarityService, @Nullable EngineFactory engineFactory, IndexEventListener indexEventListener, IndexSearcherWrapper indexSearcherWrapper, ThreadPool threadPool, BigArrays bigArrays, Engine.Warmer warmer, List<SearchOperationListener> searchOperationListener, List<IndexingOperationListener> listeners, Runnable globalCheckpointSyncer) throws IOException {
        super(shardRouting.shardId(), indexSettings);
        assert shardRouting.initializing();
        this.shardRouting = shardRouting;
        final Settings settings = indexSettings.getSettings();
        this.codecService = new CodecService(mapperService, logger);
        this.warmer = warmer;
        this.similarityService = similarityService;
        Objects.requireNonNull(store, "Store must be provided to the index shard");
        this.engineFactory = Objects.requireNonNull(engineFactory);
        this.store = store;
        this.indexSortSupplier = indexSortSupplier;
        this.indexEventListener = indexEventListener;
        this.threadPool = threadPool;
        this.mapperService = mapperService;
        this.indexCache = indexCache;
        this.internalIndexingStats = new InternalIndexingStats();
        final List<IndexingOperationListener> listenersList = new ArrayList<>(listeners);
        listenersList.add(internalIndexingStats);
        this.indexingOperationListeners = new IndexingOperationListener.CompositeListener(listenersList, logger);
        this.globalCheckpointSyncer = globalCheckpointSyncer;
        final List<SearchOperationListener> searchListenersList = new ArrayList<>(searchOperationListener);
        searchListenersList.add(searchStats);
        this.searchOperationListener = new SearchOperationListener.CompositeListener(searchListenersList, logger);
        this.getService = new ShardGetService(indexSettings, this, mapperService);
        this.shardWarmerService = new ShardIndexWarmerService(shardId, indexSettings);
        this.requestCacheStats = new ShardRequestCache();
        this.shardFieldData = new ShardFieldData();
        this.shardBitsetFilterCache = new ShardBitsetFilterCache(shardId, indexSettings);
        state = IndexShardState.CREATED;
        this.path = path;
        logger.debug("state: [CREATED]");
        this.checkIndexOnStartup = indexSettings.getValue(IndexSettings.INDEX_CHECK_ON_STARTUP);
        this.translogConfig = new TranslogConfig(shardId, shardPath().resolveTranslog(), indexSettings, bigArrays);
        if (IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.get(settings)) {
            cachingPolicy = QueryCachingPolicy.ALWAYS_CACHE;
        } else {
            cachingPolicy = new UsageTrackingQueryCachingPolicy();
        }
        indexShardOperationPermits = new IndexShardOperationPermits(shardId, logger, threadPool);
        searcherWrapper = indexSearcherWrapper;
        primaryTerm = indexSettings.getIndexMetaData().primaryTerm(shardId.id());
        refreshListeners = buildRefreshListeners();
        lastSearcherAccess.set(threadPool.relativeTimeInMillis());
        persistMetadata(path, indexSettings, shardRouting, null, logger);
    }

    public ThreadPool getThreadPool() {
        return this.threadPool;
    }

    public Store store() {
        return this.store;
    }

    public Sort getIndexSort() {
        return indexSortSupplier.get();
    }

    public ShardGetService getService() {
        return this.getService;
    }

    public ShardBitsetFilterCache shardBitsetFilterCache() {
        return shardBitsetFilterCache;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public SearchOperationListener getSearchOperationListener() {
        return this.searchOperationListener;
    }

    public ShardIndexWarmerService warmerService() {
        return this.shardWarmerService;
    }

    public ShardRequestCache requestCache() {
        return this.requestCacheStats;
    }

    public ShardFieldData fieldData() {
        return this.shardFieldData;
    }

    public long getPrimaryTerm() {
        return this.primaryTerm;
    }

    @Override
    public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    public QueryCachingPolicy getQueryCachingPolicy() {
        return cachingPolicy;
    }

    @Override
    public void updateShardState(final ShardRouting newRouting, final long newPrimaryTerm, final BiConsumer<IndexShard, ActionListener<ResyncTask>> primaryReplicaSyncer, final long applyingClusterStateVersion, final Set<String> inSyncAllocationIds, final IndexShardRoutingTable routingTable, final Set<String> pre60AllocationIds) throws IOException {
        final ShardRouting currentRouting;
        synchronized (mutex) {
            currentRouting = this.shardRouting;
            if (!newRouting.shardId().equals(shardId())) {
                throw new IllegalArgumentException("Trying to set a routing entry with shardId " + newRouting.shardId() + " on a shard with shardId " + shardId());
            }
            if ((currentRouting == null || newRouting.isSameAllocation(currentRouting)) == false) {
                throw new IllegalArgumentException("Trying to set a routing entry with a different allocation. Current " + currentRouting + ", new " + newRouting);
            }
            if (currentRouting != null && currentRouting.primary() && newRouting.primary() == false) {
                throw new IllegalArgumentException("illegal state: trying to move shard from primary mode to replica mode. Current " + currentRouting + ", new " + newRouting);
            }
            if (newRouting.primary()) {
                final Engine engine = getEngineOrNull();
                if (engine != null) {
                    engine.seqNoService().updateAllocationIdsFromMaster(applyingClusterStateVersion, inSyncAllocationIds, routingTable, pre60AllocationIds);
                }
            }
            if (state == IndexShardState.POST_RECOVERY && newRouting.active()) {
                assert currentRouting.active() == false : "we are in POST_RECOVERY, but our shard routing is active " + currentRouting;
                try {
                    getEngine().refresh("cluster_state_started");
                } catch (Exception e) {
                    logger.debug("failed to refresh due to move to cluster wide started", e);
                }
                if (newRouting.primary()) {
                    final DiscoveryNode recoverySourceNode = recoveryState.getSourceNode();
                    if (currentRouting.isRelocationTarget() == false || recoverySourceNode.getVersion().before(Version.V_6_0_0_alpha1)) {
                        getEngine().seqNoService().activatePrimaryMode(getEngine().seqNoService().getLocalCheckpoint());
                    }
                }
                changeState(IndexShardState.STARTED, "global state is [" + newRouting.state() + "]");
            } else if (state == IndexShardState.RELOCATED && (newRouting.relocating() == false || newRouting.equalsIgnoringMetaData(currentRouting) == false)) {
                throw new IndexShardRelocatedException(shardId(), "Shard is marked as relocated, cannot safely move to state " + newRouting.state());
            }
            assert newRouting.active() == false || state == IndexShardState.STARTED || state == IndexShardState.RELOCATED || state == IndexShardState.CLOSED : "routing is active, but local shard state isn't. routing: " + newRouting + ", local state: " + state;
            persistMetadata(path, indexSettings, newRouting, currentRouting, logger);
            final CountDownLatch shardStateUpdated = new CountDownLatch(1);
            if (newRouting.primary()) {
                if (newPrimaryTerm != primaryTerm) {
                    assert currentRouting.primary() == false : "term is only increased as part of primary promotion";
                    assert newRouting.initializing() == false : "a started primary shard should never update its term; " + "shard " + newRouting + ", " + "current term [" + primaryTerm + "], " + "new term [" + newPrimaryTerm + "]";
                    assert newPrimaryTerm > primaryTerm : "primary terms can only go up; current term [" + primaryTerm + "], new term [" + newPrimaryTerm + "]";
                    boolean resyncStarted = primaryReplicaResyncInProgress.compareAndSet(false, true);
                    if (resyncStarted == false) {
                        throw new IllegalStateException("cannot start resync while it's already in progress");
                    }
                    indexShardOperationPermits.asyncBlockOperations(30, TimeUnit.MINUTES, () -> {
                        shardStateUpdated.await();
                        try {
                            getEngine().rollTranslogGeneration();
                            getEngine().restoreLocalCheckpointFromTranslog();
                            getEngine().fillSeqNoGaps(newPrimaryTerm);
                            getEngine().seqNoService().updateLocalCheckpointForShard(currentRouting.allocationId().getId(), getEngine().seqNoService().getLocalCheckpoint());
                            primaryReplicaSyncer.accept(this, new ActionListener<ResyncTask>() {

                                @Override
                                public void onResponse(ResyncTask resyncTask) {
                                    logger.info("primary-replica resync completed with {} operations", resyncTask.getResyncedOperations());
                                    boolean resyncCompleted = primaryReplicaResyncInProgress.compareAndSet(true, false);
                                    assert resyncCompleted : "primary-replica resync finished but was not started";
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    boolean resyncCompleted = primaryReplicaResyncInProgress.compareAndSet(true, false);
                                    assert resyncCompleted : "primary-replica resync finished but was not started";
                                    if (state == IndexShardState.CLOSED) {
                                    } else {
                                        failShard("exception during primary-replica resync", e);
                                    }
                                }
                            });
                        } catch (final AlreadyClosedException e) {
                        }
                    }, e -> failShard("exception during primary term transition", e));
                    getEngine().seqNoService().activatePrimaryMode(getEngine().seqNoService().getLocalCheckpoint());
                    primaryTerm = newPrimaryTerm;
                }
            }
            this.shardRouting = newRouting;
            shardStateUpdated.countDown();
        }
        if (currentRouting != null && currentRouting.active() == false && newRouting.active()) {
            indexEventListener.afterIndexShardStarted(this);
        }
        if (newRouting.equals(currentRouting) == false) {
            indexEventListener.shardRoutingChanged(this, currentRouting, newRouting);
        }
    }

    public IndexShardState markAsRecovering(String reason, RecoveryState recoveryState) throws IndexShardStartedException, IndexShardRelocatedException, IndexShardRecoveringException, IndexShardClosedException {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            if (state == IndexShardState.RELOCATED) {
                throw new IndexShardRelocatedException(shardId);
            }
            if (state == IndexShardState.RECOVERING) {
                throw new IndexShardRecoveringException(shardId);
            }
            if (state == IndexShardState.POST_RECOVERY) {
                throw new IndexShardRecoveringException(shardId);
            }
            this.recoveryState = recoveryState;
            return changeState(IndexShardState.RECOVERING, reason);
        }
    }

    private final AtomicBoolean primaryReplicaResyncInProgress = new AtomicBoolean();

    public void relocated(final String reason, final Consumer<GlobalCheckpointTracker.PrimaryContext> consumer) throws IllegalIndexShardStateException, InterruptedException {
        assert shardRouting.primary() : "only primaries can be marked as relocated: " + shardRouting;
        try {
            indexShardOperationPermits.blockOperations(30, TimeUnit.MINUTES, () -> {
                assert indexShardOperationPermits.getActiveOperationsCount() == 0 : "in-flight operations in progress while moving shard state to relocated";
                verifyRelocatingState();
                final GlobalCheckpointTracker.PrimaryContext primaryContext = getEngine().seqNoService().startRelocationHandoff();
                try {
                    consumer.accept(primaryContext);
                    synchronized (mutex) {
                        verifyRelocatingState();
                        changeState(IndexShardState.RELOCATED, reason);
                    }
                    getEngine().seqNoService().completeRelocationHandoff();
                } catch (final Exception e) {
                    try {
                        getEngine().seqNoService().abortRelocationHandoff();
                    } catch (final Exception inner) {
                        e.addSuppressed(inner);
                    }
                    throw e;
                }
            });
        } catch (TimeoutException e) {
            logger.warn("timed out waiting for relocation hand-off to complete");
            failShard("timed out waiting for relocation hand-off to complete", null);
            throw new IndexShardClosedException(shardId(), "timed out waiting for relocation hand-off to complete");
        }
    }

    private void verifyRelocatingState() {
        if (state != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(shardId, state);
        }
        if (shardRouting.relocating() == false) {
            throw new IllegalIndexShardStateException(shardId, IndexShardState.STARTED, ": shard is no longer relocating " + shardRouting);
        }
        if (primaryReplicaResyncInProgress.get()) {
            throw new IllegalIndexShardStateException(shardId, IndexShardState.STARTED, ": primary relocation is forbidden while primary-replica resync is in progress " + shardRouting);
        }
    }

    @Override
    public IndexShardState state() {
        return state;
    }

    private IndexShardState changeState(IndexShardState newState, String reason) {
        assert Thread.holdsLock(mutex);
        logger.debug("state: [{}]->[{}], reason [{}]", state, newState, reason);
        IndexShardState previousState = state;
        state = newState;
        this.indexEventListener.indexShardStateChanged(this, previousState, newState, reason);
        return previousState;
    }

    public Engine.IndexResult applyIndexOperationOnPrimary(long version, VersionType versionType, SourceToParse sourceToParse, long autoGeneratedTimestamp, boolean isRetry, Consumer<Mapping> onMappingUpdate) throws IOException {
        return applyIndexOperation(SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm, version, versionType, autoGeneratedTimestamp, isRetry, Engine.Operation.Origin.PRIMARY, sourceToParse, onMappingUpdate);
    }

    public Engine.IndexResult applyIndexOperationOnReplica(long seqNo, long version, VersionType versionType, long autoGeneratedTimeStamp, boolean isRetry, SourceToParse sourceToParse, Consumer<Mapping> onMappingUpdate) throws IOException {
        return applyIndexOperation(seqNo, primaryTerm, version, versionType, autoGeneratedTimeStamp, isRetry, Engine.Operation.Origin.REPLICA, sourceToParse, onMappingUpdate);
    }

    private Engine.IndexResult applyIndexOperation(long seqNo, long opPrimaryTerm, long version, VersionType versionType, long autoGeneratedTimeStamp, boolean isRetry, Engine.Operation.Origin origin, SourceToParse sourceToParse, Consumer<Mapping> onMappingUpdate) throws IOException {
        assert opPrimaryTerm <= this.primaryTerm : "op term [ " + opPrimaryTerm + " ] > shard term [" + this.primaryTerm + "]";
        assert versionType.validateVersionForWrites(version);
        ensureWriteAllowed(origin);
        Engine.Index operation;
        try {
            operation = prepareIndex(docMapper(sourceToParse.type()), indexSettings.getIndexVersionCreated(), sourceToParse, seqNo, opPrimaryTerm, version, versionType, origin, autoGeneratedTimeStamp, isRetry);
            Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
            if (update != null) {
                onMappingUpdate.accept(update);
            }
        } catch (MapperParsingException | IllegalArgumentException | TypeMissingException e) {
            return new Engine.IndexResult(e, version, seqNo);
        } catch (Exception e) {
            verifyNotClosed(e);
            throw e;
        }
        return index(getEngine(), operation);
    }

    public static Engine.Index prepareIndex(DocumentMapperForType docMapper, Version indexCreatedVersion, SourceToParse source, long seqNo, long primaryTerm, long version, VersionType versionType, Engine.Operation.Origin origin, long autoGeneratedIdTimestamp, boolean isRetry) {
        long startTime = System.nanoTime();
        ParsedDocument doc = docMapper.getDocumentMapper().parse(source);
        if (docMapper.getMapping() != null) {
            doc.addDynamicMappingsUpdate(docMapper.getMapping());
        }
        Term uid;
        if (indexCreatedVersion.onOrAfter(Version.V_6_0_0_beta1)) {
            uid = new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id()));
        } else if (docMapper.getDocumentMapper().idFieldMapper().fieldType().indexOptions() != IndexOptions.NONE) {
            uid = new Term(IdFieldMapper.NAME, doc.id());
        } else {
            uid = new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(doc.type(), doc.id()));
        }
        return new Engine.Index(uid, doc, seqNo, primaryTerm, version, versionType, origin, startTime, autoGeneratedIdTimestamp, isRetry);
    }

    private Engine.IndexResult index(Engine engine, Engine.Index index) throws IOException {
        active.set(true);
        final Engine.IndexResult result;
        index = indexingOperationListeners.preIndex(shardId, index);
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("index [{}][{}] (seq# [{}])", index.type(), index.id(), index.seqNo());
            }
            result = engine.index(index);
        } catch (Exception e) {
            indexingOperationListeners.postIndex(shardId, index, e);
            throw e;
        }
        indexingOperationListeners.postIndex(shardId, index, result);
        return result;
    }

    public Engine.NoOpResult markSeqNoAsNoop(long seqNo, String reason) throws IOException {
        return markSeqNoAsNoop(seqNo, primaryTerm, reason, Engine.Operation.Origin.REPLICA);
    }

    private Engine.NoOpResult markSeqNoAsNoop(long seqNo, long opPrimaryTerm, String reason, Engine.Operation.Origin origin) throws IOException {
        assert opPrimaryTerm <= this.primaryTerm : "op term [ " + opPrimaryTerm + " ] > shard term [" + this.primaryTerm + "]";
        long startTime = System.nanoTime();
        ensureWriteAllowed(origin);
        final Engine.NoOp noOp = new Engine.NoOp(seqNo, opPrimaryTerm, origin, startTime, reason);
        return noOp(getEngine(), noOp);
    }

    private Engine.NoOpResult noOp(Engine engine, Engine.NoOp noOp) {
        active.set(true);
        if (logger.isTraceEnabled()) {
            logger.trace("noop (seq# [{}])", noOp.seqNo());
        }
        return engine.noOp(noOp);
    }

    public Engine.DeleteResult applyDeleteOperationOnPrimary(long version, String type, String id, VersionType versionType, Consumer<Mapping> onMappingUpdate) throws IOException {
        return applyDeleteOperation(SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm, version, type, id, versionType, Engine.Operation.Origin.PRIMARY, onMappingUpdate);
    }

    public Engine.DeleteResult applyDeleteOperationOnReplica(long seqNo, long version, String type, String id, VersionType versionType, Consumer<Mapping> onMappingUpdate) throws IOException {
        return applyDeleteOperation(seqNo, primaryTerm, version, type, id, versionType, Engine.Operation.Origin.REPLICA, onMappingUpdate);
    }

    private Engine.DeleteResult applyDeleteOperation(long seqNo, long opPrimaryTerm, long version, String type, String id, VersionType versionType, Engine.Operation.Origin origin, Consumer<Mapping> onMappingUpdate) throws IOException {
        assert opPrimaryTerm <= this.primaryTerm : "op term [ " + opPrimaryTerm + " ] > shard term [" + this.primaryTerm + "]";
        assert versionType.validateVersionForWrites(version);
        ensureWriteAllowed(origin);
        if (indexSettings().isSingleType()) {
            try {
                Mapping update = docMapper(type).getMapping();
                if (update != null) {
                    onMappingUpdate.accept(update);
                }
            } catch (MapperParsingException | IllegalArgumentException | TypeMissingException e) {
                return new Engine.DeleteResult(e, version, seqNo, false);
            }
        }
        final Term uid = extractUidForDelete(type, id);
        final Engine.Delete delete = prepareDelete(type, id, uid, seqNo, opPrimaryTerm, version, versionType, origin);
        return delete(getEngine(), delete);
    }

    private static Engine.Delete prepareDelete(String type, String id, Term uid, long seqNo, long primaryTerm, long version, VersionType versionType, Engine.Operation.Origin origin) {
        long startTime = System.nanoTime();
        return new Engine.Delete(type, id, uid, seqNo, primaryTerm, version, versionType, origin, startTime);
    }

    private Term extractUidForDelete(String type, String id) {
        if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_6_0_0_beta1)) {
            assert indexSettings.isSingleType();
            BytesRef idBytes = Uid.encodeId(id);
            return new Term(IdFieldMapper.NAME, idBytes);
        } else if (indexSettings.isSingleType()) {
            return new Term(IdFieldMapper.NAME, id);
        } else {
            return new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(type, id));
        }
    }

    private Engine.DeleteResult delete(Engine engine, Engine.Delete delete) throws IOException {
        active.set(true);
        final Engine.DeleteResult result;
        delete = indexingOperationListeners.preDelete(shardId, delete);
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("delete [{}] (seq no [{}])", delete.uid().text(), delete.seqNo());
            }
            result = engine.delete(delete);
        } catch (Exception e) {
            indexingOperationListeners.postDelete(shardId, delete, e);
            throw e;
        }
        indexingOperationListeners.postDelete(shardId, delete, result);
        return result;
    }

    public Engine.GetResult get(Engine.Get get) {
        readAllowed();
        return getEngine().get(get, this::acquireSearcher);
    }

    public void refresh(String source) {
        verifyNotClosed();
        if (logger.isTraceEnabled()) {
            logger.trace("refresh with source [{}]", source);
        }
        getEngine().refresh(source);
    }

    public long getWritingBytes() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            return 0;
        }
        return engine.getWritingBytes();
    }

    public RefreshStats refreshStats() {
        int listeners = refreshListeners.pendingCount();
        return new RefreshStats(refreshMetric.count(), TimeUnit.NANOSECONDS.toMillis(refreshMetric.sum()), listeners);
    }

    public FlushStats flushStats() {
        return new FlushStats(flushMetric.count(), TimeUnit.NANOSECONDS.toMillis(flushMetric.sum()));
    }

    public DocsStats docStats() {
        long numDocs = 0;
        long numDeletedDocs = 0;
        long sizeInBytes = 0;
        try (Engine.Searcher searcher = acquireSearcher("docStats", Engine.SearcherScope.INTERNAL)) {
            markSearcherAccessed();
            for (LeafReaderContext reader : searcher.reader().leaves()) {
                final SegmentReader segmentReader = Lucene.segmentReader(reader.reader());
                SegmentCommitInfo info = segmentReader.getSegmentInfo();
                numDocs += reader.reader().numDocs();
                numDeletedDocs += reader.reader().numDeletedDocs();
                try {
                    sizeInBytes += info.sizeInBytes();
                } catch (IOException e) {
                    logger.trace((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage("failed to get size for [{}]", info.info.name), e);
                }
            }
        }
        return new DocsStats(numDocs, numDeletedDocs, sizeInBytes);
    }

    @Nullable
    public CommitStats commitStats() {
        Engine engine = getEngineOrNull();
        return engine == null ? null : engine.commitStats();
    }

    @Nullable
    public SeqNoStats seqNoStats() {
        Engine engine = getEngineOrNull();
        return engine == null ? null : engine.seqNoService().stats();
    }

    public IndexingStats indexingStats(String... types) {
        Engine engine = getEngineOrNull();
        final boolean throttled;
        final long throttleTimeInMillis;
        if (engine == null) {
            throttled = false;
            throttleTimeInMillis = 0;
        } else {
            throttled = engine.isThrottled();
            throttleTimeInMillis = engine.getIndexThrottleTimeInMillis();
        }
        return internalIndexingStats.stats(throttled, throttleTimeInMillis, types);
    }

    public SearchStats searchStats(String... groups) {
        return searchStats.stats(groups);
    }

    public GetStats getStats() {
        return getService.stats();
    }

    public StoreStats storeStats() {
        try {
            return store.stats();
        } catch (IOException e) {
            throw new ElasticsearchException("io exception while building 'store stats'", e);
        } catch (AlreadyClosedException ex) {
            return null;
        }
    }

    public MergeStats mergeStats() {
        final Engine engine = getEngineOrNull();
        if (engine == null) {
            return new MergeStats();
        }
        return engine.getMergeStats();
    }

    public SegmentsStats segmentStats(boolean includeSegmentFileSizes) {
        SegmentsStats segmentsStats = getEngine().segmentsStats(includeSegmentFileSizes);
        segmentsStats.addBitsetMemoryInBytes(shardBitsetFilterCache.getMemorySizeInBytes());
        return segmentsStats;
    }

    public WarmerStats warmerStats() {
        return shardWarmerService.stats();
    }

    public FieldDataStats fieldDataStats(String... fields) {
        return shardFieldData.stats(fields);
    }

    public TranslogStats translogStats() {
        return getEngine().getTranslog().stats();
    }

    public CompletionStats completionStats(String... fields) {
        CompletionStats completionStats = new CompletionStats();
        try (Engine.Searcher currentSearcher = acquireSearcher("completion_stats")) {
            markSearcherAccessed();
            completionStats.add(CompletionFieldStats.completionStats(currentSearcher.reader(), fields));
        }
        return completionStats;
    }

    public Engine.SyncedFlushResult syncFlush(String syncId, Engine.CommitId expectedCommitId) {
        verifyNotClosed();
        logger.trace("trying to sync flush. sync id [{}]. expected commit id [{}]]", syncId, expectedCommitId);
        Engine engine = getEngine();
        if (engine.isRecovering()) {
            throw new IllegalIndexShardStateException(shardId(), state, "syncFlush is only allowed if the engine is not recovery" + " from translog");
        }
        return engine.syncFlush(syncId, expectedCommitId);
    }

    public Engine.CommitId flush(FlushRequest request) {
        final boolean waitIfOngoing = request.waitIfOngoing();
        final boolean force = request.force();
        logger.trace("flush with {}", request);
        verifyNotClosed();
        final Engine engine = getEngine();
        if (engine.isRecovering()) {
            throw new IllegalIndexShardStateException(shardId(), state, "flush is only allowed if the engine is not recovery from translog");
        }
        final long time = System.nanoTime();
        final Engine.CommitId commitId = engine.flush(force, waitIfOngoing);
        flushMetric.inc(System.nanoTime() - time);
        return commitId;
    }

    public void trimTranslog() {
        verifyNotClosed();
        final Engine engine = getEngine();
        engine.trimTranslog();
    }

    private void rollTranslogGeneration() {
        final Engine engine = getEngine();
        engine.rollTranslogGeneration();
    }

    public void forceMerge(ForceMergeRequest forceMerge) throws IOException {
        verifyActive();
        if (logger.isTraceEnabled()) {
            logger.trace("force merge with {}", forceMerge);
        }
        Engine engine = getEngine();
        engine.forceMerge(forceMerge.flush(), forceMerge.maxNumSegments(), forceMerge.onlyExpungeDeletes(), false, false);
    }

    public org.apache.lucene.util.Version upgrade(UpgradeRequest upgrade) throws IOException {
        verifyActive();
        if (logger.isTraceEnabled()) {
            logger.trace("upgrade with {}", upgrade);
        }
        org.apache.lucene.util.Version previousVersion = minimumCompatibleVersion();
        final Engine engine = getEngine();
        engine.forceMerge(true, Integer.MAX_VALUE, false, true, upgrade.upgradeOnlyAncientSegments());
        org.apache.lucene.util.Version version = minimumCompatibleVersion();
        if (logger.isTraceEnabled()) {
            logger.trace("upgraded segments for {} from version {} to version {}", shardId, previousVersion, version);
        }
        return version;
    }

    public org.apache.lucene.util.Version minimumCompatibleVersion() {
        org.apache.lucene.util.Version luceneVersion = null;
        for (Segment segment : getEngine().segments(false)) {
            if (luceneVersion == null || luceneVersion.onOrAfter(segment.getVersion())) {
                luceneVersion = segment.getVersion();
            }
        }
        return luceneVersion == null ? indexSettings.getIndexVersionCreated().luceneVersion : luceneVersion;
    }

    public Engine.IndexCommitRef acquireIndexCommit(boolean flushFirst) throws EngineException {
        IndexShardState state = this.state;
        if (state == IndexShardState.STARTED || state == IndexShardState.RELOCATED || state == IndexShardState.CLOSED) {
            return getEngine().acquireIndexCommit(flushFirst);
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }

    public Store.MetadataSnapshot snapshotStoreMetadata() throws IOException {
        Engine.IndexCommitRef indexCommit = null;
        store.incRef();
        try {
            Engine engine;
            synchronized (mutex) {
                engine = getEngineOrNull();
                if (engine == null) {
                    return store.getMetadata(null, true);
                }
            }
            indexCommit = engine.acquireIndexCommit(false);
            return store.getMetadata(indexCommit.getIndexCommit());
        } finally {
            store.decRef();
            IOUtils.close(indexCommit);
        }
    }

    public void failShard(String reason, @Nullable Exception e) {
        getEngine().failEngine(reason, e);
    }

    public Engine.Searcher acquireSearcher(String source) {
        return acquireSearcher(source, Engine.SearcherScope.EXTERNAL);
    }

    private void markSearcherAccessed() {
        lastSearcherAccess.lazySet(threadPool.relativeTimeInMillis());
    }

    private Engine.Searcher acquireSearcher(String source, Engine.SearcherScope scope) {
        readAllowed();
        final Engine engine = getEngine();
        final Engine.Searcher searcher = engine.acquireSearcher(source, scope);
        boolean success = false;
        try {
            final Engine.Searcher wrappedSearcher = searcherWrapper == null ? searcher : searcherWrapper.wrap(searcher);
            assert wrappedSearcher != null;
            success = true;
            return wrappedSearcher;
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to wrap searcher", ex);
        } finally {
            if (success == false) {
                Releasables.close(success, searcher);
            }
        }
    }

    public void close(String reason, boolean flushEngine) throws IOException {
        synchronized (mutex) {
            try {
                changeState(IndexShardState.CLOSED, reason);
            } finally {
                final Engine engine = this.currentEngineReference.getAndSet(null);
                try {
                    if (engine != null && flushEngine) {
                        engine.flushAndClose();
                    }
                } finally {
                    IOUtils.close(engine, refreshListeners);
                    indexShardOperationPermits.close();
                }
            }
        }
    }

    public IndexShard postRecovery(String reason) throws IndexShardStartedException, IndexShardRelocatedException, IndexShardClosedException {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            if (state == IndexShardState.RELOCATED) {
                throw new IndexShardRelocatedException(shardId);
            }
            getEngine().refresh("post_recovery");
            recoveryState.setStage(RecoveryState.Stage.DONE);
            changeState(IndexShardState.POST_RECOVERY, reason);
        }
        return this;
    }

    public void prepareForIndexRecovery() {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState.setStage(RecoveryState.Stage.INDEX);
        assert currentEngineReference.get() == null;
    }

    public Engine.Result applyTranslogOperation(Translog.Operation operation, Engine.Operation.Origin origin, Consumer<Mapping> onMappingUpdate) throws IOException {
        final Engine.Result result;
        switch(operation.opType()) {
            case INDEX:
                final Translog.Index index = (Translog.Index) operation;
                result = applyIndexOperation(index.seqNo(), index.primaryTerm(), index.version(), index.versionType().versionTypeForReplicationAndRecovery(), index.getAutoGeneratedIdTimestamp(), true, origin, source(shardId.getIndexName(), index.type(), index.id(), index.source(), XContentFactory.xContentType(index.source())).routing(index.routing()).parent(index.parent()), onMappingUpdate);
                break;
            case DELETE:
                final Translog.Delete delete = (Translog.Delete) operation;
                result = applyDeleteOperation(delete.seqNo(), delete.primaryTerm(), delete.version(), delete.type(), delete.id(), delete.versionType().versionTypeForReplicationAndRecovery(), origin, onMappingUpdate);
                break;
            case NO_OP:
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                result = markSeqNoAsNoop(noOp.seqNo(), noOp.primaryTerm(), noOp.reason(), origin);
                break;
            default:
                throw new IllegalStateException("No operation defined for [" + operation + "]");
        }
        return result;
    }

    int runTranslogRecovery(Engine engine, Translog.Snapshot snapshot) throws IOException {
        recoveryState.getTranslog().totalOperations(snapshot.totalOperations());
        recoveryState.getTranslog().totalOperationsOnStart(snapshot.totalOperations());
        int opsRecovered = 0;
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            try {
                logger.trace("[translog] recover op {}", operation);
                Engine.Result result = applyTranslogOperation(operation, Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY, update -> {
                    throw new IllegalArgumentException("unexpected mapping update: " + update);
                });
                ExceptionsHelper.reThrowIfNotNull(result.getFailure());
                opsRecovered++;
                recoveryState.getTranslog().incrementRecoveredOperations();
            } catch (Exception e) {
                if (ExceptionsHelper.status(e) == RestStatus.BAD_REQUEST) {
                    logger.info("ignoring recovery of a corrupt translog entry", e);
                } else {
                    throw e;
                }
            }
        }
        return opsRecovered;
    }

    public void performTranslogRecovery(boolean indexExists) throws IOException {
        if (indexExists == false) {
            final RecoveryState.Translog translogStats = recoveryState().getTranslog();
            translogStats.totalOperations(0);
            translogStats.totalOperationsOnStart(0);
        }
        internalPerformTranslogRecovery(false, indexExists);
        assert recoveryState.getStage() == RecoveryState.Stage.TRANSLOG : "TRANSLOG stage expected but was: " + recoveryState.getStage();
    }

    private void internalPerformTranslogRecovery(boolean skipTranslogRecovery, boolean indexExists) throws IOException {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState.setStage(RecoveryState.Stage.VERIFY_INDEX);
        if (Booleans.isTrue(checkIndexOnStartup)) {
            try {
                checkIndex();
            } catch (IOException ex) {
                throw new RecoveryFailedException(recoveryState, "check index failed", ex);
            }
        }
        recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
        final EngineConfig.OpenMode openMode;
        if (indexExists == false) {
            openMode = EngineConfig.OpenMode.CREATE_INDEX_AND_TRANSLOG;
        } else if (skipTranslogRecovery) {
            openMode = EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG;
        } else {
            openMode = EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG;
        }
        assert indexExists == false || assertMaxUnsafeAutoIdInCommit();
        final EngineConfig config = newEngineConfig(openMode);
        config.setEnableGcDeletes(false);
        Engine newEngine = createNewEngine(config);
        verifyNotClosed();
        if (openMode == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
            active.set(true);
            newEngine.recoverFromTranslog();
        }
    }

    private boolean assertMaxUnsafeAutoIdInCommit() throws IOException {
        final Map<String, String> userData = SegmentInfos.readLatestCommit(store.directory()).getUserData();
        if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_5_5_0) && recoveryState().getRecoverySource().getType() != RecoverySource.Type.LOCAL_SHARDS) {
            assert userData.containsKey(InternalEngine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID) : "opening index which was created post 5.5.0 but " + InternalEngine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID + " is not found in commit";
        }
        return true;
    }

    protected void onNewEngine(Engine newEngine) {
        refreshListeners.setTranslog(newEngine.getTranslog());
    }

    public void skipTranslogRecovery() throws IOException {
        assert getEngineOrNull() == null : "engine was already created";
        internalPerformTranslogRecovery(true, true);
        assert recoveryState.getTranslog().recoveredOperations() == 0;
    }

    public void performRecoveryRestart() throws IOException {
        synchronized (mutex) {
            if (state != IndexShardState.RECOVERING) {
                throw new IndexShardNotRecoveringException(shardId, state);
            }
            assert refreshListeners.pendingCount() == 0 : "we can't restart with pending listeners";
            final Engine engine = this.currentEngineReference.getAndSet(null);
            IOUtils.close(engine);
            recoveryState().setStage(RecoveryState.Stage.INIT);
        }
    }

    public RecoveryStats recoveryStats() {
        return recoveryStats;
    }

    @Override
    public RecoveryState recoveryState() {
        return this.recoveryState;
    }

    public void finalizeRecovery() {
        recoveryState().setStage(RecoveryState.Stage.FINALIZE);
        Engine engine = getEngine();
        engine.refresh("recovery_finalization");
        engine.config().setEnableGcDeletes(true);
    }

    public boolean ignoreRecoveryAttempt() {
        IndexShardState state = state();
        return state == IndexShardState.POST_RECOVERY || state == IndexShardState.RECOVERING || state == IndexShardState.STARTED || state == IndexShardState.RELOCATED || state == IndexShardState.CLOSED;
    }

    public void readAllowed() throws IllegalIndexShardStateException {
        IndexShardState state = this.state;
        if (readAllowedStates.contains(state) == false) {
            throw new IllegalIndexShardStateException(shardId, state, "operations only allowed when shard state is one of " + readAllowedStates.toString());
        }
    }

    public boolean isReadAllowed() {
        return readAllowedStates.contains(state);
    }

    private void ensureWriteAllowed(Engine.Operation.Origin origin) throws IllegalIndexShardStateException {
        IndexShardState state = this.state;
        if (origin == Engine.Operation.Origin.PRIMARY) {
            verifyPrimary();
            if (writeAllowedStatesForPrimary.contains(state) == false) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard state is one of " + writeAllowedStatesForPrimary + ", origin [" + origin + "]");
            }
        } else if (origin.isRecovery()) {
            if (state != IndexShardState.RECOVERING) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when recovering, origin [" + origin + "]");
            }
        } else {
            assert origin == Engine.Operation.Origin.REPLICA;
            verifyReplicationTarget();
            if (writeAllowedStatesForReplica.contains(state) == false) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard state is one of " + writeAllowedStatesForReplica + ", origin [" + origin + "]");
            }
        }
    }

    private void verifyPrimary() {
        if (shardRouting.primary() == false) {
            throw new IllegalStateException("shard " + shardRouting + " is not a primary");
        }
    }

    private void verifyReplicationTarget() {
        final IndexShardState state = state();
        if (shardRouting.primary() && shardRouting.active() && state != IndexShardState.RELOCATED) {
            throw new IllegalStateException("active primary shard " + shardRouting + " cannot be a replication target before " + "relocation hand off, state is [" + state + "]");
        }
    }

    private void verifyNotClosed() throws IllegalIndexShardStateException {
        verifyNotClosed(null);
    }

    private void verifyNotClosed(Exception suppressed) throws IllegalIndexShardStateException {
        IndexShardState state = this.state;
        if (state == IndexShardState.CLOSED) {
            final IllegalIndexShardStateException exc = new IndexShardClosedException(shardId, "operation only allowed when not closed");
            if (suppressed != null) {
                exc.addSuppressed(suppressed);
            }
            throw exc;
        }
    }

    protected final void verifyActive() throws IllegalIndexShardStateException {
        IndexShardState state = this.state;
        if (state != IndexShardState.STARTED && state != IndexShardState.RELOCATED) {
            throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard is active");
        }
    }

    public long getIndexBufferRAMBytesUsed() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            return 0;
        }
        try {
            return engine.getIndexBufferRAMBytesUsed();
        } catch (AlreadyClosedException ex) {
            return 0;
        }
    }

    public void addShardFailureCallback(Consumer<ShardFailure> onShardFailure) {
        this.shardEventListener.delegates.add(onShardFailure);
    }

    public void checkIdle(long inactiveTimeNS) {
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null && System.nanoTime() - engineOrNull.getLastWriteNanos() >= inactiveTimeNS) {
            boolean wasActive = active.getAndSet(false);
            if (wasActive) {
                logger.debug("shard is now inactive");
                try {
                    indexEventListener.onShardInactive(this);
                } catch (Exception e) {
                    logger.warn("failed to notify index event listener", e);
                }
            }
        }
    }

    public boolean isActive() {
        return active.get();
    }

    public ShardPath shardPath() {
        return path;
    }

    public boolean recoverFromLocalShards(BiConsumer<String, MappingMetaData> mappingUpdateConsumer, List<IndexShard> localShards) throws IOException {
        assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard";
        assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS : "invalid recovery type: " + recoveryState.getRecoverySource();
        final List<LocalShardSnapshot> snapshots = new ArrayList<>();
        try {
            for (IndexShard shard : localShards) {
                snapshots.add(new LocalShardSnapshot(shard));
            }
            assert shardRouting.primary() : "recover from local shards only makes sense if the shard is a primary shard";
            StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
            return storeRecovery.recoverFromLocalShards(mappingUpdateConsumer, this, snapshots);
        } finally {
            IOUtils.close(snapshots);
        }
    }

    public boolean recoverFromStore() {
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        assert shardRouting.initializing() : "can only start recovery on initializing shard";
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromStore(this);
    }

    public boolean restoreFromRepository(Repository repository) {
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT : "invalid recovery type: " + recoveryState.getRecoverySource();
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromRepository(this, repository);
    }

    boolean shouldFlush() {
        final Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                final Translog translog = engine.getTranslog();
                return translog.shouldFlush();
            } catch (final AlreadyClosedException e) {
            }
        }
        return false;
    }

    boolean shouldRollTranslogGeneration() {
        final Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                final Translog translog = engine.getTranslog();
                return translog.shouldRollGeneration();
            } catch (final AlreadyClosedException e) {
            }
        }
        return false;
    }

    public void onSettingsChanged() {
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null) {
            engineOrNull.onSettingsChanged();
        }
    }

    public Closeable acquireTranslogRetentionLock() {
        Engine engine = getEngine();
        return engine.getTranslog().acquireRetentionLock();
    }

    public List<Segment> segments(boolean verbose) {
        return getEngine().segments(verbose);
    }

    public void flushAndCloseEngine() throws IOException {
        getEngine().flushAndClose();
    }

    public Translog getTranslog() {
        return getEngine().getTranslog();
    }

    public String getHistoryUUID() {
        return getEngine().getHistoryUUID();
    }

    public IndexEventListener getIndexEventListener() {
        return indexEventListener;
    }

    public void activateThrottling() {
        try {
            getEngine().activateThrottling();
        } catch (AlreadyClosedException ex) {
        }
    }

    public void deactivateThrottling() {
        try {
            getEngine().deactivateThrottling();
        } catch (AlreadyClosedException ex) {
        }
    }

    private void handleRefreshException(Exception e) {
        if (e instanceof AlreadyClosedException) {
        } else if (e instanceof RefreshFailedEngineException) {
            RefreshFailedEngineException rfee = (RefreshFailedEngineException) e;
            if (rfee.getCause() instanceof InterruptedException) {
            } else if (rfee.getCause() instanceof ClosedByInterruptException) {
            } else if (rfee.getCause() instanceof ThreadInterruptedException) {
            } else {
                if (state != IndexShardState.CLOSED) {
                    logger.warn("Failed to perform engine refresh", e);
                }
            }
        } else {
            if (state != IndexShardState.CLOSED) {
                logger.warn("Failed to perform engine refresh", e);
            }
        }
    }

    public void writeIndexingBuffer() {
        try {
            Engine engine = getEngine();
            engine.writeIndexingBuffer();
        } catch (Exception e) {
            handleRefreshException(e);
        }
    }

    public void updateLocalCheckpointForShard(final String allocationId, final long checkpoint) {
        verifyPrimary();
        verifyNotClosed();
        getEngine().seqNoService().updateLocalCheckpointForShard(allocationId, checkpoint);
    }

    public void updateGlobalCheckpointForShard(final String allocationId, final long globalCheckpoint) {
        verifyPrimary();
        verifyNotClosed();
        getEngine().seqNoService().updateGlobalCheckpointForShard(allocationId, globalCheckpoint);
    }

    public void waitForOpsToComplete(final long seqNo) throws InterruptedException {
        getEngine().seqNoService().waitForOpsToComplete(seqNo);
    }

    public void initiateTracking(final String allocationId) {
        verifyPrimary();
        getEngine().seqNoService().initiateTracking(allocationId);
        active.compareAndSet(false, true);
    }

    public void markAllocationIdAsInSync(final String allocationId, final long localCheckpoint) throws InterruptedException {
        verifyPrimary();
        getEngine().seqNoService().markAllocationIdAsInSync(allocationId, localCheckpoint);
    }

    public long getLocalCheckpoint() {
        return getEngine().seqNoService().getLocalCheckpoint();
    }

    public long getGlobalCheckpoint() {
        return getEngine().seqNoService().getGlobalCheckpoint();
    }

    public ObjectLongMap<String> getInSyncGlobalCheckpoints() {
        verifyPrimary();
        verifyNotClosed();
        return getEngine().seqNoService().getInSyncGlobalCheckpoints();
    }

    public void maybeSyncGlobalCheckpoint(final String reason) {
        verifyPrimary();
        verifyNotClosed();
        if (state == IndexShardState.RELOCATED) {
            return;
        }
        final SeqNoStats stats = getEngine().seqNoService().stats();
        if (stats.getMaxSeqNo() == stats.getGlobalCheckpoint()) {
            final ObjectLongMap<String> globalCheckpoints = getInSyncGlobalCheckpoints();
            final String allocationId = routingEntry().allocationId().getId();
            assert globalCheckpoints.containsKey(allocationId);
            final long globalCheckpoint = globalCheckpoints.get(allocationId);
            final boolean syncNeeded = StreamSupport.stream(globalCheckpoints.values().spliterator(), false).anyMatch(v -> v.value < globalCheckpoint);
            if (syncNeeded) {
                logger.trace("syncing global checkpoint for [{}]", reason);
                globalCheckpointSyncer.run();
            }
        }
    }

    public ReplicationGroup getReplicationGroup() {
        verifyPrimary();
        verifyNotClosed();
        return getEngine().seqNoService().getReplicationGroup();
    }

    public void updateGlobalCheckpointOnReplica(final long globalCheckpoint, final String reason) {
        verifyReplicationTarget();
        final SequenceNumbersService seqNoService = getEngine().seqNoService();
        final long localCheckpoint = seqNoService.getLocalCheckpoint();
        if (globalCheckpoint > localCheckpoint) {
            assert state() != IndexShardState.POST_RECOVERY && state() != IndexShardState.STARTED && state() != IndexShardState.RELOCATED : "supposedly in-sync shard copy received a global checkpoint [" + globalCheckpoint + "] " + "that is higher than its local checkpoint [" + localCheckpoint + "]";
            return;
        }
        seqNoService.updateGlobalCheckpointOnReplica(globalCheckpoint, reason);
    }

    public void activateWithPrimaryContext(final GlobalCheckpointTracker.PrimaryContext primaryContext) {
        verifyPrimary();
        assert shardRouting.isRelocationTarget() : "only relocation target can update allocation IDs from primary context: " + shardRouting;
        assert primaryContext.getCheckpointStates().containsKey(routingEntry().allocationId().getId()) && getEngine().seqNoService().getLocalCheckpoint() == primaryContext.getCheckpointStates().get(routingEntry().allocationId().getId()).getLocalCheckpoint();
        getEngine().seqNoService().activateWithPrimaryContext(primaryContext);
    }

    public boolean pendingInSync() {
        verifyPrimary();
        return getEngine().seqNoService().pendingInSync();
    }

    public void noopUpdate(String type) {
        internalIndexingStats.noopUpdate(type);
    }

    private void checkIndex() throws IOException {
        if (store.tryIncRef()) {
            try {
                doCheckIndex();
            } finally {
                store.decRef();
            }
        }
    }

    private void doCheckIndex() throws IOException {
        long timeNS = System.nanoTime();
        if (!Lucene.indexExists(store.directory())) {
            return;
        }
        BytesStreamOutput os = new BytesStreamOutput();
        PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name());
        if ("checksum".equals(checkIndexOnStartup)) {
            IOException corrupt = null;
            MetadataSnapshot metadata = snapshotStoreMetadata();
            for (Map.Entry<String, StoreFileMetaData> entry : metadata.asMap().entrySet()) {
                try {
                    Store.checkIntegrity(entry.getValue(), store.directory());
                    out.println("checksum passed: " + entry.getKey());
                } catch (IOException exc) {
                    out.println("checksum failed: " + entry.getKey());
                    exc.printStackTrace(out);
                    corrupt = exc;
                }
            }
            out.flush();
            if (corrupt != null) {
                logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
                throw corrupt;
            }
        } else {
            try (CheckIndex checkIndex = new CheckIndex(store.directory())) {
                checkIndex.setInfoStream(out);
                CheckIndex.Status status = checkIndex.checkIndex();
                out.flush();
                if (!status.clean) {
                    if (state == IndexShardState.CLOSED) {
                        return;
                    }
                    logger.warn("check index [failure]\n{}", os.bytes().utf8ToString());
                    if ("fix".equals(checkIndexOnStartup)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("fixing index, writing new segments file ...");
                        }
                        checkIndex.exorciseIndex(status);
                        if (logger.isDebugEnabled()) {
                            logger.debug("index fixed, wrote new segments file \"{}\"", status.segmentsFileName);
                        }
                    } else {
                        throw new IllegalStateException("index check failure but can't fix it");
                    }
                }
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("check index [success]\n{}", os.bytes().utf8ToString());
        }
        recoveryState.getVerifyIndex().checkIndexTime(Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - timeNS)));
    }

    Engine getEngine() {
        Engine engine = getEngineOrNull();
        if (engine == null) {
            throw new AlreadyClosedException("engine is closed");
        }
        return engine;
    }

    protected Engine getEngineOrNull() {
        return this.currentEngineReference.get();
    }

    public void startRecovery(RecoveryState recoveryState, PeerRecoveryTargetService recoveryTargetService, PeerRecoveryTargetService.RecoveryListener recoveryListener, RepositoriesService repositoriesService, BiConsumer<String, MappingMetaData> mappingUpdateConsumer, IndicesService indicesService) {
        assert recoveryState.getRecoverySource().equals(shardRouting.recoverySource());
        switch(recoveryState.getRecoverySource().getType()) {
            case EMPTY_STORE:
            case EXISTING_STORE:
                markAsRecovering("from store", recoveryState);
                threadPool.generic().execute(() -> {
                    try {
                        if (recoverFromStore()) {
                            recoveryListener.onRecoveryDone(recoveryState);
                        }
                    } catch (Exception e) {
                        recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                    }
                });
                break;
            case PEER:
                try {
                    markAsRecovering("from " + recoveryState.getSourceNode(), recoveryState);
                    recoveryTargetService.startRecovery(this, recoveryState.getSourceNode(), recoveryListener);
                } catch (Exception e) {
                    failShard("corrupted preexisting index", e);
                    recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                }
                break;
            case SNAPSHOT:
                markAsRecovering("from snapshot", recoveryState);
                SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) recoveryState.getRecoverySource();
                threadPool.generic().execute(() -> {
                    try {
                        final Repository repository = repositoriesService.repository(recoverySource.snapshot().getRepository());
                        if (restoreFromRepository(repository)) {
                            recoveryListener.onRecoveryDone(recoveryState);
                        }
                    } catch (Exception e) {
                        recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                    }
                });
                break;
            case LOCAL_SHARDS:
                final IndexMetaData indexMetaData = indexSettings().getIndexMetaData();
                final Index resizeSourceIndex = indexMetaData.getResizeSourceIndex();
                final List<IndexShard> startedShards = new ArrayList<>();
                final IndexService sourceIndexService = indicesService.indexService(resizeSourceIndex);
                final Set<ShardId> requiredShards;
                final int numShards;
                if (sourceIndexService != null) {
                    requiredShards = IndexMetaData.selectRecoverFromShards(shardId().id(), sourceIndexService.getMetaData(), indexMetaData.getNumberOfShards());
                    for (IndexShard shard : sourceIndexService) {
                        if (shard.state() == IndexShardState.STARTED && requiredShards.contains(shard.shardId())) {
                            startedShards.add(shard);
                        }
                    }
                    numShards = requiredShards.size();
                } else {
                    numShards = -1;
                    requiredShards = Collections.emptySet();
                }
                if (numShards == startedShards.size()) {
                    assert requiredShards.isEmpty() == false;
                    markAsRecovering("from local shards", recoveryState);
                    threadPool.generic().execute(() -> {
                        try {
                            if (recoverFromLocalShards(mappingUpdateConsumer, startedShards.stream().filter((s) -> requiredShards.contains(s.shardId())).collect(Collectors.toList()))) {
                                recoveryListener.onRecoveryDone(recoveryState);
                            }
                        } catch (Exception e) {
                            recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                        }
                    });
                } else {
                    final RuntimeException e;
                    if (numShards == -1) {
                        e = new IndexNotFoundException(resizeSourceIndex);
                    } else {
                        e = new IllegalStateException("not all required shards of index " + resizeSourceIndex + " are started yet, expected " + numShards + " found " + startedShards.size() + " can't recover shard " + shardId());
                    }
                    throw e;
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown recovery source " + recoveryState.getRecoverySource());
        }
    }

    class ShardEventListener implements Engine.EventListener {

        private final CopyOnWriteArrayList<Consumer<ShardFailure>> delegates = new CopyOnWriteArrayList<>();

        @Override
        public void onFailedEngine(String reason, @Nullable Exception failure) {
            final ShardFailure shardFailure = new ShardFailure(shardRouting, reason, failure);
            for (Consumer<ShardFailure> listener : delegates) {
                try {
                    listener.accept(shardFailure);
                } catch (Exception inner) {
                    inner.addSuppressed(failure);
                    logger.warn("exception while notifying engine failure", inner);
                }
            }
        }
    }

    private Engine createNewEngine(EngineConfig config) {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new AlreadyClosedException(shardId + " can't create engine - shard is closed");
            }
            assert this.currentEngineReference.get() == null;
            Engine engine = newEngine(config);
            onNewEngine(engine);
            this.currentEngineReference.set(engine);
        }
        Engine engine = getEngineOrNull();
        if (engine != null) {
            engine.onSettingsChanged();
        }
        return engine;
    }

    protected Engine newEngine(EngineConfig config) {
        return engineFactory.newReadWriteEngine(config);
    }

    private static void persistMetadata(final ShardPath shardPath, final IndexSettings indexSettings, final ShardRouting newRouting, @Nullable final ShardRouting currentRouting, final Logger logger) throws IOException {
        assert newRouting != null : "newRouting must not be null";
        final ShardId shardId = newRouting.shardId();
        if (currentRouting == null || currentRouting.primary() != newRouting.primary() || currentRouting.allocationId().equals(newRouting.allocationId()) == false) {
            assert currentRouting == null || currentRouting.isSameAllocation(newRouting);
            final String writeReason;
            if (currentRouting == null) {
                writeReason = "initial state with allocation id [" + newRouting.allocationId() + "]";
            } else {
                writeReason = "routing changed from " + currentRouting + " to " + newRouting;
            }
            logger.trace("{} writing shard state, reason [{}]", shardId, writeReason);
            final ShardStateMetaData newShardStateMetadata = new ShardStateMetaData(newRouting.primary(), indexSettings.getUUID(), newRouting.allocationId());
            ShardStateMetaData.FORMAT.write(newShardStateMetadata, shardPath.getShardStatePath());
        } else {
            logger.trace("{} skip writing shard state, has been written before", shardId);
        }
    }

    private DocumentMapperForType docMapper(String type) {
        return mapperService.documentMapperWithAutoCreate(type);
    }

    private EngineConfig newEngineConfig(EngineConfig.OpenMode openMode) {
        Sort indexSort = indexSortSupplier.get();
        final boolean forceNewHistoryUUID;
        switch(shardRouting.recoverySource().getType()) {
            case EXISTING_STORE:
            case PEER:
                forceNewHistoryUUID = false;
                break;
            case EMPTY_STORE:
            case SNAPSHOT:
            case LOCAL_SHARDS:
                forceNewHistoryUUID = true;
                break;
            default:
                throw new AssertionError("unknown recovery type: [" + shardRouting.recoverySource().getType() + "]");
        }
        return new EngineConfig(openMode, shardId, shardRouting.allocationId().getId(), threadPool, indexSettings, warmer, store, indexSettings.getMergePolicy(), mapperService.indexAnalyzer(), similarityService.similarity(mapperService), codecService, shardEventListener, indexCache.query(), cachingPolicy, forceNewHistoryUUID, translogConfig, IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.get(indexSettings.getSettings()), Arrays.asList(refreshListeners, new RefreshMetricUpdater(refreshMetric)), indexSort, this::runTranslogRecovery);
    }

    public void acquirePrimaryOperationPermit(ActionListener<Releasable> onPermitAcquired, String executorOnDelay) {
        verifyNotClosed();
        verifyPrimary();
        indexShardOperationPermits.acquire(onPermitAcquired, executorOnDelay, false);
    }

    private final Object primaryTermMutex = new Object();

    public void acquireReplicaOperationPermit(final long operationPrimaryTerm, final long globalCheckpoint, final ActionListener<Releasable> onPermitAcquired, final String executorOnDelay) {
        verifyNotClosed();
        verifyReplicationTarget();
        final boolean globalCheckpointUpdated;
        if (operationPrimaryTerm > primaryTerm) {
            synchronized (primaryTermMutex) {
                if (operationPrimaryTerm > primaryTerm) {
                    IndexShardState shardState = state();
                    if (shardState != IndexShardState.POST_RECOVERY && shardState != IndexShardState.STARTED && shardState != IndexShardState.RELOCATED) {
                        throw new IndexShardNotStartedException(shardId, shardState);
                    }
                    try {
                        indexShardOperationPermits.blockOperations(30, TimeUnit.MINUTES, () -> {
                            assert operationPrimaryTerm > primaryTerm : "shard term already update.  op term [" + operationPrimaryTerm + "], shardTerm [" + primaryTerm + "]";
                            primaryTerm = operationPrimaryTerm;
                            updateGlobalCheckpointOnReplica(globalCheckpoint, "primary term transition");
                            final long currentGlobalCheckpoint = getGlobalCheckpoint();
                            final long localCheckpoint;
                            if (currentGlobalCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                                localCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
                            } else {
                                localCheckpoint = currentGlobalCheckpoint;
                            }
                            logger.trace("detected new primary with primary term [{}], resetting local checkpoint from [{}] to [{}]", operationPrimaryTerm, getLocalCheckpoint(), localCheckpoint);
                            getEngine().seqNoService().resetLocalCheckpoint(localCheckpoint);
                            getEngine().getTranslog().rollGeneration();
                        });
                        globalCheckpointUpdated = true;
                    } catch (final Exception e) {
                        onPermitAcquired.onFailure(e);
                        return;
                    }
                } else {
                    globalCheckpointUpdated = false;
                }
            }
        } else {
            globalCheckpointUpdated = false;
        }
        assert operationPrimaryTerm <= primaryTerm : "operation primary term [" + operationPrimaryTerm + "] should be at most [" + primaryTerm + "]";
        indexShardOperationPermits.acquire(new ActionListener<Releasable>() {

            @Override
            public void onResponse(final Releasable releasable) {
                if (operationPrimaryTerm < primaryTerm) {
                    releasable.close();
                    final String message = String.format(Locale.ROOT, "%s operation primary term [%d] is too old (current [%d])", shardId, operationPrimaryTerm, primaryTerm);
                    onPermitAcquired.onFailure(new IllegalStateException(message));
                } else {
                    if (globalCheckpointUpdated == false) {
                        try {
                            updateGlobalCheckpointOnReplica(globalCheckpoint, "operation");
                        } catch (Exception e) {
                            releasable.close();
                            onPermitAcquired.onFailure(e);
                            return;
                        }
                    }
                    onPermitAcquired.onResponse(releasable);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                onPermitAcquired.onFailure(e);
            }
        }, executorOnDelay, true);
    }

    public int getActiveOperationsCount() {
        return indexShardOperationPermits.getActiveOperationsCount();
    }

    private final AsyncIOProcessor<Translog.Location> translogSyncProcessor = new AsyncIOProcessor<Translog.Location>(logger, 1024) {

        @Override
        protected void write(List<Tuple<Translog.Location, Consumer<Exception>>> candidates) throws IOException {
            try {
                final Engine engine = getEngine();
                engine.getTranslog().ensureSynced(candidates.stream().map(Tuple::v1));
            } catch (AlreadyClosedException ex) {
            } catch (IOException ex) {
                logger.debug("failed to sync translog", ex);
                throw ex;
            }
        }
    };

    public final void sync(Translog.Location location, Consumer<Exception> syncListener) {
        verifyNotClosed();
        translogSyncProcessor.put(location, syncListener);
    }

    public final void sync() throws IOException {
        verifyNotClosed();
        getEngine().getTranslog().sync();
    }

    public Translog.Durability getTranslogDurability() {
        return indexSettings.getTranslogDurability();
    }

    private final AtomicBoolean flushOrRollRunning = new AtomicBoolean();

    public void afterWriteOperation() {
        if (shouldFlush() || shouldRollTranslogGeneration()) {
            if (flushOrRollRunning.compareAndSet(false, true)) {
                if (shouldFlush()) {
                    logger.debug("submitting async flush request");
                    final AbstractRunnable flush = new AbstractRunnable() {

                        @Override
                        public void onFailure(final Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to flush index", e);
                            }
                        }

                        @Override
                        protected void doRun() throws IOException {
                            flush(new FlushRequest());
                        }

                        @Override
                        public void onAfter() {
                            flushOrRollRunning.compareAndSet(true, false);
                            afterWriteOperation();
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(flush);
                } else if (shouldRollTranslogGeneration()) {
                    logger.debug("submitting async roll translog generation request");
                    final AbstractRunnable roll = new AbstractRunnable() {

                        @Override
                        public void onFailure(final Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to roll translog generation", e);
                            }
                        }

                        @Override
                        protected void doRun() throws Exception {
                            rollTranslogGeneration();
                        }

                        @Override
                        public void onAfter() {
                            flushOrRollRunning.compareAndSet(true, false);
                            afterWriteOperation();
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(roll);
                } else {
                    flushOrRollRunning.compareAndSet(true, false);
                }
            }
        }
    }

    private RefreshListeners buildRefreshListeners() {
        return new RefreshListeners(indexSettings::getMaxRefreshListeners, () -> refresh("too_many_listeners"), threadPool.executor(ThreadPool.Names.LISTENER)::execute, logger);
    }

    public static final class ShardFailure {

        public final ShardRouting routing;

        public final String reason;

        @Nullable
        public final Exception cause;

        public ShardFailure(ShardRouting routing, String reason, @Nullable Exception cause) {
            this.routing = routing;
            this.reason = reason;
            this.cause = cause;
        }
    }

    EngineFactory getEngineFactory() {
        return engineFactory;
    }

    public boolean scheduledRefresh() {
        boolean listenerNeedsRefresh = refreshListeners.refreshNeeded();
        if (isReadAllowed() && (listenerNeedsRefresh || getEngine().refreshNeeded())) {
            if (listenerNeedsRefresh == false && isSearchIdle() && indexSettings.isExplicitRefresh() == false) {
                setRefreshPending();
                return false;
            } else {
                refresh("schedule");
                return true;
            }
        }
        return false;
    }

    final boolean isSearchIdle() {
        return (threadPool.relativeTimeInMillis() - lastSearcherAccess.get()) >= indexSettings.getSearchIdleAfter().getMillis();
    }

    final long getLastSearcherAccess() {
        return lastSearcherAccess.get();
    }

    private void setRefreshPending() {
        Engine engine = getEngine();
        Translog.Location lastWriteLocation = engine.getTranslog().getLastWriteLocation();
        Translog.Location location;
        do {
            location = this.pendingRefreshLocation.get();
            if (location != null && lastWriteLocation.compareTo(location) <= 0) {
                break;
            }
        } while (pendingRefreshLocation.compareAndSet(location, lastWriteLocation) == false);
    }

    public final void awaitShardSearchActive(Consumer<Boolean> listener) {
        if (isSearchIdle()) {
            markSearcherAccessed();
        }
        final Translog.Location location = pendingRefreshLocation.get();
        if (location != null) {
            addRefreshListener(location, (b) -> {
                pendingRefreshLocation.compareAndSet(location, null);
                listener.accept(true);
            });
        } else {
            listener.accept(false);
        }
    }

    public void addRefreshListener(Translog.Location location, Consumer<Boolean> listener) {
        final boolean readAllowed;
        if (isReadAllowed()) {
            readAllowed = true;
        } else {
            synchronized (mutex) {
                readAllowed = isReadAllowed();
            }
        }
        if (readAllowed) {
            refreshListeners.addOrNotify(location, listener);
        } else {
            listener.accept(false);
        }
    }

    private static class RefreshMetricUpdater implements ReferenceManager.RefreshListener {

        private final MeanMetric refreshMetric;

        private long currentRefreshStartTime;

        private Thread callingThread = null;

        private RefreshMetricUpdater(MeanMetric refreshMetric) {
            this.refreshMetric = refreshMetric;
        }

        @Override
        public void beforeRefresh() throws IOException {
            if (Assertions.ENABLED) {
                assert callingThread == null : "beforeRefresh was called by " + callingThread.getName() + " without a corresponding call to afterRefresh";
                callingThread = Thread.currentThread();
            }
            currentRefreshStartTime = System.nanoTime();
        }

        @Override
        public void afterRefresh(boolean didRefresh) throws IOException {
            if (Assertions.ENABLED) {
                assert callingThread != null : "afterRefresh called but not beforeRefresh";
                assert callingThread == Thread.currentThread() : "beforeRefreshed called by a different thread. current [" + Thread.currentThread().getName() + "], thread that called beforeRefresh [" + callingThread.getName() + "]";
                callingThread = null;
            }
            refreshMetric.inc(System.nanoTime() - currentRefreshStartTime);
        }
    }
}