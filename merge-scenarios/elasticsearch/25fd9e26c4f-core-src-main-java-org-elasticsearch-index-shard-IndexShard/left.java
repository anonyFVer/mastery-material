package org.elasticsearch.index.shard;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
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
import org.elasticsearch.index.engine.EngineClosedException;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.engine.RefreshFailedEngineException;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.ShardFieldData;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.get.ShardGetService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.ShardSearchStats;
import org.elasticsearch.index.seqno.GlobalCheckpointService;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.snapshots.IndexShardRepository;
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
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTargetService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.search.suggest.completion.CompletionFieldStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.search.suggest.completion2x.Completion090PostingsFormat;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

    private final IndexFieldDataService indexFieldDataService;

    private final ShardBitsetFilterCache shardBitsetFilterCache;

    private final Object mutex = new Object();

    private final String checkIndexOnStartup;

    private final CodecService codecService;

    private final Engine.Warmer warmer;

    private final SnapshotDeletionPolicy deletionPolicy;

    private final SimilarityService similarityService;

    private final TranslogConfig translogConfig;

    private final IndexEventListener indexEventListener;

    private final QueryCachingPolicy cachingPolicy;

    private final AtomicLong writingBytes = new AtomicLong();

    private final SearchOperationListener searchOperationListener;

    protected volatile ShardRouting shardRouting;

    protected volatile IndexShardState state;

    protected volatile long primaryTerm;

    protected final AtomicReference<Engine> currentEngineReference = new AtomicReference<>();

    protected final EngineFactory engineFactory;

    private final IndexingOperationListener indexingOperationListeners;

    @Nullable
    private RecoveryState recoveryState;

    private final RecoveryStats recoveryStats = new RecoveryStats();

    private final MeanMetric refreshMetric = new MeanMetric();

    private final MeanMetric flushMetric = new MeanMetric();

    private final ShardEventListener shardEventListener = new ShardEventListener();

    private final ShardPath path;

    private final IndexShardOperationsLock indexShardOperationsLock;

    private static final EnumSet<IndexShardState> readAllowedStates = EnumSet.of(IndexShardState.STARTED, IndexShardState.RELOCATED, IndexShardState.POST_RECOVERY);

    public static final EnumSet<IndexShardState> writeAllowedStatesForPrimary = EnumSet.of(IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED);

    private static final EnumSet<IndexShardState> writeAllowedStatesForReplica = EnumSet.of(IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED, IndexShardState.RELOCATED);

    private final IndexSearcherWrapper searcherWrapper;

    private final Runnable globalCheckpointSyncer;

    private final AtomicBoolean active = new AtomicBoolean();

    @Nullable
    private final RefreshListeners refreshListeners;

    public IndexShard(ShardRouting shardRouting, IndexSettings indexSettings, ShardPath path, Store store, IndexCache indexCache, MapperService mapperService, SimilarityService similarityService, IndexFieldDataService indexFieldDataService, @Nullable EngineFactory engineFactory, IndexEventListener indexEventListener, IndexSearcherWrapper indexSearcherWrapper, ThreadPool threadPool, BigArrays bigArrays, Engine.Warmer warmer, Runnable globalCheckpointSyncer, List<SearchOperationListener> searchOperationListener, List<IndexingOperationListener> listeners) throws IOException {
        super(shardRouting.shardId(), indexSettings);
        assert shardRouting.initializing();
        this.shardRouting = shardRouting;
        final Settings settings = indexSettings.getSettings();
        this.codecService = new CodecService(mapperService, logger);
        this.warmer = warmer;
        this.deletionPolicy = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
        this.similarityService = similarityService;
        Objects.requireNonNull(store, "Store must be provided to the index shard");
        this.engineFactory = engineFactory == null ? new InternalEngineFactory() : engineFactory;
        this.store = store;
        this.indexEventListener = indexEventListener;
        this.threadPool = threadPool;
        this.mapperService = mapperService;
        this.indexCache = indexCache;
        this.internalIndexingStats = new InternalIndexingStats();
        final List<IndexingOperationListener> listenersList = new ArrayList<>(listeners);
        listenersList.add(internalIndexingStats);
        this.indexingOperationListeners = new IndexingOperationListener.CompositeListener(listenersList, logger);
        final List<SearchOperationListener> searchListenersList = new ArrayList<>(searchOperationListener);
        searchListenersList.add(searchStats);
        this.searchOperationListener = new SearchOperationListener.CompositeListener(searchListenersList, logger);
        this.globalCheckpointSyncer = globalCheckpointSyncer;
        this.getService = new ShardGetService(indexSettings, this, mapperService);
        this.shardWarmerService = new ShardIndexWarmerService(shardId, indexSettings);
        this.requestCacheStats = new ShardRequestCache();
        this.shardFieldData = new ShardFieldData();
        this.indexFieldDataService = indexFieldDataService;
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
        indexShardOperationsLock = new IndexShardOperationsLock(shardId, logger, threadPool);
        searcherWrapper = indexSearcherWrapper;
        primaryTerm = indexSettings.getIndexMetaData().primaryTerm(shardId.id());
        refreshListeners = buildRefreshListeners();
        persistMetadata(shardRouting, null);
    }

    public Store store() {
        return this.store;
    }

    public boolean canIndex() {
        return true;
    }

    public ShardGetService getService() {
        return this.getService;
    }

    public ShardBitsetFilterCache shardBitsetFilterCache() {
        return shardBitsetFilterCache;
    }

    public IndexFieldDataService indexFieldDataService() {
        return indexFieldDataService;
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

    Runnable getGlobalCheckpointSyncer() {
        return globalCheckpointSyncer;
    }

    public long getPrimaryTerm() {
        return this.primaryTerm;
    }

    public void updatePrimaryTerm(final long newTerm) {
        synchronized (mutex) {
            if (newTerm != primaryTerm) {
                assert shardRouting.primary() == false : "a primary shard should never update it's term. shard: " + shardRouting + " current term [" + primaryTerm + "] new term [" + newTerm + "]";
                assert newTerm > primaryTerm : "primary terms can only go up. current [" + primaryTerm + "], new [" + newTerm + "]";
                primaryTerm = newTerm;
            }
        }
    }

    @Override
    public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    public QueryCachingPolicy getQueryCachingPolicy() {
        return cachingPolicy;
    }

    public void updateRoutingEntry(final ShardRouting newRouting) throws IOException {
        final ShardRouting currentRouting = this.shardRouting;
        if (!newRouting.shardId().equals(shardId())) {
            throw new IllegalArgumentException("Trying to set a routing entry with shardId " + newRouting.shardId() + " on a shard with shardId " + shardId() + "");
        }
        if ((currentRouting == null || newRouting.isSameAllocation(currentRouting)) == false) {
            throw new IllegalArgumentException("Trying to set a routing entry with a different allocation. Current " + currentRouting + ", new " + newRouting);
        }
        if (currentRouting != null) {
            if (!newRouting.primary() && currentRouting.primary()) {
                logger.warn("suspect illegal state: trying to move shard from primary mode to replica mode");
            }
            if (currentRouting.equals(newRouting)) {
                return;
            }
        }
        if (state == IndexShardState.POST_RECOVERY) {
            if (newRouting.state() == ShardRoutingState.STARTED || newRouting.state() == ShardRoutingState.RELOCATING) {
                try {
                    getEngine().refresh("cluster_state_started");
                } catch (Exception e) {
                    logger.debug("failed to refresh due to move to cluster wide started", e);
                }
                boolean movedToStarted = false;
                synchronized (mutex) {
                    if (state == IndexShardState.POST_RECOVERY) {
                        changeState(IndexShardState.STARTED, "global state is [" + newRouting.state() + "]");
                        movedToStarted = true;
                    } else {
                        logger.debug("state [{}] not changed, not in POST_RECOVERY, global state is [{}]", state, newRouting.state());
                    }
                }
                if (movedToStarted) {
                    indexEventListener.afterIndexShardStarted(this);
                }
            }
        }
        if (state == IndexShardState.RELOCATED && (newRouting.relocating() == false || newRouting.equalsIgnoringMetaData(currentRouting) == false)) {
            throw new IndexShardRelocatedException(shardId(), "Shard is marked as relocated, cannot safely move to state " + newRouting.state());
        }
        this.shardRouting = newRouting;
        indexEventListener.shardRoutingChanged(this, currentRouting, newRouting);
        persistMetadata(newRouting, currentRouting);
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

    public void relocated(String reason) throws IllegalIndexShardStateException, InterruptedException {
        try {
            indexShardOperationsLock.blockOperations(30, TimeUnit.MINUTES, () -> {
                assert indexShardOperationsLock.getActiveOperationsCount() == 0 : "in-flight operations in progress while moving shard state to relocated";
                synchronized (mutex) {
                    if (state != IndexShardState.STARTED) {
                        throw new IndexShardNotStartedException(shardId, state);
                    }
                    changeState(IndexShardState.RELOCATED, reason);
                }
            });
        } catch (TimeoutException e) {
            logger.warn("timed out waiting for relocation hand-off to complete");
            failShard("timed out waiting for relocation hand-off to complete", null);
            throw new IndexShardClosedException(shardId(), "timed out waiting for relocation hand-off to complete");
        }
    }

    public IndexShardState state() {
        return state;
    }

    private IndexShardState changeState(IndexShardState newState, String reason) {
        logger.debug("state: [{}]->[{}], reason [{}]", state, newState, reason);
        IndexShardState previousState = state;
        state = newState;
        this.indexEventListener.indexShardStateChanged(this, previousState, newState, reason);
        return previousState;
    }

    public Engine.Index prepareIndexOnPrimary(SourceToParse source, long version, VersionType versionType) {
        try {
            verifyPrimary();
            return prepareIndex(docMapper(source.type()), source, SequenceNumbersService.UNASSIGNED_SEQ_NO, version, versionType, Engine.Operation.Origin.PRIMARY);
        } catch (Exception e) {
            verifyNotClosed(e);
            throw e;
        }
    }

    public Engine.Index prepareIndexOnReplica(SourceToParse source, long seqNo, long version, VersionType versionType) {
        try {
            verifyReplicationTarget();
            return prepareIndex(docMapper(source.type()), source, seqNo, version, versionType, Engine.Operation.Origin.REPLICA);
        } catch (Exception e) {
            verifyNotClosed(e);
            throw e;
        }
    }

    static Engine.Index prepareIndex(DocumentMapperForType docMapper, SourceToParse source, long seqNo, long version, VersionType versionType, Engine.Operation.Origin origin) {
        long startTime = System.nanoTime();
        ParsedDocument doc = docMapper.getDocumentMapper().parse(source);
        if (docMapper.getMapping() != null) {
            doc.addDynamicMappingsUpdate(docMapper.getMapping());
        }
        MappedFieldType uidFieldType = docMapper.getDocumentMapper().uidMapper().fieldType();
        Query uidQuery = uidFieldType.termQuery(doc.uid(), null);
        Term uid = MappedFieldType.extractTerm(uidQuery);
        doc.seqNo().setLongValue(seqNo);
        return new Engine.Index(uid, doc, seqNo, version, versionType, origin, startTime);
    }

    public boolean index(Engine.Index index) {
        ensureWriteAllowed(index);
        Engine engine = getEngine();
        return index(engine, index);
    }

    private boolean index(Engine engine, Engine.Index index) {
        active.set(true);
        index = indexingOperationListeners.preIndex(index);
        final boolean created;
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("index [{}][{}]{}", index.type(), index.id(), index.docs());
            }
            created = engine.index(index);
            index.endTime(System.nanoTime());
        } catch (Exception e) {
            indexingOperationListeners.postIndex(index, e);
            throw e;
        }
        indexingOperationListeners.postIndex(index, created);
        return created;
    }

    public Engine.Delete prepareDeleteOnPrimary(String type, String id, long version, VersionType versionType) {
        verifyPrimary();
        final DocumentMapper documentMapper = docMapper(type).getDocumentMapper();
        final MappedFieldType uidFieldType = documentMapper.uidMapper().fieldType();
        final Query uidQuery = uidFieldType.termQuery(Uid.createUid(type, id), null);
        final Term uid = MappedFieldType.extractTerm(uidQuery);
        return prepareDelete(type, id, uid, SequenceNumbersService.UNASSIGNED_SEQ_NO, version, versionType, Engine.Operation.Origin.PRIMARY);
    }

    public Engine.Delete prepareDeleteOnReplica(String type, String id, long seqNo, long version, VersionType versionType) {
        verifyReplicationTarget();
        final DocumentMapper documentMapper = docMapper(type).getDocumentMapper();
        final MappedFieldType uidFieldType = documentMapper.uidMapper().fieldType();
        final Query uidQuery = uidFieldType.termQuery(Uid.createUid(type, id), null);
        final Term uid = MappedFieldType.extractTerm(uidQuery);
        return prepareDelete(type, id, uid, seqNo, version, versionType, Engine.Operation.Origin.REPLICA);
    }

    static Engine.Delete prepareDelete(String type, String id, Term uid, long seqNo, long version, VersionType versionType, Engine.Operation.Origin origin) {
        long startTime = System.nanoTime();
        return new Engine.Delete(type, id, uid, seqNo, version, versionType, origin, startTime, false);
    }

    public void delete(Engine.Delete delete) {
        ensureWriteAllowed(delete);
        Engine engine = getEngine();
        delete(engine, delete);
    }

    private void delete(Engine engine, Engine.Delete delete) {
        active.set(true);
        delete = indexingOperationListeners.preDelete(delete);
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("delete [{}]", delete.uid().text());
            }
            engine.delete(delete);
            delete.endTime(System.nanoTime());
        } catch (Exception e) {
            indexingOperationListeners.postDelete(delete, e);
            throw e;
        }
        indexingOperationListeners.postDelete(delete);
    }

    public Engine.GetResult get(Engine.Get get) {
        readAllowed();
        return getEngine().get(get, this::acquireSearcher);
    }

    public void refresh(String source) {
        verifyNotClosed();
        if (canIndex()) {
            long bytes = getEngine().getIndexBufferRAMBytesUsed();
            writingBytes.addAndGet(bytes);
            try {
                if (logger.isTraceEnabled()) {
                    logger.trace("refresh with source [{}] indexBufferRAMBytesUsed [{}]", source, new ByteSizeValue(bytes));
                }
                long time = System.nanoTime();
                getEngine().refresh(source);
                refreshMetric.inc(System.nanoTime() - time);
            } finally {
                if (logger.isTraceEnabled()) {
                    logger.trace("remove [{}] writing bytes for shard [{}]", new ByteSizeValue(bytes), shardId());
                }
                writingBytes.addAndGet(-bytes);
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("refresh with source [{}]", source);
            }
            long time = System.nanoTime();
            getEngine().refresh(source);
            refreshMetric.inc(System.nanoTime() - time);
        }
    }

    public long getWritingBytes() {
        return writingBytes.get();
    }

    public RefreshStats refreshStats() {
        return new RefreshStats(refreshMetric.count(), TimeUnit.NANOSECONDS.toMillis(refreshMetric.sum()));
    }

    public FlushStats flushStats() {
        return new FlushStats(flushMetric.count(), TimeUnit.NANOSECONDS.toMillis(flushMetric.sum()));
    }

    public DocsStats docStats() {
        readAllowed();
        final Engine engine = getEngine();
        return engine.getDocStats();
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
        try (final Engine.Searcher currentSearcher = acquireSearcher("completion_stats")) {
            completionStats.add(CompletionFieldStats.completionStats(currentSearcher.reader(), fields));
            Completion090PostingsFormat postingsFormat = ((Completion090PostingsFormat) PostingsFormat.forName(Completion090PostingsFormat.CODEC_NAME));
            completionStats.add(postingsFormat.completionStats(currentSearcher.reader(), fields));
        }
        return completionStats;
    }

    public Engine.SyncedFlushResult syncFlush(String syncId, Engine.CommitId expectedCommitId) {
        verifyStartedOrRecovering();
        logger.trace("trying to sync flush. sync id [{}]. expected commit id [{}]]", syncId, expectedCommitId);
        return getEngine().syncFlush(syncId, expectedCommitId);
    }

    public Engine.CommitId flush(FlushRequest request) throws ElasticsearchException {
        boolean waitIfOngoing = request.waitIfOngoing();
        boolean force = request.force();
        if (logger.isTraceEnabled()) {
            logger.trace("flush with {}", request);
        }
        verifyStartedOrRecovering();
        long time = System.nanoTime();
        Engine.CommitId commitId = getEngine().flush(force, waitIfOngoing);
        flushMetric.inc(System.nanoTime() - time);
        return commitId;
    }

    public void forceMerge(ForceMergeRequest forceMerge) throws IOException {
        verifyStarted();
        if (logger.isTraceEnabled()) {
            logger.trace("force merge with {}", forceMerge);
        }
        getEngine().forceMerge(forceMerge.flush(), forceMerge.maxNumSegments(), forceMerge.onlyExpungeDeletes(), false, false);
    }

    public org.apache.lucene.util.Version upgrade(UpgradeRequest upgrade) throws IOException {
        verifyStarted();
        if (logger.isTraceEnabled()) {
            logger.trace("upgrade with {}", upgrade);
        }
        org.apache.lucene.util.Version previousVersion = minimumCompatibleVersion();
        getEngine().forceMerge(true, Integer.MAX_VALUE, false, true, upgrade.upgradeOnlyAncientSegments());
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

    public IndexCommit snapshotIndex(boolean flushFirst) throws EngineException {
        IndexShardState state = this.state;
        if (state == IndexShardState.STARTED || state == IndexShardState.RELOCATED || state == IndexShardState.CLOSED) {
            return getEngine().snapshotIndex(flushFirst);
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }

    public void releaseSnapshot(IndexCommit snapshot) throws IOException {
        deletionPolicy.release(snapshot);
    }

    public void failShard(String reason, @Nullable Exception e) {
        getEngine().failEngine(reason, e);
    }

    public Engine.Searcher acquireSearcher(String source) {
        readAllowed();
        final Engine engine = getEngine();
        final Engine.Searcher searcher = engine.acquireSearcher(source);
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
                    IOUtils.close(engine);
                    indexShardOperationsLock.close();
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

    public int performBatchRecovery(Iterable<Translog.Operation> operations) {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        active.set(true);
        Engine engine = getEngine();
        return engine.config().getTranslogRecoveryPerformer().performBatchRecovery(engine, operations);
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
        if (Booleans.parseBoolean(checkIndexOnStartup, false)) {
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
        final EngineConfig config = newEngineConfig(openMode);
        config.setEnableGcDeletes(false);
        Engine newEngine = createNewEngine(config);
        verifyNotClosed();
        if (openMode == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
            active.set(true);
            newEngine.recoverFromTranslog();
        }
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
            final Engine engine = this.currentEngineReference.getAndSet(null);
            IOUtils.close(engine);
            recoveryState().setStage(RecoveryState.Stage.INIT);
        }
    }

    public RecoveryStats recoveryStats() {
        return recoveryStats;
    }

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

    private void ensureWriteAllowed(Engine.Operation op) throws IllegalIndexShardStateException {
        Engine.Operation.Origin origin = op.origin();
        IndexShardState state = this.state;
        if (origin == Engine.Operation.Origin.PRIMARY) {
            if (writeAllowedStatesForPrimary.contains(state) == false) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard state is one of " + writeAllowedStatesForPrimary + ", origin [" + origin + "]");
            }
        } else if (origin.isRecovery()) {
            if (state != IndexShardState.RECOVERING) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when recovering, origin [" + origin + "]");
            }
        } else {
            assert origin == Engine.Operation.Origin.REPLICA;
            if (writeAllowedStatesForReplica.contains(state) == false) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when shard state is one of " + writeAllowedStatesForReplica + ", origin [" + origin + "]");
            }
        }
    }

    private void verifyPrimary() {
        if (shardRouting.primary() == false) {
            throw new IllegalStateException("shard is not a primary " + shardRouting);
        }
    }

    private void verifyReplicationTarget() {
        final IndexShardState state = state();
        if (shardRouting.primary() && shardRouting.active() && state != IndexShardState.RELOCATED) {
            throw new IllegalStateException("active primary shard cannot be a replication target before " + " relocation hand off " + shardRouting + ", state is [" + state + "]");
        }
    }

    protected final void verifyStartedOrRecovering() throws IllegalIndexShardStateException {
        IndexShardState state = this.state;
        if (state != IndexShardState.STARTED && state != IndexShardState.RECOVERING && state != IndexShardState.POST_RECOVERY) {
            throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when started/recovering");
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

    protected final void verifyStarted() throws IllegalIndexShardStateException {
        IndexShardState state = this.state;
        if (state != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(shardId, state);
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

    public void addShardFailureCallback(Callback<ShardFailure> onShardFailure) {
        this.shardEventListener.delegates.add(onShardFailure);
    }

    public void checkIdle(long inactiveTimeNS) {
        Engine engineOrNull = getEngineOrNull();
        if (engineOrNull != null && System.nanoTime() - engineOrNull.getLastWriteNanos() >= inactiveTimeNS) {
            boolean wasActive = active.getAndSet(false);
            if (wasActive) {
                logger.debug("shard is now inactive");
                indexEventListener.onShardInactive(this);
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
        boolean shouldExist = shardRouting.allocatedPostIndexCreate(indexSettings.getIndexMetaData());
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromStore(this, shouldExist);
    }

    public boolean restoreFromRepository(IndexShardRepository repository) {
        assert shardRouting.primary() : "recover from store only makes sense if the shard is a primary shard";
        StoreRecovery storeRecovery = new StoreRecovery(shardId, logger);
        return storeRecovery.recoverFromRepository(this, repository);
    }

    boolean shouldFlush() {
        Engine engine = getEngineOrNull();
        if (engine != null) {
            try {
                Translog translog = engine.getTranslog();
                return translog.sizeInBytes() > indexSettings.getFlushThresholdSize().bytes();
            } catch (AlreadyClosedException | EngineClosedException ex) {
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

    public Translog.View acquireTranslogView() {
        Engine engine = getEngine();
        assert engine.getTranslog() != null : "translog must not be null";
        return engine.getTranslog().newView();
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

    public IndexEventListener getIndexEventListener() {
        return indexEventListener;
    }

    public void activateThrottling() {
        try {
            getEngine().activateThrottling();
        } catch (EngineClosedException ex) {
        }
    }

    public void deactivateThrottling() {
        try {
            getEngine().deactivateThrottling();
        } catch (EngineClosedException ex) {
        }
    }

    private void handleRefreshException(Exception e) {
        if (e instanceof EngineClosedException) {
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
        if (canIndex() == false) {
            throw new UnsupportedOperationException();
        }
        try {
            Engine engine = getEngine();
            long bytes = engine.getIndexBufferRAMBytesUsed();
            logger.debug("add [{}] writing bytes for shard [{}]", new ByteSizeValue(bytes), shardId());
            writingBytes.addAndGet(bytes);
            try {
                engine.writeIndexingBuffer();
            } finally {
                writingBytes.addAndGet(-bytes);
                logger.debug("remove [{}] writing bytes for shard [{}]", new ByteSizeValue(bytes), shardId());
            }
        } catch (Exception e) {
            handleRefreshException(e);
        }
    }

    public void updateLocalCheckpointForShard(String allocationId, long checkpoint) {
        verifyPrimary();
        getEngine().seqNoService().updateLocalCheckpointForShard(allocationId, checkpoint);
    }

    public void markAllocationIdAsInSync(String allocationId, long localCheckpoint) {
        verifyPrimary();
        getEngine().seqNoService().markAllocationIdAsInSync(allocationId, localCheckpoint);
    }

    public long getLocalCheckpoint() {
        return getEngine().seqNoService().getLocalCheckpoint();
    }

    public long getGlobalCheckpoint() {
        return getEngine().seqNoService().getGlobalCheckpoint();
    }

    public void updateGlobalCheckpointOnPrimary() {
        verifyPrimary();
        if (getEngine().seqNoService().updateGlobalCheckpointOnPrimary()) {
            globalCheckpointSyncer.run();
        }
    }

    public void updateGlobalCheckpointOnReplica(long checkpoint) {
        verifyReplicationTarget();
        getEngine().seqNoService().updateGlobalCheckpointOnReplica(checkpoint);
    }

    public void updateAllocationIdsFromMaster(Set<String> activeAllocationIds, Set<String> initializingAllocationIds) {
        verifyPrimary();
        Engine engine = getEngineOrNull();
        if (engine != null) {
            engine.seqNoService().updateAllocationIdsFromMaster(activeAllocationIds, initializingAllocationIds);
        }
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
            MetadataSnapshot metadata = store.getMetadata();
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
            throw new EngineClosedException(shardId);
        }
        return engine;
    }

    protected Engine getEngineOrNull() {
        return this.currentEngineReference.get();
    }

    public void startRecovery(RecoveryState recoveryState, RecoveryTargetService recoveryTargetService, RecoveryTargetService.RecoveryListener recoveryListener, RepositoriesService repositoriesService, BiConsumer<String, MappingMetaData> mappingUpdateConsumer, IndicesService indicesService) {
        switch(recoveryState.getType()) {
            case PRIMARY_RELOCATION:
            case REPLICA:
                try {
                    markAsRecovering("from " + recoveryState.getSourceNode(), recoveryState);
                    recoveryTargetService.startRecovery(this, recoveryState.getType(), recoveryState.getSourceNode(), recoveryListener);
                } catch (Exception e) {
                    failShard("corrupted preexisting index", e);
                    recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                }
                break;
            case STORE:
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
            case LOCAL_SHARDS:
                final IndexMetaData indexMetaData = indexSettings().getIndexMetaData();
                final Index mergeSourceIndex = indexMetaData.getMergeSourceIndex();
                final List<IndexShard> startedShards = new ArrayList<>();
                final IndexService sourceIndexService = indicesService.indexService(mergeSourceIndex);
                final int numShards = sourceIndexService != null ? sourceIndexService.getIndexSettings().getNumberOfShards() : -1;
                if (sourceIndexService != null) {
                    for (IndexShard shard : sourceIndexService) {
                        if (shard.state() == IndexShardState.STARTED) {
                            startedShards.add(shard);
                        }
                    }
                }
                if (numShards == startedShards.size()) {
                    markAsRecovering("from local shards", recoveryState);
                    threadPool.generic().execute(() -> {
                        try {
                            final Set<ShardId> shards = IndexMetaData.selectShrinkShards(shardId().id(), sourceIndexService.getMetaData(), +indexMetaData.getNumberOfShards());
                            if (recoverFromLocalShards(mappingUpdateConsumer, startedShards.stream().filter((s) -> shards.contains(s.shardId())).collect(Collectors.toList()))) {
                                recoveryListener.onRecoveryDone(recoveryState);
                            }
                        } catch (Exception e) {
                            recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                        }
                    });
                } else {
                    final Exception e;
                    if (numShards == -1) {
                        e = new IndexNotFoundException(mergeSourceIndex);
                    } else {
                        e = new IllegalStateException("not all shards from index " + mergeSourceIndex + " are started yet, expected " + numShards + " found " + startedShards.size() + " can't recover shard " + shardId());
                    }
                    recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, e), true);
                }
                break;
            case SNAPSHOT:
                markAsRecovering("from snapshot", recoveryState);
                threadPool.generic().execute(() -> {
                    try {
                        final IndexShardRepository indexShardRepository = repositoriesService.indexShardRepository(recoveryState.getRestoreSource().snapshot().getRepository());
                        if (restoreFromRepository(indexShardRepository)) {
                            recoveryListener.onRecoveryDone(recoveryState);
                        }
                    } catch (Exception first) {
                        recoveryListener.onRecoveryFailure(recoveryState, new RecoveryFailedException(recoveryState, null, first), true);
                    }
                });
                break;
            default:
                throw new IllegalArgumentException("Unknown recovery type " + recoveryState.getType());
        }
    }

    class ShardEventListener implements Engine.EventListener {

        private final CopyOnWriteArrayList<Callback<ShardFailure>> delegates = new CopyOnWriteArrayList<>();

        @Override
        public void onFailedEngine(String reason, @Nullable Exception failure) {
            final ShardFailure shardFailure = new ShardFailure(shardRouting, reason, failure);
            for (Callback<ShardFailure> listener : delegates) {
                try {
                    listener.handle(shardFailure);
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
                throw new EngineClosedException(shardId);
            }
            assert this.currentEngineReference.get() == null;
            this.currentEngineReference.set(newEngine(config));
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

    void persistMetadata(ShardRouting newRouting, @Nullable ShardRouting currentRouting) throws IOException {
        assert newRouting != null : "newRouting must not be null";
        if (currentRouting == null || currentRouting.primary() != newRouting.primary() || currentRouting.allocationId().equals(newRouting.allocationId()) == false) {
            assert currentRouting == null || currentRouting.isSameAllocation(newRouting);
            final String writeReason;
            if (currentRouting == null) {
                writeReason = "initial state with allocation id [" + newRouting.allocationId() + "]";
            } else {
                writeReason = "routing changed from " + currentRouting + " to " + newRouting;
            }
            logger.trace("{} writing shard state, reason [{}]", shardId, writeReason);
            final ShardStateMetaData newShardStateMetadata = new ShardStateMetaData(newRouting.primary(), getIndexUUID(), newRouting.allocationId());
            ShardStateMetaData.FORMAT.write(newShardStateMetadata, shardPath().getShardStatePath());
        } else {
            logger.trace("{} skip writing shard state, has been written before", shardId);
        }
    }

    private String getIndexUUID() {
        return indexSettings.getUUID();
    }

    private DocumentMapperForType docMapper(String type) {
        return mapperService.documentMapperWithAutoCreate(type);
    }

    private EngineConfig newEngineConfig(EngineConfig.OpenMode openMode) {
        final IndexShardRecoveryPerformer translogRecoveryPerformer = new IndexShardRecoveryPerformer(shardId, mapperService, logger);
        return new EngineConfig(openMode, shardId, threadPool, indexSettings, warmer, store, deletionPolicy, indexSettings.getMergePolicy(), mapperService.indexAnalyzer(), similarityService.similarity(mapperService), codecService, shardEventListener, translogRecoveryPerformer, indexCache.query(), cachingPolicy, translogConfig, IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.get(indexSettings.getSettings()), refreshListeners);
    }

    public void acquirePrimaryOperationLock(ActionListener<Releasable> onLockAcquired, String executorOnDelay) {
        verifyNotClosed();
        verifyPrimary();
        indexShardOperationsLock.acquire(onLockAcquired, executorOnDelay, false);
    }

    public void acquireReplicaOperationLock(long opPrimaryTerm, ActionListener<Releasable> onLockAcquired, String executorOnDelay) {
        verifyNotClosed();
        verifyReplicationTarget();
        if (primaryTerm > opPrimaryTerm) {
            throw new IllegalArgumentException(LoggerMessageFormat.format("{} operation term [{}] is too old (current [{}])", shardId, opPrimaryTerm, primaryTerm));
        }
        indexShardOperationsLock.acquire(onLockAcquired, executorOnDelay, true);
    }

    public int getActiveOperationsCount() {
        return indexShardOperationsLock.getActiveOperationsCount();
    }

    public void sync(Translog.Location location) {
        try {
            final Engine engine = getEngine();
            engine.getTranslog().ensureSynced(location);
        } catch (EngineClosedException ex) {
        } catch (IOException ex) {
            logger.debug("failed to sync translog", ex);
            throw new ElasticsearchException("failed to sync translog", ex);
        }
    }

    public Translog.Durability getTranslogDurability() {
        return indexSettings.getTranslogDurability();
    }

    private final AtomicBoolean asyncFlushRunning = new AtomicBoolean();

    public boolean maybeFlush() {
        if (shouldFlush()) {
            if (asyncFlushRunning.compareAndSet(false, true)) {
                if (shouldFlush() == false) {
                    asyncFlushRunning.compareAndSet(true, false);
                } else {
                    logger.debug("submitting async flush request");
                    final AbstractRunnable abstractRunnable = new AbstractRunnable() {

                        @Override
                        public void onFailure(Exception e) {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("failed to flush index", e);
                            }
                        }

                        @Override
                        protected void doRun() throws Exception {
                            flush(new FlushRequest());
                        }

                        @Override
                        public void onAfter() {
                            asyncFlushRunning.compareAndSet(true, false);
                            maybeFlush();
                        }
                    };
                    threadPool.executor(ThreadPool.Names.FLUSH).execute(abstractRunnable);
                    return true;
                }
            }
        }
        return false;
    }

    protected RefreshListeners buildRefreshListeners() {
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

    public boolean isRefreshNeeded() {
        return getEngine().refreshNeeded() || (refreshListeners != null && refreshListeners.refreshNeeded());
    }

    public void addRefreshListener(Translog.Location location, Consumer<Boolean> listener) {
        refreshListeners.addOrNotify(location, listener);
    }

    private class IndexShardRecoveryPerformer extends TranslogRecoveryPerformer {

        protected IndexShardRecoveryPerformer(ShardId shardId, MapperService mapperService, ESLogger logger) {
            super(shardId, mapperService, logger);
        }

        @Override
        protected void operationProcessed() {
            assert recoveryState != null;
            recoveryState.getTranslog().incrementRecoveredOperations();
        }

        @Override
        public int recoveryFromSnapshot(Engine engine, Translog.Snapshot snapshot) throws IOException {
            assert recoveryState != null;
            RecoveryState.Translog translogStats = recoveryState.getTranslog();
            translogStats.totalOperations(snapshot.totalOperations());
            translogStats.totalOperationsOnStart(snapshot.totalOperations());
            return super.recoveryFromSnapshot(engine, snapshot);
        }

        @Override
        protected void index(Engine engine, Engine.Index engineIndex) {
            IndexShard.this.index(engine, engineIndex);
        }

        @Override
        protected void delete(Engine engine, Engine.Delete engineDelete) {
            IndexShard.this.delete(engine, engineDelete);
        }
    }
}