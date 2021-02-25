package org.elasticsearch.indices;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.cache.request.ShardRequestCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStoreConfig;
import org.elasticsearch.indices.AbstractIndexShardCacheEntity.Loader;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;
import static org.elasticsearch.common.util.CollectionUtils.arrayAsArrayList;

public class IndicesService extends AbstractLifecycleComponent implements IndicesClusterStateService.AllocatedIndices<IndexShard, IndexService>, IndexService.ShardStoreDeleter {

    public static final String INDICES_SHARDS_CLOSED_TIMEOUT = "indices.shards_closed_timeout";

    public static final Setting<TimeValue> INDICES_CACHE_CLEAN_INTERVAL_SETTING = Setting.positiveTimeSetting("indices.cache.cleanup_interval", TimeValue.timeValueMinutes(1), Property.NodeScope);

    private final PluginsService pluginsService;

    private final NodeEnvironment nodeEnv;

    private final TimeValue shardsClosedTimeout;

    private final AnalysisRegistry analysisRegistry;

    private final IndicesQueriesRegistry indicesQueriesRegistry;

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final IndexScopedSettings indexScopeSetting;

    private final IndicesFieldDataCache indicesFieldDataCache;

    private final CacheCleaner cacheCleaner;

    private final ThreadPool threadPool;

    private final CircuitBreakerService circuitBreakerService;

    private volatile Map<String, IndexService> indices = emptyMap();

    private final Map<Index, List<PendingDelete>> pendingDeletes = new HashMap<>();

    private final AtomicInteger numUncompletedDeletes = new AtomicInteger();

    private final OldShardsStats oldShardsStats = new OldShardsStats();

    private final IndexStoreConfig indexStoreConfig;

    private final MapperRegistry mapperRegistry;

    private final NamedWriteableRegistry namedWriteableRegistry;

    private final IndexingMemoryController indexingMemoryController;

    private final TimeValue cleanInterval;

    private final IndicesRequestCache indicesRequestCache;

    private final IndicesQueryCache indicesQueryCache;

    private final MetaStateService metaStateService;

    @Override
    protected void doStart() {
        threadPool.schedule(this.cleanInterval, ThreadPool.Names.SAME, this.cacheCleaner);
    }

    public IndicesService(Settings settings, PluginsService pluginsService, NodeEnvironment nodeEnv, ClusterSettings clusterSettings, AnalysisRegistry analysisRegistry, IndicesQueriesRegistry indicesQueriesRegistry, IndexNameExpressionResolver indexNameExpressionResolver, MapperRegistry mapperRegistry, NamedWriteableRegistry namedWriteableRegistry, ThreadPool threadPool, IndexScopedSettings indexScopedSettings, CircuitBreakerService circuitBreakerService, MetaStateService metaStateService) {
        super(settings);
        this.threadPool = threadPool;
        this.pluginsService = pluginsService;
        this.nodeEnv = nodeEnv;
        this.shardsClosedTimeout = settings.getAsTime(INDICES_SHARDS_CLOSED_TIMEOUT, new TimeValue(1, TimeUnit.DAYS));
        this.indexStoreConfig = new IndexStoreConfig(settings);
        this.analysisRegistry = analysisRegistry;
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indicesRequestCache = new IndicesRequestCache(settings);
        this.indicesQueryCache = new IndicesQueryCache(settings);
        this.mapperRegistry = mapperRegistry;
        this.namedWriteableRegistry = namedWriteableRegistry;
        clusterSettings.addSettingsUpdateConsumer(IndexStoreConfig.INDICES_STORE_THROTTLE_TYPE_SETTING, indexStoreConfig::setRateLimitingType);
        clusterSettings.addSettingsUpdateConsumer(IndexStoreConfig.INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING, indexStoreConfig::setRateLimitingThrottle);
        indexingMemoryController = new IndexingMemoryController(settings, threadPool, () -> Iterables.flatten(this).iterator());
        this.indexScopeSetting = indexScopedSettings;
        this.circuitBreakerService = circuitBreakerService;
        this.indicesFieldDataCache = new IndicesFieldDataCache(settings, new IndexFieldDataCache.Listener() {

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                assert sizeInBytes >= 0 : "When reducing circuit breaker, it should be adjusted with a number higher or equal to 0 and not [" + sizeInBytes + "]";
                circuitBreakerService.getBreaker(CircuitBreaker.FIELDDATA).addWithoutBreaking(-sizeInBytes);
            }
        });
        this.cleanInterval = INDICES_CACHE_CLEAN_INTERVAL_SETTING.get(settings);
        this.cacheCleaner = new CacheCleaner(indicesFieldDataCache, indicesRequestCache, logger, threadPool, this.cleanInterval);
        this.metaStateService = metaStateService;
    }

    @Override
    protected void doStop() {
        ExecutorService indicesStopExecutor = Executors.newFixedThreadPool(5, EsExecutors.daemonThreadFactory("indices_shutdown"));
        final Set<Index> indices = this.indices.values().stream().map(s -> s.index()).collect(Collectors.toSet());
        final CountDownLatch latch = new CountDownLatch(indices.size());
        for (final Index index : indices) {
            indicesStopExecutor.execute(() -> {
                try {
                    removeIndex(index, "shutdown", false);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to remove index on stop [{}]", index), e);
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            if (latch.await(shardsClosedTimeout.seconds(), TimeUnit.SECONDS) == false) {
                logger.warn("Not all shards are closed yet, waited {}sec - stopping service", shardsClosedTimeout.seconds());
            }
        } catch (InterruptedException e) {
        } finally {
            indicesStopExecutor.shutdown();
        }
    }

    @Override
    protected void doClose() {
        IOUtils.closeWhileHandlingException(analysisRegistry, indexingMemoryController, indicesFieldDataCache, cacheCleaner, indicesRequestCache, indicesQueryCache);
    }

    public NodeIndicesStats stats(boolean includePrevious) {
        return stats(includePrevious, new CommonStatsFlags().all());
    }

    public NodeIndicesStats stats(boolean includePrevious, CommonStatsFlags flags) {
        CommonStats oldStats = new CommonStats(flags);
        if (includePrevious) {
            Flag[] setFlags = flags.getFlags();
            for (Flag flag : setFlags) {
                switch(flag) {
                    case Get:
                        oldStats.get.add(oldShardsStats.getStats);
                        break;
                    case Indexing:
                        oldStats.indexing.add(oldShardsStats.indexingStats);
                        break;
                    case Search:
                        oldStats.search.add(oldShardsStats.searchStats);
                        break;
                    case Merge:
                        oldStats.merge.add(oldShardsStats.mergeStats);
                        break;
                    case Refresh:
                        oldStats.refresh.add(oldShardsStats.refreshStats);
                        break;
                    case Recovery:
                        oldStats.recoveryStats.add(oldShardsStats.recoveryStats);
                        break;
                    case Flush:
                        oldStats.flush.add(oldShardsStats.flushStats);
                        break;
                }
            }
        }
        Map<Index, List<IndexShardStats>> statsByShard = new HashMap<>();
        for (IndexService indexService : this) {
            for (IndexShard indexShard : indexService) {
                try {
                    if (indexShard.routingEntry() == null) {
                        continue;
                    }
                    IndexShardStats indexShardStats = new IndexShardStats(indexShard.shardId(), new ShardStats[] { new ShardStats(indexShard.routingEntry(), indexShard.shardPath(), new CommonStats(indicesQueryCache, indexShard, flags), indexShard.commitStats(), indexShard.seqNoStats()) });
                    if (!statsByShard.containsKey(indexService.index())) {
                        statsByShard.put(indexService.index(), arrayAsArrayList(indexShardStats));
                    } else {
                        statsByShard.get(indexService.index()).add(indexShardStats);
                    }
                } catch (IllegalIndexShardStateException e) {
                    logger.trace((Supplier<?>) () -> new ParameterizedMessage("{} ignoring shard stats", indexShard.shardId()), e);
                }
            }
        }
        return new NodeIndicesStats(oldStats, statsByShard);
    }

    private void ensureChangesAllowed() {
        if (lifecycle.started() == false) {
            throw new IllegalStateException("Can't make changes to indices service, node is closed");
        }
    }

    @Override
    public Iterator<IndexService> iterator() {
        return indices.values().iterator();
    }

    public boolean hasIndex(Index index) {
        return indices.containsKey(index.getUUID());
    }

    @Override
    @Nullable
    public IndexService indexService(Index index) {
        return indices.get(index.getUUID());
    }

    public IndexService indexServiceSafe(Index index) {
        IndexService indexService = indices.get(index.getUUID());
        if (indexService == null) {
            throw new IndexNotFoundException(index);
        }
        assert indexService.indexUUID().equals(index.getUUID()) : "uuid mismatch local: " + indexService.indexUUID() + " incoming: " + index.getUUID();
        return indexService;
    }

    @Override
    public synchronized IndexService createIndex(final NodeServicesProvider nodeServicesProvider, IndexMetaData indexMetaData, List<IndexEventListener> builtInListeners, Consumer<ShardId> globalCheckpointSyncer) throws IOException {
        ensureChangesAllowed();
        if (indexMetaData.getIndexUUID().equals(IndexMetaData.INDEX_UUID_NA_VALUE)) {
            throw new IllegalArgumentException("index must have a real UUID found value: [" + indexMetaData.getIndexUUID() + "]");
        }
        final Index index = indexMetaData.getIndex();
        if (hasIndex(index)) {
            throw new IndexAlreadyExistsException(index);
        }
        List<IndexEventListener> finalListeners = new ArrayList<>(builtInListeners);
        final IndexEventListener onStoreClose = new IndexEventListener() {

            @Override
            public void onStoreClosed(ShardId shardId) {
                indicesQueryCache.onClose(shardId);
            }
        };
        finalListeners.add(onStoreClose);
        finalListeners.add(oldShardsStats);
        final IndexService indexService = createIndexService("create index", nodeServicesProvider, indexMetaData, indicesQueryCache, indicesFieldDataCache, finalListeners, globalCheckpointSyncer, indexingMemoryController);
        boolean success = false;
        try {
            indexService.getIndexEventListener().afterIndexCreated(indexService);
            indices = newMapBuilder(indices).put(index.getUUID(), indexService).immutableMap();
            success = true;
            return indexService;
        } finally {
            if (success == false) {
                indexService.close("plugins_failed", true);
            }
        }
    }

    private synchronized IndexService createIndexService(final String reason, final NodeServicesProvider nodeServicesProvider, IndexMetaData indexMetaData, IndicesQueryCache indicesQueryCache, IndicesFieldDataCache indicesFieldDataCache, List<IndexEventListener> builtInListeners, Consumer<ShardId> globalCheckpointSyncer, IndexingOperationListener... indexingOperationListeners) throws IOException {
        final Index index = indexMetaData.getIndex();
        final ClusterService clusterService = nodeServicesProvider.getClusterService();
        final Predicate<String> indexNameMatcher = (indexExpression) -> indexNameExpressionResolver.matchesIndex(index.getName(), indexExpression, clusterService.state());
        final IndexSettings idxSettings = new IndexSettings(indexMetaData, this.settings, indexNameMatcher, indexScopeSetting);
        logger.debug("creating Index [{}], shards [{}]/[{}{}] - reason [{}]", indexMetaData.getIndex(), idxSettings.getNumberOfShards(), idxSettings.getNumberOfReplicas(), idxSettings.isShadowReplicaIndex() ? "s" : "", reason);
        final IndexModule indexModule = new IndexModule(idxSettings, indexStoreConfig, analysisRegistry);
        for (IndexingOperationListener operationListener : indexingOperationListeners) {
            indexModule.addIndexOperationListener(operationListener);
        }
        pluginsService.onIndexModule(indexModule);
        for (IndexEventListener listener : builtInListeners) {
            indexModule.addIndexEventListener(listener);
        }
        return indexModule.newIndexService(nodeEnv, this, nodeServicesProvider, indicesQueryCache, mapperRegistry, globalCheckpointSyncer, indicesFieldDataCache);
    }

    public synchronized void verifyIndexMetadata(final NodeServicesProvider nodeServicesProvider, IndexMetaData metaData, IndexMetaData metaDataUpdate) throws IOException {
        final List<Closeable> closeables = new ArrayList<>();
        try {
            IndicesFieldDataCache indicesFieldDataCache = new IndicesFieldDataCache(settings, new IndexFieldDataCache.Listener() {
            });
            closeables.add(indicesFieldDataCache);
            IndicesQueryCache indicesQueryCache = new IndicesQueryCache(settings);
            closeables.add(indicesQueryCache);
            final IndexService service = createIndexService("metadata verification", nodeServicesProvider, metaData, indicesQueryCache, indicesFieldDataCache, Collections.emptyList(), s -> {
            });
            closeables.add(() -> service.close("metadata verification", false));
            for (ObjectCursor<MappingMetaData> typeMapping : metaData.getMappings().values()) {
                service.mapperService().merge(typeMapping.value.type(), typeMapping.value.source(), MapperService.MergeReason.MAPPING_RECOVERY, true);
            }
            if (metaData.equals(metaDataUpdate) == false) {
                service.updateMetaData(metaDataUpdate);
            }
        } finally {
            IOUtils.close(closeables);
        }
    }

    @Override
    public IndexShard createShard(ShardRouting shardRouting, RecoveryState recoveryState, PeerRecoveryTargetService recoveryTargetService, PeerRecoveryTargetService.RecoveryListener recoveryListener, RepositoriesService repositoriesService, NodeServicesProvider nodeServicesProvider, Callback<IndexShard.ShardFailure> onShardFailure) throws IOException {
        ensureChangesAllowed();
        IndexService indexService = indexService(shardRouting.index());
        IndexShard indexShard = indexService.createShard(shardRouting);
        indexShard.addShardFailureCallback(onShardFailure);
        indexShard.startRecovery(recoveryState, recoveryTargetService, recoveryListener, repositoriesService, (type, mapping) -> {
            assert recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS : "mapping update consumer only required by local shards recovery";
            try {
                nodeServicesProvider.getClient().admin().indices().preparePutMapping().setConcreteIndex(shardRouting.index()).setType(type).setSource(mapping.source().string()).get();
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to stringify mapping source", ex);
            }
        }, this);
        return indexShard;
    }

    @Override
    public void removeIndex(Index index, String reason) {
        try {
            removeIndex(index, reason, false);
        } catch (Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to remove index ({})", reason), e);
        }
    }

    private void removeIndex(Index index, String reason, boolean delete) {
        final String indexName = index.getName();
        try {
            final IndexService indexService;
            final IndexEventListener listener;
            synchronized (this) {
                if (hasIndex(index) == false) {
                    return;
                }
                logger.debug("[{}] closing ... (reason [{}])", indexName, reason);
                Map<String, IndexService> newIndices = new HashMap<>(indices);
                indexService = newIndices.remove(index.getUUID());
                assert indexService != null : "IndexService is null for index: " + index;
                indices = unmodifiableMap(newIndices);
                listener = indexService.getIndexEventListener();
            }
            listener.beforeIndexClosed(indexService);
            if (delete) {
                listener.beforeIndexDeleted(indexService);
            }
            logger.debug("{} closing index service (reason [{}])", index, reason);
            indexService.close(reason, delete);
            logger.debug("{} closed... (reason [{}])", index, reason);
            listener.afterIndexClosed(indexService.index(), indexService.getIndexSettings().getSettings());
            if (delete) {
                final IndexSettings indexSettings = indexService.getIndexSettings();
                listener.afterIndexDeleted(indexService.index(), indexSettings.getSettings());
                deleteIndexStore(reason, indexService.index(), indexSettings);
            }
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to remove index " + index, ex);
        }
    }

    public IndicesFieldDataCache getIndicesFieldDataCache() {
        return indicesFieldDataCache;
    }

    public CircuitBreakerService getCircuitBreakerService() {
        return circuitBreakerService;
    }

    public IndicesQueryCache getIndicesQueryCache() {
        return indicesQueryCache;
    }

    static class OldShardsStats implements IndexEventListener {

        final SearchStats searchStats = new SearchStats();

        final GetStats getStats = new GetStats();

        final IndexingStats indexingStats = new IndexingStats();

        final MergeStats mergeStats = new MergeStats();

        final RefreshStats refreshStats = new RefreshStats();

        final FlushStats flushStats = new FlushStats();

        final RecoveryStats recoveryStats = new RecoveryStats();

        @Override
        public synchronized void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
            if (indexShard != null) {
                getStats.addTotals(indexShard.getStats());
                indexingStats.addTotals(indexShard.indexingStats());
                searchStats.addTotals(indexShard.searchStats());
                mergeStats.addTotals(indexShard.mergeStats());
                refreshStats.addTotals(indexShard.refreshStats());
                flushStats.addTotals(indexShard.flushStats());
                recoveryStats.addTotals(indexShard.recoveryStats());
            }
        }
    }

    @Override
    public void deleteIndex(Index index, String reason) {
        try {
            removeIndex(index, reason, true);
        } catch (Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to delete index ({})", reason), e);
        }
    }

    @Override
    public void deleteUnassignedIndex(String reason, IndexMetaData metaData, ClusterState clusterState) {
        if (nodeEnv.hasNodeFile()) {
            String indexName = metaData.getIndex().getName();
            try {
                if (clusterState.metaData().hasIndex(indexName)) {
                    final IndexMetaData index = clusterState.metaData().index(indexName);
                    throw new IllegalStateException("Can't delete unassigned index store for [" + indexName + "] - it's still part of " + "the cluster state [" + index.getIndexUUID() + "] [" + metaData.getIndexUUID() + "]");
                }
                deleteIndexStore(reason, metaData, clusterState);
            } catch (IOException e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to delete unassigned index (reason [{}])", metaData.getIndex(), reason), e);
            }
        }
    }

    void deleteIndexStore(String reason, IndexMetaData metaData, ClusterState clusterState) throws IOException {
        if (nodeEnv.hasNodeFile()) {
            synchronized (this) {
                Index index = metaData.getIndex();
                if (hasIndex(index)) {
                    String localUUid = indexService(index).indexUUID();
                    throw new IllegalStateException("Can't delete index store for [" + index.getName() + "] - it's still part of the indices service [" + localUUid + "] [" + metaData.getIndexUUID() + "]");
                }
                if (clusterState.metaData().hasIndex(index.getName()) && (clusterState.nodes().getLocalNode().isMasterNode() == true)) {
                    final IndexMetaData idxMeta = clusterState.metaData().index(index.getName());
                    throw new IllegalStateException("Can't delete index store for [" + index.getName() + "] - it's still part of the " + "cluster state [" + idxMeta.getIndexUUID() + "] [" + metaData.getIndexUUID() + "], " + "we are master eligible, so will keep the index metadata even if no shards are left.");
                }
            }
            final IndexSettings indexSettings = buildIndexSettings(metaData);
            deleteIndexStore(reason, indexSettings.getIndex(), indexSettings);
        }
    }

    private void deleteIndexStore(String reason, Index index, IndexSettings indexSettings) throws IOException {
        deleteIndexStoreIfDeletionAllowed(reason, index, indexSettings, DEFAULT_INDEX_DELETION_PREDICATE);
    }

    private void deleteIndexStoreIfDeletionAllowed(final String reason, final Index index, final IndexSettings indexSettings, final IndexDeletionAllowedPredicate predicate) throws IOException {
        boolean success = false;
        try {
            logger.debug("{} deleting index store reason [{}]", index, reason);
            if (predicate.apply(index, indexSettings)) {
                nodeEnv.deleteIndexDirectorySafe(index, 0, indexSettings);
            }
            success = true;
        } catch (LockObtainFailedException ex) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("{} failed to delete index store - at least one shards is still locked", index), ex);
        } catch (Exception ex) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("{} failed to delete index", index), ex);
        } finally {
            if (success == false) {
                addPendingDelete(index, indexSettings);
            }
            MetaDataStateFormat.deleteMetaState(nodeEnv.indexPaths(index));
        }
    }

    @Override
    public void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings) throws IOException {
        ShardId shardId = lock.getShardId();
        logger.trace("{} deleting shard reason [{}]", shardId, reason);
        nodeEnv.deleteShardDirectoryUnderLock(lock, indexSettings);
    }

    public void deleteShardStore(String reason, ShardId shardId, ClusterState clusterState) throws IOException, ShardLockObtainFailedException {
        final IndexMetaData metaData = clusterState.getMetaData().indices().get(shardId.getIndexName());
        final IndexSettings indexSettings = buildIndexSettings(metaData);
        if (canDeleteShardContent(shardId, indexSettings) == false) {
            throw new IllegalStateException("Can't delete shard " + shardId);
        }
        nodeEnv.deleteShardDirectorySafe(shardId, indexSettings);
        logger.debug("{} deleted shard reason [{}]", shardId, reason);
        if (clusterState.nodes().getLocalNode().isMasterNode() == false && canDeleteIndexContents(shardId.getIndex(), indexSettings)) {
            if (nodeEnv.findAllShardIds(shardId.getIndex()).isEmpty()) {
                try {
                    deleteIndexStore("no longer used", metaData, clusterState);
                } catch (Exception e) {
                    throw new ElasticsearchException("failed to delete unused index after deleting its last shard (" + shardId + ")", e);
                }
            } else {
                logger.trace("[{}] still has shard stores, leaving as is", shardId.getIndex());
            }
        }
    }

    public boolean canDeleteIndexContents(Index index, IndexSettings indexSettings) {
        if (indexSettings.isOnSharedFilesystem() == false || indexSettings.getIndexMetaData().getState() == IndexMetaData.State.CLOSE) {
            final IndexService indexService = indexService(index);
            if (indexService == null && nodeEnv.hasNodeFile()) {
                return true;
            }
        } else {
            logger.trace("{} skipping index directory deletion due to shadow replicas", index);
        }
        return false;
    }

    @Override
    @Nullable
    public IndexMetaData verifyIndexIsDeleted(final Index index, final ClusterState clusterState) {
        if (clusterState.metaData().index(index) != null) {
            throw new IllegalStateException("Cannot delete index [" + index + "], it is still part of the cluster state.");
        }
        if (nodeEnv.hasNodeFile() && FileSystemUtils.exists(nodeEnv.indexPaths(index))) {
            final IndexMetaData metaData;
            try {
                metaData = metaStateService.loadIndexState(index);
            } catch (IOException e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to load state file from a stale deleted index, folders will be left on disk", index), e);
                return null;
            }
            final IndexSettings indexSettings = buildIndexSettings(metaData);
            try {
                deleteIndexStoreIfDeletionAllowed("stale deleted index", index, indexSettings, ALWAYS_TRUE);
            } catch (IOException e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to delete index on disk", metaData.getIndex()), e);
            }
            return metaData;
        }
        return null;
    }

    public boolean canDeleteShardContent(ShardId shardId, IndexSettings indexSettings) {
        assert shardId.getIndex().equals(indexSettings.getIndex());
        final IndexService indexService = indexService(shardId.getIndex());
        if (indexSettings.isOnSharedFilesystem() == false) {
            if (nodeEnv.hasNodeFile()) {
                final boolean isAllocated = indexService != null && indexService.hasShard(shardId.id());
                if (isAllocated) {
                    return false;
                } else if (indexSettings.hasCustomDataPath()) {
                    return Files.exists(nodeEnv.resolveCustomLocation(indexSettings, shardId));
                } else {
                    return FileSystemUtils.exists(nodeEnv.availableShardPaths(shardId));
                }
            }
        } else {
            logger.trace("{} skipping shard directory deletion due to shadow replicas", shardId);
        }
        return false;
    }

    private IndexSettings buildIndexSettings(IndexMetaData metaData) {
        return new IndexSettings(metaData, settings);
    }

    @Override
    public void addPendingDelete(ShardId shardId, IndexSettings settings) {
        if (shardId == null) {
            throw new IllegalArgumentException("shardId must not be null");
        }
        if (settings == null) {
            throw new IllegalArgumentException("settings must not be null");
        }
        PendingDelete pendingDelete = new PendingDelete(shardId, settings);
        addPendingDelete(shardId.getIndex(), pendingDelete);
    }

    public void addPendingDelete(Index index, IndexSettings settings) {
        PendingDelete pendingDelete = new PendingDelete(index, settings);
        addPendingDelete(index, pendingDelete);
    }

    private void addPendingDelete(Index index, PendingDelete pendingDelete) {
        synchronized (pendingDeletes) {
            List<PendingDelete> list = pendingDeletes.get(index);
            if (list == null) {
                list = new ArrayList<>();
                pendingDeletes.put(index, list);
            }
            list.add(pendingDelete);
            numUncompletedDeletes.incrementAndGet();
        }
    }

    private static final class PendingDelete implements Comparable<PendingDelete> {

        final Index index;

        final int shardId;

        final IndexSettings settings;

        final boolean deleteIndex;

        public PendingDelete(ShardId shardId, IndexSettings settings) {
            this.index = shardId.getIndex();
            this.shardId = shardId.getId();
            this.settings = settings;
            this.deleteIndex = false;
        }

        public PendingDelete(Index index, IndexSettings settings) {
            this.index = index;
            this.shardId = -1;
            this.settings = settings;
            this.deleteIndex = true;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(index).append("]");
            if (shardId != -1) {
                sb.append("[").append(shardId).append("]");
            }
            return sb.toString();
        }

        @Override
        public int compareTo(PendingDelete o) {
            return Integer.compare(shardId, o.shardId);
        }
    }

    @Override
    public void processPendingDeletes(Index index, IndexSettings indexSettings, TimeValue timeout) throws IOException, InterruptedException, ShardLockObtainFailedException {
        logger.debug("{} processing pending deletes", index);
        final long startTimeNS = System.nanoTime();
        final List<ShardLock> shardLocks = nodeEnv.lockAllForIndex(index, indexSettings, timeout.millis());
        int numRemoved = 0;
        try {
            Map<ShardId, ShardLock> locks = new HashMap<>();
            for (ShardLock lock : shardLocks) {
                locks.put(lock.getShardId(), lock);
            }
            final List<PendingDelete> remove;
            synchronized (pendingDeletes) {
                remove = pendingDeletes.remove(index);
            }
            if (remove != null && remove.isEmpty() == false) {
                numRemoved = remove.size();
                CollectionUtil.timSort(remove);
                final long maxSleepTimeMs = 10 * 1000;
                long sleepTime = 10;
                do {
                    if (remove.isEmpty()) {
                        break;
                    }
                    Iterator<PendingDelete> iterator = remove.iterator();
                    while (iterator.hasNext()) {
                        PendingDelete delete = iterator.next();
                        if (delete.deleteIndex) {
                            assert delete.shardId == -1;
                            logger.debug("{} deleting index store reason [{}]", index, "pending delete");
                            try {
                                nodeEnv.deleteIndexDirectoryUnderLock(index, indexSettings);
                                iterator.remove();
                            } catch (IOException ex) {
                                logger.debug((Supplier<?>) () -> new ParameterizedMessage("{} retry pending delete", index), ex);
                            }
                        } else {
                            assert delete.shardId != -1;
                            ShardLock shardLock = locks.get(new ShardId(delete.index, delete.shardId));
                            if (shardLock != null) {
                                try {
                                    deleteShardStore("pending delete", shardLock, delete.settings);
                                    iterator.remove();
                                } catch (IOException ex) {
                                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("{} retry pending delete", shardLock.getShardId()), ex);
                                }
                            } else {
                                logger.warn("{} no shard lock for pending delete", delete.shardId);
                                iterator.remove();
                            }
                        }
                    }
                    if (remove.isEmpty() == false) {
                        logger.warn("{} still pending deletes present for shards {} - retrying", index, remove.toString());
                        Thread.sleep(sleepTime);
                        sleepTime = Math.min(maxSleepTimeMs, sleepTime * 2);
                        logger.debug("{} schedule pending delete retry after {} ms", index, sleepTime);
                    }
                } while ((System.nanoTime() - startTimeNS) < timeout.nanos());
            }
        } finally {
            IOUtils.close(shardLocks);
            if (numRemoved > 0) {
                int remainingUncompletedDeletes = numUncompletedDeletes.addAndGet(-numRemoved);
                assert remainingUncompletedDeletes >= 0;
            }
        }
    }

    int numPendingDeletes(Index index) {
        synchronized (pendingDeletes) {
            List<PendingDelete> deleteList = pendingDeletes.get(index);
            if (deleteList == null) {
                return 0;
            }
            return deleteList.size();
        }
    }

    public boolean hasUncompletedPendingDeletes() {
        return numUncompletedDeletes.get() > 0;
    }

    public IndicesQueriesRegistry getIndicesQueryRegistry() {
        return indicesQueriesRegistry;
    }

    public AnalysisRegistry getAnalysis() {
        return analysisRegistry;
    }

    private static final class CacheCleaner implements Runnable, Releasable {

        private final IndicesFieldDataCache cache;

        private final Logger logger;

        private final ThreadPool threadPool;

        private final TimeValue interval;

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private final IndicesRequestCache requestCache;

        public CacheCleaner(IndicesFieldDataCache cache, IndicesRequestCache requestCache, Logger logger, ThreadPool threadPool, TimeValue interval) {
            this.cache = cache;
            this.requestCache = requestCache;
            this.logger = logger;
            this.threadPool = threadPool;
            this.interval = interval;
        }

        @Override
        public void run() {
            long startTimeNS = System.nanoTime();
            if (logger.isTraceEnabled()) {
                logger.trace("running periodic field data cache cleanup");
            }
            try {
                this.cache.getCache().refresh();
            } catch (Exception e) {
                logger.warn("Exception during periodic field data cache cleanup:", e);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("periodic field data cache cleanup finished in {} milliseconds", TimeValue.nsecToMSec(System.nanoTime() - startTimeNS));
            }
            try {
                this.requestCache.cleanCache();
            } catch (Exception e) {
                logger.warn("Exception during periodic request cache cleanup:", e);
            }
            if (closed.get() == false) {
                threadPool.schedule(interval, ThreadPool.Names.SAME, this);
            }
        }

        @Override
        public void close() {
            closed.compareAndSet(false, true);
        }
    }

    private static final Set<SearchType> CACHEABLE_SEARCH_TYPES = EnumSet.of(SearchType.QUERY_THEN_FETCH, SearchType.QUERY_AND_FETCH);

    public boolean canCache(ShardSearchRequest request, SearchContext context) {
        if (!CACHEABLE_SEARCH_TYPES.contains(context.searchType())) {
            return false;
        }
        IndexSettings settings = context.indexShard().indexSettings();
        if (request.requestCache() == null) {
            if (settings.getValue(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING) == false) {
                return false;
            } else if (context.size() != 0) {
                return false;
            }
        } else if (request.requestCache() == false) {
            return false;
        }
        if ((context.searcher().getIndexReader() instanceof DirectoryReader) == false) {
            return false;
        }
        if (context.nowInMillisUsed()) {
            return false;
        }
        return true;
    }

    public void clearRequestCache(IndexShard shard) {
        if (shard == null) {
            return;
        }
        indicesRequestCache.clear(new IndexShardCacheEntity(shard, null));
        logger.trace("{} explicit cache clear", shard.shardId());
    }

    public void loadIntoContext(ShardSearchRequest request, SearchContext context, QueryPhase queryPhase) throws Exception {
        assert canCache(request, context);
        final IndexShardCacheEntity entity = new IndexShardCacheEntity(context.indexShard(), out -> {
            queryPhase.execute(context);
            context.queryResult().writeToNoId(out);
        });
        final DirectoryReader directoryReader = context.searcher().getDirectoryReader();
        final BytesReference bytesReference = indicesRequestCache.getOrCompute(entity, directoryReader, request.cacheKey());
        if (entity.loadedFromCache()) {
            final QuerySearchResult result = context.queryResult();
            StreamInput in = new NamedWriteableAwareStreamInput(bytesReference.streamInput(), namedWriteableRegistry);
            result.readFromWithId(context.id(), in);
            result.shardTarget(context.shardTarget());
        }
    }

    public FieldStats<?> getFieldStats(IndexShard shard, Engine.Searcher searcher, String field, boolean useCache) throws Exception {
        MappedFieldType fieldType = shard.mapperService().fullName(field);
        if (fieldType == null) {
            return null;
        }
        if (useCache == false) {
            return fieldType.stats(searcher.reader());
        }
        BytesReference cacheKey = new BytesArray("fieldstats:" + field);
        BytesReference statsRef = cacheShardLevelResult(shard, searcher.getDirectoryReader(), cacheKey, out -> {
            out.writeOptionalWriteable(fieldType.stats(searcher.reader()));
        });
        try (StreamInput in = statsRef.streamInput()) {
            return in.readOptionalWriteable(FieldStats::readFrom);
        }
    }

    public ByteSizeValue getTotalIndexingBufferBytes() {
        return indexingMemoryController.indexingBufferSize();
    }

    private BytesReference cacheShardLevelResult(IndexShard shard, DirectoryReader reader, BytesReference cacheKey, Loader loader) throws Exception {
        IndexShardCacheEntity cacheEntity = new IndexShardCacheEntity(shard, loader);
        return indicesRequestCache.getOrCompute(cacheEntity, reader, cacheKey);
    }

    static final class IndexShardCacheEntity extends AbstractIndexShardCacheEntity {

        private final IndexShard indexShard;

        protected IndexShardCacheEntity(IndexShard indexShard, Loader loader) {
            super(loader);
            this.indexShard = indexShard;
        }

        @Override
        protected ShardRequestCache stats() {
            return indexShard.requestCache();
        }

        @Override
        public boolean isOpen() {
            return indexShard.state() != IndexShardState.CLOSED;
        }

        @Override
        public Object getCacheIdentity() {
            return indexShard;
        }
    }

    @FunctionalInterface
    interface IndexDeletionAllowedPredicate {

        boolean apply(Index index, IndexSettings indexSettings);
    }

    private final IndexDeletionAllowedPredicate DEFAULT_INDEX_DELETION_PREDICATE = (Index index, IndexSettings indexSettings) -> canDeleteIndexContents(index, indexSettings);

    private final IndexDeletionAllowedPredicate ALWAYS_TRUE = (Index index, IndexSettings indexSettings) -> true;
}