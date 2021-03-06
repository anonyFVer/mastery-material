package org.elasticsearch.index;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexSearcherWrapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;

public class IndexService extends AbstractIndexComponent implements IndicesClusterStateService.AllocatedIndex<IndexShard> {

    private final IndexEventListener eventListener;

    private final IndexFieldDataService indexFieldData;

    private final BitsetFilterCache bitsetFilterCache;

    private final NodeEnvironment nodeEnv;

    private final ShardStoreDeleter shardStoreDeleter;

    private final IndexStore indexStore;

    private final IndexSearcherWrapper searcherWrapper;

    private final IndexCache indexCache;

    private final MapperService mapperService;

    private final NamedXContentRegistry xContentRegistry;

    private final NamedWriteableRegistry namedWriteableRegistry;

    private final SimilarityService similarityService;

    private final EngineFactory engineFactory;

    private final IndexWarmer warmer;

    private volatile Map<Integer, IndexShard> shards = emptyMap();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final AtomicBoolean deleted = new AtomicBoolean(false);

    private final IndexSettings indexSettings;

    private final List<SearchOperationListener> searchOperationListeners;

    private final List<IndexingOperationListener> indexingOperationListeners;

    private volatile AsyncRefreshTask refreshTask;

    private volatile AsyncTranslogFSync fsyncTask;

    private volatile AsyncGlobalCheckpointTask globalCheckpointTask;

    private final String INDEX_TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING = "index.translog.retention.check_interval";

    private final AsyncTrimTranslogTask trimTranslogTask;

    private final ThreadPool threadPool;

    private final BigArrays bigArrays;

    private final ScriptService scriptService;

    private final Client client;

    private final CircuitBreakerService circuitBreakerService;

    private Supplier<Sort> indexSortSupplier;

    public IndexService(IndexSettings indexSettings, NodeEnvironment nodeEnv, NamedXContentRegistry xContentRegistry, SimilarityService similarityService, ShardStoreDeleter shardStoreDeleter, AnalysisRegistry registry, EngineFactory engineFactory, CircuitBreakerService circuitBreakerService, BigArrays bigArrays, ThreadPool threadPool, ScriptService scriptService, Client client, QueryCache queryCache, IndexStore indexStore, IndexEventListener eventListener, IndexModule.IndexSearcherWrapperFactory wrapperFactory, MapperRegistry mapperRegistry, IndicesFieldDataCache indicesFieldDataCache, List<SearchOperationListener> searchOperationListeners, List<IndexingOperationListener> indexingOperationListeners, NamedWriteableRegistry namedWriteableRegistry) throws IOException {
        super(indexSettings);
        this.indexSettings = indexSettings;
        this.xContentRegistry = xContentRegistry;
        this.similarityService = similarityService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.circuitBreakerService = circuitBreakerService;
        this.mapperService = new MapperService(indexSettings, registry.build(indexSettings), xContentRegistry, similarityService, mapperRegistry, () -> newQueryShardContext(0, null, System::currentTimeMillis, null));
        this.indexFieldData = new IndexFieldDataService(indexSettings, indicesFieldDataCache, circuitBreakerService, mapperService);
        if (indexSettings.getIndexSortConfig().hasIndexSort()) {
            this.indexSortSupplier = () -> indexSettings.getIndexSortConfig().buildIndexSort(mapperService::fullName, indexFieldData::getForField);
        } else {
            this.indexSortSupplier = () -> null;
        }
        this.shardStoreDeleter = shardStoreDeleter;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.scriptService = scriptService;
        this.client = client;
        this.eventListener = eventListener;
        this.nodeEnv = nodeEnv;
        this.indexStore = indexStore;
        indexFieldData.setListener(new FieldDataCacheListener(this));
        this.bitsetFilterCache = new BitsetFilterCache(indexSettings, new BitsetCacheListener(this));
        this.warmer = new IndexWarmer(indexSettings.getSettings(), threadPool, indexFieldData, bitsetFilterCache.createListener(threadPool));
        this.indexCache = new IndexCache(indexSettings, queryCache, bitsetFilterCache);
        this.engineFactory = Objects.requireNonNull(engineFactory);
        this.searcherWrapper = wrapperFactory.newWrapper(this);
        this.searchOperationListeners = Collections.unmodifiableList(searchOperationListeners);
        this.indexingOperationListeners = Collections.unmodifiableList(indexingOperationListeners);
        this.refreshTask = new AsyncRefreshTask(this);
        this.trimTranslogTask = new AsyncTrimTranslogTask(this);
        this.globalCheckpointTask = new AsyncGlobalCheckpointTask(this);
        rescheduleFsyncTask(indexSettings.getTranslogDurability());
    }

    public int numberOfShards() {
        return shards.size();
    }

    public IndexEventListener getIndexEventListener() {
        return this.eventListener;
    }

    @Override
    public Iterator<IndexShard> iterator() {
        return shards.values().iterator();
    }

    public boolean hasShard(int shardId) {
        return shards.containsKey(shardId);
    }

    @Override
    @Nullable
    public IndexShard getShardOrNull(int shardId) {
        return shards.get(shardId);
    }

    public IndexShard getShard(int shardId) {
        IndexShard indexShard = getShardOrNull(shardId);
        if (indexShard == null) {
            throw new ShardNotFoundException(new ShardId(index(), shardId));
        }
        return indexShard;
    }

    public Set<Integer> shardIds() {
        return shards.keySet();
    }

    public IndexCache cache() {
        return indexCache;
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return this.mapperService.getIndexAnalyzers();
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    public SimilarityService similarityService() {
        return similarityService;
    }

    public Supplier<Sort> getIndexSortSupplier() {
        return indexSortSupplier;
    }

    public synchronized void close(final String reason, boolean delete) throws IOException {
        if (closed.compareAndSet(false, true)) {
            deleted.compareAndSet(false, delete);
            try {
                final Set<Integer> shardIds = shardIds();
                for (final int shardId : shardIds) {
                    try {
                        removeShard(shardId, reason);
                    } catch (Exception e) {
                        logger.warn("failed to close shard", e);
                    }
                }
            } finally {
                IOUtils.close(bitsetFilterCache, indexCache, indexFieldData, mapperService, refreshTask, fsyncTask, trimTranslogTask, globalCheckpointTask);
            }
        }
    }

    public String indexUUID() {
        return indexSettings.getUUID();
    }

    private long getAvgShardSizeInBytes() throws IOException {
        long sum = 0;
        int count = 0;
        for (IndexShard indexShard : this) {
            sum += indexShard.store().stats().sizeInBytes();
            count++;
        }
        if (count == 0) {
            return -1L;
        } else {
            return sum / count;
        }
    }

    public synchronized IndexShard createShard(ShardRouting routing, Consumer<ShardId> globalCheckpointSyncer) throws IOException {
        if (closed.get()) {
            throw new IllegalStateException("Can't create shard " + routing.shardId() + ", closed");
        }
        final Settings indexSettings = this.indexSettings.getSettings();
        final ShardId shardId = routing.shardId();
        boolean success = false;
        Store store = null;
        IndexShard indexShard = null;
        ShardLock lock = null;
        try {
            lock = nodeEnv.shardLock(shardId, TimeUnit.SECONDS.toMillis(5));
            eventListener.beforeIndexShardCreated(shardId, indexSettings);
            ShardPath path;
            try {
                path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings);
            } catch (IllegalStateException ex) {
                logger.warn("{} failed to load shard path, trying to remove leftover", shardId);
                try {
                    ShardPath.deleteLeftoverShardDirectory(logger, nodeEnv, lock, this.indexSettings);
                    path = ShardPath.loadShardPath(logger, nodeEnv, shardId, this.indexSettings);
                } catch (Exception inner) {
                    ex.addSuppressed(inner);
                    throw ex;
                }
            }
            if (path == null) {
                Map<Path, Integer> dataPathToShardCount = new HashMap<>();
                for (IndexShard shard : this) {
                    Path dataPath = shard.shardPath().getRootStatePath();
                    Integer curCount = dataPathToShardCount.get(dataPath);
                    if (curCount == null) {
                        curCount = 0;
                    }
                    dataPathToShardCount.put(dataPath, curCount + 1);
                }
                path = ShardPath.selectNewPathForShard(nodeEnv, shardId, this.indexSettings, routing.getExpectedShardSize() == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE ? getAvgShardSizeInBytes() : routing.getExpectedShardSize(), dataPathToShardCount);
                logger.debug("{} creating using a new path [{}]", shardId, path);
            } else {
                logger.debug("{} creating using an existing path [{}]", shardId, path);
            }
            if (shards.containsKey(shardId.id())) {
                throw new IllegalStateException(shardId + " already exists");
            }
            logger.debug("creating shard_id {}", shardId);
            final Engine.Warmer engineWarmer = (searcher) -> {
                IndexShard shard = getShardOrNull(shardId.getId());
                if (shard != null) {
                    warmer.warm(searcher, shard, IndexService.this.indexSettings);
                }
            };
            store = new Store(shardId, this.indexSettings, indexStore.newDirectoryService(path), lock, new StoreCloseListener(shardId, () -> eventListener.onStoreClosed(shardId)));
            indexShard = new IndexShard(routing, this.indexSettings, path, store, indexSortSupplier, indexCache, mapperService, similarityService, engineFactory, eventListener, searcherWrapper, threadPool, bigArrays, engineWarmer, searchOperationListeners, indexingOperationListeners, () -> globalCheckpointSyncer.accept(shardId), circuitBreakerService);
            eventListener.indexShardStateChanged(indexShard, null, indexShard.state(), "shard created");
            eventListener.afterIndexShardCreated(indexShard);
            shards = newMapBuilder(shards).put(shardId.id(), indexShard).immutableMap();
            success = true;
            return indexShard;
        } catch (ShardLockObtainFailedException e) {
            throw new IOException("failed to obtain in-memory shard lock", e);
        } finally {
            if (success == false) {
                if (lock != null) {
                    IOUtils.closeWhileHandlingException(lock);
                }
                closeShard("initialization failed", shardId, indexShard, store, eventListener);
            }
        }
    }

    @Override
    public synchronized void removeShard(int shardId, String reason) {
        final ShardId sId = new ShardId(index(), shardId);
        final IndexShard indexShard;
        if (shards.containsKey(shardId) == false) {
            return;
        }
        logger.debug("[{}] closing... (reason: [{}])", shardId, reason);
        HashMap<Integer, IndexShard> newShards = new HashMap<>(shards);
        indexShard = newShards.remove(shardId);
        shards = unmodifiableMap(newShards);
        closeShard(reason, sId, indexShard, indexShard.store(), indexShard.getIndexEventListener());
        logger.debug("[{}] closed (reason: [{}])", shardId, reason);
    }

    private void closeShard(String reason, ShardId sId, IndexShard indexShard, Store store, IndexEventListener listener) {
        final int shardId = sId.id();
        final Settings indexSettings = this.getIndexSettings().getSettings();
        try {
            try {
                listener.beforeIndexShardClosed(sId, indexShard, indexSettings);
            } finally {
                if (indexShard != null) {
                    try {
                        final boolean flushEngine = deleted.get() == false && closed.get();
                        indexShard.close(reason, flushEngine);
                    } catch (Exception e) {
                        logger.debug(() -> new ParameterizedMessage("[{}] failed to close index shard", shardId), e);
                    }
                }
                listener.afterIndexShardClosed(sId, indexShard, indexSettings);
            }
        } finally {
            try {
                if (store != null) {
                    store.close();
                } else {
                    logger.trace("[{}] store not initialized prior to closing shard, nothing to close", shardId);
                }
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to close store on shard removal (reason: [{}])", shardId, reason), e);
            }
        }
    }

    private void onShardClose(ShardLock lock) {
        if (deleted.get()) {
            try {
                try {
                    eventListener.beforeIndexShardDeleted(lock.getShardId(), indexSettings.getSettings());
                } finally {
                    shardStoreDeleter.deleteShardStore("delete index", lock, indexSettings);
                    eventListener.afterIndexShardDeleted(lock.getShardId(), indexSettings.getSettings());
                }
            } catch (IOException e) {
                shardStoreDeleter.addPendingDelete(lock.getShardId(), indexSettings);
                logger.debug(() -> new ParameterizedMessage("[{}] failed to delete shard content - scheduled a retry", lock.getShardId().id()), e);
            }
        }
    }

    @Override
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    public QueryShardContext newQueryShardContext(int shardId, IndexReader indexReader, LongSupplier nowInMillis, String clusterAlias) {
        return new QueryShardContext(shardId, indexSettings, indexCache.bitsetFilterCache(), indexFieldData::getForField, mapperService(), similarityService(), scriptService, xContentRegistry, namedWriteableRegistry, client, indexReader, nowInMillis, clusterAlias);
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    public BigArrays getBigArrays() {
        return bigArrays;
    }

    public ScriptService getScriptService() {
        return scriptService;
    }

    List<IndexingOperationListener> getIndexOperationListeners() {
        return indexingOperationListeners;
    }

    List<SearchOperationListener> getSearchOperationListener() {
        return searchOperationListeners;
    }

    @Override
    public boolean updateMapping(IndexMetaData indexMetaData) throws IOException {
        return mapperService().updateMapping(indexMetaData);
    }

    private class StoreCloseListener implements Store.OnClose {

        private final ShardId shardId;

        private final Closeable[] toClose;

        StoreCloseListener(ShardId shardId, Closeable... toClose) {
            this.shardId = shardId;
            this.toClose = toClose;
        }

        @Override
        public void accept(ShardLock lock) {
            try {
                assert lock.getShardId().equals(shardId) : "shard id mismatch, expected: " + shardId + " but got: " + lock.getShardId();
                onShardClose(lock);
            } finally {
                try {
                    IOUtils.close(toClose);
                } catch (IOException ex) {
                    logger.debug("failed to close resource", ex);
                }
            }
        }
    }

    private static final class BitsetCacheListener implements BitsetFilterCache.Listener {

        final IndexService indexService;

        private BitsetCacheListener(IndexService indexService) {
            this.indexService = indexService;
        }

        @Override
        public void onCache(ShardId shardId, Accountable accountable) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    long ramBytesUsed = accountable != null ? accountable.ramBytesUsed() : 0L;
                    shard.shardBitsetFilterCache().onCached(ramBytesUsed);
                }
            }
        }

        @Override
        public void onRemoval(ShardId shardId, Accountable accountable) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    long ramBytesUsed = accountable != null ? accountable.ramBytesUsed() : 0L;
                    shard.shardBitsetFilterCache().onRemoval(ramBytesUsed);
                }
            }
        }
    }

    private final class FieldDataCacheListener implements IndexFieldDataCache.Listener {

        final IndexService indexService;

        FieldDataCacheListener(IndexService indexService) {
            this.indexService = indexService;
        }

        @Override
        public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    shard.fieldData().onCache(shardId, fieldName, ramUsage);
                }
            }
        }

        @Override
        public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
            if (shardId != null) {
                final IndexShard shard = indexService.getShardOrNull(shardId.id());
                if (shard != null) {
                    shard.fieldData().onRemoval(shardId, fieldName, wasEvicted, sizeInBytes);
                }
            }
        }
    }

    public IndexMetaData getMetaData() {
        return indexSettings.getIndexMetaData();
    }

    @Override
    public synchronized void updateMetaData(final IndexMetaData metadata) {
        final Translog.Durability oldTranslogDurability = indexSettings.getTranslogDurability();
        if (indexSettings.updateIndexMetaData(metadata)) {
            for (final IndexShard shard : this.shards.values()) {
                try {
                    shard.onSettingsChanged();
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("[{}] failed to notify shard about setting change", shard.shardId().id()), e);
                }
            }
            if (refreshTask.getInterval().equals(indexSettings.getRefreshInterval()) == false) {
                threadPool.executor(ThreadPool.Names.REFRESH).execute(new AbstractRunnable() {

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("forced refresh failed after interval change", e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        maybeRefreshEngine(true);
                    }

                    @Override
                    public boolean isForceExecution() {
                        return true;
                    }
                });
                rescheduleRefreshTasks();
            }
            final Translog.Durability durability = indexSettings.getTranslogDurability();
            if (durability != oldTranslogDurability) {
                rescheduleFsyncTask(durability);
            }
        }
    }

    private void rescheduleFsyncTask(Translog.Durability durability) {
        try {
            if (fsyncTask != null) {
                fsyncTask.close();
            }
        } finally {
            fsyncTask = durability == Translog.Durability.REQUEST ? null : new AsyncTranslogFSync(this);
        }
    }

    private void rescheduleRefreshTasks() {
        try {
            refreshTask.close();
        } finally {
            refreshTask = new AsyncRefreshTask(this);
        }
    }

    public interface ShardStoreDeleter {

        void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings) throws IOException;

        void addPendingDelete(ShardId shardId, IndexSettings indexSettings);
    }

    public final EngineFactory getEngineFactory() {
        return engineFactory;
    }

    final IndexSearcherWrapper getSearcherWrapper() {
        return searcherWrapper;
    }

    final IndexStore getIndexStore() {
        return indexStore;
    }

    private void maybeFSyncTranslogs() {
        if (indexSettings.getTranslogDurability() == Translog.Durability.ASYNC) {
            for (IndexShard shard : this.shards.values()) {
                try {
                    if (shard.isSyncNeeded()) {
                        shard.sync();
                    }
                } catch (AlreadyClosedException ex) {
                } catch (IOException e) {
                    logger.warn("failed to sync translog", e);
                }
            }
        }
    }

    private void maybeRefreshEngine(boolean force) {
        if (indexSettings.getRefreshInterval().millis() > 0 || force) {
            for (IndexShard shard : this.shards.values()) {
                try {
                    shard.scheduledRefresh();
                } catch (IndexShardClosedException | AlreadyClosedException ex) {
                }
            }
        }
    }

    private void maybeTrimTranslog() {
        for (IndexShard shard : this.shards.values()) {
            switch(shard.state()) {
                case CREATED:
                case RECOVERING:
                case CLOSED:
                    continue;
                case POST_RECOVERY:
                case STARTED:
                    try {
                        shard.trimTranslog();
                    } catch (IndexShardClosedException | AlreadyClosedException ex) {
                    }
                    continue;
                default:
                    throw new IllegalStateException("unknown state: " + shard.state());
            }
        }
    }

    private void maybeSyncGlobalCheckpoints() {
        for (final IndexShard shard : this.shards.values()) {
            if (shard.routingEntry().active() && shard.routingEntry().primary()) {
                switch(shard.state()) {
                    case CLOSED:
                    case CREATED:
                    case RECOVERING:
                        continue;
                    case POST_RECOVERY:
                        assert false : "shard " + shard.shardId() + " is in post-recovery but marked as active";
                        continue;
                    case STARTED:
                        try {
                            shard.acquirePrimaryOperationPermit(ActionListener.wrap(releasable -> {
                                try (Releasable ignored = releasable) {
                                    shard.maybeSyncGlobalCheckpoint("background");
                                }
                            }, e -> {
                                if (!(e instanceof AlreadyClosedException || e instanceof IndexShardClosedException)) {
                                    logger.info(new ParameterizedMessage("{} failed to execute background global checkpoint sync", shard.shardId()), e);
                                }
                            }), ThreadPool.Names.SAME, "background global checkpoint sync");
                        } catch (final AlreadyClosedException | IndexShardClosedException e) {
                        }
                        continue;
                    default:
                        throw new IllegalStateException("unknown state [" + shard.state() + "]");
                }
            }
        }
    }

    abstract static class BaseAsyncTask implements Runnable, Closeable {

        protected final IndexService indexService;

        protected final ThreadPool threadPool;

        private final TimeValue interval;

        private ScheduledFuture<?> scheduledFuture;

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private volatile Exception lastThrownException;

        BaseAsyncTask(IndexService indexService, TimeValue interval) {
            this.indexService = indexService;
            this.threadPool = indexService.getThreadPool();
            this.interval = interval;
            onTaskCompletion();
        }

        boolean mustReschedule() {
            return indexService.closed.get() == false && closed.get() == false && interval.millis() > 0;
        }

        private synchronized void onTaskCompletion() {
            if (mustReschedule()) {
                if (indexService.logger.isTraceEnabled()) {
                    indexService.logger.trace("scheduling {} every {}", toString(), interval);
                }
                this.scheduledFuture = threadPool.schedule(interval, getThreadPool(), BaseAsyncTask.this);
            } else {
                indexService.logger.trace("scheduled {} disabled", toString());
                this.scheduledFuture = null;
            }
        }

        boolean isScheduled() {
            return scheduledFuture != null;
        }

        @Override
        public final void run() {
            try {
                runInternal();
            } catch (Exception ex) {
                if (lastThrownException == null || sameException(lastThrownException, ex) == false) {
                    indexService.logger.warn(() -> new ParameterizedMessage("failed to run task {} - suppressing re-occurring exceptions unless the exception changes", toString()), ex);
                    lastThrownException = ex;
                }
            } finally {
                onTaskCompletion();
            }
        }

        private static boolean sameException(Exception left, Exception right) {
            if (left.getClass() == right.getClass()) {
                if (Objects.equals(left.getMessage(), right.getMessage())) {
                    StackTraceElement[] stackTraceLeft = left.getStackTrace();
                    StackTraceElement[] stackTraceRight = right.getStackTrace();
                    if (stackTraceLeft.length == stackTraceRight.length) {
                        for (int i = 0; i < stackTraceLeft.length; i++) {
                            if (stackTraceLeft[i].equals(stackTraceRight[i]) == false) {
                                return false;
                            }
                        }
                        return true;
                    }
                }
            }
            return false;
        }

        protected abstract void runInternal();

        protected String getThreadPool() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public synchronized void close() {
            if (closed.compareAndSet(false, true)) {
                FutureUtils.cancel(scheduledFuture);
                scheduledFuture = null;
            }
        }

        TimeValue getInterval() {
            return interval;
        }

        boolean isClosed() {
            return this.closed.get();
        }
    }

    static final class AsyncTranslogFSync extends BaseAsyncTask {

        AsyncTranslogFSync(IndexService indexService) {
            super(indexService, indexService.getIndexSettings().getTranslogSyncInterval());
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.FLUSH;
        }

        @Override
        protected void runInternal() {
            indexService.maybeFSyncTranslogs();
        }

        @Override
        public String toString() {
            return "translog_sync";
        }
    }

    final class AsyncRefreshTask extends BaseAsyncTask {

        AsyncRefreshTask(IndexService indexService) {
            super(indexService, indexService.getIndexSettings().getRefreshInterval());
        }

        @Override
        protected void runInternal() {
            indexService.maybeRefreshEngine(false);
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.REFRESH;
        }

        @Override
        public String toString() {
            return "refresh";
        }
    }

    final class AsyncTrimTranslogTask extends BaseAsyncTask {

        AsyncTrimTranslogTask(IndexService indexService) {
            super(indexService, indexService.getIndexSettings().getSettings().getAsTime(INDEX_TRANSLOG_RETENTION_CHECK_INTERVAL_SETTING, TimeValue.timeValueMinutes(10)));
        }

        @Override
        protected void runInternal() {
            indexService.maybeTrimTranslog();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public String toString() {
            return "trim_translog";
        }
    }

    public static final Setting<TimeValue> GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING = Setting.timeSetting("index.global_checkpoint_sync.interval", new TimeValue(30, TimeUnit.SECONDS), new TimeValue(0, TimeUnit.MILLISECONDS), Property.Dynamic, Property.IndexScope);

    final class AsyncGlobalCheckpointTask extends BaseAsyncTask {

        AsyncGlobalCheckpointTask(final IndexService indexService) {
            super(indexService, GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.get(indexService.getIndexSettings().getSettings()));
        }

        @Override
        protected void runInternal() {
            indexService.maybeSyncGlobalCheckpoints();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public String toString() {
            return "global_checkpoint_sync";
        }
    }

    AsyncRefreshTask getRefreshTask() {
        return refreshTask;
    }

    AsyncTranslogFSync getFsyncTask() {
        return fsyncTask;
    }

    AsyncGlobalCheckpointTask getGlobalCheckpointTask() {
        return globalCheckpointTask;
    }

    public boolean clearCaches(boolean queryCache, boolean fieldDataCache, String... fields) {
        boolean clearedAtLeastOne = false;
        if (queryCache) {
            clearedAtLeastOne = true;
            indexCache.query().clear("api");
        }
        if (fieldDataCache) {
            clearedAtLeastOne = true;
            if (fields.length == 0) {
                indexFieldData.clear();
            } else {
                for (String field : fields) {
                    indexFieldData.clearField(field);
                }
            }
        }
        if (clearedAtLeastOne == false) {
            if (fields.length == 0) {
                indexCache.clear("api");
                indexFieldData.clear();
            } else {
                for (String field : fields) {
                    indexFieldData.clearField(field);
                }
            }
        }
        return clearedAtLeastOne;
    }
}