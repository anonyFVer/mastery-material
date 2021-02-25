package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.LoggerInfoStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ElasticsearchMergePolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.TranslogRecoveryPerformer;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class InternalEngine extends Engine {

    private volatile long lastDeleteVersionPruneTimeMSec;

    private final Translog translog;

    private final ElasticsearchConcurrentMergeScheduler mergeScheduler;

    private final IndexWriter indexWriter;

    private final SearcherFactory searcherFactory;

    private final SearcherManager searcherManager;

    private final Lock flushLock = new ReentrantLock();

    private final ReentrantLock optimizeLock = new ReentrantLock();

    private final LiveVersionMap versionMap;

    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();

    private final AtomicBoolean versionMapRefreshPending = new AtomicBoolean();

    private volatile SegmentInfos lastCommittedSegmentInfos;

    private final IndexThrottle throttle;

    private final AtomicInteger throttleRequestCount = new AtomicInteger();

    private final EngineConfig.OpenMode openMode;

    private final AtomicBoolean allowCommits = new AtomicBoolean(true);

    public InternalEngine(EngineConfig engineConfig) throws EngineException {
        super(engineConfig);
        openMode = engineConfig.getOpenMode();
        this.versionMap = new LiveVersionMap();
        store.incRef();
        IndexWriter writer = null;
        Translog translog = null;
        SearcherManager manager = null;
        EngineMergeScheduler scheduler = null;
        boolean success = false;
        try {
            this.lastDeleteVersionPruneTimeMSec = engineConfig.getThreadPool().estimatedTimeInMillis();
            mergeScheduler = scheduler = new EngineMergeScheduler(engineConfig.getShardId(), engineConfig.getIndexSettings());
            throttle = new IndexThrottle();
            this.searcherFactory = new SearchFactory(logger, isClosed, engineConfig);
            try {
                writer = createWriter(openMode == EngineConfig.OpenMode.CREATE_INDEX_AND_TRANSLOG);
                indexWriter = writer;
                translog = openTranslog(engineConfig, writer);
                assert translog.getGeneration() != null;
            } catch (IOException | TranslogCorruptedException e) {
                throw new EngineCreationFailureException(shardId, "failed to create engine", e);
            } catch (AssertionError e) {
                if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                    throw new EngineCreationFailureException(shardId, "failed to create engine", e);
                } else {
                    throw e;
                }
            }
            this.translog = translog;
            manager = createSearcherManager();
            this.searcherManager = manager;
            this.versionMap.setManager(searcherManager);
            allowCommits.compareAndSet(true, openMode != EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG);
            if (engineConfig.getRefreshListeners() != null) {
                searcherManager.addListener(engineConfig.getRefreshListeners());
                engineConfig.getRefreshListeners().setTranslog(translog);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(writer, translog, manager, scheduler);
                versionMap.clear();
                if (isClosed.get() == false) {
                    store.decRef();
                }
            }
        }
        logger.trace("created new InternalEngine");
    }

    @Override
    public InternalEngine recoverFromTranslog() throws IOException {
        flushLock.lock();
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (openMode != EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
                throw new IllegalStateException("Can't recover from translog with open mode: " + openMode);
            }
            if (allowCommits.get()) {
                throw new IllegalStateException("Engine has already been recovered");
            }
            try {
                recoverFromTranslog(engineConfig.getTranslogRecoveryPerformer());
            } catch (Exception e) {
                try {
                    allowCommits.set(false);
                    failEngine("failed to recover from translog", e);
                } catch (Exception inner) {
                    e.addSuppressed(inner);
                }
                throw e;
            }
        } finally {
            flushLock.unlock();
        }
        return this;
    }

    private void recoverFromTranslog(TranslogRecoveryPerformer handler) throws IOException {
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        final int opsRecovered;
        try {
            Translog.Snapshot snapshot = translog.newSnapshot();
            opsRecovered = handler.recoveryFromSnapshot(this, snapshot);
        } catch (Exception e) {
            throw new EngineException(shardId, "failed to recover from translog", e);
        }
        assert allowCommits.get() == false : "commits are allowed but shouldn't";
        allowCommits.set(true);
        if (opsRecovered > 0) {
            logger.trace("flushing post recovery from translog. ops recovered [{}]. committed translog id [{}]. current id [{}]", opsRecovered, translogGeneration == null ? null : translogGeneration.translogFileGeneration, translog.currentFileGeneration());
            flush(true, true);
        } else if (translog.isCurrent(translogGeneration) == false) {
            commitIndexWriter(indexWriter, translog, lastCommittedSegmentInfos.getUserData().get(Engine.SYNC_COMMIT_ID));
        }
    }

    private Translog openTranslog(EngineConfig engineConfig, IndexWriter writer) throws IOException {
        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        Translog.TranslogGeneration generation = null;
        if (openMode == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
            generation = loadTranslogIdFromCommit(writer);
            if (generation == null) {
                throw new IllegalStateException("no translog generation present in commit data but translog is expected to exist");
            }
            if (generation != null && generation.translogUUID == null) {
                throw new IndexFormatTooOldException("trasnlog", "translog has no generation nor a UUID - this might be an index from a previous version consider upgrading to N-1 first");
            }
        }
        final Translog translog = new Translog(translogConfig, generation);
        if (generation == null || generation.translogUUID == null) {
            assert openMode != EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG : "OpenMode must not be " + EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG;
            if (generation == null) {
                logger.debug("no translog ID present in the current generation - creating one");
            } else if (generation.translogUUID == null) {
                logger.debug("upgraded translog to pre 2.0 format, associating translog with index - writing translog UUID");
            }
            boolean success = false;
            try {
                commitIndexWriter(writer, translog, openMode == EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG ? writer.getCommitData().get(SYNC_COMMIT_ID) : null);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(translog);
                }
            }
        }
        return translog;
    }

    @Override
    public Translog getTranslog() {
        ensureOpen();
        return translog;
    }

    @Nullable
    private Translog.TranslogGeneration loadTranslogIdFromCommit(IndexWriter writer) throws IOException {
        final Map<String, String> commitUserData = writer.getCommitData();
        if (commitUserData.containsKey("translog_id")) {
            assert commitUserData.containsKey(Translog.TRANSLOG_UUID_KEY) == false : "legacy commit contains translog UUID";
            return new Translog.TranslogGeneration(null, Long.parseLong(commitUserData.get("translog_id")));
        } else if (commitUserData.containsKey(Translog.TRANSLOG_GENERATION_KEY)) {
            if (commitUserData.containsKey(Translog.TRANSLOG_UUID_KEY) == false) {
                throw new IllegalStateException("commit doesn't contain translog UUID");
            }
            final String translogUUID = commitUserData.get(Translog.TRANSLOG_UUID_KEY);
            final long translogGen = Long.parseLong(commitUserData.get(Translog.TRANSLOG_GENERATION_KEY));
            return new Translog.TranslogGeneration(translogUUID, translogGen);
        }
        return null;
    }

    private SearcherManager createSearcherManager() throws EngineException {
        boolean success = false;
        SearcherManager searcherManager = null;
        try {
            try {
                final DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter), shardId);
                searcherManager = new SearcherManager(directoryReader, searcherFactory);
                lastCommittedSegmentInfos = readLastCommittedSegmentInfos(searcherManager, store);
                success = true;
                return searcherManager;
            } catch (IOException e) {
                maybeFailEngine("start", e);
                try {
                    indexWriter.rollback();
                } catch (IOException inner) {
                    e.addSuppressed(inner);
                }
                throw new EngineCreationFailureException(shardId, "failed to open reader on writer", e);
            }
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(searcherManager, indexWriter);
            }
        }
    }

    @Override
    public GetResult get(Get get, Function<String, Searcher> searcherFactory) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (get.realtime()) {
                VersionValue versionValue = versionMap.getUnderLock(get.uid());
                if (versionValue != null) {
                    if (versionValue.delete()) {
                        return GetResult.NOT_EXISTS;
                    }
                    if (get.versionType().isVersionConflictForReads(versionValue.version(), get.version())) {
                        Uid uid = Uid.createUid(get.uid().text());
                        throw new VersionConflictEngineException(shardId, uid.type(), uid.id(), get.versionType().explainConflictForReads(versionValue.version(), get.version()));
                    }
                    Translog.Operation op = translog.read(versionValue.translogLocation());
                    if (op != null) {
                        return new GetResult(true, versionValue.version(), op.getSource());
                    }
                }
            }
            return getFromSearcher(get, searcherFactory);
        }
    }

    private boolean checkVersionConflict(final Operation op, final long currentVersion, final long expectedVersion, final boolean deleted) {
        if (op.versionType().isVersionConflictForWrites(currentVersion, expectedVersion, deleted)) {
            if (op.origin().isRecovery()) {
                return true;
            } else {
                throw new VersionConflictEngineException(shardId, op.type(), op.id(), op.versionType().explainConflictForWrites(currentVersion, expectedVersion, deleted));
            }
        }
        return false;
    }

    private long checkDeletedAndGCed(VersionValue versionValue) {
        long currentVersion;
        if (engineConfig.isEnableGcDeletes() && versionValue.delete() && (engineConfig.getThreadPool().estimatedTimeInMillis() - versionValue.time()) > getGcDeletesInMillis()) {
            currentVersion = Versions.NOT_FOUND;
        } else {
            currentVersion = versionValue.version();
        }
        return currentVersion;
    }

    private static VersionValueSupplier NEW_VERSION_VALUE = (u, t, l) -> new VersionValue(u, l);

    @FunctionalInterface
    private interface VersionValueSupplier {

        VersionValue apply(long updatedVersion, long time, Translog.Location location);
    }

    private <T extends Engine.Operation> void maybeAddToTranslog(final T op, final long updatedVersion, final Function<T, Translog.Operation> toTranslogOp, final VersionValueSupplier toVersionValue) throws IOException {
        if (op.origin() != Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
            final Translog.Location translogLocation = translog.add(toTranslogOp.apply(op));
            op.setTranslogLocation(translogLocation);
            versionMap.putUnderLock(op.uid().bytes(), toVersionValue.apply(updatedVersion, engineConfig.getThreadPool().estimatedTimeInMillis(), op.getTranslogLocation()));
        } else {
            versionMap.putUnderLock(op.uid().bytes(), toVersionValue.apply(updatedVersion, engineConfig.getThreadPool().estimatedTimeInMillis(), null));
        }
    }

    @Override
    public boolean index(Index index) {
        final boolean created;
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (index.origin().isRecovery()) {
                created = innerIndex(index);
            } else {
                try (Releasable r = throttle.acquireThrottle()) {
                    created = innerIndex(index);
                }
            }
        } catch (IllegalStateException | IOException e) {
            try {
                maybeFailEngine("index", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new IndexFailedEngineException(shardId, index.type(), index.id(), e);
        }
        return created;
    }

    private boolean innerIndex(Index index) throws IOException {
        try (Releasable ignored = acquireLock(index.uid())) {
            lastWriteNanos = index.startTime();
            final long currentVersion;
            final boolean deleted;
            final VersionValue versionValue = versionMap.getUnderLock(index.uid());
            if (versionValue == null) {
                currentVersion = loadCurrentVersionFromIndex(index.uid());
                deleted = currentVersion == Versions.NOT_FOUND;
            } else {
                currentVersion = checkDeletedAndGCed(versionValue);
                deleted = versionValue.delete();
            }
            final long expectedVersion = index.version();
            if (checkVersionConflict(index, currentVersion, expectedVersion, deleted))
                return false;
            final long updatedVersion = updateVersion(index, currentVersion, expectedVersion);
            final boolean created = indexOrUpdate(index, currentVersion, versionValue);
            maybeAddToTranslog(index, updatedVersion, Translog.Index::new, NEW_VERSION_VALUE);
            return created;
        }
    }

    private long updateVersion(Engine.Operation op, long currentVersion, long expectedVersion) {
        final long updatedVersion = op.versionType().updateVersion(currentVersion, expectedVersion);
        op.updateVersion(updatedVersion);
        return updatedVersion;
    }

    private boolean indexOrUpdate(final Index index, final long currentVersion, final VersionValue versionValue) throws IOException {
        final boolean created;
        if (currentVersion == Versions.NOT_FOUND) {
            created = true;
            index(index, indexWriter);
        } else {
            created = update(index, versionValue, indexWriter);
        }
        return created;
    }

    private static void index(final Index index, final IndexWriter indexWriter) throws IOException {
        if (index.docs().size() > 1) {
            indexWriter.addDocuments(index.docs());
        } else {
            indexWriter.addDocument(index.docs().get(0));
        }
    }

    private static boolean update(final Index index, final VersionValue versionValue, final IndexWriter indexWriter) throws IOException {
        final boolean created;
        if (versionValue != null) {
            created = versionValue.delete();
        } else {
            created = false;
        }
        if (index.docs().size() > 1) {
            indexWriter.updateDocuments(index.uid(), index.docs());
        } else {
            indexWriter.updateDocument(index.uid(), index.docs().get(0));
        }
        return created;
    }

    @Override
    public void delete(Delete delete) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            innerDelete(delete);
        } catch (IllegalStateException | IOException e) {
            try {
                maybeFailEngine("delete", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new DeleteFailedEngineException(shardId, delete, e);
        }
        maybePruneDeletedTombstones();
    }

    private void maybePruneDeletedTombstones() {
        if (engineConfig.isEnableGcDeletes() && engineConfig.getThreadPool().estimatedTimeInMillis() - lastDeleteVersionPruneTimeMSec > getGcDeletesInMillis() * 0.25) {
            pruneDeletedTombstones();
        }
    }

    private void innerDelete(Delete delete) throws IOException {
        try (Releasable ignored = acquireLock(delete.uid())) {
            lastWriteNanos = delete.startTime();
            final long currentVersion;
            final boolean deleted;
            final VersionValue versionValue = versionMap.getUnderLock(delete.uid());
            if (versionValue == null) {
                currentVersion = loadCurrentVersionFromIndex(delete.uid());
                deleted = currentVersion == Versions.NOT_FOUND;
            } else {
                currentVersion = checkDeletedAndGCed(versionValue);
                deleted = versionValue.delete();
            }
            final long expectedVersion = delete.version();
            if (checkVersionConflict(delete, currentVersion, expectedVersion, deleted))
                return;
            final long updatedVersion = updateVersion(delete, currentVersion, expectedVersion);
            final boolean found = deleteIfFound(delete, currentVersion, deleted, versionValue);
            delete.updateVersion(updatedVersion, found);
            maybeAddToTranslog(delete, updatedVersion, Translog.Delete::new, DeleteVersionValue::new);
        }
    }

    private boolean deleteIfFound(Delete delete, long currentVersion, boolean deleted, VersionValue versionValue) throws IOException {
        final boolean found;
        if (currentVersion == Versions.NOT_FOUND) {
            found = false;
        } else if (versionValue != null && deleted) {
            found = false;
        } else {
            indexWriter.deleteDocuments(delete.uid());
            found = true;
        }
        return found;
    }

    @Override
    public void refresh(String source) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            searcherManager.maybeRefreshBlocking();
        } catch (AlreadyClosedException e) {
            ensureOpen();
            maybeFailEngine("refresh", e);
        } catch (EngineClosedException e) {
            throw e;
        } catch (Exception e) {
            try {
                failEngine("refresh failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new RefreshFailedEngineException(shardId, e);
        }
        maybePruneDeletedTombstones();
        versionMapRefreshPending.set(false);
        mergeScheduler.refreshConfig();
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            final long versionMapBytes = versionMap.ramBytesUsedForRefresh();
            final long indexingBufferBytes = indexWriter.ramBytesUsed();
            final boolean useRefresh = versionMapRefreshPending.get() || (indexingBufferBytes / 4 < versionMapBytes);
            if (useRefresh) {
                logger.debug("use refresh to write indexing buffer (heap size=[{}]), to also clear version map (heap size=[{}])", new ByteSizeValue(indexingBufferBytes), new ByteSizeValue(versionMapBytes));
                refresh("write indexing buffer");
            } else {
                logger.debug("use IndexWriter.flush to write indexing buffer (heap size=[{}]) since version map is small (heap size=[{}])", new ByteSizeValue(indexingBufferBytes), new ByteSizeValue(versionMapBytes));
                indexWriter.flush();
            }
        } catch (AlreadyClosedException e) {
            ensureOpen();
            maybeFailEngine("writeIndexingBuffer", e);
        } catch (EngineClosedException e) {
            throw e;
        } catch (Exception e) {
            try {
                failEngine("writeIndexingBuffer failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new RefreshFailedEngineException(shardId, e);
        }
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException {
        ensureOpen();
        if (indexWriter.hasUncommittedChanges()) {
            logger.trace("can't sync commit [{}]. have pending changes", syncId);
            return SyncedFlushResult.PENDING_OPERATIONS;
        }
        if (expectedCommitId.idsEqual(lastCommittedSegmentInfos.getId()) == false) {
            logger.trace("can't sync commit [{}]. current commit id is not equal to expected.", syncId);
            return SyncedFlushResult.COMMIT_MISMATCH;
        }
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            ensureCanFlush();
            if (indexWriter.hasUncommittedChanges()) {
                logger.trace("can't sync commit [{}]. have pending changes", syncId);
                return SyncedFlushResult.PENDING_OPERATIONS;
            }
            if (expectedCommitId.idsEqual(lastCommittedSegmentInfos.getId()) == false) {
                logger.trace("can't sync commit [{}]. current commit id is not equal to expected.", syncId);
                return SyncedFlushResult.COMMIT_MISMATCH;
            }
            logger.trace("starting sync commit [{}]", syncId);
            commitIndexWriter(indexWriter, translog, syncId);
            logger.debug("successfully sync committed. sync id [{}].", syncId);
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            return SyncedFlushResult.SUCCESS;
        } catch (IOException ex) {
            maybeFailEngine("sync commit", ex);
            throw new EngineException(shardId, "failed to sync commit", ex);
        }
    }

    final boolean tryRenewSyncCommit() {
        boolean renewed = false;
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            ensureCanFlush();
            String syncId = lastCommittedSegmentInfos.getUserData().get(SYNC_COMMIT_ID);
            if (syncId != null && translog.totalOperations() == 0 && indexWriter.hasUncommittedChanges()) {
                logger.trace("start renewing sync commit [{}]", syncId);
                commitIndexWriter(indexWriter, translog, syncId);
                logger.debug("successfully sync committed. sync id [{}].", syncId);
                lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                renewed = true;
            }
        } catch (IOException ex) {
            maybeFailEngine("renew sync commit", ex);
            throw new EngineException(shardId, "failed to renew sync commit", ex);
        }
        if (renewed) {
            refresh("renew sync commit");
        }
        return renewed;
    }

    @Override
    public CommitId flush() throws EngineException {
        return flush(false, false);
    }

    @Override
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        ensureOpen();
        final byte[] newCommitId;
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (flushLock.tryLock() == false) {
                if (waitIfOngoing) {
                    logger.trace("waiting for in-flight flush to finish");
                    flushLock.lock();
                    logger.trace("acquired flush lock after blocking");
                } else {
                    throw new FlushNotAllowedEngineException(shardId, "already flushing...");
                }
            } else {
                logger.trace("acquired flush lock immediately");
            }
            try {
                if (indexWriter.hasUncommittedChanges() || force) {
                    ensureCanFlush();
                    try {
                        translog.prepareCommit();
                        logger.trace("starting commit for flush; commitTranslog=true");
                        commitIndexWriter(indexWriter, translog, null);
                        logger.trace("finished commit for flush");
                        refresh("version_table_flush");
                        translog.commit();
                    } catch (Exception e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
                }
                store.incRef();
                try {
                    lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                } catch (Exception e) {
                    if (isClosed.get() == false) {
                        try {
                            logger.warn("failed to read latest segment infos on flush", e);
                        } catch (Exception inner) {
                            e.addSuppressed(inner);
                        }
                        if (Lucene.isCorruptionException(e)) {
                            throw new FlushFailedEngineException(shardId, e);
                        }
                    }
                } finally {
                    store.decRef();
                }
                newCommitId = lastCommittedSegmentInfos.getId();
            } catch (FlushFailedEngineException ex) {
                maybeFailEngine("flush", ex);
                throw ex;
            } finally {
                flushLock.unlock();
            }
        }
        if (engineConfig.isEnableGcDeletes()) {
            pruneDeletedTombstones();
        }
        return new CommitId(newCommitId);
    }

    private void pruneDeletedTombstones() {
        long timeMSec = engineConfig.getThreadPool().estimatedTimeInMillis();
        for (Map.Entry<BytesRef, VersionValue> entry : versionMap.getAllTombstones()) {
            BytesRef uid = entry.getKey();
            try (Releasable ignored = acquireLock(uid)) {
                VersionValue versionValue = versionMap.getTombstoneUnderLock(uid);
                if (versionValue != null) {
                    if (timeMSec - versionValue.time() > getGcDeletesInMillis()) {
                        versionMap.removeTombstoneUnderLock(uid);
                    }
                }
            }
        }
        lastDeleteVersionPruneTimeMSec = timeMSec;
    }

    @Override
    public void forceMerge(final boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, final boolean upgrade, final boolean upgradeOnlyAncientSegments) throws EngineException, EngineClosedException, IOException {
        assert indexWriter.getConfig().getMergePolicy() instanceof ElasticsearchMergePolicy : "MergePolicy is " + indexWriter.getConfig().getMergePolicy().getClass().getName();
        ElasticsearchMergePolicy mp = (ElasticsearchMergePolicy) indexWriter.getConfig().getMergePolicy();
        optimizeLock.lock();
        try {
            ensureOpen();
            if (upgrade) {
                logger.info("starting segment upgrade upgradeOnlyAncientSegments={}", upgradeOnlyAncientSegments);
                mp.setUpgradeInProgress(true, upgradeOnlyAncientSegments);
            }
            store.incRef();
            try {
                if (onlyExpungeDeletes) {
                    assert upgrade == false;
                    indexWriter.forceMergeDeletes(true);
                } else if (maxNumSegments <= 0) {
                    assert upgrade == false;
                    indexWriter.maybeMerge();
                } else {
                    indexWriter.forceMerge(maxNumSegments, true);
                }
                if (flush) {
                    if (tryRenewSyncCommit() == false) {
                        flush(false, true);
                    }
                }
                if (upgrade) {
                    logger.info("finished segment upgrade");
                }
            } finally {
                store.decRef();
            }
        } catch (Exception e) {
            try {
                maybeFailEngine("force merge", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        } finally {
            try {
                mp.setUpgradeInProgress(false, false);
            } finally {
                optimizeLock.unlock();
            }
        }
    }

    @Override
    public IndexCommit acquireIndexCommit(final boolean flushFirst) throws EngineException {
        if (flushFirst) {
            logger.trace("start flush for snapshot");
            flush(false, true);
            logger.trace("finish flush for snapshot");
        }
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            logger.trace("pulling snapshot");
            return deletionPolicy.snapshot();
        } catch (IOException e) {
            throw new SnapshotFailedEngineException(shardId, e);
        }
    }

    @Override
    protected boolean maybeFailEngine(String source, Exception e) {
        boolean shouldFail = super.maybeFailEngine(source, e);
        if (shouldFail) {
            return true;
        }
        if (e instanceof AlreadyClosedException) {
            if (indexWriter.isOpen() == false && indexWriter.getTragicException() != null) {
                final Exception tragedy = indexWriter.getTragicException() instanceof Exception ? (Exception) indexWriter.getTragicException() : new Exception(indexWriter.getTragicException());
                failEngine("already closed by tragic event on the index writer", tragedy);
            } else if (translog.isOpen() == false && translog.getTragicException() != null) {
                failEngine("already closed by tragic event on the translog", translog.getTragicException());
            }
            return true;
        } else if (e != null && ((indexWriter.isOpen() == false && indexWriter.getTragicException() == e) || (translog.isOpen() == false && translog.getTragicException() == e))) {
            failEngine(source, e);
            return true;
        }
        return false;
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    protected final void writerSegmentStats(SegmentsStats stats) {
        stats.addVersionMapMemoryInBytes(versionMap.ramBytesUsed());
        stats.addIndexWriterMemoryInBytes(indexWriter.ramBytesUsed());
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return indexWriter.ramBytesUsed() + versionMap.ramBytesUsedForRefresh();
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        try (ReleasableLock lock = readLock.acquire()) {
            Segment[] segmentsArr = getSegmentInfo(lastCommittedSegmentInfos, verbose);
            Set<OnGoingMerge> onGoingMerges = mergeScheduler.onGoingMerges();
            for (OnGoingMerge onGoingMerge : onGoingMerges) {
                for (SegmentCommitInfo segmentInfoPerCommit : onGoingMerge.getMergedSegments()) {
                    for (Segment segment : segmentsArr) {
                        if (segment.getName().equals(segmentInfoPerCommit.info.name)) {
                            segment.mergeId = onGoingMerge.getId();
                            break;
                        }
                    }
                }
            }
            return Arrays.asList(segmentsArr);
        }
    }

    @Override
    protected final void closeNoLock(String reason) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread() : "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                this.versionMap.clear();
                try {
                    IOUtils.close(searcherManager);
                } catch (Exception e) {
                    logger.warn("Failed to close SearcherManager", e);
                }
                try {
                    IOUtils.close(translog);
                } catch (Exception e) {
                    logger.warn("Failed to close translog", e);
                }
                logger.trace("rollback indexWriter");
                try {
                    indexWriter.rollback();
                } catch (AlreadyClosedException e) {
                }
                logger.trace("rollback indexWriter done");
            } catch (Exception e) {
                logger.warn("failed to rollback writer on close", e);
            } finally {
                store.decRef();
                logger.debug("engine closed [{}]", reason);
            }
        }
    }

    @Override
    protected SearcherManager getSearcherManager() {
        return searcherManager;
    }

    private Releasable acquireLock(BytesRef uid) {
        return keyedLock.acquire(uid);
    }

    private Releasable acquireLock(Term uid) {
        return acquireLock(uid.bytes());
    }

    private long loadCurrentVersionFromIndex(Term uid) throws IOException {
        try (final Searcher searcher = acquireSearcher("load_version")) {
            return Versions.loadVersion(searcher.reader(), uid);
        }
    }

    private IndexWriter createWriter(boolean create) throws IOException {
        try {
            final IndexWriterConfig iwc = new IndexWriterConfig(engineConfig.getAnalyzer());
            iwc.setCommitOnClose(false);
            iwc.setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND);
            iwc.setIndexDeletionPolicy(deletionPolicy);
            boolean verbose = false;
            try {
                verbose = Boolean.parseBoolean(System.getProperty("tests.verbose"));
            } catch (Exception ignore) {
            }
            iwc.setInfoStream(verbose ? InfoStream.getDefault() : new LoggerInfoStream(logger));
            iwc.setMergeScheduler(mergeScheduler);
            MergePolicy mergePolicy = config().getMergePolicy();
            mergePolicy = new ElasticsearchMergePolicy(mergePolicy);
            iwc.setMergePolicy(mergePolicy);
            iwc.setSimilarity(engineConfig.getSimilarity());
            iwc.setRAMBufferSizeMB(engineConfig.getIndexingBufferSize().mbFrac());
            iwc.setCodec(engineConfig.getCodec());
            iwc.setUseCompoundFile(true);
            return new IndexWriter(store.directory(), iwc);
        } catch (LockObtainFailedException ex) {
            logger.warn("could not lock IndexWriter", ex);
            throw ex;
        }
    }

    static final class SearchFactory extends EngineSearcherFactory {

        private final Engine.Warmer warmer;

        private final ESLogger logger;

        private final AtomicBoolean isEngineClosed;

        SearchFactory(ESLogger logger, AtomicBoolean isEngineClosed, EngineConfig engineConfig) {
            super(engineConfig);
            warmer = engineConfig.getWarmer();
            this.logger = logger;
            this.isEngineClosed = isEngineClosed;
        }

        @Override
        public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader) throws IOException {
            IndexSearcher searcher = super.newSearcher(reader, previousReader);
            if (reader instanceof LeafReader && isMergedSegment((LeafReader) reader)) {
                return searcher;
            }
            if (warmer != null) {
                try {
                    assert searcher.getIndexReader() instanceof ElasticsearchDirectoryReader : "this class needs an ElasticsearchDirectoryReader but got: " + searcher.getIndexReader().getClass();
                    warmer.warm(new Searcher("top_reader_warming", searcher));
                } catch (Exception e) {
                    if (isEngineClosed.get() == false) {
                        logger.warn("failed to prepare/warm", e);
                    }
                }
            }
            return searcher;
        }
    }

    @Override
    public void activateThrottling() {
        int count = throttleRequestCount.incrementAndGet();
        assert count >= 1 : "invalid post-increment throttleRequestCount=" + count;
        if (count == 1) {
            throttle.activate();
        }
    }

    @Override
    public void deactivateThrottling() {
        int count = throttleRequestCount.decrementAndGet();
        assert count >= 0 : "invalid post-decrement throttleRequestCount=" + count;
        if (count == 0) {
            throttle.deactivate();
        }
    }

    public long getIndexThrottleTimeInMillis() {
        return throttle.getThrottleTimeInMillis();
    }

    long getGcDeletesInMillis() {
        return engineConfig.getIndexSettings().getGcDeletesInMillis();
    }

    LiveIndexWriterConfig getCurrentIndexWriterConfig() {
        return indexWriter.getConfig();
    }

    private final class EngineMergeScheduler extends ElasticsearchConcurrentMergeScheduler {

        private final AtomicInteger numMergesInFlight = new AtomicInteger(0);

        private final AtomicBoolean isThrottling = new AtomicBoolean();

        EngineMergeScheduler(ShardId shardId, IndexSettings indexSettings) {
            super(shardId, indexSettings);
        }

        @Override
        public synchronized void beforeMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMergeCount();
            if (numMergesInFlight.incrementAndGet() > maxNumMerges) {
                if (isThrottling.getAndSet(true) == false) {
                    logger.info("now throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    activateThrottling();
                }
            }
        }

        @Override
        public synchronized void afterMerge(OnGoingMerge merge) {
            int maxNumMerges = mergeScheduler.getMaxMergeCount();
            if (numMergesInFlight.decrementAndGet() < maxNumMerges) {
                if (isThrottling.getAndSet(false)) {
                    logger.info("stop throttling indexing: numMergesInFlight={}, maxNumMerges={}", numMergesInFlight, maxNumMerges);
                    deactivateThrottling();
                }
            }
            if (indexWriter.hasPendingMerges() == false && System.nanoTime() - lastWriteNanos >= engineConfig.getFlushMergesAfter().nanos()) {
                engineConfig.getThreadPool().executor(ThreadPool.Names.FLUSH).execute(new AbstractRunnable() {

                    @Override
                    public void onFailure(Exception e) {
                        if (isClosed.get() == false) {
                            logger.warn("failed to flush after merge has finished");
                        }
                    }

                    @Override
                    protected void doRun() throws Exception {
                        if (tryRenewSyncCommit() == false) {
                            flush();
                        }
                    }
                });
            }
        }

        @Override
        protected void handleMergeException(final Directory dir, final Throwable exc) {
            logger.error("failed to merge", exc);
            engineConfig.getThreadPool().generic().execute(new AbstractRunnable() {

                @Override
                public void onFailure(Exception e) {
                    logger.debug("merge failure action rejected", e);
                }

                @Override
                protected void doRun() throws Exception {
                    MergePolicy.MergeException e = new MergePolicy.MergeException(exc, dir);
                    failEngine("merge failed", e);
                }
            });
        }
    }

    private void commitIndexWriter(IndexWriter writer, Translog translog, String syncId) throws IOException {
        ensureCanFlush();
        try {
            Translog.TranslogGeneration translogGeneration = translog.getGeneration();
            logger.trace("committing writer with translog id [{}]  and sync id [{}] ", translogGeneration.translogFileGeneration, syncId);
            Map<String, String> commitData = new HashMap<>(2);
            commitData.put(Translog.TRANSLOG_GENERATION_KEY, Long.toString(translogGeneration.translogFileGeneration));
            commitData.put(Translog.TRANSLOG_UUID_KEY, translogGeneration.translogUUID);
            if (syncId != null) {
                commitData.put(Engine.SYNC_COMMIT_ID, syncId);
            }
            indexWriter.setCommitData(commitData);
            writer.commit();
        } catch (Exception ex) {
            try {
                failEngine("lucene commit failed", ex);
            } catch (Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        } catch (AssertionError e) {
            if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                EngineException engineException = new EngineException(shardId, "failed to commit engine", e);
                try {
                    failEngine("lucene commit failed", engineException);
                } catch (Exception inner) {
                    engineException.addSuppressed(inner);
                }
                throw engineException;
            } else {
                throw e;
            }
        }
    }

    private void ensureCanFlush() {
        if (allowCommits.get() == false) {
            throw new FlushNotAllowedEngineException(shardId, "flushes are disabled - pending translog recovery");
        }
    }

    public void onSettingsChanged() {
        mergeScheduler.refreshConfig();
        maybePruneDeletedTombstones();
    }

    public MergeStats getMergeStats() {
        return mergeScheduler.stats();
    }

    @Override
    public DocsStats getDocStats() {
        final int numDocs = indexWriter.numDocs();
        final int maxDoc = indexWriter.maxDoc();
        return new DocsStats(numDocs, maxDoc - numDocs);
    }
}