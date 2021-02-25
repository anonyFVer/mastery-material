package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.Assertions;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.LoggerInfoStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ElasticsearchMergePolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

public class InternalEngine extends Engine {

    private volatile long lastDeleteVersionPruneTimeMSec;

    private final Translog translog;

    private final ElasticsearchConcurrentMergeScheduler mergeScheduler;

    private final IndexWriter indexWriter;

    private final ExternalSearcherManager externalSearcherManager;

    private final SearcherManager internalSearcherManager;

    private final Lock flushLock = new ReentrantLock();

    private final ReentrantLock optimizeLock = new ReentrantLock();

    private final LiveVersionMap versionMap = new LiveVersionMap();

    private volatile SegmentInfos lastCommittedSegmentInfos;

    private final IndexThrottle throttle;

    private final LocalCheckpointTracker localCheckpointTracker;

    private final CombinedDeletionPolicy combinedDeletionPolicy;

    private final AtomicInteger throttleRequestCount = new AtomicInteger();

    private final AtomicBoolean pendingTranslogRecovery = new AtomicBoolean(false);

    public static final String MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID = "max_unsafe_auto_id_timestamp";

    private final AtomicLong maxUnsafeAutoIdTimestamp = new AtomicLong(-1);

    private final AtomicLong maxSeqNoOfNonAppendOnlyOperations = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);

    private final CounterMetric numVersionLookups = new CounterMetric();

    private final CounterMetric numIndexVersionsLookups = new CounterMetric();

    private final CounterMetric numDocDeletes = new CounterMetric();

    private final CounterMetric numDocAppends = new CounterMetric();

    private final CounterMetric numDocUpdates = new CounterMetric();

    private final NumericDocValuesField softDeleteField = Lucene.newSoftDeleteField();

    private final boolean softDeleteEnabled;

    private final SoftDeletesPolicy softDeletesPolicy;

    private final LastRefreshedCheckpointListener lastRefreshedCheckpointListener;

    private final AtomicLong writingBytes = new AtomicLong();

    private final AtomicBoolean trackTranslogLocation = new AtomicBoolean(false);

    @Nullable
    private final String historyUUID;

    public InternalEngine(EngineConfig engineConfig) {
        this(engineConfig, LocalCheckpointTracker::new);
    }

    InternalEngine(final EngineConfig engineConfig, final BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) {
        super(engineConfig);
        if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
            maxUnsafeAutoIdTimestamp.set(Long.MAX_VALUE);
        }
        final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(), engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis());
        store.incRef();
        IndexWriter writer = null;
        Translog translog = null;
        ExternalSearcherManager externalSearcherManager = null;
        SearcherManager internalSearcherManager = null;
        EngineMergeScheduler scheduler = null;
        boolean success = false;
        try {
            this.lastDeleteVersionPruneTimeMSec = engineConfig.getThreadPool().relativeTimeInMillis();
            mergeScheduler = scheduler = new EngineMergeScheduler(engineConfig.getShardId(), engineConfig.getIndexSettings());
            throttle = new IndexThrottle();
            try {
                translog = openTranslog(engineConfig, translogDeletionPolicy, engineConfig.getGlobalCheckpointSupplier());
                assert translog.getGeneration() != null;
                this.translog = translog;
                this.localCheckpointTracker = createLocalCheckpointTracker(localCheckpointTrackerSupplier);
                this.softDeleteEnabled = engineConfig.getIndexSettings().isSoftDeleteEnabled();
                this.softDeletesPolicy = newSoftDeletesPolicy();
                this.combinedDeletionPolicy = new CombinedDeletionPolicy(logger, translogDeletionPolicy, softDeletesPolicy, translog::getLastSyncedGlobalCheckpoint);
                writer = createWriter();
                bootstrapAppendOnlyInfoFromWriter(writer);
                historyUUID = loadHistoryUUID(writer);
                indexWriter = writer;
            } catch (IOException | TranslogCorruptedException e) {
                throw new EngineCreationFailureException(shardId, "failed to create engine", e);
            } catch (AssertionError e) {
                if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                    throw new EngineCreationFailureException(shardId, "failed to create engine", e);
                } else {
                    throw e;
                }
            }
            externalSearcherManager = createSearcherManager(new SearchFactory(logger, isClosed, engineConfig));
            internalSearcherManager = externalSearcherManager.internalSearcherManager;
            this.internalSearcherManager = internalSearcherManager;
            this.externalSearcherManager = externalSearcherManager;
            internalSearcherManager.addListener(versionMap);
            assert pendingTranslogRecovery.get() == false : "translog recovery can't be pending before we set it";
            pendingTranslogRecovery.set(true);
            for (ReferenceManager.RefreshListener listener : engineConfig.getExternalRefreshListener()) {
                this.externalSearcherManager.addListener(listener);
            }
            for (ReferenceManager.RefreshListener listener : engineConfig.getInternalRefreshListener()) {
                this.internalSearcherManager.addListener(listener);
            }
            this.lastRefreshedCheckpointListener = new LastRefreshedCheckpointListener(localCheckpointTracker.getCheckpoint());
            this.internalSearcherManager.addListener(lastRefreshedCheckpointListener);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(writer, translog, internalSearcherManager, externalSearcherManager, scheduler);
                if (isClosed.get() == false) {
                    store.decRef();
                }
            }
        }
        logger.trace("created new InternalEngine");
    }

    private LocalCheckpointTracker createLocalCheckpointTracker(BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) throws IOException {
        final long maxSeqNo;
        final long localCheckpoint;
        final SequenceNumbers.CommitInfo seqNoStats = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(store.readLastCommittedSegmentsInfo().userData.entrySet());
        maxSeqNo = seqNoStats.maxSeqNo;
        localCheckpoint = seqNoStats.localCheckpoint;
        logger.trace("recovered maximum sequence number [{}] and local checkpoint [{}]", maxSeqNo, localCheckpoint);
        return localCheckpointTrackerSupplier.apply(maxSeqNo, localCheckpoint);
    }

    private SoftDeletesPolicy newSoftDeletesPolicy() throws IOException {
        final Map<String, String> commitUserData = store.readLastCommittedSegmentsInfo().userData;
        final long lastMinRetainedSeqNo;
        if (commitUserData.containsKey(Engine.MIN_RETAINED_SEQNO)) {
            lastMinRetainedSeqNo = Long.parseLong(commitUserData.get(Engine.MIN_RETAINED_SEQNO));
        } else {
            lastMinRetainedSeqNo = Long.parseLong(commitUserData.get(SequenceNumbers.MAX_SEQ_NO)) + 1;
        }
        return new SoftDeletesPolicy(translog::getLastSyncedGlobalCheckpoint, lastMinRetainedSeqNo, engineConfig.getIndexSettings().getSoftDeleteRetentionOperations());
    }

    @SuppressForbidden(reason = "reference counting is required here")
    private static final class ExternalSearcherManager extends ReferenceManager<IndexSearcher> {

        private final SearcherFactory searcherFactory;

        private final SearcherManager internalSearcherManager;

        ExternalSearcherManager(SearcherManager internalSearcherManager, SearcherFactory searcherFactory) throws IOException {
            IndexSearcher acquire = internalSearcherManager.acquire();
            try {
                IndexReader indexReader = acquire.getIndexReader();
                assert indexReader instanceof ElasticsearchDirectoryReader : "searcher's IndexReader should be an ElasticsearchDirectoryReader, but got " + indexReader;
                indexReader.incRef();
                current = SearcherManager.getSearcher(searcherFactory, indexReader, null);
            } finally {
                internalSearcherManager.release(acquire);
            }
            this.searcherFactory = searcherFactory;
            this.internalSearcherManager = internalSearcherManager;
        }

        @Override
        protected IndexSearcher refreshIfNeeded(IndexSearcher referenceToRefresh) throws IOException {
            internalSearcherManager.maybeRefreshBlocking();
            IndexSearcher acquire = internalSearcherManager.acquire();
            try {
                final IndexReader previousReader = referenceToRefresh.getIndexReader();
                assert previousReader instanceof ElasticsearchDirectoryReader : "searcher's IndexReader should be an ElasticsearchDirectoryReader, but got " + previousReader;
                final IndexReader newReader = acquire.getIndexReader();
                if (newReader == previousReader) {
                    return null;
                } else {
                    newReader.incRef();
                    return SearcherManager.getSearcher(searcherFactory, newReader, previousReader);
                }
            } finally {
                internalSearcherManager.release(acquire);
            }
        }

        @Override
        protected boolean tryIncRef(IndexSearcher reference) {
            return reference.getIndexReader().tryIncRef();
        }

        @Override
        protected int getRefCount(IndexSearcher reference) {
            return reference.getIndexReader().getRefCount();
        }

        @Override
        protected void decRef(IndexSearcher reference) throws IOException {
            reference.getIndexReader().decRef();
        }
    }

    @Override
    public void restoreLocalCheckpointFromTranslog() throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            ensureOpen();
            final long localCheckpoint = localCheckpointTracker.getCheckpoint();
            try (Translog.Snapshot snapshot = getTranslog().newSnapshotFromMinSeqNo(localCheckpoint + 1)) {
                Translog.Operation operation;
                while ((operation = snapshot.next()) != null) {
                    if (operation.seqNo() > localCheckpoint) {
                        localCheckpointTracker.markSeqNoAsCompleted(operation.seqNo());
                    }
                }
            }
        }
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            ensureOpen();
            final long localCheckpoint = localCheckpointTracker.getCheckpoint();
            final long maxSeqNo = localCheckpointTracker.getMaxSeqNo();
            int numNoOpsAdded = 0;
            for (long seqNo = localCheckpoint + 1; seqNo <= maxSeqNo; seqNo = localCheckpointTracker.getCheckpoint() + 1) {
                innerNoOp(new NoOp(seqNo, primaryTerm, Operation.Origin.PRIMARY, System.nanoTime(), "filling gaps"));
                numNoOpsAdded++;
                assert seqNo <= localCheckpointTracker.getCheckpoint() : "local checkpoint did not advance; was [" + seqNo + "], now [" + localCheckpointTracker.getCheckpoint() + "]";
            }
            return numNoOpsAdded;
        }
    }

    private void bootstrapAppendOnlyInfoFromWriter(IndexWriter writer) {
        for (Map.Entry<String, String> entry : writer.getLiveCommitData()) {
            final String key = entry.getKey();
            if (key.equals(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID)) {
                assert maxUnsafeAutoIdTimestamp.get() == -1 : "max unsafe timestamp was assigned already [" + maxUnsafeAutoIdTimestamp.get() + "]";
                maxUnsafeAutoIdTimestamp.set(Long.parseLong(entry.getValue()));
            }
            if (key.equals(SequenceNumbers.MAX_SEQ_NO)) {
                assert maxSeqNoOfNonAppendOnlyOperations.get() == -1 : "max unsafe append-only seq# was assigned already [" + maxSeqNoOfNonAppendOnlyOperations.get() + "]";
                maxSeqNoOfNonAppendOnlyOperations.set(Long.parseLong(entry.getValue()));
            }
        }
    }

    @Override
    public InternalEngine recoverFromTranslog(long recoverUpToSeqNo) throws IOException {
        flushLock.lock();
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (pendingTranslogRecovery.get() == false) {
                throw new IllegalStateException("Engine has already been recovered");
            }
            try {
                recoverFromTranslogInternal(recoverUpToSeqNo);
            } catch (Exception e) {
                try {
                    pendingTranslogRecovery.set(true);
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

    @Override
    public void skipTranslogRecovery() {
        assert pendingTranslogRecovery.get() : "translogRecovery is not pending but should be";
        pendingTranslogRecovery.set(false);
    }

    private void recoverFromTranslogInternal(long recoverUpToSeqNo) throws IOException {
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        final int opsRecovered;
        final long translogFileGen = Long.parseLong(lastCommittedSegmentInfos.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
        try (Translog.Snapshot snapshot = translog.newSnapshotFromGen(new Translog.TranslogGeneration(translog.getTranslogUUID(), translogFileGen), recoverUpToSeqNo)) {
            opsRecovered = config().getTranslogRecoveryRunner().run(this, snapshot);
        } catch (Exception e) {
            throw new EngineException(shardId, "failed to recover from translog", e);
        }
        assert pendingTranslogRecovery.get() : "translogRecovery is not pending but should be";
        pendingTranslogRecovery.set(false);
        if (opsRecovered > 0) {
            logger.trace("flushing post recovery from translog. ops recovered [{}]. committed translog id [{}]. current id [{}]", opsRecovered, translogGeneration == null ? null : translogGeneration.translogFileGeneration, translog.currentFileGeneration());
            commitIndexWriter(indexWriter, translog, null);
            refreshLastCommittedSegmentInfos();
            refresh("translog_recovery");
        }
        translog.trimUnreferencedReaders();
    }

    private Translog openTranslog(EngineConfig engineConfig, TranslogDeletionPolicy translogDeletionPolicy, LongSupplier globalCheckpointSupplier) throws IOException {
        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        final String translogUUID = loadTranslogUUIDFromLastCommit();
        return new Translog(translogConfig, translogUUID, translogDeletionPolicy, globalCheckpointSupplier, engineConfig.getPrimaryTermSupplier());
    }

    Translog getTranslog() {
        ensureOpen();
        return translog;
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return getTranslog().syncNeeded();
    }

    @Override
    public boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException {
        final boolean synced = translog.ensureSynced(locations);
        if (synced) {
            revisitIndexDeletionPolicyOnTranslogSynced();
        }
        return synced;
    }

    @Override
    public void syncTranslog() throws IOException {
        translog.sync();
        revisitIndexDeletionPolicyOnTranslogSynced();
    }

    @Override
    public Translog.Snapshot newSnapshotFromMinSeqNo(long minSeqNo) throws IOException {
        return getTranslog().newSnapshotFromMinSeqNo(minSeqNo);
    }

    @Override
    public Translog.Snapshot readHistoryOperations(String source, MapperService mapperService, long startingSeqNo) throws IOException {
        if (engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
            return newLuceneChangesSnapshot(source, mapperService, Math.max(0, startingSeqNo), Long.MAX_VALUE, false);
        } else {
            return getTranslog().newSnapshotFromMinSeqNo(startingSeqNo);
        }
    }

    @Override
    public int estimateNumberOfHistoryOperations(String source, MapperService mapperService, long startingSeqNo) throws IOException {
        if (engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
            try (Translog.Snapshot snapshot = newLuceneChangesSnapshot(source, mapperService, Math.max(0, startingSeqNo), Long.MAX_VALUE, false)) {
                return snapshot.totalOperations();
            } catch (IOException ex) {
                maybeFailEngine(source, ex);
                throw ex;
            }
        } else {
            return getTranslog().estimateTotalOperationsFromMinSeq(startingSeqNo);
        }
    }

    @Override
    public TranslogStats getTranslogStats() {
        return getTranslog().stats();
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return getTranslog().getLastWriteLocation();
    }

    private void revisitIndexDeletionPolicyOnTranslogSynced() throws IOException {
        if (combinedDeletionPolicy.hasUnreferencedCommits()) {
            indexWriter.deleteUnusedFiles();
            translog.trimUnreferencedReaders();
        }
    }

    @Override
    public String getHistoryUUID() {
        return historyUUID;
    }

    @Override
    public long getWritingBytes() {
        return writingBytes.get();
    }

    @Nullable
    private String loadTranslogUUIDFromLastCommit() throws IOException {
        final Map<String, String> commitUserData = store.readLastCommittedSegmentsInfo().getUserData();
        if (commitUserData.containsKey(Translog.TRANSLOG_GENERATION_KEY) == false) {
            throw new IllegalStateException("commit doesn't contain translog generation id");
        }
        return commitUserData.get(Translog.TRANSLOG_UUID_KEY);
    }

    private String loadHistoryUUID(final IndexWriter writer) throws IOException {
        final String uuid = commitDataAsMap(writer).get(HISTORY_UUID_KEY);
        if (uuid == null) {
            throw new IllegalStateException("commit doesn't contain history uuid");
        }
        return uuid;
    }

    private ExternalSearcherManager createSearcherManager(SearchFactory externalSearcherFactory) throws EngineException {
        boolean success = false;
        SearcherManager internalSearcherManager = null;
        try {
            try {
                final DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter), shardId);
                internalSearcherManager = new SearcherManager(directoryReader, new RamAccountingSearcherFactory(engineConfig.getCircuitBreakerService()));
                lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
                ExternalSearcherManager externalSearcherManager = new ExternalSearcherManager(internalSearcherManager, externalSearcherFactory);
                success = true;
                return externalSearcherManager;
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
                IOUtils.closeWhileHandlingException(internalSearcherManager, indexWriter);
            }
        }
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        assert Objects.equals(get.uid().field(), IdFieldMapper.NAME) : get.uid().field();
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            SearcherScope scope;
            if (get.realtime()) {
                VersionValue versionValue = null;
                try (Releasable ignore = versionMap.acquireLock(get.uid().bytes())) {
                    versionValue = getVersionFromMap(get.uid().bytes());
                }
                if (versionValue != null) {
                    if (versionValue.isDelete()) {
                        return GetResult.NOT_EXISTS;
                    }
                    if (get.versionType().isVersionConflictForReads(versionValue.version, get.version())) {
                        throw new VersionConflictEngineException(shardId, get.type(), get.id(), get.versionType().explainConflictForReads(versionValue.version, get.version()));
                    }
                    if (get.isReadFromTranslog()) {
                        if (versionValue.getLocation() != null) {
                            try {
                                Translog.Operation operation = translog.readOperation(versionValue.getLocation());
                                if (operation != null) {
                                    TranslogLeafReader reader = new TranslogLeafReader((Translog.Index) operation, engineConfig.getIndexSettings().getIndexVersionCreated());
                                    return new GetResult(new Searcher("realtime_get", new IndexSearcher(reader)), new VersionsAndSeqNoResolver.DocIdAndVersion(0, ((Translog.Index) operation).version(), reader, 0));
                                }
                            } catch (IOException e) {
                                maybeFailEngine("realtime_get", e);
                                throw new EngineException(shardId, "failed to read operation from translog", e);
                            }
                        } else {
                            trackTranslogLocation.set(true);
                        }
                    }
                    refresh("realtime_get", SearcherScope.INTERNAL);
                }
                scope = SearcherScope.INTERNAL;
            } else {
                scope = SearcherScope.EXTERNAL;
            }
            return getFromSearcher(get, searcherFactory, scope);
        }
    }

    enum OpVsLuceneDocStatus {

        OP_NEWER, OP_STALE_OR_EQUAL, LUCENE_DOC_NOT_FOUND
    }

    private OpVsLuceneDocStatus compareOpToLuceneDocBasedOnSeqNo(final Operation op) throws IOException {
        assert op.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO : "resolving ops based on seq# but no seqNo is found";
        final OpVsLuceneDocStatus status;
        VersionValue versionValue = getVersionFromMap(op.uid().bytes());
        assert incrementVersionLookup();
        if (versionValue != null) {
            if (op.seqNo() > versionValue.seqNo || (op.seqNo() == versionValue.seqNo && op.primaryTerm() > versionValue.term))
                status = OpVsLuceneDocStatus.OP_NEWER;
            else {
                status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
            }
        } else {
            assert incrementIndexVersionLookup();
            try (Searcher searcher = acquireSearcher("load_seq_no", SearcherScope.INTERNAL)) {
                DocIdAndSeqNo docAndSeqNo = VersionsAndSeqNoResolver.loadDocIdAndSeqNo(searcher.reader(), op.uid());
                if (docAndSeqNo == null) {
                    status = OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND;
                } else if (op.seqNo() > docAndSeqNo.seqNo) {
                    status = OpVsLuceneDocStatus.OP_NEWER;
                } else if (op.seqNo() == docAndSeqNo.seqNo) {
                    final long existingTerm = VersionsAndSeqNoResolver.loadPrimaryTerm(docAndSeqNo, op.uid().field());
                    if (op.primaryTerm() > existingTerm) {
                        status = OpVsLuceneDocStatus.OP_NEWER;
                    } else {
                        status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
                    }
                } else {
                    status = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
                }
            }
        }
        return status;
    }

    private VersionValue resolveDocVersion(final Operation op) throws IOException {
        assert incrementVersionLookup();
        VersionValue versionValue = getVersionFromMap(op.uid().bytes());
        if (versionValue == null) {
            assert incrementIndexVersionLookup();
            final long currentVersion = loadCurrentVersionFromIndex(op.uid());
            if (currentVersion != Versions.NOT_FOUND) {
                versionValue = new IndexVersionValue(null, currentVersion, SequenceNumbers.UNASSIGNED_SEQ_NO, 0L);
            }
        } else if (engineConfig.isEnableGcDeletes() && versionValue.isDelete() && (engineConfig.getThreadPool().relativeTimeInMillis() - ((DeleteVersionValue) versionValue).time) > getGcDeletesInMillis()) {
            versionValue = null;
        }
        return versionValue;
    }

    private VersionValue getVersionFromMap(BytesRef id) {
        if (versionMap.isUnsafe()) {
            synchronized (versionMap) {
                if (versionMap.isUnsafe()) {
                    refresh("unsafe_version_map", SearcherScope.INTERNAL);
                }
                versionMap.enforceSafeAccess();
            }
        }
        return versionMap.getUnderLock(id);
    }

    private boolean canOptimizeAddDocument(Index index) {
        if (index.getAutoGeneratedIdTimestamp() != IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP) {
            assert index.getAutoGeneratedIdTimestamp() >= 0 : "autoGeneratedIdTimestamp must be positive but was: " + index.getAutoGeneratedIdTimestamp();
            switch(index.origin()) {
                case PRIMARY:
                    assertPrimaryCanOptimizeAddDocument(index);
                    return true;
                case PEER_RECOVERY:
                case REPLICA:
                    assert index.version() == 1 && index.versionType() == null : "version: " + index.version() + " type: " + index.versionType();
                    return true;
                case LOCAL_TRANSLOG_RECOVERY:
                    assert index.isRetry();
                    return true;
                default:
                    throw new IllegalArgumentException("unknown origin " + index.origin());
            }
        }
        return false;
    }

    protected boolean assertPrimaryCanOptimizeAddDocument(final Index index) {
        assert (index.version() == Versions.MATCH_ANY && index.versionType() == VersionType.INTERNAL) : "version: " + index.version() + " type: " + index.versionType();
        return true;
    }

    private boolean assertIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        if (origin == Operation.Origin.PRIMARY) {
            assertPrimaryIncomingSequenceNumber(origin, seqNo);
        } else {
            assert seqNo >= 0 : "recovery or replica ops should have an assigned seq no.; origin: " + origin;
        }
        return true;
    }

    protected boolean assertPrimaryIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        assert seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : "primary operations must never have an assigned sequence number but was [" + seqNo + "]";
        return true;
    }

    private long generateSeqNoForOperation(final Operation operation) {
        assert operation.origin() == Operation.Origin.PRIMARY;
        return doGenerateSeqNoForOperation(operation);
    }

    long doGenerateSeqNoForOperation(final Operation operation) {
        return localCheckpointTracker.generateSeqNo();
    }

    private long getPrimaryTerm() {
        return engineConfig.getPrimaryTermSupplier().getAsLong();
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        assert Objects.equals(index.uid().field(), IdFieldMapper.NAME) : index.uid().field();
        final boolean doThrottle = index.origin().isRecovery() == false;
        try (ReleasableLock releasableLock = readLock.acquire()) {
            ensureOpen();
            assert assertIncomingSequenceNumber(index.origin(), index.seqNo());
            try (Releasable ignored = versionMap.acquireLock(index.uid().bytes());
                Releasable indexThrottle = doThrottle ? () -> {
                } : throttle.acquireThrottle()) {
                lastWriteNanos = index.startTime();
                final IndexingStrategy plan = indexingStrategyForOperation(index);
                final IndexResult indexResult;
                if (plan.earlyResultOnPreFlightError.isPresent()) {
                    indexResult = plan.earlyResultOnPreFlightError.get();
                    assert indexResult.getResultType() == Result.Type.FAILURE : indexResult.getResultType();
                } else if (plan.indexIntoLucene || plan.addStaleOpToLucene) {
                    indexResult = indexIntoLucene(index, plan);
                } else {
                    indexResult = new IndexResult(plan.versionForIndexing, getPrimaryTerm(), plan.seqNoForIndexing, plan.currentNotFoundOrDeleted);
                }
                if (index.origin() != Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
                    final Translog.Location location;
                    if (indexResult.getResultType() == Result.Type.SUCCESS) {
                        location = translog.add(new Translog.Index(index, indexResult));
                    } else if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        final NoOp noOp = new NoOp(indexResult.getSeqNo(), index.primaryTerm(), index.origin(), index.startTime(), indexResult.getFailure().getMessage());
                        location = innerNoOp(noOp).getTranslogLocation();
                    } else {
                        location = null;
                    }
                    indexResult.setTranslogLocation(location);
                }
                if (plan.indexIntoLucene && indexResult.getResultType() == Result.Type.SUCCESS) {
                    final Translog.Location translogLocation = trackTranslogLocation.get() ? indexResult.getTranslogLocation() : null;
                    versionMap.maybePutIndexUnderLock(index.uid().bytes(), new IndexVersionValue(translogLocation, plan.versionForIndexing, plan.seqNoForIndexing, index.primaryTerm()));
                }
                if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    localCheckpointTracker.markSeqNoAsCompleted(indexResult.getSeqNo());
                }
                indexResult.setTook(System.nanoTime() - index.startTime());
                indexResult.freeze();
                return indexResult;
            }
        } catch (RuntimeException | IOException e) {
            try {
                maybeFailEngine("index", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
    }

    protected final IndexingStrategy planIndexingAsNonPrimary(Index index) throws IOException {
        assertNonPrimaryOrigin(index);
        final IndexingStrategy plan;
        final boolean appendOnlyRequest = canOptimizeAddDocument(index);
        if (appendOnlyRequest && mayHaveBeenIndexedBefore(index) == false && index.seqNo() > maxSeqNoOfNonAppendOnlyOperations.get()) {
            assert index.version() == 1L : "can optimize on replicas but incoming version is [" + index.version() + "]";
            plan = IndexingStrategy.optimizedAppendOnly(index.seqNo());
        } else {
            if (appendOnlyRequest == false) {
                maxSeqNoOfNonAppendOnlyOperations.updateAndGet(curr -> Math.max(index.seqNo(), curr));
                assert maxSeqNoOfNonAppendOnlyOperations.get() >= index.seqNo() : "max_seqno of non-append-only was not updated;" + "max_seqno non-append-only [" + maxSeqNoOfNonAppendOnlyOperations.get() + "], seqno of index [" + index.seqNo() + "]";
            }
            versionMap.enforceSafeAccess();
            if (index.seqNo() <= localCheckpointTracker.getCheckpoint()) {
                plan = IndexingStrategy.processButSkipLucene(false, index.seqNo(), index.version());
            } else {
                final OpVsLuceneDocStatus opVsLucene = compareOpToLuceneDocBasedOnSeqNo(index);
                if (opVsLucene == OpVsLuceneDocStatus.OP_STALE_OR_EQUAL) {
                    plan = IndexingStrategy.processAsStaleOp(softDeleteEnabled, index.seqNo(), index.version());
                } else {
                    plan = IndexingStrategy.processNormally(opVsLucene == OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND, index.seqNo(), index.version());
                }
            }
        }
        return plan;
    }

    protected IndexingStrategy indexingStrategyForOperation(final Index index) throws IOException {
        if (index.origin() == Operation.Origin.PRIMARY) {
            return planIndexingAsPrimary(index);
        } else {
            return planIndexingAsNonPrimary(index);
        }
    }

    protected final IndexingStrategy planIndexingAsPrimary(Index index) throws IOException {
        assert index.origin() == Operation.Origin.PRIMARY : "planing as primary but origin isn't. got " + index.origin();
        final IndexingStrategy plan;
        if (canOptimizeAddDocument(index)) {
            if (mayHaveBeenIndexedBefore(index)) {
                plan = IndexingStrategy.overrideExistingAsIfNotThere(generateSeqNoForOperation(index), 1L);
                versionMap.enforceSafeAccess();
            } else {
                plan = IndexingStrategy.optimizedAppendOnly(generateSeqNoForOperation(index));
            }
        } else {
            versionMap.enforceSafeAccess();
            final VersionValue versionValue = resolveDocVersion(index);
            final long currentVersion;
            final boolean currentNotFoundOrDeleted;
            if (versionValue == null) {
                currentVersion = Versions.NOT_FOUND;
                currentNotFoundOrDeleted = true;
            } else {
                currentVersion = versionValue.version;
                currentNotFoundOrDeleted = versionValue.isDelete();
            }
            if (index.versionType().isVersionConflictForWrites(currentVersion, index.version(), currentNotFoundOrDeleted)) {
                final VersionConflictEngineException e = new VersionConflictEngineException(shardId, index, currentVersion, currentNotFoundOrDeleted);
                plan = IndexingStrategy.skipDueToVersionConflict(e, currentNotFoundOrDeleted, currentVersion, getPrimaryTerm());
            } else {
                plan = IndexingStrategy.processNormally(currentNotFoundOrDeleted, generateSeqNoForOperation(index), index.versionType().updateVersion(currentVersion, index.version()));
            }
        }
        return plan;
    }

    private IndexResult indexIntoLucene(Index index, IndexingStrategy plan) throws IOException {
        assert plan.seqNoForIndexing >= 0 : "ops should have an assigned seq no.; origin: " + index.origin();
        assert plan.versionForIndexing >= 0 : "version must be set. got " + plan.versionForIndexing;
        assert plan.indexIntoLucene || plan.addStaleOpToLucene;
        index.parsedDoc().updateSeqID(plan.seqNoForIndexing, index.primaryTerm());
        index.parsedDoc().version().setLongValue(plan.versionForIndexing);
        try {
            if (plan.addStaleOpToLucene) {
                addStaleDocs(index.docs(), indexWriter);
            } else if (plan.useLuceneUpdateDocument) {
                updateDocs(index.uid(), index.docs(), indexWriter);
            } else {
                assert assertDocDoesNotExist(index, canOptimizeAddDocument(index) == false);
                addDocs(index.docs(), indexWriter);
            }
            return new IndexResult(plan.versionForIndexing, getPrimaryTerm(), plan.seqNoForIndexing, plan.currentNotFoundOrDeleted);
        } catch (Exception ex) {
            if (indexWriter.getTragicException() == null) {
                return new IndexResult(ex, Versions.MATCH_ANY, getPrimaryTerm(), plan.seqNoForIndexing);
            } else {
                throw ex;
            }
        }
    }

    private boolean mayHaveBeenIndexedBefore(Index index) {
        assert canOptimizeAddDocument(index);
        final boolean mayHaveBeenIndexBefore;
        if (index.isRetry()) {
            mayHaveBeenIndexBefore = true;
            maxUnsafeAutoIdTimestamp.updateAndGet(curr -> Math.max(index.getAutoGeneratedIdTimestamp(), curr));
            assert maxUnsafeAutoIdTimestamp.get() >= index.getAutoGeneratedIdTimestamp();
        } else {
            mayHaveBeenIndexBefore = maxUnsafeAutoIdTimestamp.get() >= index.getAutoGeneratedIdTimestamp();
        }
        return mayHaveBeenIndexBefore;
    }

    long getMaxSeqNoOfNonAppendOnlyOperations() {
        return maxSeqNoOfNonAppendOnlyOperations.get();
    }

    private void addDocs(final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        if (docs.size() > 1) {
            indexWriter.addDocuments(docs);
        } else {
            indexWriter.addDocument(docs.get(0));
        }
        numDocAppends.inc(docs.size());
    }

    private void addStaleDocs(final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        assert softDeleteEnabled : "Add history documents but soft-deletes is disabled";
        docs.forEach(d -> d.add(softDeleteField));
        if (docs.size() > 1) {
            indexWriter.addDocuments(docs);
        } else {
            indexWriter.addDocument(docs.get(0));
        }
    }

    protected static final class IndexingStrategy {

        final boolean currentNotFoundOrDeleted;

        final boolean useLuceneUpdateDocument;

        final long seqNoForIndexing;

        final long versionForIndexing;

        final boolean indexIntoLucene;

        final boolean addStaleOpToLucene;

        final Optional<IndexResult> earlyResultOnPreFlightError;

        private IndexingStrategy(boolean currentNotFoundOrDeleted, boolean useLuceneUpdateDocument, boolean indexIntoLucene, boolean addStaleOpToLucene, long seqNoForIndexing, long versionForIndexing, IndexResult earlyResultOnPreFlightError) {
            assert useLuceneUpdateDocument == false || indexIntoLucene : "use lucene update is set to true, but we're not indexing into lucene";
            assert (indexIntoLucene && earlyResultOnPreFlightError != null) == false : "can only index into lucene or have a preflight result but not both." + "indexIntoLucene: " + indexIntoLucene + "  earlyResultOnPreFlightError:" + earlyResultOnPreFlightError;
            this.currentNotFoundOrDeleted = currentNotFoundOrDeleted;
            this.useLuceneUpdateDocument = useLuceneUpdateDocument;
            this.seqNoForIndexing = seqNoForIndexing;
            this.versionForIndexing = versionForIndexing;
            this.indexIntoLucene = indexIntoLucene;
            this.addStaleOpToLucene = addStaleOpToLucene;
            this.earlyResultOnPreFlightError = earlyResultOnPreFlightError == null ? Optional.empty() : Optional.of(earlyResultOnPreFlightError);
        }

        static IndexingStrategy optimizedAppendOnly(long seqNoForIndexing) {
            return new IndexingStrategy(true, false, true, false, seqNoForIndexing, 1, null);
        }

        static IndexingStrategy skipDueToVersionConflict(VersionConflictEngineException e, boolean currentNotFoundOrDeleted, long currentVersion, long term) {
            final IndexResult result = new IndexResult(e, currentVersion, term);
            return new IndexingStrategy(currentNotFoundOrDeleted, false, false, false, SequenceNumbers.UNASSIGNED_SEQ_NO, Versions.NOT_FOUND, result);
        }

        static IndexingStrategy processNormally(boolean currentNotFoundOrDeleted, long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(currentNotFoundOrDeleted, currentNotFoundOrDeleted == false, true, false, seqNoForIndexing, versionForIndexing, null);
        }

        static IndexingStrategy overrideExistingAsIfNotThere(long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(true, true, true, false, seqNoForIndexing, versionForIndexing, null);
        }

        static IndexingStrategy processButSkipLucene(boolean currentNotFoundOrDeleted, long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(currentNotFoundOrDeleted, false, false, false, seqNoForIndexing, versionForIndexing, null);
        }

        static IndexingStrategy processAsStaleOp(boolean addStaleOpToLucene, long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(false, false, false, addStaleOpToLucene, seqNoForIndexing, versionForIndexing, null);
        }
    }

    private boolean assertDocDoesNotExist(final Index index, final boolean allowDeleted) throws IOException {
        final VersionValue versionValue = versionMap.getVersionForAssert(index.uid().bytes());
        if (versionValue != null) {
            if (versionValue.isDelete() == false || allowDeleted == false) {
                throw new AssertionError("doc [" + index.type() + "][" + index.id() + "] exists in version map (version " + versionValue + ")");
            }
        } else {
            try (Searcher searcher = acquireSearcher("assert doc doesn't exist", SearcherScope.INTERNAL)) {
                final long docsWithId = searcher.searcher().count(new TermQuery(index.uid()));
                if (docsWithId > 0) {
                    throw new AssertionError("doc [" + index.type() + "][" + index.id() + "] exists [" + docsWithId + "] times in index");
                }
            }
        }
        return true;
    }

    private void updateDocs(final Term uid, final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        if (softDeleteEnabled) {
            if (docs.size() > 1) {
                indexWriter.softUpdateDocuments(uid, docs, softDeleteField);
            } else {
                indexWriter.softUpdateDocument(uid, docs.get(0), softDeleteField);
            }
        } else {
            if (docs.size() > 1) {
                indexWriter.updateDocuments(uid, docs);
            } else {
                indexWriter.updateDocument(uid, docs.get(0));
            }
        }
        numDocUpdates.inc(docs.size());
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        versionMap.enforceSafeAccess();
        assert Objects.equals(delete.uid().field(), IdFieldMapper.NAME) : delete.uid().field();
        assert assertIncomingSequenceNumber(delete.origin(), delete.seqNo());
        final DeleteResult deleteResult;
        try (ReleasableLock ignored = readLock.acquire();
            Releasable ignored2 = versionMap.acquireLock(delete.uid().bytes())) {
            ensureOpen();
            lastWriteNanos = delete.startTime();
            final DeletionStrategy plan = deletionStrategyForOperation(delete);
            if (plan.earlyResultOnPreflightError.isPresent()) {
                deleteResult = plan.earlyResultOnPreflightError.get();
            } else if (plan.deleteFromLucene || plan.addStaleOpToLucene) {
                deleteResult = deleteInLucene(delete, plan);
            } else {
                deleteResult = new DeleteResult(plan.versionOfDeletion, getPrimaryTerm(), plan.seqNoOfDeletion, plan.currentlyDeleted == false);
            }
            if (delete.origin() != Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
                final Translog.Location location;
                if (deleteResult.getResultType() == Result.Type.SUCCESS) {
                    location = translog.add(new Translog.Delete(delete, deleteResult));
                } else if (deleteResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    location = translog.add(new Translog.NoOp(deleteResult.getSeqNo(), delete.primaryTerm(), deleteResult.getFailure().getMessage()));
                } else {
                    location = null;
                }
                deleteResult.setTranslogLocation(location);
            }
            if (deleteResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                localCheckpointTracker.markSeqNoAsCompleted(deleteResult.getSeqNo());
            }
            deleteResult.setTook(System.nanoTime() - delete.startTime());
            deleteResult.freeze();
        } catch (RuntimeException | IOException e) {
            try {
                maybeFailEngine("index", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw e;
        }
        maybePruneDeletes();
        return deleteResult;
    }

    protected DeletionStrategy deletionStrategyForOperation(final Delete delete) throws IOException {
        if (delete.origin() == Operation.Origin.PRIMARY) {
            return planDeletionAsPrimary(delete);
        } else {
            return planDeletionAsNonPrimary(delete);
        }
    }

    protected final DeletionStrategy planDeletionAsNonPrimary(Delete delete) throws IOException {
        assertNonPrimaryOrigin(delete);
        maxSeqNoOfNonAppendOnlyOperations.updateAndGet(curr -> Math.max(delete.seqNo(), curr));
        assert maxSeqNoOfNonAppendOnlyOperations.get() >= delete.seqNo() : "max_seqno of non-append-only was not updated;" + "max_seqno non-append-only [" + maxSeqNoOfNonAppendOnlyOperations.get() + "], seqno of delete [" + delete.seqNo() + "]";
        final DeletionStrategy plan;
        if (delete.seqNo() <= localCheckpointTracker.getCheckpoint()) {
            plan = DeletionStrategy.processButSkipLucene(false, delete.seqNo(), delete.version());
        } else {
            final OpVsLuceneDocStatus opVsLucene = compareOpToLuceneDocBasedOnSeqNo(delete);
            if (opVsLucene == OpVsLuceneDocStatus.OP_STALE_OR_EQUAL) {
                plan = DeletionStrategy.processAsStaleOp(softDeleteEnabled, false, delete.seqNo(), delete.version());
            } else {
                plan = DeletionStrategy.processNormally(opVsLucene == OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND, delete.seqNo(), delete.version());
            }
        }
        return plan;
    }

    protected boolean assertNonPrimaryOrigin(final Operation operation) {
        assert operation.origin() != Operation.Origin.PRIMARY : "planing as primary but got " + operation.origin();
        return true;
    }

    protected final DeletionStrategy planDeletionAsPrimary(Delete delete) throws IOException {
        assert delete.origin() == Operation.Origin.PRIMARY : "planing as primary but got " + delete.origin();
        final VersionValue versionValue = resolveDocVersion(delete);
        assert incrementVersionLookup();
        final long currentVersion;
        final boolean currentlyDeleted;
        if (versionValue == null) {
            currentVersion = Versions.NOT_FOUND;
            currentlyDeleted = true;
        } else {
            currentVersion = versionValue.version;
            currentlyDeleted = versionValue.isDelete();
        }
        final DeletionStrategy plan;
        if (delete.versionType().isVersionConflictForWrites(currentVersion, delete.version(), currentlyDeleted)) {
            final VersionConflictEngineException e = new VersionConflictEngineException(shardId, delete, currentVersion, currentlyDeleted);
            plan = DeletionStrategy.skipDueToVersionConflict(e, currentVersion, getPrimaryTerm(), currentlyDeleted);
        } else {
            plan = DeletionStrategy.processNormally(currentlyDeleted, generateSeqNoForOperation(delete), delete.versionType().updateVersion(currentVersion, delete.version()));
        }
        return plan;
    }

    private DeleteResult deleteInLucene(Delete delete, DeletionStrategy plan) throws IOException {
        try {
            if (softDeleteEnabled) {
                final ParsedDocument tombstone = engineConfig.getTombstoneDocSupplier().newDeleteTombstoneDoc(delete.type(), delete.id());
                assert tombstone.docs().size() == 1 : "Tombstone doc should have single doc [" + tombstone + "]";
                tombstone.updateSeqID(plan.seqNoOfDeletion, delete.primaryTerm());
                tombstone.version().setLongValue(plan.versionOfDeletion);
                final ParseContext.Document doc = tombstone.docs().get(0);
                assert doc.getField(SeqNoFieldMapper.TOMBSTONE_NAME) != null : "Delete tombstone document but _tombstone field is not set [" + doc + " ]";
                doc.add(softDeleteField);
                if (plan.addStaleOpToLucene || plan.currentlyDeleted) {
                    indexWriter.addDocument(doc);
                } else {
                    indexWriter.softUpdateDocument(delete.uid(), doc, softDeleteField);
                }
            } else if (plan.currentlyDeleted == false) {
                indexWriter.deleteDocuments(delete.uid());
            }
            if (plan.deleteFromLucene) {
                numDocDeletes.inc();
                versionMap.putDeleteUnderLock(delete.uid().bytes(), new DeleteVersionValue(plan.versionOfDeletion, plan.seqNoOfDeletion, delete.primaryTerm(), engineConfig.getThreadPool().relativeTimeInMillis()));
            }
            return new DeleteResult(plan.versionOfDeletion, getPrimaryTerm(), plan.seqNoOfDeletion, plan.currentlyDeleted == false);
        } catch (Exception ex) {
            if (indexWriter.getTragicException() == null) {
                return new DeleteResult(ex, plan.versionOfDeletion, getPrimaryTerm(), plan.seqNoOfDeletion, plan.currentlyDeleted == false);
            } else {
                throw ex;
            }
        }
    }

    protected static final class DeletionStrategy {

        final boolean deleteFromLucene;

        final boolean addStaleOpToLucene;

        final boolean currentlyDeleted;

        final long seqNoOfDeletion;

        final long versionOfDeletion;

        final Optional<DeleteResult> earlyResultOnPreflightError;

        private DeletionStrategy(boolean deleteFromLucene, boolean addStaleOpToLucene, boolean currentlyDeleted, long seqNoOfDeletion, long versionOfDeletion, DeleteResult earlyResultOnPreflightError) {
            assert (deleteFromLucene && earlyResultOnPreflightError != null) == false : "can only delete from lucene or have a preflight result but not both." + "deleteFromLucene: " + deleteFromLucene + "  earlyResultOnPreFlightError:" + earlyResultOnPreflightError;
            this.deleteFromLucene = deleteFromLucene;
            this.addStaleOpToLucene = addStaleOpToLucene;
            this.currentlyDeleted = currentlyDeleted;
            this.seqNoOfDeletion = seqNoOfDeletion;
            this.versionOfDeletion = versionOfDeletion;
            this.earlyResultOnPreflightError = earlyResultOnPreflightError == null ? Optional.empty() : Optional.of(earlyResultOnPreflightError);
        }

        static DeletionStrategy skipDueToVersionConflict(VersionConflictEngineException e, long currentVersion, long term, boolean currentlyDeleted) {
            final long unassignedSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            final DeleteResult deleteResult = new DeleteResult(e, currentVersion, term, unassignedSeqNo, currentlyDeleted == false);
            return new DeletionStrategy(false, false, currentlyDeleted, unassignedSeqNo, Versions.NOT_FOUND, deleteResult);
        }

        static DeletionStrategy processNormally(boolean currentlyDeleted, long seqNoOfDeletion, long versionOfDeletion) {
            return new DeletionStrategy(true, false, currentlyDeleted, seqNoOfDeletion, versionOfDeletion, null);
        }

        public static DeletionStrategy processButSkipLucene(boolean currentlyDeleted, long seqNoOfDeletion, long versionOfDeletion) {
            return new DeletionStrategy(false, false, currentlyDeleted, seqNoOfDeletion, versionOfDeletion, null);
        }

        static DeletionStrategy processAsStaleOp(boolean addStaleOpToLucene, boolean currentlyDeleted, long seqNoOfDeletion, long versionOfDeletion) {
            return new DeletionStrategy(false, addStaleOpToLucene, currentlyDeleted, seqNoOfDeletion, versionOfDeletion, null);
        }
    }

    @Override
    public void maybePruneDeletes() {
        if (engineConfig.isEnableGcDeletes() && engineConfig.getThreadPool().relativeTimeInMillis() - lastDeleteVersionPruneTimeMSec > getGcDeletesInMillis() * 0.25) {
            pruneDeletedTombstones();
        }
    }

    @Override
    public NoOpResult noOp(final NoOp noOp) {
        NoOpResult noOpResult;
        try (ReleasableLock ignored = readLock.acquire()) {
            noOpResult = innerNoOp(noOp);
        } catch (final Exception e) {
            noOpResult = new NoOpResult(getPrimaryTerm(), noOp.seqNo(), e);
        }
        return noOpResult;
    }

    private NoOpResult innerNoOp(final NoOp noOp) throws IOException {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread();
        assert noOp.seqNo() > SequenceNumbers.NO_OPS_PERFORMED;
        final long seqNo = noOp.seqNo();
        try {
            Exception failure = null;
            if (softDeleteEnabled) {
                try {
                    final ParsedDocument tombstone = engineConfig.getTombstoneDocSupplier().newNoopTombstoneDoc(noOp.reason());
                    tombstone.updateSeqID(noOp.seqNo(), noOp.primaryTerm());
                    tombstone.version().setLongValue(1L);
                    assert tombstone.docs().size() == 1 : "Tombstone should have a single doc [" + tombstone + "]";
                    final ParseContext.Document doc = tombstone.docs().get(0);
                    assert doc.getField(SeqNoFieldMapper.TOMBSTONE_NAME) != null : "Noop tombstone document but _tombstone field is not set [" + doc + " ]";
                    doc.add(softDeleteField);
                    indexWriter.addDocument(doc);
                } catch (Exception ex) {
                    if (maybeFailEngine("noop", ex)) {
                        throw ex;
                    }
                    failure = ex;
                }
            }
            final NoOpResult noOpResult = failure != null ? new NoOpResult(getPrimaryTerm(), noOp.seqNo(), failure) : new NoOpResult(getPrimaryTerm(), noOp.seqNo());
            if (noOp.origin() != Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
                final Translog.Location location = translog.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
                noOpResult.setTranslogLocation(location);
            }
            noOpResult.setTook(System.nanoTime() - noOp.startTime());
            noOpResult.freeze();
            return noOpResult;
        } finally {
            if (seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                localCheckpointTracker.markSeqNoAsCompleted(seqNo);
            }
        }
    }

    @Override
    public void refresh(String source) throws EngineException {
        refresh(source, SearcherScope.EXTERNAL);
    }

    final void refresh(String source, SearcherScope scope) throws EngineException {
        final long bytes = indexWriter.ramBytesUsed() + versionMap.ramBytesUsedForRefresh();
        writingBytes.addAndGet(bytes);
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (store.tryIncRef()) {
                try {
                    switch(scope) {
                        case EXTERNAL:
                            externalSearcherManager.maybeRefreshBlocking();
                            break;
                        case INTERNAL:
                            internalSearcherManager.maybeRefreshBlocking();
                            break;
                        default:
                            throw new IllegalArgumentException("unknown scope: " + scope);
                    }
                } finally {
                    store.decRef();
                }
            }
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("refresh failed source[" + source + "]", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new RefreshFailedEngineException(shardId, e);
        } finally {
            writingBytes.addAndGet(-bytes);
        }
        maybePruneDeletes();
        mergeScheduler.refreshConfig();
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {
        refresh("write indexing buffer", SearcherScope.INTERNAL);
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
            refresh("sync_flush", SearcherScope.INTERNAL);
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
            long translogGenOfLastCommit = Long.parseLong(lastCommittedSegmentInfos.userData.get(Translog.TRANSLOG_GENERATION_KEY));
            if (syncId != null && indexWriter.hasUncommittedChanges() && translog.totalOperationsByMinGen(translogGenOfLastCommit) == 0) {
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
            refresh("renew sync commit", SearcherScope.INTERNAL);
        }
        return renewed;
    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        ensureOpen();
        final long translogGenerationOfLastCommit = Long.parseLong(lastCommittedSegmentInfos.userData.get(Translog.TRANSLOG_GENERATION_KEY));
        final long flushThreshold = config().getIndexSettings().getFlushThresholdSize().getBytes();
        if (translog.sizeInBytesByMinGen(translogGenerationOfLastCommit) < flushThreshold) {
            return false;
        }
        final long translogGenerationOfNewCommit = translog.getMinGenerationForSeqNo(localCheckpointTracker.getCheckpoint() + 1).translogFileGeneration;
        return translogGenerationOfLastCommit < translogGenerationOfNewCommit || localCheckpointTracker.getCheckpoint() == localCheckpointTracker.getMaxSeqNo();
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
                    return new CommitId(lastCommittedSegmentInfos.getId());
                }
            } else {
                logger.trace("acquired flush lock immediately");
            }
            try {
                if (indexWriter.hasUncommittedChanges() || force || shouldPeriodicallyFlush()) {
                    ensureCanFlush();
                    try {
                        translog.rollGeneration();
                        logger.trace("starting commit for flush; commitTranslog=true");
                        commitIndexWriter(indexWriter, translog, null);
                        logger.trace("finished commit for flush");
                        refresh("version_table_flush", SearcherScope.INTERNAL);
                        translog.trimUnreferencedReaders();
                    } catch (AlreadyClosedException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new FlushFailedEngineException(shardId, e);
                    }
                    refreshLastCommittedSegmentInfos();
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

    private void refreshLastCommittedSegmentInfos() {
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
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            translog.rollGeneration();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to roll translog", e);
        }
    }

    @Override
    public void trimUnreferencedTranslogFiles() throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            translog.trimUnreferencedReaders();
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to trim translog", e);
        }
    }

    @Override
    public boolean shouldRollTranslogGeneration() {
        return getTranslog().shouldRollGeneration();
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            translog.trimOperations(belowTerm, aboveSeqNo);
        } catch (AlreadyClosedException e) {
            failOnTragicEvent(e);
            throw e;
        } catch (Exception e) {
            try {
                failEngine("translog operations trimming failed", e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new EngineException(shardId, "failed to trim translog operations", e);
        }
    }

    private void pruneDeletedTombstones() {
        final long timeMSec = engineConfig.getThreadPool().relativeTimeInMillis();
        final long maxTimestampToPrune = timeMSec - engineConfig.getIndexSettings().getGcDeletesInMillis();
        versionMap.pruneTombstones(maxTimestampToPrune, localCheckpointTracker.getCheckpoint());
        lastDeleteVersionPruneTimeMSec = timeMSec;
    }

    void clearDeletedTombstones() {
        versionMap.pruneTombstones(Long.MAX_VALUE, localCheckpointTracker.getMaxSeqNo());
    }

    final Collection<DeleteVersionValue> getDeletedTombstones() {
        return versionMap.getAllTombstones().values();
    }

    @Override
    public void forceMerge(final boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, final boolean upgrade, final boolean upgradeOnlyAncientSegments) throws EngineException, IOException {
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
        } catch (AlreadyClosedException ex) {
            ensureOpen(ex);
            failOnTragicEvent(ex);
            throw ex;
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
    public IndexCommitRef acquireLastIndexCommit(final boolean flushFirst) throws EngineException {
        if (flushFirst) {
            logger.trace("start flush for snapshot");
            flush(false, true);
            logger.trace("finish flush for snapshot");
        }
        final IndexCommit lastCommit = combinedDeletionPolicy.acquireIndexCommit(false);
        return new Engine.IndexCommitRef(lastCommit, () -> releaseIndexCommit(lastCommit));
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        final IndexCommit safeCommit = combinedDeletionPolicy.acquireIndexCommit(true);
        return new Engine.IndexCommitRef(safeCommit, () -> releaseIndexCommit(safeCommit));
    }

    private void releaseIndexCommit(IndexCommit snapshot) throws IOException {
        if (combinedDeletionPolicy.releaseCommit(snapshot)) {
            ensureOpen();
            indexWriter.deleteUnusedFiles();
        }
    }

    private boolean failOnTragicEvent(AlreadyClosedException ex) {
        final boolean engineFailed;
        if (indexWriter.isOpen() == false && indexWriter.getTragicException() != null) {
            final Exception tragicException;
            if (indexWriter.getTragicException() instanceof Exception) {
                tragicException = (Exception) indexWriter.getTragicException();
            } else {
                tragicException = new RuntimeException(indexWriter.getTragicException());
            }
            failEngine("already closed by tragic event on the index writer", tragicException);
            engineFailed = true;
        } else if (translog.isOpen() == false && translog.getTragicException() != null) {
            failEngine("already closed by tragic event on the translog", translog.getTragicException());
            engineFailed = true;
        } else if (failedEngine.get() == null && isClosed.get() == false) {
            throw new AssertionError("Unexpected AlreadyClosedException", ex);
        } else {
            engineFailed = false;
        }
        return engineFailed;
    }

    @Override
    protected boolean maybeFailEngine(String source, Exception e) {
        boolean shouldFail = super.maybeFailEngine(source, e);
        if (shouldFail) {
            return true;
        }
        if (e instanceof AlreadyClosedException) {
            return failOnTragicEvent((AlreadyClosedException) e);
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
        stats.updateMaxUnsafeAutoIdTimestamp(maxUnsafeAutoIdTimestamp.get());
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
    protected final void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread() : "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                this.versionMap.clear();
                if (internalSearcherManager != null) {
                    internalSearcherManager.removeListener(versionMap);
                }
                try {
                    IOUtils.close(externalSearcherManager, internalSearcherManager);
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
                } catch (AlreadyClosedException ex) {
                    failOnTragicEvent(ex);
                    throw ex;
                }
                logger.trace("rollback indexWriter done");
            } catch (Exception e) {
                logger.warn("failed to rollback writer on close", e);
            } finally {
                try {
                    store.decRef();
                    logger.debug("engine closed [{}]", reason);
                } finally {
                    closedLatch.countDown();
                }
            }
        }
    }

    @Override
    public Searcher acquireSearcher(String source, SearcherScope scope) {
        store.incRef();
        Releasable releasable = store::decRef;
        try {
            final ReferenceManager<IndexSearcher> referenceManager;
            switch(scope) {
                case INTERNAL:
                    referenceManager = internalSearcherManager;
                    break;
                case EXTERNAL:
                    referenceManager = externalSearcherManager;
                    break;
                default:
                    throw new IllegalStateException("unknown scope: " + scope);
            }
            EngineSearcher engineSearcher = new EngineSearcher(source, referenceManager, store, logger);
            releasable = null;
            return engineSearcher;
        } catch (AlreadyClosedException ex) {
            throw ex;
        } catch (Exception ex) {
            ensureOpen(ex);
            logger.error(() -> new ParameterizedMessage("failed to acquire searcher, source {}", source), ex);
            throw new EngineException(shardId, "failed to acquire searcher, source " + source, ex);
        } finally {
            Releasables.close(releasable);
        }
    }

    private long loadCurrentVersionFromIndex(Term uid) throws IOException {
        assert incrementIndexVersionLookup();
        try (Searcher searcher = acquireSearcher("load_version", SearcherScope.INTERNAL)) {
            return VersionsAndSeqNoResolver.loadVersion(searcher.reader(), uid);
        }
    }

    private IndexWriter createWriter() throws IOException {
        try {
            final IndexWriterConfig iwc = getIndexWriterConfig();
            return createWriter(store.directory(), iwc);
        } catch (LockObtainFailedException ex) {
            logger.warn("could not lock IndexWriter", ex);
            throw ex;
        }
    }

    IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
        if (Assertions.ENABLED) {
            return new AssertingIndexWriter(directory, iwc);
        } else {
            return new IndexWriter(directory, iwc);
        }
    }

    private IndexWriterConfig getIndexWriterConfig() {
        final IndexWriterConfig iwc = new IndexWriterConfig(engineConfig.getAnalyzer());
        iwc.setCommitOnClose(false);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        iwc.setIndexDeletionPolicy(combinedDeletionPolicy);
        boolean verbose = false;
        try {
            verbose = Boolean.parseBoolean(System.getProperty("tests.verbose"));
        } catch (Exception ignore) {
        }
        iwc.setInfoStream(verbose ? InfoStream.getDefault() : new LoggerInfoStream(logger));
        iwc.setMergeScheduler(mergeScheduler);
        MergePolicy mergePolicy = config().getMergePolicy();
        if (softDeleteEnabled) {
            iwc.setSoftDeletesField(Lucene.SOFT_DELETE_FIELD);
            mergePolicy = new RecoverySourcePruneMergePolicy(SourceFieldMapper.RECOVERY_SOURCE_NAME, softDeletesPolicy::getRetentionQuery, new SoftDeletesRetentionMergePolicy(Lucene.SOFT_DELETE_FIELD, softDeletesPolicy::getRetentionQuery, mergePolicy));
        }
        iwc.setMergePolicy(new ElasticsearchMergePolicy(mergePolicy));
        iwc.setSimilarity(engineConfig.getSimilarity());
        iwc.setRAMBufferSizeMB(engineConfig.getIndexingBufferSize().getMbFrac());
        iwc.setCodec(engineConfig.getCodec());
        iwc.setUseCompoundFile(true);
        if (config().getIndexSort() != null) {
            iwc.setIndexSort(config().getIndexSort());
        }
        return iwc;
    }

    static final class SearchFactory extends EngineSearcherFactory {

        private final Engine.Warmer warmer;

        private final Logger logger;

        private final AtomicBoolean isEngineClosed;

        SearchFactory(Logger logger, AtomicBoolean isEngineClosed, EngineConfig engineConfig) {
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

    @Override
    public boolean isThrottled() {
        return throttle.isThrottled();
    }

    @Override
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
            engineConfig.getThreadPool().generic().execute(new AbstractRunnable() {

                @Override
                public void onFailure(Exception e) {
                    logger.debug("merge failure action rejected", e);
                }

                @Override
                protected void doRun() throws Exception {
                    failEngine("merge failed", new MergePolicy.MergeException(exc, dir));
                }
            });
        }
    }

    protected void commitIndexWriter(final IndexWriter writer, final Translog translog, @Nullable final String syncId) throws IOException {
        ensureCanFlush();
        try {
            final long localCheckpoint = localCheckpointTracker.getCheckpoint();
            final Translog.TranslogGeneration translogGeneration = translog.getMinGenerationForSeqNo(localCheckpoint + 1);
            final String translogFileGeneration = Long.toString(translogGeneration.translogFileGeneration);
            final String translogUUID = translogGeneration.translogUUID;
            final String localCheckpointValue = Long.toString(localCheckpoint);
            writer.setLiveCommitData(() -> {
                final Map<String, String> commitData = new HashMap<>(6);
                commitData.put(Translog.TRANSLOG_GENERATION_KEY, translogFileGeneration);
                commitData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
                commitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, localCheckpointValue);
                if (syncId != null) {
                    commitData.put(Engine.SYNC_COMMIT_ID, syncId);
                }
                commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(localCheckpointTracker.getMaxSeqNo()));
                commitData.put(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp.get()));
                commitData.put(HISTORY_UUID_KEY, historyUUID);
                if (softDeleteEnabled) {
                    commitData.put(Engine.MIN_RETAINED_SEQNO, Long.toString(softDeletesPolicy.getMinRetainedSeqNo()));
                }
                logger.trace("committing writer with commit data [{}]", commitData);
                return commitData.entrySet().iterator();
            });
            writer.commit();
        } catch (final Exception ex) {
            try {
                failEngine("lucene commit failed", ex);
            } catch (final Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        } catch (final AssertionError e) {
            if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                final EngineException engineException = new EngineException(shardId, "failed to commit engine", e);
                try {
                    failEngine("lucene commit failed", engineException);
                } catch (final Exception inner) {
                    engineException.addSuppressed(inner);
                }
                throw engineException;
            } else {
                throw e;
            }
        }
    }

    private void ensureCanFlush() {
        if (pendingTranslogRecovery.get()) {
            throw new IllegalStateException(shardId.toString() + " flushes are disabled - pending translog recovery");
        }
    }

    public void onSettingsChanged() {
        mergeScheduler.refreshConfig();
        maybePruneDeletes();
        if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
            this.maxUnsafeAutoIdTimestamp.set(Long.MAX_VALUE);
        }
        final TranslogDeletionPolicy translogDeletionPolicy = translog.getDeletionPolicy();
        final IndexSettings indexSettings = engineConfig.getIndexSettings();
        translogDeletionPolicy.setRetentionAgeInMillis(indexSettings.getTranslogRetentionAge().getMillis());
        translogDeletionPolicy.setRetentionSizeInBytes(indexSettings.getTranslogRetentionSize().getBytes());
        softDeletesPolicy.setRetentionOperations(indexSettings.getSoftDeleteRetentionOperations());
    }

    public MergeStats getMergeStats() {
        return mergeScheduler.stats();
    }

    LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return getTranslog().getLastSyncedGlobalCheckpoint();
    }

    @Override
    public long getLocalCheckpoint() {
        return localCheckpointTracker.getCheckpoint();
    }

    @Override
    public void waitForOpsToComplete(long seqNo) throws InterruptedException {
        localCheckpointTracker.waitForOpsToComplete(seqNo);
    }

    @Override
    public void resetLocalCheckpoint(long localCheckpoint) {
        localCheckpointTracker.resetCheckpoint(localCheckpoint);
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return localCheckpointTracker.getStats(globalCheckpoint);
    }

    long getNumIndexVersionsLookups() {
        return numIndexVersionsLookups.count();
    }

    long getNumVersionLookups() {
        return numVersionLookups.count();
    }

    private boolean incrementVersionLookup() {
        numVersionLookups.inc();
        return true;
    }

    private boolean incrementIndexVersionLookup() {
        numIndexVersionsLookups.inc();
        return true;
    }

    int getVersionMapSize() {
        return versionMap.getAllCurrent().size();
    }

    boolean isSafeAccessRequired() {
        return versionMap.isSafeAccessRequired();
    }

    long getNumDocDeletes() {
        return numDocDeletes.count();
    }

    long getNumDocAppends() {
        return numDocAppends.count();
    }

    long getNumDocUpdates() {
        return numDocUpdates.count();
    }

    @Override
    public Translog.Snapshot newLuceneChangesSnapshot(String source, MapperService mapperService, long minSeqNo, long maxSeqNo, boolean requiredFullRange) throws IOException {
        ensureOpen();
        if (lastRefreshedCheckpoint() < maxSeqNo) {
            refresh(source, SearcherScope.INTERNAL);
        }
        Searcher searcher = acquireSearcher(source, SearcherScope.INTERNAL);
        try {
            LuceneChangesSnapshot snapshot = new LuceneChangesSnapshot(searcher, mapperService, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE, minSeqNo, maxSeqNo, requiredFullRange);
            searcher = null;
            return snapshot;
        } finally {
            IOUtils.close(searcher);
        }
    }

    @Override
    public boolean hasCompleteOperationHistory(String source, MapperService mapperService, long startingSeqNo) throws IOException {
        if (engineConfig.getIndexSettings().isSoftDeleteEnabled()) {
            return getMinRetainedSeqNo() <= startingSeqNo;
        } else {
            final long currentLocalCheckpoint = getLocalCheckpointTracker().getCheckpoint();
            final LocalCheckpointTracker tracker = new LocalCheckpointTracker(startingSeqNo, startingSeqNo - 1);
            try (Translog.Snapshot snapshot = getTranslog().newSnapshotFromMinSeqNo(startingSeqNo)) {
                Translog.Operation operation;
                while ((operation = snapshot.next()) != null) {
                    if (operation.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        tracker.markSeqNoAsCompleted(operation.seqNo());
                    }
                }
            }
            return tracker.getCheckpoint() >= currentLocalCheckpoint;
        }
    }

    final long getMinRetainedSeqNo() {
        assert softDeleteEnabled : Thread.currentThread().getName();
        return softDeletesPolicy.getMinRetainedSeqNo();
    }

    @Override
    public Closeable acquireRetentionLockForPeerRecovery() {
        final Closeable translogLock = translog.acquireRetentionLock();
        final Releasable softDeletesLock = softDeletesPolicy.acquireRetentionLock();
        return () -> IOUtils.close(translogLock, softDeletesLock);
    }

    @Override
    public boolean isRecovering() {
        return pendingTranslogRecovery.get();
    }

    private static Map<String, String> commitDataAsMap(final IndexWriter indexWriter) {
        Map<String, String> commitData = new HashMap<>(6);
        for (Map.Entry<String, String> entry : indexWriter.getLiveCommitData()) {
            commitData.put(entry.getKey(), entry.getValue());
        }
        return commitData;
    }

    private final class AssertingIndexWriter extends IndexWriter {

        AssertingIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
            super(d, conf);
        }

        @Override
        public long updateDocument(Term term, Iterable<? extends IndexableField> doc) throws IOException {
            assert softDeleteEnabled == false : "Call #updateDocument but soft-deletes is enabled";
            return super.updateDocument(term, doc);
        }

        @Override
        public long updateDocuments(Term delTerm, Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
            assert softDeleteEnabled == false : "Call #updateDocuments but soft-deletes is enabled";
            return super.updateDocuments(delTerm, docs);
        }

        @Override
        public long deleteDocuments(Term... terms) throws IOException {
            assert softDeleteEnabled == false : "Call #deleteDocuments but soft-deletes is enabled";
            return super.deleteDocuments(terms);
        }

        @Override
        public long softUpdateDocument(Term term, Iterable<? extends IndexableField> doc, Field... softDeletes) throws IOException {
            assert softDeleteEnabled : "Call #softUpdateDocument but soft-deletes is disabled";
            return super.softUpdateDocument(term, doc, softDeletes);
        }

        @Override
        public long softUpdateDocuments(Term term, Iterable<? extends Iterable<? extends IndexableField>> docs, Field... softDeletes) throws IOException {
            assert softDeleteEnabled : "Call #softUpdateDocuments but soft-deletes is disabled";
            return super.softUpdateDocuments(term, docs, softDeletes);
        }
    }

    final long lastRefreshedCheckpoint() {
        return lastRefreshedCheckpointListener.refreshedCheckpoint.get();
    }

    private final class LastRefreshedCheckpointListener implements ReferenceManager.RefreshListener {

        final AtomicLong refreshedCheckpoint;

        private long pendingCheckpoint;

        LastRefreshedCheckpointListener(long initialLocalCheckpoint) {
            this.refreshedCheckpoint = new AtomicLong(initialLocalCheckpoint);
        }

        @Override
        public void beforeRefresh() {
            pendingCheckpoint = localCheckpointTracker.getCheckpoint();
        }

        @Override
        public void afterRefresh(boolean didRefresh) {
            if (didRefresh) {
                refreshedCheckpoint.set(pendingCheckpoint);
            }
        }
    }
}