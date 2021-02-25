package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SnapshotDeletionPolicy;
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
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.LoggerInfoStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndSeqNo;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.ElasticsearchMergePolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.IOException;
import java.util.Arrays;
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

public class InternalEngine extends Engine {

    private volatile long lastDeleteVersionPruneTimeMSec;

    private final Translog translog;

    private final ElasticsearchConcurrentMergeScheduler mergeScheduler;

    private final IndexWriter indexWriter;

    private final SearcherManager externalSearcherManager;

    private final SearcherManager internalSearcherManager;

    private final Lock flushLock = new ReentrantLock();

    private final ReentrantLock optimizeLock = new ReentrantLock();

    private final LiveVersionMap versionMap = new LiveVersionMap();

    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();

    private volatile SegmentInfos lastCommittedSegmentInfos;

    private final IndexThrottle throttle;

    private final SequenceNumbersService seqNoService;

    private final String uidField;

    private final CombinedDeletionPolicy deletionPolicy;

    private final AtomicInteger throttleRequestCount = new AtomicInteger();

    private final EngineConfig.OpenMode openMode;

    private final AtomicBoolean pendingTranslogRecovery = new AtomicBoolean(false);

    public static final String MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID = "max_unsafe_auto_id_timestamp";

    private final AtomicLong maxUnsafeAutoIdTimestamp = new AtomicLong(-1);

    private final CounterMetric numVersionLookups = new CounterMetric();

    private final CounterMetric numIndexVersionsLookups = new CounterMetric();

    private final AtomicLong writingBytes = new AtomicLong();

    @Nullable
    private final String historyUUID;

    public InternalEngine(EngineConfig engineConfig) {
        this(engineConfig, InternalEngine::sequenceNumberService);
    }

    InternalEngine(final EngineConfig engineConfig, final BiFunction<EngineConfig, SeqNoStats, SequenceNumbersService> seqNoServiceSupplier) {
        super(engineConfig);
        openMode = engineConfig.getOpenMode();
        if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
            maxUnsafeAutoIdTimestamp.set(Long.MAX_VALUE);
        }
        this.uidField = engineConfig.getIndexSettings().isSingleType() ? IdFieldMapper.NAME : UidFieldMapper.NAME;
        final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(), engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis());
        this.deletionPolicy = new CombinedDeletionPolicy(new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy()), translogDeletionPolicy, openMode);
        store.incRef();
        IndexWriter writer = null;
        Translog translog = null;
        SearcherManager externalSearcherManager = null;
        SearcherManager internalSearcherManager = null;
        EngineMergeScheduler scheduler = null;
        boolean success = false;
        try {
            this.lastDeleteVersionPruneTimeMSec = engineConfig.getThreadPool().relativeTimeInMillis();
            mergeScheduler = scheduler = new EngineMergeScheduler(engineConfig.getShardId(), engineConfig.getIndexSettings());
            throttle = new IndexThrottle();
            try {
                final SeqNoStats seqNoStats;
                switch(openMode) {
                    case OPEN_INDEX_AND_TRANSLOG:
                        writer = createWriter(false);
                        final long globalCheckpoint = Translog.readGlobalCheckpoint(engineConfig.getTranslogConfig().getTranslogPath());
                        seqNoStats = store.loadSeqNoStats(globalCheckpoint);
                        break;
                    case OPEN_INDEX_CREATE_TRANSLOG:
                        writer = createWriter(false);
                        seqNoStats = store.loadSeqNoStats(SequenceNumbers.UNASSIGNED_SEQ_NO);
                        break;
                    case CREATE_INDEX_AND_TRANSLOG:
                        writer = createWriter(true);
                        seqNoStats = new SeqNoStats(SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.UNASSIGNED_SEQ_NO);
                        break;
                    default:
                        throw new IllegalArgumentException(openMode.toString());
                }
                logger.trace("recovered [{}]", seqNoStats);
                seqNoService = seqNoServiceSupplier.apply(engineConfig, seqNoStats);
                updateMaxUnsafeAutoIdTimestampFromWriter(writer);
                historyUUID = loadOrGenerateHistoryUUID(writer, engineConfig.getForceNewHistoryUUID());
                Objects.requireNonNull(historyUUID, "history uuid should not be null");
                indexWriter = writer;
                translog = openTranslog(engineConfig, writer, translogDeletionPolicy, () -> seqNoService.getGlobalCheckpoint());
                assert translog.getGeneration() != null;
                this.translog = translog;
                updateWriterOnOpen();
            } catch (IOException | TranslogCorruptedException e) {
                throw new EngineCreationFailureException(shardId, "failed to create engine", e);
            } catch (AssertionError e) {
                if (ExceptionsHelper.stackTrace(e).contains("org.apache.lucene.index.IndexWriter.filesExist")) {
                    throw new EngineCreationFailureException(shardId, "failed to create engine", e);
                } else {
                    throw e;
                }
            }
            internalSearcherManager = createSearcherManager(new SearcherFactory(), false);
            externalSearcherManager = createSearcherManager(new SearchFactory(logger, isClosed, engineConfig), true);
            this.internalSearcherManager = internalSearcherManager;
            this.externalSearcherManager = externalSearcherManager;
            internalSearcherManager.addListener(versionMap);
            assert pendingTranslogRecovery.get() == false : "translog recovery can't be pending before we set it";
            pendingTranslogRecovery.set(openMode == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG);
            for (ReferenceManager.RefreshListener listener : engineConfig.getRefreshListeners()) {
                this.externalSearcherManager.addListener(listener);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(writer, translog, externalSearcherManager, internalSearcherManager, scheduler);
                if (isClosed.get() == false) {
                    store.decRef();
                }
            }
        }
        logger.trace("created new InternalEngine");
    }

    @Override
    public void restoreLocalCheckpointFromTranslog() throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            ensureOpen();
            final long localCheckpoint = seqNoService.getLocalCheckpoint();
            try (Translog.Snapshot snapshot = getTranslog().newSnapshotFrom(localCheckpoint + 1)) {
                Translog.Operation operation;
                while ((operation = snapshot.next()) != null) {
                    if (operation.seqNo() > localCheckpoint) {
                        seqNoService.markSeqNoAsCompleted(operation.seqNo());
                    }
                }
            }
        }
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            ensureOpen();
            final long localCheckpoint = seqNoService.getLocalCheckpoint();
            final long maxSeqNo = seqNoService.getMaxSeqNo();
            int numNoOpsAdded = 0;
            for (long seqNo = localCheckpoint + 1; seqNo <= maxSeqNo; seqNo = seqNoService.getLocalCheckpoint() + 1) {
                innerNoOp(new NoOp(seqNo, primaryTerm, Operation.Origin.PRIMARY, System.nanoTime(), "filling gaps"));
                numNoOpsAdded++;
                assert seqNo <= seqNoService.getLocalCheckpoint() : "local checkpoint did not advance; was [" + seqNo + "], now [" + seqNoService.getLocalCheckpoint() + "]";
            }
            return numNoOpsAdded;
        }
    }

    private void updateMaxUnsafeAutoIdTimestampFromWriter(IndexWriter writer) {
        long commitMaxUnsafeAutoIdTimestamp = Long.MIN_VALUE;
        for (Map.Entry<String, String> entry : writer.getLiveCommitData()) {
            if (entry.getKey().equals(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID)) {
                commitMaxUnsafeAutoIdTimestamp = Long.parseLong(entry.getValue());
                break;
            }
        }
        maxUnsafeAutoIdTimestamp.set(Math.max(maxUnsafeAutoIdTimestamp.get(), commitMaxUnsafeAutoIdTimestamp));
    }

    static SequenceNumbersService sequenceNumberService(final EngineConfig engineConfig, final SeqNoStats seqNoStats) {
        return new SequenceNumbersService(engineConfig.getShardId(), engineConfig.getAllocationId(), engineConfig.getIndexSettings(), seqNoStats.getMaxSeqNo(), seqNoStats.getLocalCheckpoint(), seqNoStats.getGlobalCheckpoint());
    }

    @Override
    public InternalEngine recoverFromTranslog() throws IOException {
        flushLock.lock();
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            if (openMode != EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
                throw new IllegalStateException("Can't recover from translog with open mode: " + openMode);
            }
            if (pendingTranslogRecovery.get() == false) {
                throw new IllegalStateException("Engine has already been recovered");
            }
            try {
                recoverFromTranslogInternal();
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

    private void recoverFromTranslogInternal() throws IOException {
        Translog.TranslogGeneration translogGeneration = translog.getGeneration();
        final int opsRecovered;
        final long translogGen = Long.parseLong(lastCommittedSegmentInfos.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
        try (Translog.Snapshot snapshot = translog.newSnapshotFromGen(translogGen)) {
            opsRecovered = config().getTranslogRecoveryRunner().run(this, snapshot);
        } catch (Exception e) {
            throw new EngineException(shardId, "failed to recover from translog", e);
        }
        assert pendingTranslogRecovery.get() : "translogRecovery is not pending but should be";
        pendingTranslogRecovery.set(false);
        if (opsRecovered > 0) {
            logger.trace("flushing post recovery from translog. ops recovered [{}]. committed translog id [{}]. current id [{}]", opsRecovered, translogGeneration == null ? null : translogGeneration.translogFileGeneration, translog.currentFileGeneration());
            flush(true, true);
            refresh("translog_recovery");
        } else if (translog.isCurrent(translogGeneration) == false) {
            commitIndexWriter(indexWriter, translog, lastCommittedSegmentInfos.getUserData().get(Engine.SYNC_COMMIT_ID));
            refreshLastCommittedSegmentInfos();
        } else if (lastCommittedSegmentInfos.getUserData().containsKey(HISTORY_UUID_KEY) == false) {
            assert historyUUID != null;
            commitIndexWriter(indexWriter, translog, lastCommittedSegmentInfos.getUserData().get(Engine.SYNC_COMMIT_ID));
            refreshLastCommittedSegmentInfos();
        }
        translog.trimUnreferencedReaders();
    }

    private Translog openTranslog(EngineConfig engineConfig, IndexWriter writer, TranslogDeletionPolicy translogDeletionPolicy, LongSupplier globalCheckpointSupplier) throws IOException {
        assert openMode != null;
        final TranslogConfig translogConfig = engineConfig.getTranslogConfig();
        String translogUUID = null;
        if (openMode == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
            translogUUID = loadTranslogUUIDFromCommit(writer);
            if (translogUUID == null) {
                throw new IndexFormatTooOldException("translog", "translog has no generation nor a UUID - this might be an index from a previous version consider upgrading to N-1 first");
            }
        }
        return new Translog(translogConfig, translogUUID, translogDeletionPolicy, globalCheckpointSupplier);
    }

    private void updateWriterOnOpen() throws IOException {
        Objects.requireNonNull(historyUUID);
        final Map<String, String> commitUserData = commitDataAsMap(indexWriter);
        boolean needsCommit = false;
        if (historyUUID.equals(commitUserData.get(HISTORY_UUID_KEY)) == false) {
            needsCommit = true;
        } else {
            assert config().getForceNewHistoryUUID() == false : "config forced a new history uuid but it didn't change";
            assert openMode != EngineConfig.OpenMode.CREATE_INDEX_AND_TRANSLOG : "new index but it already has an existing history uuid";
        }
        if (translog.getTranslogUUID().equals(commitUserData.get(Translog.TRANSLOG_UUID_KEY)) == false) {
            needsCommit = true;
        } else {
            assert openMode == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG : "translog uuid didn't change but open mode is " + openMode;
        }
        if (needsCommit) {
            commitIndexWriter(indexWriter, translog, openMode == EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG ? commitUserData.get(SYNC_COMMIT_ID) : null);
        }
    }

    @Override
    public Translog getTranslog() {
        ensureOpen();
        return translog;
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
    private String loadTranslogUUIDFromCommit(IndexWriter writer) throws IOException {
        final Map<String, String> commitUserData = commitDataAsMap(writer);
        if (commitUserData.containsKey(Translog.TRANSLOG_UUID_KEY)) {
            if (commitUserData.containsKey(Translog.TRANSLOG_GENERATION_KEY) == false) {
                throw new IllegalStateException("commit doesn't contain translog generation id");
            }
            return commitUserData.get(Translog.TRANSLOG_UUID_KEY);
        } else {
            return null;
        }
    }

    private String loadOrGenerateHistoryUUID(final IndexWriter writer, boolean forceNew) throws IOException {
        String uuid = commitDataAsMap(writer).get(HISTORY_UUID_KEY);
        if (uuid == null || forceNew) {
            assert forceNew || openMode == EngineConfig.OpenMode.CREATE_INDEX_AND_TRANSLOG || config().getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_rc1) : "existing index was created after 6_0_0_rc1 but has no history uuid";
            uuid = UUIDs.randomBase64UUID();
        }
        return uuid;
    }

    private SearcherManager createSearcherManager(SearcherFactory searcherFactory, boolean readSegmentsInfo) throws EngineException {
        boolean success = false;
        SearcherManager searcherManager = null;
        try {
            try {
                final DirectoryReader directoryReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(indexWriter), shardId);
                searcherManager = new SearcherManager(directoryReader, searcherFactory);
                if (readSegmentsInfo) {
                    lastCommittedSegmentInfos = readLastCommittedSegmentInfos(searcherManager, store);
                }
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
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        assert Objects.equals(get.uid().field(), uidField) : get.uid().field();
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            SearcherScope scope;
            if (get.realtime()) {
                VersionValue versionValue = versionMap.getUnderLock(get.uid());
                if (versionValue != null) {
                    if (versionValue.isDelete()) {
                        return GetResult.NOT_EXISTS;
                    }
                    if (get.versionType().isVersionConflictForReads(versionValue.version, get.version())) {
                        throw new VersionConflictEngineException(shardId, get.type(), get.id(), get.versionType().explainConflictForReads(versionValue.version, get.version()));
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
        final VersionValue versionValue = versionMap.getUnderLock(op.uid());
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
        VersionValue versionValue = versionMap.getUnderLock(op.uid());
        if (versionValue == null) {
            assert incrementIndexVersionLookup();
            final long currentVersion = loadCurrentVersionFromIndex(op.uid());
            if (currentVersion != Versions.NOT_FOUND) {
                versionValue = new VersionValue(currentVersion, SequenceNumbers.UNASSIGNED_SEQ_NO, 0L);
            }
        } else if (engineConfig.isEnableGcDeletes() && versionValue.isDelete() && (engineConfig.getThreadPool().relativeTimeInMillis() - ((DeleteVersionValue) versionValue).time) > getGcDeletesInMillis()) {
            versionValue = null;
        }
        return versionValue;
    }

    private OpVsLuceneDocStatus compareOpToLuceneDocBasedOnVersions(final Operation op) throws IOException {
        assert op.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO : "op is resolved based on versions but have a seq#";
        assert op.version() >= 0 : "versions should be non-negative. got " + op.version();
        final VersionValue versionValue = resolveDocVersion(op);
        if (versionValue == null) {
            return OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND;
        } else {
            return op.versionType().isVersionConflictForWrites(versionValue.version, op.version(), versionValue.isDelete()) ? OpVsLuceneDocStatus.OP_STALE_OR_EQUAL : OpVsLuceneDocStatus.OP_NEWER;
        }
    }

    private boolean canOptimizeAddDocument(Index index) {
        if (index.getAutoGeneratedIdTimestamp() != IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP) {
            assert index.getAutoGeneratedIdTimestamp() >= 0 : "autoGeneratedIdTimestamp must be positive but was: " + index.getAutoGeneratedIdTimestamp();
            switch(index.origin()) {
                case PRIMARY:
                    assert (index.version() == Versions.MATCH_ANY && index.versionType() == VersionType.INTERNAL) : "version: " + index.version() + " type: " + index.versionType();
                    return true;
                case PEER_RECOVERY:
                case REPLICA:
                    assert index.version() == 1 && index.versionType() == VersionType.EXTERNAL : "version: " + index.version() + " type: " + index.versionType();
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

    private boolean assertVersionType(final Engine.Operation operation) {
        if (operation.origin() == Operation.Origin.REPLICA || operation.origin() == Operation.Origin.PEER_RECOVERY || operation.origin() == Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
            assert operation.versionType() == operation.versionType().versionTypeForReplicationAndRecovery() : "unexpected version type in request from [" + operation.origin().name() + "] " + "found [" + operation.versionType().name() + "] " + "expected [" + operation.versionType().versionTypeForReplicationAndRecovery().name() + "]";
        }
        return true;
    }

    private boolean assertIncomingSequenceNumber(final Engine.Operation.Origin origin, final long seqNo) {
        if (engineConfig.getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_alpha1) && origin == Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
            assert seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : "old op recovering but it already has a seq no.;" + " index version: " + engineConfig.getIndexSettings().getIndexVersionCreated() + ", seqNo: " + seqNo;
        } else if (origin == Operation.Origin.PRIMARY) {
            assert assertOriginPrimarySequenceNumber(seqNo);
        } else if (engineConfig.getIndexSettings().getIndexVersionCreated().onOrAfter(Version.V_6_0_0_alpha1)) {
            assert seqNo >= 0 : "recovery or replica ops should have an assigned seq no.; origin: " + origin;
        }
        return true;
    }

    protected boolean assertOriginPrimarySequenceNumber(final long seqNo) {
        assert seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : "primary operations must never have an assigned sequence number but was [" + seqNo + "]";
        return true;
    }

    private boolean assertSequenceNumberBeforeIndexing(final Engine.Operation.Origin origin, final long seqNo) {
        if (engineConfig.getIndexSettings().getIndexVersionCreated().onOrAfter(Version.V_6_0_0_alpha1) || origin == Operation.Origin.PRIMARY) {
            assert seqNo >= 0 : "ops should have an assigned seq no.; origin: " + origin;
        }
        return true;
    }

    private long generateSeqNoForOperation(final Operation operation) {
        assert operation.origin() == Operation.Origin.PRIMARY;
        return doGenerateSeqNoForOperation(operation);
    }

    protected long doGenerateSeqNoForOperation(final Operation operation) {
        return seqNoService.generateSeqNo();
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        assert Objects.equals(index.uid().field(), uidField) : index.uid().field();
        final boolean doThrottle = index.origin().isRecovery() == false;
        try (ReleasableLock releasableLock = readLock.acquire()) {
            ensureOpen();
            assert assertIncomingSequenceNumber(index.origin(), index.seqNo());
            assert assertVersionType(index);
            try (Releasable ignored = acquireLock(index.uid());
                Releasable indexThrottle = doThrottle ? () -> {
                } : throttle.acquireThrottle()) {
                lastWriteNanos = index.startTime();
                final IndexingStrategy plan;
                if (index.origin() == Operation.Origin.PRIMARY) {
                    plan = planIndexingAsPrimary(index);
                } else {
                    plan = planIndexingAsNonPrimary(index);
                }
                final IndexResult indexResult;
                if (plan.earlyResultOnPreFlightError.isPresent()) {
                    indexResult = plan.earlyResultOnPreFlightError.get();
                    assert indexResult.hasFailure();
                } else if (plan.indexIntoLucene) {
                    indexResult = indexIntoLucene(index, plan);
                } else {
                    indexResult = new IndexResult(plan.versionForIndexing, plan.seqNoForIndexing, plan.currentNotFoundOrDeleted);
                }
                if (index.origin() != Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
                    final Translog.Location location;
                    if (indexResult.hasFailure() == false) {
                        location = translog.add(new Translog.Index(index, indexResult));
                    } else if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                        location = translog.add(new Translog.NoOp(indexResult.getSeqNo(), index.primaryTerm(), indexResult.getFailure().getMessage()));
                    } else {
                        location = null;
                    }
                    indexResult.setTranslogLocation(location);
                }
                if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    seqNoService.markSeqNoAsCompleted(indexResult.getSeqNo());
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

    private IndexingStrategy planIndexingAsNonPrimary(Index index) throws IOException {
        final IndexingStrategy plan;
        if (canOptimizeAddDocument(index) && mayHaveBeenIndexedBefore(index) == false) {
            assert index.version() == 1L : "can optimize on replicas but incoming version is [" + index.version() + "]";
            plan = IndexingStrategy.optimizedAppendOnly(index.seqNo());
        } else {
            assert index.versionType().versionTypeForReplicationAndRecovery() == index.versionType() : "resolving out of order delivery based on versioning but version type isn't fit for it. got [" + index.versionType() + "]";
            final OpVsLuceneDocStatus opVsLucene;
            if (index.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                assert config().getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_alpha1) : "index is newly created but op has no sequence numbers. op: " + index;
                opVsLucene = compareOpToLuceneDocBasedOnVersions(index);
            } else if (index.seqNo() <= seqNoService.getLocalCheckpoint()) {
                opVsLucene = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
            } else {
                opVsLucene = compareOpToLuceneDocBasedOnSeqNo(index);
            }
            if (opVsLucene == OpVsLuceneDocStatus.OP_STALE_OR_EQUAL) {
                plan = IndexingStrategy.processButSkipLucene(false, index.seqNo(), index.version());
            } else {
                plan = IndexingStrategy.processNormally(opVsLucene == OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND, index.seqNo(), index.version());
            }
        }
        return plan;
    }

    private IndexingStrategy planIndexingAsPrimary(Index index) throws IOException {
        assert index.origin() == Operation.Origin.PRIMARY : "planing as primary but origin isn't. got " + index.origin();
        final IndexingStrategy plan;
        if (canOptimizeAddDocument(index)) {
            if (mayHaveBeenIndexedBefore(index)) {
                plan = IndexingStrategy.overrideExistingAsIfNotThere(generateSeqNoForOperation(index), 1L);
            } else {
                plan = IndexingStrategy.optimizedAppendOnly(generateSeqNoForOperation(index));
            }
        } else {
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
                plan = IndexingStrategy.skipDueToVersionConflict(e, currentNotFoundOrDeleted, currentVersion);
            } else {
                plan = IndexingStrategy.processNormally(currentNotFoundOrDeleted, generateSeqNoForOperation(index), index.versionType().updateVersion(currentVersion, index.version()));
            }
        }
        return plan;
    }

    private IndexResult indexIntoLucene(Index index, IndexingStrategy plan) throws IOException {
        assert assertSequenceNumberBeforeIndexing(index.origin(), plan.seqNoForIndexing);
        assert plan.versionForIndexing >= 0 : "version must be set. got " + plan.versionForIndexing;
        assert plan.indexIntoLucene;
        index.parsedDoc().updateSeqID(plan.seqNoForIndexing, index.primaryTerm());
        index.parsedDoc().version().setLongValue(plan.versionForIndexing);
        try {
            if (plan.useLuceneUpdateDocument) {
                update(index.uid(), index.docs(), indexWriter);
            } else {
                assert assertDocDoesNotExist(index, canOptimizeAddDocument(index) == false);
                index(index.docs(), indexWriter);
            }
            versionMap.putUnderLock(index.uid().bytes(), new VersionValue(plan.versionForIndexing, plan.seqNoForIndexing, index.primaryTerm()));
            return new IndexResult(plan.versionForIndexing, plan.seqNoForIndexing, plan.currentNotFoundOrDeleted);
        } catch (Exception ex) {
            if (indexWriter.getTragicException() == null) {
                return new IndexResult(ex, Versions.MATCH_ANY, plan.seqNoForIndexing);
            } else {
                throw ex;
            }
        }
    }

    private boolean mayHaveBeenIndexedBefore(Index index) {
        assert canOptimizeAddDocument(index);
        boolean mayHaveBeenIndexBefore;
        long deOptimizeTimestamp = maxUnsafeAutoIdTimestamp.get();
        if (index.isRetry()) {
            mayHaveBeenIndexBefore = true;
            do {
                deOptimizeTimestamp = maxUnsafeAutoIdTimestamp.get();
                if (deOptimizeTimestamp >= index.getAutoGeneratedIdTimestamp()) {
                    break;
                }
            } while (maxUnsafeAutoIdTimestamp.compareAndSet(deOptimizeTimestamp, index.getAutoGeneratedIdTimestamp()) == false);
            assert maxUnsafeAutoIdTimestamp.get() >= index.getAutoGeneratedIdTimestamp();
        } else {
            mayHaveBeenIndexBefore = deOptimizeTimestamp >= index.getAutoGeneratedIdTimestamp();
        }
        return mayHaveBeenIndexBefore;
    }

    private static void index(final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        if (docs.size() > 1) {
            indexWriter.addDocuments(docs);
        } else {
            indexWriter.addDocument(docs.get(0));
        }
    }

    private static final class IndexingStrategy {

        final boolean currentNotFoundOrDeleted;

        final boolean useLuceneUpdateDocument;

        final long seqNoForIndexing;

        final long versionForIndexing;

        final boolean indexIntoLucene;

        final Optional<IndexResult> earlyResultOnPreFlightError;

        private IndexingStrategy(boolean currentNotFoundOrDeleted, boolean useLuceneUpdateDocument, boolean indexIntoLucene, long seqNoForIndexing, long versionForIndexing, IndexResult earlyResultOnPreFlightError) {
            assert useLuceneUpdateDocument == false || indexIntoLucene : "use lucene update is set to true, but we're not indexing into lucene";
            assert (indexIntoLucene && earlyResultOnPreFlightError != null) == false : "can only index into lucene or have a preflight result but not both." + "indexIntoLucene: " + indexIntoLucene + "  earlyResultOnPreFlightError:" + earlyResultOnPreFlightError;
            this.currentNotFoundOrDeleted = currentNotFoundOrDeleted;
            this.useLuceneUpdateDocument = useLuceneUpdateDocument;
            this.seqNoForIndexing = seqNoForIndexing;
            this.versionForIndexing = versionForIndexing;
            this.indexIntoLucene = indexIntoLucene;
            this.earlyResultOnPreFlightError = earlyResultOnPreFlightError == null ? Optional.empty() : Optional.of(earlyResultOnPreFlightError);
        }

        static IndexingStrategy optimizedAppendOnly(long seqNoForIndexing) {
            return new IndexingStrategy(true, false, true, seqNoForIndexing, 1, null);
        }

        static IndexingStrategy skipDueToVersionConflict(VersionConflictEngineException e, boolean currentNotFoundOrDeleted, long currentVersion) {
            final IndexResult result = new IndexResult(e, currentVersion);
            return new IndexingStrategy(currentNotFoundOrDeleted, false, false, SequenceNumbers.UNASSIGNED_SEQ_NO, Versions.NOT_FOUND, result);
        }

        static IndexingStrategy processNormally(boolean currentNotFoundOrDeleted, long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(currentNotFoundOrDeleted, currentNotFoundOrDeleted == false, true, seqNoForIndexing, versionForIndexing, null);
        }

        static IndexingStrategy overrideExistingAsIfNotThere(long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(true, true, true, seqNoForIndexing, versionForIndexing, null);
        }

        static IndexingStrategy processButSkipLucene(boolean currentNotFoundOrDeleted, long seqNoForIndexing, long versionForIndexing) {
            return new IndexingStrategy(currentNotFoundOrDeleted, false, false, seqNoForIndexing, versionForIndexing, null);
        }
    }

    private boolean assertDocDoesNotExist(final Index index, final boolean allowDeleted) throws IOException {
        final VersionValue versionValue = versionMap.getUnderLock(index.uid());
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

    private static void update(final Term uid, final List<ParseContext.Document> docs, final IndexWriter indexWriter) throws IOException {
        if (docs.size() > 1) {
            indexWriter.updateDocuments(uid, docs);
        } else {
            indexWriter.updateDocument(uid, docs.get(0));
        }
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        assert Objects.equals(delete.uid().field(), uidField) : delete.uid().field();
        assert assertVersionType(delete);
        assert assertIncomingSequenceNumber(delete.origin(), delete.seqNo());
        final DeleteResult deleteResult;
        try (ReleasableLock ignored = readLock.acquire();
            Releasable ignored2 = acquireLock(delete.uid())) {
            ensureOpen();
            lastWriteNanos = delete.startTime();
            final DeletionStrategy plan;
            if (delete.origin() == Operation.Origin.PRIMARY) {
                plan = planDeletionAsPrimary(delete);
            } else {
                plan = planDeletionAsNonPrimary(delete);
            }
            if (plan.earlyResultOnPreflightError.isPresent()) {
                deleteResult = plan.earlyResultOnPreflightError.get();
            } else if (plan.deleteFromLucene) {
                deleteResult = deleteInLucene(delete, plan);
            } else {
                deleteResult = new DeleteResult(plan.versionOfDeletion, plan.seqNoOfDeletion, plan.currentlyDeleted == false);
            }
            if (delete.origin() != Operation.Origin.LOCAL_TRANSLOG_RECOVERY) {
                final Translog.Location location;
                if (deleteResult.hasFailure() == false) {
                    location = translog.add(new Translog.Delete(delete, deleteResult));
                } else if (deleteResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    location = translog.add(new Translog.NoOp(deleteResult.getSeqNo(), delete.primaryTerm(), deleteResult.getFailure().getMessage()));
                } else {
                    location = null;
                }
                deleteResult.setTranslogLocation(location);
            }
            if (deleteResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                seqNoService.markSeqNoAsCompleted(deleteResult.getSeqNo());
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
        maybePruneDeletedTombstones();
        return deleteResult;
    }

    private DeletionStrategy planDeletionAsNonPrimary(Delete delete) throws IOException {
        assert delete.origin() != Operation.Origin.PRIMARY : "planing as primary but got " + delete.origin();
        assert delete.versionType().versionTypeForReplicationAndRecovery() == delete.versionType() : "resolving out of order delivery based on versioning but version type isn't fit for it. got [" + delete.versionType() + "]";
        final OpVsLuceneDocStatus opVsLucene;
        if (delete.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            assert config().getIndexSettings().getIndexVersionCreated().before(Version.V_6_0_0_alpha1) : "index is newly created but op has no sequence numbers. op: " + delete;
            opVsLucene = compareOpToLuceneDocBasedOnVersions(delete);
        } else if (delete.seqNo() <= seqNoService.getLocalCheckpoint()) {
            opVsLucene = OpVsLuceneDocStatus.OP_STALE_OR_EQUAL;
        } else {
            opVsLucene = compareOpToLuceneDocBasedOnSeqNo(delete);
        }
        final DeletionStrategy plan;
        if (opVsLucene == OpVsLuceneDocStatus.OP_STALE_OR_EQUAL) {
            plan = DeletionStrategy.processButSkipLucene(false, delete.seqNo(), delete.version());
        } else {
            plan = DeletionStrategy.processNormally(opVsLucene == OpVsLuceneDocStatus.LUCENE_DOC_NOT_FOUND, delete.seqNo(), delete.version());
        }
        return plan;
    }

    private DeletionStrategy planDeletionAsPrimary(Delete delete) throws IOException {
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
            plan = DeletionStrategy.skipDueToVersionConflict(e, currentVersion, currentlyDeleted);
        } else {
            plan = DeletionStrategy.processNormally(currentlyDeleted, generateSeqNoForOperation(delete), delete.versionType().updateVersion(currentVersion, delete.version()));
        }
        return plan;
    }

    private DeleteResult deleteInLucene(Delete delete, DeletionStrategy plan) throws IOException {
        try {
            if (plan.currentlyDeleted == false) {
                indexWriter.deleteDocuments(delete.uid());
            }
            versionMap.putUnderLock(delete.uid().bytes(), new DeleteVersionValue(plan.versionOfDeletion, plan.seqNoOfDeletion, delete.primaryTerm(), engineConfig.getThreadPool().relativeTimeInMillis()));
            return new DeleteResult(plan.versionOfDeletion, plan.seqNoOfDeletion, plan.currentlyDeleted == false);
        } catch (Exception ex) {
            if (indexWriter.getTragicException() == null) {
                return new DeleteResult(ex, plan.versionOfDeletion, plan.seqNoOfDeletion, plan.currentlyDeleted == false);
            } else {
                throw ex;
            }
        }
    }

    private static final class DeletionStrategy {

        final boolean deleteFromLucene;

        final boolean currentlyDeleted;

        final long seqNoOfDeletion;

        final long versionOfDeletion;

        final Optional<DeleteResult> earlyResultOnPreflightError;

        private DeletionStrategy(boolean deleteFromLucene, boolean currentlyDeleted, long seqNoOfDeletion, long versionOfDeletion, DeleteResult earlyResultOnPreflightError) {
            assert (deleteFromLucene && earlyResultOnPreflightError != null) == false : "can only delete from lucene or have a preflight result but not both." + "deleteFromLucene: " + deleteFromLucene + "  earlyResultOnPreFlightError:" + earlyResultOnPreflightError;
            this.deleteFromLucene = deleteFromLucene;
            this.currentlyDeleted = currentlyDeleted;
            this.seqNoOfDeletion = seqNoOfDeletion;
            this.versionOfDeletion = versionOfDeletion;
            this.earlyResultOnPreflightError = earlyResultOnPreflightError == null ? Optional.empty() : Optional.of(earlyResultOnPreflightError);
        }

        static DeletionStrategy skipDueToVersionConflict(VersionConflictEngineException e, long currentVersion, boolean currentlyDeleted) {
            final long unassignedSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            final DeleteResult deleteResult = new DeleteResult(e, currentVersion, unassignedSeqNo, currentlyDeleted == false);
            return new DeletionStrategy(false, currentlyDeleted, unassignedSeqNo, Versions.NOT_FOUND, deleteResult);
        }

        static DeletionStrategy processNormally(boolean currentlyDeleted, long seqNoOfDeletion, long versionOfDeletion) {
            return new DeletionStrategy(true, currentlyDeleted, seqNoOfDeletion, versionOfDeletion, null);
        }

        public static DeletionStrategy processButSkipLucene(boolean currentlyDeleted, long seqNoOfDeletion, long versionOfDeletion) {
            return new DeletionStrategy(false, currentlyDeleted, seqNoOfDeletion, versionOfDeletion, null);
        }
    }

    private void maybePruneDeletedTombstones() {
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
            noOpResult = new NoOpResult(noOp.seqNo(), e);
        }
        return noOpResult;
    }

    private NoOpResult innerNoOp(final NoOp noOp) throws IOException {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread();
        assert noOp.seqNo() > SequenceNumbers.NO_OPS_PERFORMED;
        final long seqNo = noOp.seqNo();
        try {
            final NoOpResult noOpResult = new NoOpResult(noOp.seqNo());
            final Translog.Location location = translog.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
            noOpResult.setTranslogLocation(location);
            noOpResult.setTook(System.nanoTime() - noOp.startTime());
            noOpResult.freeze();
            return noOpResult;
        } finally {
            if (seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                seqNoService.markSeqNoAsCompleted(seqNo);
            }
        }
    }

    @Override
    public void refresh(String source) throws EngineException {
        refresh(source, SearcherScope.EXTERNAL);
    }

    final void refresh(String source, SearcherScope scope) throws EngineException {
        long bytes = 0;
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            bytes = indexWriter.ramBytesUsed();
            switch(scope) {
                case EXTERNAL:
                    writingBytes.addAndGet(bytes);
                    externalSearcherManager.maybeRefreshBlocking();
                    break;
                case INTERNAL:
                    final long versionMapBytes = versionMap.ramBytesUsedForRefresh();
                    bytes += versionMapBytes;
                    writingBytes.addAndGet(bytes);
                    internalSearcherManager.maybeRefreshBlocking();
                    break;
                default:
                    throw new IllegalArgumentException("unknown scope: " + scope);
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
        maybePruneDeletedTombstones();
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
            if (syncId != null && translog.uncommittedOperations() == 0 && indexWriter.hasUncommittedChanges()) {
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
                if (indexWriter.hasUncommittedChanges() || force) {
                    ensureCanFlush();
                    try {
                        translog.rollGeneration();
                        logger.trace("starting commit for flush; commitTranslog=true");
                        commitIndexWriter(indexWriter, translog, null);
                        logger.trace("finished commit for flush");
                        refresh("version_table_flush", SearcherScope.INTERNAL);
                        translog.trimUnreferencedReaders();
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
    public void trimTranslog() throws EngineException {
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

    private void pruneDeletedTombstones() {
        long timeMSec = engineConfig.getThreadPool().relativeTimeInMillis();
        for (Map.Entry<BytesRef, DeleteVersionValue> entry : versionMap.getAllTombstones()) {
            BytesRef uid = entry.getKey();
            try (Releasable ignored = acquireLock(uid)) {
                DeleteVersionValue versionValue = versionMap.getTombstoneUnderLock(uid);
                if (versionValue != null) {
                    if (timeMSec - versionValue.time > getGcDeletesInMillis()) {
                        versionMap.removeTombstoneUnderLock(uid);
                    }
                }
            }
        }
        lastDeleteVersionPruneTimeMSec = timeMSec;
    }

    void clearDeletedTombstones() {
        versionMap.clearTombstones();
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
            ensureOpen();
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
    public IndexCommitRef acquireIndexCommit(final boolean flushFirst) throws EngineException {
        if (flushFirst) {
            logger.trace("start flush for snapshot");
            flush(false, true);
            logger.trace("finish flush for snapshot");
        }
        try (ReleasableLock lock = readLock.acquire()) {
            logger.trace("pulling snapshot");
            return new IndexCommitRef(deletionPolicy.getIndexDeletionPolicy());
        } catch (IOException e) {
            throw new SnapshotFailedEngineException(shardId, e);
        }
    }

    @SuppressWarnings("finally")
    private boolean failOnTragicEvent(AlreadyClosedException ex) {
        final boolean engineFailed;
        if (indexWriter.isOpen() == false && indexWriter.getTragicException() != null) {
            if (indexWriter.getTragicException() instanceof Error) {
                try {
                    logger.error("tragic event in index writer", ex);
                } finally {
                    throw (Error) indexWriter.getTragicException();
                }
            } else {
                failEngine("already closed by tragic event on the index writer", (Exception) indexWriter.getTragicException());
                engineFailed = true;
            }
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
    protected SearcherManager getSearcherManager(String source, SearcherScope scope) {
        switch(scope) {
            case INTERNAL:
                return internalSearcherManager;
            case EXTERNAL:
                return externalSearcherManager;
            default:
                throw new IllegalStateException("unknown scope: " + scope);
        }
    }

    private Releasable acquireLock(BytesRef uid) {
        return keyedLock.acquire(uid);
    }

    private Releasable acquireLock(Term uid) {
        return acquireLock(uid.bytes());
    }

    private long loadCurrentVersionFromIndex(Term uid) throws IOException {
        assert incrementIndexVersionLookup();
        try (Searcher searcher = acquireSearcher("load_version", SearcherScope.INTERNAL)) {
            return VersionsAndSeqNoResolver.loadVersion(searcher.reader(), uid);
        }
    }

    private IndexWriter createWriter(boolean create) throws IOException {
        try {
            final IndexWriterConfig iwc = getIndexWriterConfig(create);
            return createWriter(store.directory(), iwc);
        } catch (LockObtainFailedException ex) {
            logger.warn("could not lock IndexWriter", ex);
            throw ex;
        }
    }

    IndexWriter createWriter(Directory directory, IndexWriterConfig iwc) throws IOException {
        return new IndexWriter(directory, iwc);
    }

    private IndexWriterConfig getIndexWriterConfig(boolean create) {
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

    protected void commitIndexWriter(final IndexWriter writer, final Translog translog, @Nullable final String syncId) throws IOException {
        ensureCanFlush();
        try {
            final long localCheckpoint = seqNoService.getLocalCheckpoint();
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
                commitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(seqNoService.getMaxSeqNo()));
                commitData.put(MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp.get()));
                commitData.put(HISTORY_UUID_KEY, historyUUID);
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
        maybePruneDeletedTombstones();
        if (engineConfig.isAutoGeneratedIDsOptimizationEnabled() == false) {
            this.maxUnsafeAutoIdTimestamp.set(Long.MAX_VALUE);
        }
        final TranslogDeletionPolicy translogDeletionPolicy = translog.getDeletionPolicy();
        final IndexSettings indexSettings = engineConfig.getIndexSettings();
        translogDeletionPolicy.setRetentionAgeInMillis(indexSettings.getTranslogRetentionAge().getMillis());
        translogDeletionPolicy.setRetentionSizeInBytes(indexSettings.getTranslogRetentionSize().getBytes());
    }

    public MergeStats getMergeStats() {
        return mergeScheduler.stats();
    }

    public final SequenceNumbersService seqNoService() {
        return seqNoService;
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

    boolean indexWriterHasDeletions() {
        return indexWriter.hasDeletions();
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
}