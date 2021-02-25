package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public abstract class Engine implements Closeable {

    public static final String SYNC_COMMIT_ID = "sync_id";

    public static final String HISTORY_UUID_KEY = "history_uuid";

    public static final String MIN_RETAINED_SEQNO = "min_retained_seq_no";

    protected final ShardId shardId;

    protected final String allocationId;

    protected final Logger logger;

    protected final EngineConfig engineConfig;

    protected final Store store;

    protected final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final CountDownLatch closedLatch = new CountDownLatch(1);

    protected final EventListener eventListener;

    protected final ReentrantLock failEngineLock = new ReentrantLock();

    protected final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

    protected final ReleasableLock readLock = new ReleasableLock(rwl.readLock());

    protected final ReleasableLock writeLock = new ReleasableLock(rwl.writeLock());

    protected final SetOnce<Exception> failedEngine = new SetOnce<>();

    protected volatile long lastWriteNanos = System.nanoTime();

    protected Engine(EngineConfig engineConfig) {
        Objects.requireNonNull(engineConfig.getStore(), "Store must be provided to the engine");
        this.engineConfig = engineConfig;
        this.shardId = engineConfig.getShardId();
        this.allocationId = engineConfig.getAllocationId();
        this.store = engineConfig.getStore();
        this.logger = Loggers.getLogger(Engine.class, engineConfig.getIndexSettings().getSettings(), engineConfig.getShardId());
        this.eventListener = engineConfig.getEventListener();
    }

    protected static long guardedRamBytesUsed(Accountable a) {
        if (a == null) {
            return 0;
        }
        return a.ramBytesUsed();
    }

    protected static boolean isMergedSegment(LeafReader reader) {
        final Map<String, String> diagnostics = Lucene.segmentReader(reader).getSegmentInfo().info.getDiagnostics();
        final String source = diagnostics.get(IndexWriter.SOURCE);
        assert Arrays.asList(IndexWriter.SOURCE_ADDINDEXES_READERS, IndexWriter.SOURCE_FLUSH, IndexWriter.SOURCE_MERGE).contains(source) : "Unknown source " + source;
        return IndexWriter.SOURCE_MERGE.equals(source);
    }

    public final EngineConfig config() {
        return engineConfig;
    }

    protected abstract SegmentInfos getLastCommittedSegmentInfos();

    public MergeStats getMergeStats() {
        return new MergeStats();
    }

    public abstract String getHistoryUUID();

    public abstract long getWritingBytes();

    protected static final class IndexThrottle {

        private final CounterMetric throttleTimeMillisMetric = new CounterMetric();

        private volatile long startOfThrottleNS;

        private static final ReleasableLock NOOP_LOCK = new ReleasableLock(new NoOpLock());

        private final ReleasableLock lockReference = new ReleasableLock(new ReentrantLock());

        private volatile ReleasableLock lock = NOOP_LOCK;

        public Releasable acquireThrottle() {
            return lock.acquire();
        }

        public void activate() {
            assert lock == NOOP_LOCK : "throttling activated while already active";
            startOfThrottleNS = System.nanoTime();
            lock = lockReference;
        }

        public void deactivate() {
            assert lock != NOOP_LOCK : "throttling deactivated but not active";
            lock = NOOP_LOCK;
            assert startOfThrottleNS > 0 : "Bad state of startOfThrottleNS";
            long throttleTimeNS = System.nanoTime() - startOfThrottleNS;
            if (throttleTimeNS >= 0) {
                throttleTimeMillisMetric.inc(TimeValue.nsecToMSec(throttleTimeNS));
            }
        }

        long getThrottleTimeInMillis() {
            long currentThrottleNS = 0;
            if (isThrottled() && startOfThrottleNS != 0) {
                currentThrottleNS += System.nanoTime() - startOfThrottleNS;
                if (currentThrottleNS < 0) {
                    currentThrottleNS = 0;
                }
            }
            return throttleTimeMillisMetric.count() + TimeValue.nsecToMSec(currentThrottleNS);
        }

        boolean isThrottled() {
            return lock != NOOP_LOCK;
        }
    }

    public abstract long getIndexThrottleTimeInMillis();

    public abstract boolean isThrottled();

    public abstract void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException;

    protected static final class NoOpLock implements Lock {

        @Override
        public void lock() {
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
        }

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public void unlock() {
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("NoOpLock can't provide a condition");
        }
    }

    public abstract IndexResult index(Index index) throws IOException;

    public abstract DeleteResult delete(Delete delete) throws IOException;

    public abstract NoOpResult noOp(NoOp noOp);

    public abstract static class Result {

        private final Operation.TYPE operationType;

        private final Result.Type resultType;

        private final long version;

        private final long seqNo;

        private final Exception failure;

        private final SetOnce<Boolean> freeze = new SetOnce<>();

        private final Mapping requiredMappingUpdate;

        private Translog.Location translogLocation;

        private long took;

        protected Result(Operation.TYPE operationType, Exception failure, long version, long seqNo) {
            this.operationType = operationType;
            this.failure = Objects.requireNonNull(failure);
            this.version = version;
            this.seqNo = seqNo;
            this.requiredMappingUpdate = null;
            this.resultType = Type.FAILURE;
        }

        protected Result(Operation.TYPE operationType, long version, long seqNo) {
            this.operationType = operationType;
            this.version = version;
            this.seqNo = seqNo;
            this.failure = null;
            this.requiredMappingUpdate = null;
            this.resultType = Type.SUCCESS;
        }

        protected Result(Operation.TYPE operationType, Mapping requiredMappingUpdate) {
            this.operationType = operationType;
            this.version = Versions.NOT_FOUND;
            this.seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            this.failure = null;
            this.requiredMappingUpdate = requiredMappingUpdate;
            this.resultType = Type.MAPPING_UPDATE_REQUIRED;
        }

        public Type getResultType() {
            return resultType;
        }

        public long getVersion() {
            return version;
        }

        public long getSeqNo() {
            return seqNo;
        }

        public Mapping getRequiredMappingUpdate() {
            return requiredMappingUpdate;
        }

        public Translog.Location getTranslogLocation() {
            return translogLocation;
        }

        public Exception getFailure() {
            return failure;
        }

        public long getTook() {
            return took;
        }

        public Operation.TYPE getOperationType() {
            return operationType;
        }

        void setTranslogLocation(Translog.Location translogLocation) {
            if (freeze.get() == null) {
                this.translogLocation = translogLocation;
            } else {
                throw new IllegalStateException("result is already frozen");
            }
        }

        void setTook(long took) {
            if (freeze.get() == null) {
                this.took = took;
            } else {
                throw new IllegalStateException("result is already frozen");
            }
        }

        void freeze() {
            freeze.set(true);
        }

        public enum Type {

            SUCCESS, FAILURE, MAPPING_UPDATE_REQUIRED
        }
    }

    public static class IndexResult extends Result {

        private final boolean created;

        public IndexResult(long version, long seqNo, boolean created) {
            super(Operation.TYPE.INDEX, version, seqNo);
            this.created = created;
        }

        public IndexResult(Exception failure, long version) {
            this(failure, version, SequenceNumbers.UNASSIGNED_SEQ_NO);
        }

        public IndexResult(Exception failure, long version, long seqNo) {
            super(Operation.TYPE.INDEX, failure, version, seqNo);
            this.created = false;
        }

        public IndexResult(Mapping requiredMappingUpdate) {
            super(Operation.TYPE.INDEX, requiredMappingUpdate);
            this.created = false;
        }

        public boolean isCreated() {
            return created;
        }
    }

    public static class DeleteResult extends Result {

        private final boolean found;

        public DeleteResult(long version, long seqNo, boolean found) {
            super(Operation.TYPE.DELETE, version, seqNo);
            this.found = found;
        }

        public DeleteResult(Exception failure, long version) {
            this(failure, version, SequenceNumbers.UNASSIGNED_SEQ_NO, false);
        }

        public DeleteResult(Exception failure, long version, long seqNo, boolean found) {
            super(Operation.TYPE.DELETE, failure, version, seqNo);
            this.found = found;
        }

        public DeleteResult(Mapping requiredMappingUpdate) {
            super(Operation.TYPE.DELETE, requiredMappingUpdate);
            this.found = false;
        }

        public boolean isFound() {
            return found;
        }
    }

    public static class NoOpResult extends Result {

        NoOpResult(long seqNo) {
            super(Operation.TYPE.NO_OP, 0, seqNo);
        }

        NoOpResult(long seqNo, Exception failure) {
            super(Operation.TYPE.NO_OP, failure, 0, seqNo);
        }
    }

    public abstract SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException;

    public enum SyncedFlushResult {

        SUCCESS, COMMIT_MISMATCH, PENDING_OPERATIONS
    }

    protected final GetResult getFromSearcher(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory, SearcherScope scope) throws EngineException {
        final Searcher searcher = searcherFactory.apply("get", scope);
        final DocIdAndVersion docIdAndVersion;
        try {
            docIdAndVersion = VersionsAndSeqNoResolver.loadDocIdAndVersion(searcher.reader(), get.uid());
        } catch (Exception e) {
            Releasables.closeWhileHandlingException(searcher);
            throw new EngineException(shardId, "Couldn't resolve version", e);
        }
        if (docIdAndVersion != null) {
            if (get.versionType().isVersionConflictForReads(docIdAndVersion.version, get.version())) {
                Releasables.close(searcher);
                throw new VersionConflictEngineException(shardId, get.type(), get.id(), get.versionType().explainConflictForReads(docIdAndVersion.version, get.version()));
            }
        }
        if (docIdAndVersion != null) {
            return new GetResult(searcher, docIdAndVersion);
        } else {
            Releasables.close(searcher);
            return GetResult.NOT_EXISTS;
        }
    }

    public abstract GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException;

    public final Searcher acquireSearcher(String source) throws EngineException {
        return acquireSearcher(source, SearcherScope.EXTERNAL);
    }

    public abstract Searcher acquireSearcher(String source, SearcherScope scope) throws EngineException;

    public enum SearcherScope {

        EXTERNAL, INTERNAL
    }

    public abstract boolean isTranslogSyncNeeded();

    public abstract boolean ensureTranslogSynced(Stream<Translog.Location> locations) throws IOException;

    public abstract void syncTranslog() throws IOException;

    public abstract Closeable acquireRetentionLockForPeerRecovery();

    public abstract Translog.Snapshot newSnapshotFromMinSeqNo(long minSeqNo) throws IOException;

    public abstract TranslogStats getTranslogStats();

    public abstract Translog.Location getTranslogLastWriteLocation();

    public abstract Translog.Snapshot newLuceneChangesSnapshot(String source, MapperService mapperService, long minSeqNo, long maxSeqNo, boolean requiredFullRange) throws IOException;

    public abstract Translog.Snapshot readHistoryOperations(String source, MapperService mapperService, long startingSeqNo) throws IOException;

    public abstract int estimateNumberOfHistoryOperations(String source, MapperService mapperService, long startingSeqNo) throws IOException;

    public abstract boolean hasCompleteOperationHistory(String source, MapperService mapperService, long startingSeqNo) throws IOException;

    protected final void ensureOpen(Exception suppressed) {
        if (isClosed.get()) {
            AlreadyClosedException ace = new AlreadyClosedException(shardId + " engine is closed", failedEngine.get());
            if (suppressed != null) {
                ace.addSuppressed(suppressed);
            }
            throw ace;
        }
    }

    protected final void ensureOpen() {
        ensureOpen(null);
    }

    public CommitStats commitStats() {
        return new CommitStats(getLastCommittedSegmentInfos());
    }

    public abstract long getLocalCheckpoint();

    public abstract void waitForOpsToComplete(long seqNo) throws InterruptedException;

    public abstract void resetLocalCheckpoint(long localCheckpoint);

    public abstract SeqNoStats getSeqNoStats(long globalCheckpoint);

    public abstract long getLastSyncedGlobalCheckpoint();

    public final SegmentsStats segmentsStats(boolean includeSegmentFileSizes) {
        ensureOpen();
        Set<String> segmentName = new HashSet<>();
        SegmentsStats stats = new SegmentsStats();
        try (Searcher searcher = acquireSearcher("segments_stats", SearcherScope.INTERNAL)) {
            for (LeafReaderContext ctx : searcher.reader().getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                fillSegmentStats(segmentReader, includeSegmentFileSizes, stats);
                segmentName.add(segmentReader.getSegmentName());
            }
        }
        try (Searcher searcher = acquireSearcher("segments_stats", SearcherScope.EXTERNAL)) {
            for (LeafReaderContext ctx : searcher.reader().getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                if (segmentName.contains(segmentReader.getSegmentName()) == false) {
                    fillSegmentStats(segmentReader, includeSegmentFileSizes, stats);
                }
            }
        }
        writerSegmentStats(stats);
        return stats;
    }

    private void fillSegmentStats(SegmentReader segmentReader, boolean includeSegmentFileSizes, SegmentsStats stats) {
        stats.add(1, segmentReader.ramBytesUsed());
        stats.addTermsMemoryInBytes(guardedRamBytesUsed(segmentReader.getPostingsReader()));
        stats.addStoredFieldsMemoryInBytes(guardedRamBytesUsed(segmentReader.getFieldsReader()));
        stats.addTermVectorsMemoryInBytes(guardedRamBytesUsed(segmentReader.getTermVectorsReader()));
        stats.addNormsMemoryInBytes(guardedRamBytesUsed(segmentReader.getNormsReader()));
        stats.addPointsMemoryInBytes(guardedRamBytesUsed(segmentReader.getPointsReader()));
        stats.addDocValuesMemoryInBytes(guardedRamBytesUsed(segmentReader.getDocValuesReader()));
        if (includeSegmentFileSizes) {
            stats.addFileSizes(getSegmentFileSizes(segmentReader));
        }
    }

    private ImmutableOpenMap<String, Long> getSegmentFileSizes(SegmentReader segmentReader) {
        Directory directory = null;
        SegmentCommitInfo segmentCommitInfo = segmentReader.getSegmentInfo();
        boolean useCompoundFile = segmentCommitInfo.info.getUseCompoundFile();
        if (useCompoundFile) {
            try {
                directory = engineConfig.getCodec().compoundFormat().getCompoundReader(segmentReader.directory(), segmentCommitInfo.info, IOContext.READ);
            } catch (IOException e) {
                logger.warn(() -> new ParameterizedMessage("Error when opening compound reader for Directory [{}] and SegmentCommitInfo [{}]", segmentReader.directory(), segmentCommitInfo), e);
                return ImmutableOpenMap.of();
            }
        } else {
            directory = segmentReader.directory();
        }
        assert directory != null;
        String[] files;
        if (useCompoundFile) {
            try {
                files = directory.listAll();
            } catch (IOException e) {
                final Directory finalDirectory = directory;
                logger.warn(() -> new ParameterizedMessage("Couldn't list Compound Reader Directory [{}]", finalDirectory), e);
                return ImmutableOpenMap.of();
            }
        } else {
            try {
                files = segmentReader.getSegmentInfo().files().toArray(new String[] {});
            } catch (IOException e) {
                logger.warn(() -> new ParameterizedMessage("Couldn't list Directory from SegmentReader [{}] and SegmentInfo [{}]", segmentReader, segmentReader.getSegmentInfo()), e);
                return ImmutableOpenMap.of();
            }
        }
        ImmutableOpenMap.Builder<String, Long> map = ImmutableOpenMap.builder();
        for (String file : files) {
            String extension = IndexFileNames.getExtension(file);
            long length = 0L;
            try {
                length = directory.fileLength(file);
            } catch (NoSuchFileException | FileNotFoundException e) {
                final Directory finalDirectory = directory;
                logger.warn(() -> new ParameterizedMessage("Tried to query fileLength but file is gone [{}] [{}]", finalDirectory, file), e);
            } catch (IOException e) {
                final Directory finalDirectory = directory;
                logger.warn(() -> new ParameterizedMessage("Error when trying to query fileLength [{}] [{}]", finalDirectory, file), e);
            }
            if (length == 0L) {
                continue;
            }
            map.put(extension, length);
        }
        if (useCompoundFile && directory != null) {
            try {
                directory.close();
            } catch (IOException e) {
                final Directory finalDirectory = directory;
                logger.warn(() -> new ParameterizedMessage("Error when closing compound reader on Directory [{}]", finalDirectory), e);
            }
        }
        return map.build();
    }

    protected void writerSegmentStats(SegmentsStats stats) {
        stats.addVersionMapMemoryInBytes(0);
        stats.addIndexWriterMemoryInBytes(0);
    }

    public abstract long getIndexBufferRAMBytesUsed();

    protected Segment[] getSegmentInfo(SegmentInfos lastCommittedSegmentInfos, boolean verbose) {
        ensureOpen();
        Map<String, Segment> segments = new HashMap<>();
        try (Searcher searcher = acquireSearcher("segments", SearcherScope.EXTERNAL)) {
            for (LeafReaderContext ctx : searcher.reader().getContext().leaves()) {
                fillSegmentInfo(Lucene.segmentReader(ctx.reader()), verbose, true, segments);
            }
        }
        try (Searcher searcher = acquireSearcher("segments", SearcherScope.INTERNAL)) {
            for (LeafReaderContext ctx : searcher.reader().getContext().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(ctx.reader());
                if (segments.containsKey(segmentReader.getSegmentName()) == false) {
                    fillSegmentInfo(segmentReader, verbose, false, segments);
                }
            }
        }
        if (lastCommittedSegmentInfos != null) {
            SegmentInfos infos = lastCommittedSegmentInfos;
            for (SegmentCommitInfo info : infos) {
                Segment segment = segments.get(info.info.name);
                if (segment == null) {
                    segment = new Segment(info.info.name);
                    segment.search = false;
                    segment.committed = true;
                    segment.docCount = info.info.maxDoc();
                    segment.delDocCount = info.getDelCount();
                    segment.version = info.info.getVersion();
                    segment.compound = info.info.getUseCompoundFile();
                    try {
                        segment.sizeInBytes = info.sizeInBytes();
                    } catch (IOException e) {
                        logger.trace(() -> new ParameterizedMessage("failed to get size for [{}]", info.info.name), e);
                    }
                    segments.put(info.info.name, segment);
                } else {
                    segment.committed = true;
                }
            }
        }
        Segment[] segmentsArr = segments.values().toArray(new Segment[segments.values().size()]);
        Arrays.sort(segmentsArr, Comparator.comparingLong(Segment::getGeneration));
        return segmentsArr;
    }

    private void fillSegmentInfo(SegmentReader segmentReader, boolean verbose, boolean search, Map<String, Segment> segments) {
        SegmentCommitInfo info = segmentReader.getSegmentInfo();
        assert segments.containsKey(info.info.name) == false;
        Segment segment = new Segment(info.info.name);
        segment.search = search;
        segment.docCount = segmentReader.numDocs();
        segment.delDocCount = segmentReader.numDeletedDocs();
        segment.version = info.info.getVersion();
        segment.compound = info.info.getUseCompoundFile();
        try {
            segment.sizeInBytes = info.sizeInBytes();
        } catch (IOException e) {
            logger.trace(() -> new ParameterizedMessage("failed to get size for [{}]", info.info.name), e);
        }
        segment.memoryInBytes = segmentReader.ramBytesUsed();
        segment.segmentSort = info.info.getIndexSort();
        if (verbose) {
            segment.ramTree = Accountables.namedAccountable("root", segmentReader);
        }
        segment.attributes = info.info.getAttributes();
        segments.put(info.info.name, segment);
    }

    public abstract List<Segment> segments(boolean verbose);

    public final boolean refreshNeeded() {
        if (store.tryIncRef()) {
            try {
                try (Searcher searcher = acquireSearcher("refresh_needed", SearcherScope.EXTERNAL)) {
                    return searcher.getDirectoryReader().isCurrent() == false;
                }
            } catch (IOException e) {
                logger.error("failed to access searcher manager", e);
                failEngine("failed to access searcher manager", e);
                throw new EngineException(shardId, "failed to access searcher manager", e);
            } finally {
                store.decRef();
            }
        }
        return false;
    }

    @Nullable
    public abstract void refresh(String source) throws EngineException;

    public abstract void writeIndexingBuffer() throws EngineException;

    public abstract boolean shouldPeriodicallyFlush();

    public abstract CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException;

    public abstract CommitId flush() throws EngineException;

    public abstract void trimUnreferencedTranslogFiles() throws EngineException;

    public abstract boolean shouldRollTranslogGeneration();

    public abstract void rollTranslogGeneration() throws EngineException;

    public void forceMerge(boolean flush) throws IOException {
        forceMerge(flush, 1, false, false, false);
    }

    public abstract void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade, boolean upgradeOnlyAncientSegments) throws EngineException, IOException;

    public abstract IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException;

    public abstract IndexCommitRef acquireSafeIndexCommit() throws EngineException;

    @SuppressWarnings("finally")
    private void maybeDie(final String maybeMessage, final Throwable maybeFatal) {
        ExceptionsHelper.maybeError(maybeFatal, logger).ifPresent(error -> {
            try {
                logger.error(maybeMessage, error);
            } finally {
                throw error;
            }
        });
    }

    public void failEngine(String reason, @Nullable Exception failure) {
        if (failure != null) {
            maybeDie(reason, failure);
        }
        if (failEngineLock.tryLock()) {
            store.incRef();
            try {
                if (failedEngine.get() != null) {
                    logger.warn(() -> new ParameterizedMessage("tried to fail engine but engine is already failed. ignoring. [{}]", reason), failure);
                    return;
                }
                failedEngine.set((failure != null) ? failure : new IllegalStateException(reason));
                try {
                    closeNoLock("engine failed on: [" + reason + "]", closedLatch);
                } finally {
                    logger.warn(() -> new ParameterizedMessage("failed engine [{}]", reason), failure);
                    if (Lucene.isCorruptionException(failure)) {
                        try {
                            store.markStoreCorrupted(new IOException("failed engine (reason: [" + reason + "])", ExceptionsHelper.unwrapCorruption(failure)));
                        } catch (IOException e) {
                            logger.warn("Couldn't mark store corrupted", e);
                        }
                    }
                    eventListener.onFailedEngine(reason, failure);
                }
            } catch (Exception inner) {
                if (failure != null)
                    inner.addSuppressed(failure);
                logger.warn("failEngine threw exception", inner);
            } finally {
                store.decRef();
            }
        } else {
            logger.debug(() -> new ParameterizedMessage("tried to fail engine but could not acquire lock - engine should be failed by now [{}]", reason), failure);
        }
    }

    protected boolean maybeFailEngine(String source, Exception e) {
        if (Lucene.isCorruptionException(e)) {
            failEngine("corrupt file (source: [" + source + "])", e);
            return true;
        }
        return false;
    }

    public interface EventListener {

        default void onFailedEngine(String reason, @Nullable Exception e) {
        }
    }

    public static class Searcher implements Releasable {

        private final String source;

        private final IndexSearcher searcher;

        public Searcher(String source, IndexSearcher searcher) {
            this.source = source;
            this.searcher = searcher;
        }

        public String source() {
            return source;
        }

        public IndexReader reader() {
            return searcher.getIndexReader();
        }

        public DirectoryReader getDirectoryReader() {
            if (reader() instanceof DirectoryReader) {
                return (DirectoryReader) reader();
            }
            throw new IllegalStateException("Can't use " + reader().getClass() + " as a directory reader");
        }

        public IndexSearcher searcher() {
            return searcher;
        }

        @Override
        public void close() {
        }
    }

    public abstract static class Operation {

        public enum TYPE {

            INDEX, DELETE, NO_OP;

            private final String lowercase;

            TYPE() {
                this.lowercase = this.toString().toLowerCase(Locale.ROOT);
            }

            public String getLowercase() {
                return lowercase;
            }
        }

        private final Term uid;

        private final long version;

        private final long seqNo;

        private final long primaryTerm;

        private final VersionType versionType;

        private final Origin origin;

        private final long startTime;

        public Operation(Term uid, long seqNo, long primaryTerm, long version, VersionType versionType, Origin origin, long startTime) {
            this.uid = uid;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
            this.versionType = versionType;
            this.origin = origin;
            this.startTime = startTime;
        }

        public enum Origin {

            PRIMARY, REPLICA, PEER_RECOVERY, LOCAL_TRANSLOG_RECOVERY;

            public boolean isRecovery() {
                return this == PEER_RECOVERY || this == LOCAL_TRANSLOG_RECOVERY;
            }
        }

        public Origin origin() {
            return this.origin;
        }

        public Term uid() {
            return this.uid;
        }

        public long version() {
            return this.version;
        }

        public long seqNo() {
            return seqNo;
        }

        public long primaryTerm() {
            return primaryTerm;
        }

        public abstract int estimatedSizeInBytes();

        public VersionType versionType() {
            return this.versionType;
        }

        public long startTime() {
            return this.startTime;
        }

        public abstract String type();

        abstract String id();

        public abstract TYPE operationType();
    }

    public static class Index extends Operation {

        private final ParsedDocument doc;

        private final long autoGeneratedIdTimestamp;

        private final boolean isRetry;

        public Index(Term uid, ParsedDocument doc, long seqNo, long primaryTerm, long version, VersionType versionType, Origin origin, long startTime, long autoGeneratedIdTimestamp, boolean isRetry) {
            super(uid, seqNo, primaryTerm, version, versionType, origin, startTime);
            assert (origin == Origin.PRIMARY) == (versionType != null) : "invalid version_type=" + versionType + " for origin=" + origin;
            this.doc = doc;
            this.isRetry = isRetry;
            this.autoGeneratedIdTimestamp = autoGeneratedIdTimestamp;
        }

        public Index(Term uid, long primaryTerm, ParsedDocument doc) {
            this(uid, primaryTerm, doc, Versions.MATCH_ANY);
        }

        Index(Term uid, long primaryTerm, ParsedDocument doc, long version) {
            this(uid, doc, SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm, version, VersionType.INTERNAL, Origin.PRIMARY, System.nanoTime(), -1, false);
        }

        public ParsedDocument parsedDoc() {
            return this.doc;
        }

        @Override
        public String type() {
            return this.doc.type();
        }

        @Override
        public String id() {
            return this.doc.id();
        }

        @Override
        public TYPE operationType() {
            return TYPE.INDEX;
        }

        public String routing() {
            return this.doc.routing();
        }

        public List<Document> docs() {
            return this.doc.docs();
        }

        public BytesReference source() {
            return this.doc.source();
        }

        @Override
        public int estimatedSizeInBytes() {
            return (id().length() + type().length()) * 2 + source().length() + 12;
        }

        public long getAutoGeneratedIdTimestamp() {
            return autoGeneratedIdTimestamp;
        }

        public boolean isRetry() {
            return isRetry;
        }
    }

    public static class Delete extends Operation {

        private final String type;

        private final String id;

        public Delete(String type, String id, Term uid, long seqNo, long primaryTerm, long version, VersionType versionType, Origin origin, long startTime) {
            super(uid, seqNo, primaryTerm, version, versionType, origin, startTime);
            assert (origin == Origin.PRIMARY) == (versionType != null) : "invalid version_type=" + versionType + " for origin=" + origin;
            this.type = Objects.requireNonNull(type);
            this.id = Objects.requireNonNull(id);
        }

        public Delete(String type, String id, Term uid, long primaryTerm) {
            this(type, id, uid, SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm, Versions.MATCH_ANY, VersionType.INTERNAL, Origin.PRIMARY, System.nanoTime());
        }

        public Delete(Delete template, VersionType versionType) {
            this(template.type(), template.id(), template.uid(), template.seqNo(), template.primaryTerm(), template.version(), versionType, template.origin(), template.startTime());
        }

        @Override
        public String type() {
            return this.type;
        }

        @Override
        public String id() {
            return this.id;
        }

        @Override
        public TYPE operationType() {
            return TYPE.DELETE;
        }

        @Override
        public int estimatedSizeInBytes() {
            return (uid().field().length() + uid().text().length()) * 2 + 20;
        }
    }

    public static class NoOp extends Operation {

        private final String reason;

        public String reason() {
            return reason;
        }

        public NoOp(final long seqNo, final long primaryTerm, final Origin origin, final long startTime, final String reason) {
            super(null, seqNo, primaryTerm, Versions.NOT_FOUND, null, origin, startTime);
            this.reason = reason;
        }

        @Override
        public Term uid() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long version() {
            throw new UnsupportedOperationException();
        }

        @Override
        public VersionType versionType() {
            throw new UnsupportedOperationException();
        }

        @Override
        String id() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TYPE operationType() {
            return TYPE.NO_OP;
        }

        @Override
        public int estimatedSizeInBytes() {
            return 2 * reason.length() + 2 * Long.BYTES;
        }
    }

    public static class Get {

        private final boolean realtime;

        private final Term uid;

        private final String type, id;

        private final boolean readFromTranslog;

        private long version = Versions.MATCH_ANY;

        private VersionType versionType = VersionType.INTERNAL;

        public Get(boolean realtime, boolean readFromTranslog, String type, String id, Term uid) {
            this.realtime = realtime;
            this.type = type;
            this.id = id;
            this.uid = uid;
            this.readFromTranslog = readFromTranslog;
        }

        public boolean realtime() {
            return this.realtime;
        }

        public String type() {
            return type;
        }

        public String id() {
            return id;
        }

        public Term uid() {
            return uid;
        }

        public long version() {
            return version;
        }

        public Get version(long version) {
            this.version = version;
            return this;
        }

        public VersionType versionType() {
            return versionType;
        }

        public Get versionType(VersionType versionType) {
            this.versionType = versionType;
            return this;
        }

        public boolean isReadFromTranslog() {
            return readFromTranslog;
        }
    }

    public static class GetResult implements Releasable {

        private final boolean exists;

        private final long version;

        private final DocIdAndVersion docIdAndVersion;

        private final Searcher searcher;

        public static final GetResult NOT_EXISTS = new GetResult(false, Versions.NOT_FOUND, null, null);

        private GetResult(boolean exists, long version, DocIdAndVersion docIdAndVersion, Searcher searcher) {
            this.exists = exists;
            this.version = version;
            this.docIdAndVersion = docIdAndVersion;
            this.searcher = searcher;
        }

        public GetResult(Searcher searcher, DocIdAndVersion docIdAndVersion) {
            this(true, docIdAndVersion.version, docIdAndVersion, searcher);
        }

        public boolean exists() {
            return exists;
        }

        public long version() {
            return this.version;
        }

        public Searcher searcher() {
            return this.searcher;
        }

        public DocIdAndVersion docIdAndVersion() {
            return docIdAndVersion;
        }

        @Override
        public void close() {
            Releasables.close(searcher);
        }
    }

    protected abstract void closeNoLock(String reason, CountDownLatch closedLatch);

    public void flushAndClose() throws IOException {
        if (isClosed.get() == false) {
            logger.trace("flushAndClose now acquire writeLock");
            try (ReleasableLock lock = writeLock.acquire()) {
                logger.trace("flushAndClose now acquired writeLock");
                try {
                    logger.debug("flushing shard on close - this might take some time to sync files to disk");
                    try {
                        flush();
                    } catch (AlreadyClosedException ex) {
                        logger.debug("engine already closed - skipping flushAndClose");
                    }
                } finally {
                    close();
                }
            }
        }
        awaitPendingClose();
    }

    @Override
    public void close() throws IOException {
        if (isClosed.get() == false) {
            logger.debug("close now acquiring writeLock");
            try (ReleasableLock lock = writeLock.acquire()) {
                logger.debug("close acquired writeLock");
                closeNoLock("api", closedLatch);
            }
        }
        awaitPendingClose();
    }

    private void awaitPendingClose() {
        try {
            closedLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static class CommitId implements Writeable {

        private final byte[] id;

        public CommitId(byte[] id) {
            assert id != null;
            this.id = Arrays.copyOf(id, id.length);
        }

        public CommitId(StreamInput in) throws IOException {
            assert in != null;
            this.id = in.readByteArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByteArray(id);
        }

        @Override
        public String toString() {
            return Base64.getEncoder().encodeToString(id);
        }

        public boolean idsEqual(byte[] id) {
            return Arrays.equals(id, this.id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CommitId commitId = (CommitId) o;
            if (!Arrays.equals(id, commitId.id)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(id);
        }
    }

    public static class IndexCommitRef implements Closeable {

        private final AtomicBoolean closed = new AtomicBoolean();

        private final CheckedRunnable<IOException> onClose;

        private final IndexCommit indexCommit;

        IndexCommitRef(IndexCommit indexCommit, CheckedRunnable<IOException> onClose) {
            this.indexCommit = indexCommit;
            this.onClose = onClose;
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                onClose.run();
            }
        }

        public IndexCommit getIndexCommit() {
            return indexCommit;
        }
    }

    public void onSettingsChanged() {
    }

    public long getLastWriteNanos() {
        return this.lastWriteNanos;
    }

    public interface Warmer {

        void warm(Engine.Searcher searcher);
    }

    public abstract void activateThrottling();

    public abstract void deactivateThrottling();

    public abstract void restoreLocalCheckpointFromTranslog() throws IOException;

    public abstract int fillSeqNoGaps(long primaryTerm) throws IOException;

    public abstract Engine recoverFromTranslog() throws IOException;

    public abstract void skipTranslogRecovery();

    public boolean isRecovering() {
        return false;
    }

    public abstract void maybePruneDeletes();
}