package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
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
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public abstract class Engine implements Closeable {

    public static final String SYNC_COMMIT_ID = "sync_id";

    protected final ShardId shardId;

    protected final Logger logger;

    protected final EngineConfig engineConfig;

    protected final Store store;

    protected final AtomicBoolean isClosed = new AtomicBoolean(false);

    protected final EventListener eventListener;

    protected final SnapshotDeletionPolicy deletionPolicy;

    protected final ReentrantLock failEngineLock = new ReentrantLock();

    protected final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

    protected final ReleasableLock readLock = new ReleasableLock(rwl.readLock());

    protected final ReleasableLock writeLock = new ReleasableLock(rwl.writeLock());

    protected final SetOnce<Exception> failedEngine = new SetOnce<>();

    protected volatile long lastWriteNanos = System.nanoTime();

    protected Engine(EngineConfig engineConfig) {
        Objects.requireNonNull(engineConfig.getStore(), "Store must be provided to the engine");
        Objects.requireNonNull(engineConfig.getDeletionPolicy(), "Snapshot deletion policy must be provided to the engine");
        this.engineConfig = engineConfig;
        this.shardId = engineConfig.getShardId();
        this.store = engineConfig.getStore();
        this.logger = Loggers.getLogger(Engine.class, engineConfig.getIndexSettings().getSettings(), engineConfig.getShardId());
        this.eventListener = engineConfig.getEventListener();
        this.deletionPolicy = engineConfig.getDeletionPolicy();
    }

    protected static long guardedRamBytesUsed(Accountable a) {
        if (a == null) {
            return 0;
        }
        return a.ramBytesUsed();
    }

    protected static SegmentReader segmentReader(LeafReader reader) {
        if (reader instanceof SegmentReader) {
            return (SegmentReader) reader;
        } else if (reader instanceof FilterLeafReader) {
            final FilterLeafReader fReader = (FilterLeafReader) reader;
            return segmentReader(FilterLeafReader.unwrap(fReader));
        }
        throw new IllegalStateException("Can not extract segment reader from given index reader [" + reader + "]");
    }

    protected static boolean isMergedSegment(LeafReader reader) {
        final Map<String, String> diagnostics = segmentReader(reader).getSegmentInfo().info.getDiagnostics();
        final String source = diagnostics.get(IndexWriter.SOURCE);
        assert Arrays.asList(IndexWriter.SOURCE_ADDINDEXES_READERS, IndexWriter.SOURCE_FLUSH, IndexWriter.SOURCE_MERGE).contains(source) : "Unknown source " + source;
        return IndexWriter.SOURCE_MERGE.equals(source);
    }

    protected Searcher newSearcher(String source, IndexSearcher searcher, SearcherManager manager) {
        return new EngineSearcher(source, searcher, manager, store, logger);
    }

    public final EngineConfig config() {
        return engineConfig;
    }

    protected abstract SegmentInfos getLastCommittedSegmentInfos();

    public MergeStats getMergeStats() {
        return new MergeStats();
    }

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

    public long getIndexThrottleTimeInMillis() {
        return 0;
    }

    public boolean isThrottled() {
        return false;
    }

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

    public abstract void index(Index operation) throws EngineException;

    public abstract void delete(Delete delete) throws EngineException;

    public abstract SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) throws EngineException;

    public enum SyncedFlushResult {

        SUCCESS, COMMIT_MISMATCH, PENDING_OPERATIONS
    }

    protected final GetResult getFromSearcher(Get get, Function<String, Searcher> searcherFactory) throws EngineException {
        final Searcher searcher = searcherFactory.apply("get");
        final Versions.DocIdAndVersion docIdAndVersion;
        try {
            docIdAndVersion = Versions.loadDocIdAndVersion(searcher.reader(), get.uid());
        } catch (Exception e) {
            Releasables.closeWhileHandlingException(searcher);
            throw new EngineException(shardId, "Couldn't resolve version", e);
        }
        if (docIdAndVersion != null) {
            if (get.versionType().isVersionConflictForReads(docIdAndVersion.version, get.version())) {
                Releasables.close(searcher);
                Uid uid = Uid.createUid(get.uid().text());
                throw new VersionConflictEngineException(shardId, uid.type(), uid.id(), get.versionType().explainConflictForReads(docIdAndVersion.version, get.version()));
            }
        }
        if (docIdAndVersion != null) {
            return new GetResult(searcher, docIdAndVersion);
        } else {
            Releasables.close(searcher);
            return GetResult.NOT_EXISTS;
        }
    }

    public final GetResult get(Get get) throws EngineException {
        return get(get, this::acquireSearcher);
    }

    public abstract GetResult get(Get get, Function<String, Searcher> searcherFactory) throws EngineException;

    public final Searcher acquireSearcher(String source) throws EngineException {
        boolean success = false;
        store.incRef();
        try {
            final SearcherManager manager = getSearcherManager();
            final IndexSearcher searcher = manager.acquire();
            try {
                final Searcher retVal = newSearcher(source, searcher, manager);
                success = true;
                return retVal;
            } finally {
                if (!success) {
                    manager.release(searcher);
                }
            }
        } catch (EngineClosedException ex) {
            throw ex;
        } catch (Exception ex) {
            ensureOpen();
            logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to acquire searcher, source {}", source), ex);
            throw new EngineException(shardId, "failed to acquire searcher, source " + source, ex);
        } finally {
            if (!success) {
                store.decRef();
            }
        }
    }

    public abstract Translog getTranslog();

    protected void ensureOpen() {
        if (isClosed.get()) {
            throw new EngineClosedException(shardId, failedEngine.get());
        }
    }

    public CommitStats commitStats() {
        return new CommitStats(getLastCommittedSegmentInfos());
    }

    protected static SegmentInfos readLastCommittedSegmentInfos(final SearcherManager sm, final Store store) throws IOException {
        IndexSearcher searcher = sm.acquire();
        try {
            IndexCommit latestCommit = ((DirectoryReader) searcher.getIndexReader()).getIndexCommit();
            return Lucene.readSegmentInfos(latestCommit);
        } catch (IOException e) {
            try {
                return store.readLastCommittedSegmentsInfo();
            } catch (IOException e2) {
                e2.addSuppressed(e);
                throw e2;
            }
        } finally {
            sm.release(searcher);
        }
    }

    public final SegmentsStats segmentsStats(boolean includeSegmentFileSizes) {
        ensureOpen();
        try (final Searcher searcher = acquireSearcher("segments_stats")) {
            SegmentsStats stats = new SegmentsStats();
            for (LeafReaderContext reader : searcher.reader().leaves()) {
                final SegmentReader segmentReader = segmentReader(reader.reader());
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
            writerSegmentStats(stats);
            return stats;
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
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Error when opening compound reader for Directory [{}] and SegmentCommitInfo [{}]", segmentReader.directory(), segmentCommitInfo), e);
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
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Couldn't list Compound Reader Directory [{}]", finalDirectory), e);
                return ImmutableOpenMap.of();
            }
        } else {
            try {
                files = segmentReader.getSegmentInfo().files().toArray(new String[] {});
            } catch (IOException e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Couldn't list Directory from SegmentReader [{}] and SegmentInfo [{}]", segmentReader, segmentReader.getSegmentInfo()), e);
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
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Tried to query fileLength but file is gone [{}] [{}]", finalDirectory, file), e);
            } catch (IOException e) {
                final Directory finalDirectory = directory;
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Error when trying to query fileLength [{}] [{}]", finalDirectory, file), e);
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
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Error when closing compound reader on Directory [{}]", finalDirectory), e);
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
        Searcher searcher = acquireSearcher("segments");
        try {
            for (LeafReaderContext reader : searcher.reader().leaves()) {
                SegmentCommitInfo info = segmentReader(reader.reader()).getSegmentInfo();
                assert !segments.containsKey(info.info.name);
                Segment segment = new Segment(info.info.name);
                segment.search = true;
                segment.docCount = reader.reader().numDocs();
                segment.delDocCount = reader.reader().numDeletedDocs();
                segment.version = info.info.getVersion();
                segment.compound = info.info.getUseCompoundFile();
                try {
                    segment.sizeInBytes = info.sizeInBytes();
                } catch (IOException e) {
                    logger.trace((Supplier<?>) () -> new ParameterizedMessage("failed to get size for [{}]", info.info.name), e);
                }
                final SegmentReader segmentReader = segmentReader(reader.reader());
                segment.memoryInBytes = segmentReader.ramBytesUsed();
                if (verbose) {
                    segment.ramTree = Accountables.namedAccountable("root", segmentReader);
                }
                segments.put(info.info.name, segment);
            }
        } finally {
            searcher.close();
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
                        logger.trace((Supplier<?>) () -> new ParameterizedMessage("failed to get size for [{}]", info.info.name), e);
                    }
                    segments.put(info.info.name, segment);
                } else {
                    segment.committed = true;
                }
            }
        }
        Segment[] segmentsArr = segments.values().toArray(new Segment[segments.values().size()]);
        Arrays.sort(segmentsArr, new Comparator<Segment>() {

            @Override
            public int compare(Segment o1, Segment o2) {
                return (int) (o1.getGeneration() - o2.getGeneration());
            }
        });
        return segmentsArr;
    }

    public abstract List<Segment> segments(boolean verbose);

    public final boolean refreshNeeded() {
        if (store.tryIncRef()) {
            try {
                return getSearcherManager().isSearcherCurrent() == false;
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

    public abstract CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException;

    public abstract CommitId flush() throws EngineException;

    public void forceMerge(boolean flush) throws IOException {
        forceMerge(flush, 1, false, false, false);
    }

    public abstract void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade, boolean upgradeOnlyAncientSegments) throws EngineException, IOException;

    public abstract IndexCommit acquireIndexCommit(boolean flushFirst) throws EngineException;

    public void failEngine(String reason, @Nullable Exception failure) {
        if (failEngineLock.tryLock()) {
            store.incRef();
            try {
                if (failedEngine.get() != null) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("tried to fail engine but engine is already failed. ignoring. [{}]", reason), failure);
                    return;
                }
                failedEngine.set((failure != null) ? failure : new IllegalStateException(reason));
                try {
                    closeNoLock("engine failed on: [" + reason + "]");
                } finally {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed engine [{}]", reason), failure);
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
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("tried to fail engine but could not acquire lock - engine should be failed by now [{}]", reason), failure);
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

        private final Term uid;

        private long version;

        private final VersionType versionType;

        private final Origin origin;

        private Translog.Location location;

        private final long startTime;

        private long endTime;

        public Operation(Term uid, long version, VersionType versionType, Origin origin, long startTime) {
            this.uid = uid;
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

        public void updateVersion(long version) {
            this.version = version;
        }

        public void setTranslogLocation(Translog.Location location) {
            this.location = location;
        }

        public Translog.Location getTranslogLocation() {
            return this.location;
        }

        public int sizeInBytes() {
            if (location != null) {
                return location.size;
            } else {
                return estimatedSizeInBytes();
            }
        }

        protected abstract int estimatedSizeInBytes();

        public VersionType versionType() {
            return this.versionType;
        }

        public long startTime() {
            return this.startTime;
        }

        public void endTime(long endTime) {
            this.endTime = endTime;
        }

        public long endTime() {
            return this.endTime;
        }

        abstract String type();

        abstract String id();
    }

    public static class Index extends Operation {

        private final ParsedDocument doc;

        private final long autoGeneratedIdTimestamp;

        private final boolean isRetry;

        private boolean created;

        public Index(Term uid, ParsedDocument doc, long version, VersionType versionType, Origin origin, long startTime, long autoGeneratedIdTimestamp, boolean isRetry) {
            super(uid, version, versionType, origin, startTime);
            this.doc = doc;
            this.isRetry = isRetry;
            this.autoGeneratedIdTimestamp = autoGeneratedIdTimestamp;
        }

        public Index(Term uid, ParsedDocument doc) {
            this(uid, doc, Versions.MATCH_ANY);
        }

        Index(Term uid, ParsedDocument doc, long version) {
            this(uid, doc, version, VersionType.INTERNAL, Origin.PRIMARY, System.nanoTime(), -1, false);
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

        public String routing() {
            return this.doc.routing();
        }

        public long timestamp() {
            return this.doc.timestamp();
        }

        public long ttl() {
            return this.doc.ttl();
        }

        @Override
        public void updateVersion(long version) {
            super.updateVersion(version);
            this.doc.version().setLongValue(version);
        }

        public String parent() {
            return this.doc.parent();
        }

        public List<Document> docs() {
            return this.doc.docs();
        }

        public BytesReference source() {
            return this.doc.source();
        }

        public boolean isCreated() {
            return created;
        }

        public void setCreated(boolean created) {
            this.created = created;
        }

        @Override
        protected int estimatedSizeInBytes() {
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

        private boolean found;

        public Delete(String type, String id, Term uid, long version, VersionType versionType, Origin origin, long startTime, boolean found) {
            super(uid, version, versionType, origin, startTime);
            this.type = type;
            this.id = id;
            this.found = found;
        }

        public Delete(String type, String id, Term uid) {
            this(type, id, uid, Versions.MATCH_ANY, VersionType.INTERNAL, Origin.PRIMARY, System.nanoTime(), false);
        }

        public Delete(Delete template, VersionType versionType) {
            this(template.type(), template.id(), template.uid(), template.version(), versionType, template.origin(), template.startTime(), template.found());
        }

        @Override
        public String type() {
            return this.type;
        }

        @Override
        public String id() {
            return this.id;
        }

        public void updateVersion(long version, boolean found) {
            updateVersion(version);
            this.found = found;
        }

        public boolean found() {
            return this.found;
        }

        @Override
        protected int estimatedSizeInBytes() {
            return (uid().field().length() + uid().text().length()) * 2 + 20;
        }
    }

    public static class Get {

        private final boolean realtime;

        private final Term uid;

        private long version = Versions.MATCH_ANY;

        private VersionType versionType = VersionType.INTERNAL;

        public Get(boolean realtime, Term uid) {
            this.realtime = realtime;
            this.uid = uid;
        }

        public boolean realtime() {
            return this.realtime;
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
    }

    public static class GetResult implements Releasable {

        private final boolean exists;

        private final long version;

        private final Versions.DocIdAndVersion docIdAndVersion;

        private final Searcher searcher;

        public static final GetResult NOT_EXISTS = new GetResult(false, Versions.NOT_FOUND, null, null);

        private GetResult(boolean exists, long version, Versions.DocIdAndVersion docIdAndVersion, Searcher searcher) {
            this.exists = exists;
            this.version = version;
            this.docIdAndVersion = docIdAndVersion;
            this.searcher = searcher;
        }

        public GetResult(Searcher searcher, Versions.DocIdAndVersion docIdAndVersion) {
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

        public Versions.DocIdAndVersion docIdAndVersion() {
            return docIdAndVersion;
        }

        @Override
        public void close() {
            release();
        }

        public void release() {
            Releasables.close(searcher);
        }
    }

    protected abstract SearcherManager getSearcherManager();

    protected abstract void closeNoLock(String reason);

    public void flushAndClose() throws IOException {
        if (isClosed.get() == false) {
            logger.trace("flushAndClose now acquire writeLock");
            try (ReleasableLock lock = writeLock.acquire()) {
                logger.trace("flushAndClose now acquired writeLock");
                try {
                    logger.debug("flushing shard on close - this might take some time to sync files to disk");
                    try {
                        flush();
                    } catch (EngineClosedException ex) {
                        logger.debug("engine already closed - skipping flushAndClose");
                    }
                } finally {
                    close();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed.get() == false) {
            logger.debug("close now acquiring writeLock");
            try (ReleasableLock lock = writeLock.acquire()) {
                logger.debug("close acquired writeLock");
                closeNoLock("api");
            }
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

    public void onSettingsChanged() {
    }

    public long getLastWriteNanos() {
        return this.lastWriteNanos;
    }

    public DocsStats getDocStats() {
        try (Engine.Searcher searcher = acquireSearcher("doc_stats")) {
            IndexReader reader = searcher.reader();
            return new DocsStats(reader.numDocs(), reader.numDeletedDocs());
        }
    }

    public interface Warmer {

        void warm(Engine.Searcher searcher);
    }

    public abstract void activateThrottling();

    public abstract void deactivateThrottling();

    public abstract Engine recoverFromTranslog() throws IOException;

    public boolean isRecovering() {
        return false;
    }
}