package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.translog.Translog;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class ShadowEngine extends Engine {

    public static final String NONEXISTENT_INDEX_RETRY_WAIT = "index.shadow.wait_for_initial_commit";

    public static final TimeValue DEFAULT_NONEXISTENT_INDEX_RETRY_WAIT = TimeValue.timeValueSeconds(5);

    private volatile SearcherManager searcherManager;

    private volatile SegmentInfos lastCommittedSegmentInfos;

    public ShadowEngine(EngineConfig engineConfig) {
        super(engineConfig);
        if (engineConfig.getRefreshListeners() != null) {
            throw new IllegalArgumentException("ShadowEngine doesn't support RefreshListeners");
        }
        SearcherFactory searcherFactory = new EngineSearcherFactory(engineConfig);
        final long nonexistentRetryTime = engineConfig.getIndexSettings().getSettings().getAsTime(NONEXISTENT_INDEX_RETRY_WAIT, DEFAULT_NONEXISTENT_INDEX_RETRY_WAIT).getMillis();
        try {
            DirectoryReader reader = null;
            store.incRef();
            boolean success = false;
            try {
                if (Lucene.waitForIndex(store.directory(), nonexistentRetryTime)) {
                    reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(store.directory()), shardId);
                    this.searcherManager = new SearcherManager(reader, searcherFactory);
                    this.lastCommittedSegmentInfos = readLastCommittedSegmentInfos(searcherManager, store);
                    success = true;
                } else {
                    throw new IllegalStateException("failed to open a shadow engine after" + nonexistentRetryTime + "ms, " + "directory is not an index");
                }
            } catch (Exception e) {
                logger.warn("failed to create new reader", e);
                throw e;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(reader);
                    store.decRef();
                }
            }
        } catch (IOException ex) {
            throw new EngineCreationFailureException(shardId, "failed to open index reader", ex);
        }
        logger.trace("created new ShadowEngine");
    }

    @Override
    public IndexResult index(Index index) {
        throw new UnsupportedOperationException(shardId + " index operation not allowed on shadow engine");
    }

    @Override
    public DeleteResult delete(Delete delete) {
        throw new UnsupportedOperationException(shardId + " delete operation not allowed on shadow engine");
    }

    @Override
    public SyncedFlushResult syncFlush(String syncId, CommitId expectedCommitId) {
        throw new UnsupportedOperationException(shardId + " sync commit operation not allowed on shadow engine");
    }

    @Override
    public CommitId flush() throws EngineException {
        return flush(false, false);
    }

    @Override
    public CommitId flush(boolean force, boolean waitIfOngoing) throws EngineException {
        logger.trace("skipping FLUSH on shadow engine");
        refresh("flush");
        store.incRef();
        try (ReleasableLock lock = readLock.acquire()) {
            lastCommittedSegmentInfos = readLastCommittedSegmentInfos(searcherManager, store);
        } catch (Exception e) {
            if (isClosed.get() == false) {
                logger.warn("failed to read latest segment infos on flush", e);
                if (Lucene.isCorruptionException(e)) {
                    throw new FlushFailedEngineException(shardId, e);
                }
            }
        } finally {
            store.decRef();
        }
        return new CommitId(lastCommittedSegmentInfos.getId());
    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, boolean upgrade, boolean upgradeOnlyAncientSegments) throws EngineException {
        logger.trace("skipping FORCE-MERGE on shadow engine");
    }

    @Override
    public GetResult get(Get get, Function<String, Searcher> searcherFacotry) throws EngineException {
        return getFromSearcher(get, searcherFacotry);
    }

    @Override
    public Translog getTranslog() {
        throw new UnsupportedOperationException("shadow engines don't have translogs");
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        try (ReleasableLock lock = readLock.acquire()) {
            Segment[] segmentsArr = getSegmentInfo(lastCommittedSegmentInfos, verbose);
            for (int i = 0; i < segmentsArr.length; i++) {
                segmentsArr[i].committed = true;
            }
            return Arrays.asList(segmentsArr);
        }
    }

    @Override
    public void refresh(String source) throws EngineException {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            searcherManager.maybeRefreshBlocking();
        } catch (AlreadyClosedException e) {
            throw new AssertionError(e);
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
    }

    @Override
    public IndexCommit acquireIndexCommit(boolean flushFirst) throws EngineException {
        throw new UnsupportedOperationException("Can not take snapshot from a shadow engine");
    }

    @Override
    protected SearcherManager getSearcherManager() {
        return searcherManager;
    }

    @Override
    protected void closeNoLock(String reason) {
        if (isClosed.compareAndSet(false, true)) {
            try {
                logger.debug("shadow replica close searcher manager refCount: {}", store.refCount());
                IOUtils.close(searcherManager);
            } catch (Exception e) {
                logger.warn("shadow replica failed to close searcher manager", e);
            } finally {
                store.decRef();
            }
        }
    }

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        throw new UnsupportedOperationException("ShadowEngine has no IndexWriter");
    }

    @Override
    public void writeIndexingBuffer() {
        throw new UnsupportedOperationException("ShadowEngine has no IndexWriter");
    }

    @Override
    public void activateThrottling() {
        throw new UnsupportedOperationException("ShadowEngine has no IndexWriter");
    }

    @Override
    public void deactivateThrottling() {
        throw new UnsupportedOperationException("ShadowEngine has no IndexWriter");
    }

    @Override
    public SequenceNumbersService seqNoService() {
        throw new UnsupportedOperationException("ShadowEngine doesn't track sequence numbers");
    }

    @Override
    public Engine recoverFromTranslog() throws IOException {
        throw new UnsupportedOperationException("can't recover on a shadow engine");
    }
}