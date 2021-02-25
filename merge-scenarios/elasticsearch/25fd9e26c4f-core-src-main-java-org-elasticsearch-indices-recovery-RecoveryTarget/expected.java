package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.TranslogRecoveryPerformer;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RecoveryTarget extends AbstractRefCounted implements RecoveryTargetHandler {

    private final Logger logger;

    private static final AtomicLong idGenerator = new AtomicLong();

    private final String RECOVERY_PREFIX = "recovery.";

    private final ShardId shardId;

    private final long recoveryId;

    private final IndexShard indexShard;

    private final DiscoveryNode sourceNode;

    private final String tempFilePrefix;

    private final Store store;

    private final PeerRecoveryTargetService.RecoveryListener listener;

    private final Callback<Long> ensureClusterStateVersionCallback;

    private final AtomicBoolean finished = new AtomicBoolean();

    private final ConcurrentMap<String, IndexOutput> openIndexOutputs = ConcurrentCollections.newConcurrentMap();

    private final CancellableThreads cancellableThreads;

    private volatile long lastAccessTime = System.nanoTime();

    private final Map<String, String> tempFileNames = ConcurrentCollections.newConcurrentMap();

    private RecoveryTarget(RecoveryTarget copyFrom) {
        this(copyFrom.indexShard, copyFrom.sourceNode, copyFrom.listener, copyFrom.cancellableThreads, copyFrom.recoveryId, copyFrom.ensureClusterStateVersionCallback);
    }

    public RecoveryTarget(IndexShard indexShard, DiscoveryNode sourceNode, PeerRecoveryTargetService.RecoveryListener listener, Callback<Long> ensureClusterStateVersionCallback) {
        this(indexShard, sourceNode, listener, new CancellableThreads(), idGenerator.incrementAndGet(), ensureClusterStateVersionCallback);
    }

    private RecoveryTarget(IndexShard indexShard, DiscoveryNode sourceNode, PeerRecoveryTargetService.RecoveryListener listener, CancellableThreads cancellableThreads, long recoveryId, Callback<Long> ensureClusterStateVersionCallback) {
        super("recovery_status");
        this.cancellableThreads = cancellableThreads;
        this.recoveryId = recoveryId;
        this.listener = listener;
        this.logger = Loggers.getLogger(getClass(), indexShard.indexSettings().getSettings(), indexShard.shardId());
        this.indexShard = indexShard;
        this.sourceNode = sourceNode;
        this.shardId = indexShard.shardId();
        this.tempFilePrefix = RECOVERY_PREFIX + UUIDs.base64UUID() + ".";
        this.store = indexShard.store();
        this.ensureClusterStateVersionCallback = ensureClusterStateVersionCallback;
        store.incRef();
        indexShard.recoveryStats().incCurrentAsTarget();
    }

    public long recoveryId() {
        return recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public IndexShard indexShard() {
        ensureRefCount();
        return indexShard;
    }

    public DiscoveryNode sourceNode() {
        return this.sourceNode;
    }

    public RecoveryState state() {
        return indexShard.recoveryState();
    }

    public CancellableThreads CancellableThreads() {
        return cancellableThreads;
    }

    public long lastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime() {
        lastAccessTime = System.nanoTime();
    }

    public Store store() {
        ensureRefCount();
        return store;
    }

    public RecoveryState.Stage stage() {
        return state().getStage();
    }

    public void renameAllTempFiles() throws IOException {
        ensureRefCount();
        store.renameTempFilesSafe(tempFileNames);
    }

    RecoveryTarget resetRecovery() throws IOException {
        ensureRefCount();
        if (finished.compareAndSet(false, true)) {
            decRef();
        }
        indexShard.performRecoveryRestart();
        return new RecoveryTarget(this);
    }

    public void cancel(String reason) {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("recovery canceled (reason: [{}])", reason);
                cancellableThreads.cancel(reason);
            } finally {
                decRef();
            }
        }
    }

    public void fail(RecoveryFailedException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
                listener.onRecoveryFailure(state(), e, sendShardFailure);
            } finally {
                try {
                    cancellableThreads.cancel("failed recovery [" + ExceptionsHelper.stackTrace(e) + "]");
                } finally {
                    decRef();
                }
            }
        }
    }

    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            assert tempFileNames.isEmpty() : "not all temporary files are renamed";
            try {
                indexShard.postRecovery("peer recovery done");
            } finally {
                decRef();
            }
            listener.onRecoveryDone(state());
        }
    }

    public String getTempNameForFile(String origFile) {
        return tempFilePrefix + origFile;
    }

    public IndexOutput getOpenIndexOutput(String key) {
        ensureRefCount();
        return openIndexOutputs.get(key);
    }

    public IndexOutput removeOpenIndexOutputs(String name) {
        ensureRefCount();
        return openIndexOutputs.remove(name);
    }

    public IndexOutput openAndPutIndexOutput(String fileName, StoreFileMetaData metaData, Store store) throws IOException {
        ensureRefCount();
        String tempFileName = getTempNameForFile(fileName);
        if (tempFileNames.containsKey(tempFileName)) {
            throw new IllegalStateException("output for file [" + fileName + "] has already been created");
        }
        tempFileNames.put(tempFileName, fileName);
        IndexOutput indexOutput = store.createVerifyingOutput(tempFileName, metaData, IOContext.DEFAULT);
        openIndexOutputs.put(fileName, indexOutput);
        return indexOutput;
    }

    @Override
    protected void closeInternal() {
        try {
            Iterator<Entry<String, IndexOutput>> iterator = openIndexOutputs.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, IndexOutput> entry = iterator.next();
                logger.trace("closing IndexOutput file [{}]", entry.getValue());
                try {
                    entry.getValue().close();
                } catch (Exception e) {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("error while closing recovery output [{}]", entry.getValue()), e);
                }
                iterator.remove();
            }
            for (String file : tempFileNames.keySet()) {
                logger.trace("cleaning temporary file [{}]", file);
                store.deleteQuiet(file);
            }
        } finally {
            store.decRef();
            indexShard.recoveryStats().decCurrentAsTarget();
        }
    }

    @Override
    public String toString() {
        return shardId + " [" + recoveryId + "]";
    }

    private void ensureRefCount() {
        if (refCount() <= 0) {
            throw new ElasticsearchException("RecoveryStatus is used but it's refcount is 0. Probably a mismatch between incRef/decRef " + "calls");
        }
    }

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps, long maxUnsafeAutoIdTimestamp) throws IOException {
        state().getTranslog().totalOperations(totalTranslogOps);
        indexShard().skipTranslogRecovery(maxUnsafeAutoIdTimestamp);
    }

    @Override
    public FinalizeResponse finalizeRecovery() {
        final IndexShard indexShard = indexShard();
        indexShard.finalizeRecovery();
        return new FinalizeResponse(indexShard.routingEntry().allocationId().getId(), indexShard.getLocalCheckpoint());
    }

    @Override
    public void ensureClusterStateVersion(long clusterStateVersion) {
        ensureClusterStateVersionCallback.handle(clusterStateVersion);
    }

    @Override
    public void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps) throws TranslogRecoveryPerformer.BatchOperationException {
        final RecoveryState.Translog translog = state().getTranslog();
        translog.totalOperations(totalTranslogOps);
        assert indexShard().recoveryState() == state();
        indexShard().performBatchRecovery(operations);
    }

    @Override
    public void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames, List<Long> phase1ExistingFileSizes, int totalTranslogOps) {
        final RecoveryState.Index index = state().getIndex();
        for (int i = 0; i < phase1ExistingFileNames.size(); i++) {
            index.addFileDetail(phase1ExistingFileNames.get(i), phase1ExistingFileSizes.get(i), true);
        }
        for (int i = 0; i < phase1FileNames.size(); i++) {
            index.addFileDetail(phase1FileNames.get(i), phase1FileSizes.get(i), false);
        }
        state().getTranslog().totalOperations(totalTranslogOps);
        state().getTranslog().totalOperationsOnStart(totalTranslogOps);
    }

    @Override
    public void cleanFiles(int totalTranslogOps, Store.MetadataSnapshot sourceMetaData) throws IOException {
        state().getTranslog().totalOperations(totalTranslogOps);
        renameAllTempFiles();
        final Store store = store();
        try {
            store.cleanupAndVerify("recovery CleanFilesRequestHandler", sourceMetaData);
        } catch (CorruptIndexException | IndexFormatTooNewException | IndexFormatTooOldException ex) {
            try {
                try {
                    store.removeCorruptionMarker();
                } finally {
                    Lucene.cleanLuceneIndex(store.directory());
                }
            } catch (Exception e) {
                logger.debug("Failed to clean lucene index", e);
                ex.addSuppressed(e);
            }
            RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
            fail(rfe, true);
            throw rfe;
        } catch (Exception ex) {
            RecoveryFailedException rfe = new RecoveryFailedException(state(), "failed to clean after recovery", ex);
            fail(rfe, true);
            throw rfe;
        }
    }

    @Override
    public void writeFileChunk(StoreFileMetaData fileMetaData, long position, BytesReference content, boolean lastChunk, int totalTranslogOps) throws IOException {
        final Store store = store();
        final String name = fileMetaData.name();
        state().getTranslog().totalOperations(totalTranslogOps);
        final RecoveryState.Index indexState = state().getIndex();
        IndexOutput indexOutput;
        if (position == 0) {
            indexOutput = openAndPutIndexOutput(name, fileMetaData, store);
        } else {
            indexOutput = getOpenIndexOutput(name);
        }
        BytesRefIterator iterator = content.iterator();
        BytesRef scratch;
        while ((scratch = iterator.next()) != null) {
            indexOutput.writeBytes(scratch.bytes, scratch.offset, scratch.length);
        }
        indexState.addRecoveredBytesToFile(name, content.length());
        if (indexOutput.getFilePointer() >= fileMetaData.length() || lastChunk) {
            try {
                Store.verify(indexOutput);
            } finally {
                indexOutput.close();
            }
            final String temporaryFileName = getTempNameForFile(name);
            assert Arrays.asList(store.directory().listAll()).contains(temporaryFileName) : "expected: [" + temporaryFileName + "] in " + Arrays.toString(store.directory().listAll());
            store.directory().sync(Collections.singleton(temporaryFileName));
            IndexOutput remove = removeOpenIndexOutputs(name);
            assert remove == null || remove == indexOutput;
        }
    }
}