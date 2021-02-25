package org.elasticsearch.index.shard;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

final class StoreRecovery {

    private final Logger logger;

    private final ShardId shardId;

    StoreRecovery(ShardId shardId, Logger logger) {
        this.logger = logger;
        this.shardId = shardId;
    }

    boolean recoverFromStore(final IndexShard indexShard) {
        if (canRecover(indexShard)) {
            RecoverySource.Type recoveryType = indexShard.recoveryState().getRecoverySource().getType();
            assert recoveryType == RecoverySource.Type.EMPTY_STORE || recoveryType == RecoverySource.Type.EXISTING_STORE : "expected store recovery type but was: " + recoveryType;
            return executeRecovery(indexShard, () -> {
                logger.debug("starting recovery from store ...");
                internalRecoverFromStore(indexShard);
            });
        }
        return false;
    }

    boolean recoverFromLocalShards(BiConsumer<String, MappingMetaData> mappingUpdateConsumer, final IndexShard indexShard, final List<LocalShardSnapshot> shards) throws IOException {
        if (canRecover(indexShard)) {
            RecoverySource.Type recoveryType = indexShard.recoveryState().getRecoverySource().getType();
            assert recoveryType == RecoverySource.Type.LOCAL_SHARDS : "expected local shards recovery type: " + recoveryType;
            if (shards.isEmpty()) {
                throw new IllegalArgumentException("shards must not be empty");
            }
            Set<Index> indices = shards.stream().map((s) -> s.getIndex()).collect(Collectors.toSet());
            if (indices.size() > 1) {
                throw new IllegalArgumentException("can't add shards from more than one index");
            }
            IndexMetaData sourceMetaData = shards.get(0).getIndexMetaData();
            for (ObjectObjectCursor<String, MappingMetaData> mapping : sourceMetaData.getMappings()) {
                mappingUpdateConsumer.accept(mapping.key, mapping.value);
            }
            indexShard.mapperService().merge(sourceMetaData, MapperService.MergeReason.MAPPING_RECOVERY);
            Sort indexSort = indexShard.getIndexSort();
            final boolean hasNested = indexShard.mapperService().hasNested();
            final boolean isSplit = sourceMetaData.getNumberOfShards() < indexShard.indexSettings().getNumberOfShards();
            assert isSplit == false || sourceMetaData.getCreationVersion().onOrAfter(Version.V_6_0_0_alpha1) : "for split we require a " + "single type but the index is created before 6.0.0";
            return executeRecovery(indexShard, () -> {
                logger.debug("starting recovery from local shards {}", shards);
                try {
                    final Directory directory = indexShard.store().directory();
                    final Directory[] sources = shards.stream().map(LocalShardSnapshot::getSnapshotDirectory).toArray(Directory[]::new);
                    final long maxSeqNo = shards.stream().mapToLong(LocalShardSnapshot::maxSeqNo).max().getAsLong();
                    final long maxUnsafeAutoIdTimestamp = shards.stream().mapToLong(LocalShardSnapshot::maxUnsafeAutoIdTimestamp).max().getAsLong();
                    addIndices(indexShard.recoveryState().getIndex(), directory, indexSort, sources, maxSeqNo, maxUnsafeAutoIdTimestamp, indexShard.indexSettings().getIndexMetaData(), indexShard.shardId().id(), isSplit, hasNested);
                    internalRecoverFromStore(indexShard);
                    indexShard.getEngine().forceMerge(false, -1, false, false, false);
                } catch (IOException ex) {
                    throw new IndexShardRecoveryException(indexShard.shardId(), "failed to recover from local shards", ex);
                }
            });
        }
        return false;
    }

    void addIndices(final RecoveryState.Index indexRecoveryStats, final Directory target, final Sort indexSort, final Directory[] sources, final long maxSeqNo, final long maxUnsafeAutoIdTimestamp, IndexMetaData indexMetaData, int shardId, boolean split, boolean hasNested) throws IOException {
        Lucene.cleanLuceneIndex(target);
        assert sources.length > 0;
        final int luceneIndexCreatedVersionMajor = Lucene.readSegmentInfos(sources[0]).getIndexCreatedVersionMajor();
        new SegmentInfos(luceneIndexCreatedVersionMajor).commit(target);
        final Directory hardLinkOrCopyTarget = new org.apache.lucene.store.HardlinkCopyDirectoryWrapper(target);
        IndexWriterConfig iwc = new IndexWriterConfig(null).setSoftDeletesField(Lucene.SOFT_DELETES_FIELD).setCommitOnClose(false).setMergePolicy(NoMergePolicy.INSTANCE).setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        if (indexSort != null) {
            iwc.setIndexSort(indexSort);
        }
        try (IndexWriter writer = new IndexWriter(new StatsDirectoryWrapper(hardLinkOrCopyTarget, indexRecoveryStats), iwc)) {
            writer.addIndexes(sources);
            if (split) {
                writer.deleteDocuments(new ShardSplittingQuery(indexMetaData, shardId, hasNested));
            }
            writer.setLiveCommitData(() -> {
                final HashMap<String, String> liveCommitData = new HashMap<>(3);
                liveCommitData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
                liveCommitData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(maxSeqNo));
                liveCommitData.put(InternalEngine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, Long.toString(maxUnsafeAutoIdTimestamp));
                return liveCommitData.entrySet().iterator();
            });
            writer.commit();
        }
    }

    static final class StatsDirectoryWrapper extends FilterDirectory {

        private final RecoveryState.Index index;

        StatsDirectoryWrapper(Directory in, RecoveryState.Index indexRecoveryStats) {
            super(in);
            this.index = indexRecoveryStats;
        }

        @Override
        public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
            final long l = from.fileLength(src);
            final AtomicBoolean copies = new AtomicBoolean(false);
            in.copyFrom(new FilterDirectory(from) {

                @Override
                public IndexInput openInput(String name, IOContext context) throws IOException {
                    index.addFileDetail(dest, l, false);
                    copies.set(true);
                    final IndexInput input = in.openInput(name, context);
                    return new IndexInput("StatsDirectoryWrapper(" + input.toString() + ")") {

                        @Override
                        public void close() throws IOException {
                            input.close();
                        }

                        @Override
                        public long getFilePointer() {
                            throw new UnsupportedOperationException("only straight copies are supported");
                        }

                        @Override
                        public void seek(long pos) throws IOException {
                            throw new UnsupportedOperationException("seeks are not supported");
                        }

                        @Override
                        public long length() {
                            return input.length();
                        }

                        @Override
                        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
                            throw new UnsupportedOperationException("slices are not supported");
                        }

                        @Override
                        public byte readByte() throws IOException {
                            throw new UnsupportedOperationException("use a buffer if you wanna perform well");
                        }

                        @Override
                        public void readBytes(byte[] b, int offset, int len) throws IOException {
                            input.readBytes(b, offset, len);
                            index.addRecoveredBytesToFile(dest, len);
                        }
                    };
                }
            }, src, dest, context);
            if (copies.get() == false) {
                index.addFileDetail(dest, l, true);
            } else {
                assert index.getFileDetails(dest) != null : "File [" + dest + "] has no file details";
                assert index.getFileDetails(dest).recovered() == l : index.getFileDetails(dest).toString();
            }
        }
    }

    boolean recoverFromRepository(final IndexShard indexShard, Repository repository) {
        if (canRecover(indexShard)) {
            RecoverySource.Type recoveryType = indexShard.recoveryState().getRecoverySource().getType();
            assert recoveryType == RecoverySource.Type.SNAPSHOT : "expected snapshot recovery type: " + recoveryType;
            SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) indexShard.recoveryState().getRecoverySource();
            return executeRecovery(indexShard, () -> {
                logger.debug("restoring from {} ...", indexShard.recoveryState().getRecoverySource());
                restore(indexShard, repository, recoverySource);
            });
        }
        return false;
    }

    private boolean canRecover(IndexShard indexShard) {
        if (indexShard.state() == IndexShardState.CLOSED) {
            return false;
        }
        if (indexShard.routingEntry().primary() == false) {
            throw new IndexShardRecoveryException(shardId, "Trying to recover when the shard is in backup state", null);
        }
        return true;
    }

    private boolean executeRecovery(final IndexShard indexShard, Runnable recoveryRunnable) throws IndexShardRecoveryException {
        try {
            recoveryRunnable.run();
            final IndexShardState shardState = indexShard.state();
            final RecoveryState recoveryState = indexShard.recoveryState();
            assert shardState != IndexShardState.CREATED && shardState != IndexShardState.RECOVERING : "recovery process of " + shardId + " didn't get to post_recovery. shardState [" + shardState + "]";
            if (logger.isTraceEnabled()) {
                RecoveryState.Index index = recoveryState.getIndex();
                StringBuilder sb = new StringBuilder();
                sb.append("    index    : files           [").append(index.totalFileCount()).append("] with total_size [").append(new ByteSizeValue(index.totalBytes())).append("], took[").append(TimeValue.timeValueMillis(index.time())).append("]\n");
                sb.append("             : recovered_files [").append(index.recoveredFileCount()).append("] with total_size [").append(new ByteSizeValue(index.recoveredBytes())).append("]\n");
                sb.append("             : reusing_files   [").append(index.reusedFileCount()).append("] with total_size [").append(new ByteSizeValue(index.reusedBytes())).append("]\n");
                sb.append("    verify_index    : took [").append(TimeValue.timeValueMillis(recoveryState.getVerifyIndex().time())).append("], check_index [").append(timeValueMillis(recoveryState.getVerifyIndex().checkIndexTime())).append("]\n");
                sb.append("    translog : number_of_operations [").append(recoveryState.getTranslog().recoveredOperations()).append("], took [").append(TimeValue.timeValueMillis(recoveryState.getTranslog().time())).append("]");
                logger.trace("recovery completed from [shard_store], took [{}]\n{}", timeValueMillis(recoveryState.getTimer().time()), sb);
            } else if (logger.isDebugEnabled()) {
                logger.debug("recovery completed from [shard_store], took [{}]", timeValueMillis(recoveryState.getTimer().time()));
            }
            return true;
        } catch (IndexShardRecoveryException e) {
            if (indexShard.state() == IndexShardState.CLOSED) {
                return false;
            }
            if ((e.getCause() instanceof IndexShardClosedException) || (e.getCause() instanceof IndexShardNotStartedException)) {
                return false;
            }
            throw e;
        } catch (IndexShardClosedException | IndexShardNotStartedException e) {
        } catch (Exception e) {
            if (indexShard.state() == IndexShardState.CLOSED) {
                return false;
            }
            throw new IndexShardRecoveryException(shardId, "failed recovery", e);
        }
        return false;
    }

    private void internalRecoverFromStore(IndexShard indexShard) throws IndexShardRecoveryException {
        final RecoveryState recoveryState = indexShard.recoveryState();
        final boolean indexShouldExists = recoveryState.getRecoverySource().getType() != RecoverySource.Type.EMPTY_STORE;
        indexShard.prepareForIndexRecovery();
        long version = -1;
        SegmentInfos si = null;
        final Store store = indexShard.store();
        store.incRef();
        try {
            try {
                store.failIfCorrupted();
                try {
                    si = store.readLastCommittedSegmentsInfo();
                } catch (Exception e) {
                    String files = "_unknown_";
                    try {
                        files = Arrays.toString(store.directory().listAll());
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        files += " (failure=" + ExceptionsHelper.detailedMessage(inner) + ")";
                    }
                    if (indexShouldExists) {
                        throw new IndexShardRecoveryException(shardId, "shard allocated for local recovery (post api), should exist, but doesn't, current files: " + files, e);
                    }
                }
                if (si != null) {
                    if (indexShouldExists) {
                        version = si.getVersion();
                    } else {
                        logger.trace("cleaning existing shard, shouldn't exists");
                        Lucene.cleanLuceneIndex(store.directory());
                        si = null;
                    }
                }
            } catch (Exception e) {
                throw new IndexShardRecoveryException(shardId, "failed to fetch index version after copying it over", e);
            }
            recoveryState.getIndex().updateVersion(version);
            if (recoveryState.getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
                assert indexShouldExists;
                store.bootstrapNewHistory();
                final SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
                final long maxSeqNo = Long.parseLong(segmentInfos.userData.get(SequenceNumbers.MAX_SEQ_NO));
                final String translogUUID = Translog.createEmptyTranslog(indexShard.shardPath().resolveTranslog(), maxSeqNo, shardId, indexShard.getPendingPrimaryTerm());
                store.associateIndexWithNewTranslog(translogUUID);
            } else if (indexShouldExists) {
                try {
                    final RecoveryState.Index index = recoveryState.getIndex();
                    if (si != null) {
                        addRecoveredFileDetails(si, store, index);
                    }
                } catch (IOException e) {
                    logger.debug("failed to list file details", e);
                }
            } else {
                store.createEmpty();
                final String translogUUID = Translog.createEmptyTranslog(indexShard.shardPath().resolveTranslog(), SequenceNumbers.NO_OPS_PERFORMED, shardId, indexShard.getPendingPrimaryTerm());
                store.associateIndexWithNewTranslog(translogUUID);
            }
            indexShard.openEngineAndRecoverFromTranslog();
            indexShard.getEngine().fillSeqNoGaps(indexShard.getPendingPrimaryTerm());
            indexShard.finalizeRecovery();
            indexShard.postRecovery("post recovery from shard_store");
        } catch (EngineException | IOException e) {
            throw new IndexShardRecoveryException(shardId, "failed to recover from gateway", e);
        } finally {
            store.decRef();
        }
    }

    private void addRecoveredFileDetails(SegmentInfos si, Store store, RecoveryState.Index index) throws IOException {
        final Directory directory = store.directory();
        for (String name : Lucene.files(si)) {
            long length = directory.fileLength(name);
            index.addFileDetail(name, length, true);
        }
    }

    private void restore(final IndexShard indexShard, final Repository repository, final SnapshotRecoverySource restoreSource) {
        final RecoveryState.Translog translogState = indexShard.recoveryState().getTranslog();
        if (restoreSource == null) {
            throw new IndexShardRestoreFailedException(shardId, "empty restore source");
        }
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] restoring shard [{}]", restoreSource.snapshot(), shardId);
        }
        try {
            translogState.totalOperations(0);
            translogState.totalOperationsOnStart(0);
            indexShard.prepareForIndexRecovery();
            ShardId snapshotShardId = shardId;
            final String indexName = restoreSource.index();
            if (!shardId.getIndexName().equals(indexName)) {
                snapshotShardId = new ShardId(indexName, IndexMetaData.INDEX_UUID_NA_VALUE, shardId.id());
            }
            final IndexId indexId = repository.getRepositoryData().resolveIndexId(indexName);
            repository.restoreShard(indexShard, restoreSource.snapshot().getSnapshotId(), restoreSource.version(), indexId, snapshotShardId, indexShard.recoveryState());
            final Store store = indexShard.store();
            store.bootstrapNewHistory();
            final SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
            final long maxSeqNo = Long.parseLong(segmentInfos.userData.get(SequenceNumbers.MAX_SEQ_NO));
            final String translogUUID = Translog.createEmptyTranslog(indexShard.shardPath().resolveTranslog(), maxSeqNo, shardId, indexShard.getPendingPrimaryTerm());
            store.associateIndexWithNewTranslog(translogUUID);
            assert indexShard.shardRouting.primary() : "only primary shards can recover from store";
            indexShard.openEngineAndRecoverFromTranslog();
            indexShard.getEngine().fillSeqNoGaps(indexShard.getPendingPrimaryTerm());
            indexShard.finalizeRecovery();
            indexShard.postRecovery("restore done");
        } catch (Exception e) {
            throw new IndexShardRestoreFailedException(shardId, "restore failed", e);
        }
    }
}