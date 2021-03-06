package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.RateLimitingInputStream;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.snapshots.InvalidSnapshotNameException;
import org.elasticsearch.snapshots.SnapshotCreationException;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo.canonicalName;

public abstract class BlobStoreRepository extends AbstractLifecycleComponent implements Repository {

    private BlobContainer snapshotsBlobContainer;

    protected final RepositoryMetaData metadata;

    protected final NamedXContentRegistry namedXContentRegistry;

    private static final int BUFFER_SIZE = 4096;

    private static final String SNAPSHOT_PREFIX = "snap-";

    private static final String SNAPSHOT_CODEC = "snapshot";

    private static final String INDEX_FILE_PREFIX = "index-";

    private static final String INDEX_LATEST_BLOB = "index.latest";

    private static final String INCOMPATIBLE_SNAPSHOTS_BLOB = "incompatible-snapshots";

    private static final String TESTS_FILE = "tests-";

    private static final String METADATA_NAME_FORMAT = "meta-%s.dat";

    private static final String METADATA_CODEC = "metadata";

    private static final String INDEX_METADATA_CODEC = "index-metadata";

    private static final String SNAPSHOT_NAME_FORMAT = SNAPSHOT_PREFIX + "%s.dat";

    private static final String SNAPSHOT_INDEX_PREFIX = "index-";

    private static final String SNAPSHOT_INDEX_NAME_FORMAT = SNAPSHOT_INDEX_PREFIX + "%s";

    private static final String SNAPSHOT_INDEX_CODEC = "snapshots";

    private static final String DATA_BLOB_PREFIX = "__";

    private final RateLimiter snapshotRateLimiter;

    private final RateLimiter restoreRateLimiter;

    private final CounterMetric snapshotRateLimitingTimeInNanos = new CounterMetric();

    private final CounterMetric restoreRateLimitingTimeInNanos = new CounterMetric();

    private ChecksumBlobStoreFormat<MetaData> globalMetaDataFormat;

    private ChecksumBlobStoreFormat<IndexMetaData> indexMetaDataFormat;

    private ChecksumBlobStoreFormat<SnapshotInfo> snapshotFormat;

    private final boolean readOnly;

    private final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshot> indexShardSnapshotFormat;

    private final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshots> indexShardSnapshotsFormat;

    protected BlobStoreRepository(RepositoryMetaData metadata, Settings globalSettings, NamedXContentRegistry namedXContentRegistry) {
        super(globalSettings);
        this.metadata = metadata;
        this.namedXContentRegistry = namedXContentRegistry;
        snapshotRateLimiter = getRateLimiter(metadata.settings(), "max_snapshot_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB));
        restoreRateLimiter = getRateLimiter(metadata.settings(), "max_restore_bytes_per_sec", new ByteSizeValue(40, ByteSizeUnit.MB));
        readOnly = metadata.settings().getAsBoolean("readonly", false);
        indexShardSnapshotFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_CODEC, SNAPSHOT_NAME_FORMAT, BlobStoreIndexShardSnapshot::fromXContent, namedXContentRegistry, isCompress());
        indexShardSnapshotsFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_INDEX_CODEC, SNAPSHOT_INDEX_NAME_FORMAT, BlobStoreIndexShardSnapshots::fromXContent, namedXContentRegistry, isCompress());
        ByteSizeValue chunkSize = chunkSize();
        if (chunkSize != null && chunkSize.getBytes() <= 0) {
            throw new IllegalArgumentException("the chunk size cannot be negative: [" + chunkSize + "]");
        }
    }

    @Override
    protected void doStart() {
        this.snapshotsBlobContainer = blobStore().blobContainer(basePath());
        globalMetaDataFormat = new ChecksumBlobStoreFormat<>(METADATA_CODEC, METADATA_NAME_FORMAT, MetaData::fromXContent, namedXContentRegistry, isCompress());
        indexMetaDataFormat = new ChecksumBlobStoreFormat<>(INDEX_METADATA_CODEC, METADATA_NAME_FORMAT, IndexMetaData::fromXContent, namedXContentRegistry, isCompress());
        snapshotFormat = new ChecksumBlobStoreFormat<>(SNAPSHOT_CODEC, SNAPSHOT_NAME_FORMAT, SnapshotInfo::fromXContentInternal, namedXContentRegistry, isCompress());
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
        try {
            blobStore().close();
        } catch (Exception t) {
            logger.warn("cannot close blob store", t);
        }
    }

    protected abstract BlobStore blobStore();

    protected abstract BlobPath basePath();

    protected boolean isCompress() {
        return false;
    }

    protected ByteSizeValue chunkSize() {
        return null;
    }

    @Override
    public RepositoryMetaData getMetadata() {
        return metadata;
    }

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData clusterMetaData) {
        if (isReadOnly()) {
            throw new RepositoryException(metadata.name(), "cannot create snapshot in a readonly repository");
        }
        try {
            final String snapshotName = snapshotId.getName();
            final RepositoryData repositoryData = getRepositoryData();
            if (repositoryData.getAllSnapshotIds().stream().anyMatch(s -> s.getName().equals(snapshotName))) {
                throw new InvalidSnapshotNameException(metadata.name(), snapshotId.getName(), "snapshot with the same name already exists");
            }
            if (snapshotFormat.exists(snapshotsBlobContainer, snapshotId.getUUID())) {
                throw new InvalidSnapshotNameException(metadata.name(), snapshotId.getName(), "snapshot with the same name already exists");
            }
            globalMetaDataFormat.write(clusterMetaData, snapshotsBlobContainer, snapshotId.getUUID());
            for (IndexId index : indices) {
                final IndexMetaData indexMetaData = clusterMetaData.index(index.getName());
                final BlobPath indexPath = basePath().add("indices").add(index.getId());
                final BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(indexPath);
                indexMetaDataFormat.write(indexMetaData, indexMetaDataBlobContainer, snapshotId.getUUID());
            }
        } catch (IOException ex) {
            throw new SnapshotCreationException(metadata.name(), snapshotId, ex);
        }
    }

    @Override
    public void deleteSnapshot(SnapshotId snapshotId, long repositoryStateId) {
        if (isReadOnly()) {
            throw new RepositoryException(metadata.name(), "cannot delete snapshot from a readonly repository");
        }
        final RepositoryData repositoryData = getRepositoryData();
        SnapshotInfo snapshot = null;
        try {
            snapshot = getSnapshotInfo(snapshotId);
        } catch (SnapshotMissingException ex) {
            throw ex;
        } catch (IllegalStateException | SnapshotException | ElasticsearchParseException ex) {
            logger.warn(() -> new ParameterizedMessage("cannot read snapshot file [{}]", snapshotId), ex);
        }
        try {
            final RepositoryData updatedRepositoryData = repositoryData.removeSnapshot(snapshotId);
            writeIndexGen(updatedRepositoryData, repositoryStateId);
            deleteSnapshotBlobIgnoringErrors(snapshot, snapshotId.getUUID());
            deleteGlobalMetaDataBlobIgnoringErrors(snapshot, snapshotId.getUUID());
            if (snapshot != null) {
                final List<String> indices = snapshot.indices();
                for (String index : indices) {
                    final IndexId indexId = repositoryData.resolveIndexId(index);
                    IndexMetaData indexMetaData = null;
                    try {
                        indexMetaData = getSnapshotIndexMetaData(snapshotId, indexId);
                    } catch (ElasticsearchParseException | IOException ex) {
                        logger.warn(() -> new ParameterizedMessage("[{}] [{}] failed to read metadata for index", snapshotId, index), ex);
                    }
                    deleteIndexMetaDataBlobIgnoringErrors(snapshot, indexId);
                    if (indexMetaData != null) {
                        for (int shardId = 0; shardId < indexMetaData.getNumberOfShards(); shardId++) {
                            try {
                                delete(snapshotId, snapshot.version(), indexId, new ShardId(indexMetaData.getIndex(), shardId));
                            } catch (SnapshotException ex) {
                                final int finalShardId = shardId;
                                logger.warn(() -> new ParameterizedMessage("[{}] failed to delete shard data for shard [{}][{}]", snapshotId, index, finalShardId), ex);
                            }
                        }
                    }
                }
            }
            final Collection<IndexId> indicesToCleanUp = Sets.newHashSet(repositoryData.getIndices().values());
            indicesToCleanUp.removeAll(updatedRepositoryData.getIndices().values());
            final BlobContainer indicesBlobContainer = blobStore().blobContainer(basePath().add("indices"));
            for (final IndexId indexId : indicesToCleanUp) {
                try {
                    indicesBlobContainer.deleteBlob(indexId.getId());
                } catch (DirectoryNotEmptyException dnee) {
                    logger.debug(() -> new ParameterizedMessage("[{}] index [{}] no longer part of any snapshots in the repository, but failed to clean up " + "its index folder due to the directory not being empty.", metadata.name(), indexId), dnee);
                } catch (IOException ioe) {
                    logger.debug(() -> new ParameterizedMessage("[{}] index [{}] no longer part of any snapshots in the repository, but failed to clean up " + "its index folder.", metadata.name(), indexId), ioe);
                }
            }
        } catch (IOException | ResourceNotFoundException ex) {
            throw new RepositoryException(metadata.name(), "failed to delete snapshot [" + snapshotId + "]", ex);
        }
    }

    private void deleteSnapshotBlobIgnoringErrors(final SnapshotInfo snapshotInfo, final String blobId) {
        try {
            snapshotFormat.delete(snapshotsBlobContainer, blobId);
        } catch (IOException e) {
            if (snapshotInfo != null) {
                logger.warn(() -> new ParameterizedMessage("[{}] Unable to delete snapshot file [{}]", snapshotInfo.snapshotId(), blobId), e);
            } else {
                logger.warn(() -> new ParameterizedMessage("Unable to delete snapshot file [{}]", blobId), e);
            }
        }
    }

    private void deleteGlobalMetaDataBlobIgnoringErrors(final SnapshotInfo snapshotInfo, final String blobId) {
        try {
            globalMetaDataFormat.delete(snapshotsBlobContainer, blobId);
        } catch (IOException e) {
            if (snapshotInfo != null) {
                logger.warn(() -> new ParameterizedMessage("[{}] Unable to delete global metadata file [{}]", snapshotInfo.snapshotId(), blobId), e);
            } else {
                logger.warn(() -> new ParameterizedMessage("Unable to delete global metadata file [{}]", blobId), e);
            }
        }
    }

    private void deleteIndexMetaDataBlobIgnoringErrors(final SnapshotInfo snapshotInfo, final IndexId indexId) {
        final SnapshotId snapshotId = snapshotInfo.snapshotId();
        BlobContainer indexMetaDataBlobContainer = blobStore().blobContainer(basePath().add("indices").add(indexId.getId()));
        try {
            indexMetaDataFormat.delete(indexMetaDataBlobContainer, snapshotId.getUUID());
        } catch (IOException ex) {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to delete metadata for index [{}]", snapshotId, indexId.getName()), ex);
        }
    }

    @Override
    public SnapshotInfo finalizeSnapshot(final SnapshotId snapshotId, final List<IndexId> indices, final long startTime, final String failure, final int totalShards, final List<SnapshotShardFailure> shardFailures, final long repositoryStateId, final boolean includeGlobalState) {
        SnapshotInfo blobStoreSnapshot = new SnapshotInfo(snapshotId, indices.stream().map(IndexId::getName).collect(Collectors.toList()), startTime, failure, System.currentTimeMillis(), totalShards, shardFailures, includeGlobalState);
        try {
            snapshotFormat.write(blobStoreSnapshot, snapshotsBlobContainer, snapshotId.getUUID());
            final RepositoryData repositoryData = getRepositoryData();
            writeIndexGen(repositoryData.addSnapshot(snapshotId, blobStoreSnapshot.state(), indices), repositoryStateId);
        } catch (FileAlreadyExistsException ex) {
            throw new RepositoryException(metadata.name(), "Blob already exists while " + "finalizing snapshot, assume the snapshot has already been saved", ex);
        } catch (IOException ex) {
            throw new RepositoryException(metadata.name(), "failed to update snapshot in repository", ex);
        }
        return blobStoreSnapshot;
    }

    @Override
    public SnapshotInfo getSnapshotInfo(final SnapshotId snapshotId) {
        try {
            return snapshotFormat.read(snapshotsBlobContainer, snapshotId.getUUID());
        } catch (NoSuchFileException ex) {
            throw new SnapshotMissingException(metadata.name(), snapshotId, ex);
        } catch (IOException | NotXContentException ex) {
            throw new SnapshotException(metadata.name(), snapshotId, "failed to get snapshots", ex);
        }
    }

    @Override
    public MetaData getSnapshotGlobalMetaData(final SnapshotId snapshotId) {
        try {
            return globalMetaDataFormat.read(snapshotsBlobContainer, snapshotId.getUUID());
        } catch (NoSuchFileException ex) {
            throw new SnapshotMissingException(metadata.name(), snapshotId, ex);
        } catch (IOException ex) {
            throw new SnapshotException(metadata.name(), snapshotId, "failed to read global metadata", ex);
        }
    }

    @Override
    public IndexMetaData getSnapshotIndexMetaData(final SnapshotId snapshotId, final IndexId index) throws IOException {
        final BlobPath indexPath = basePath().add("indices").add(index.getId());
        return indexMetaDataFormat.read(blobStore().blobContainer(indexPath), snapshotId.getUUID());
    }

    private RateLimiter getRateLimiter(Settings repositorySettings, String setting, ByteSizeValue defaultRate) {
        ByteSizeValue maxSnapshotBytesPerSec = repositorySettings.getAsBytesSize(setting, settings.getAsBytesSize(setting, defaultRate));
        if (maxSnapshotBytesPerSec.getBytes() <= 0) {
            return null;
        } else {
            return new RateLimiter.SimpleRateLimiter(maxSnapshotBytesPerSec.getMbFrac());
        }
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        return snapshotRateLimitingTimeInNanos.count();
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return restoreRateLimitingTimeInNanos.count();
    }

    @Override
    public String startVerification() {
        try {
            if (isReadOnly()) {
                return null;
            } else {
                String seed = UUIDs.randomBase64UUID();
                byte[] testBytes = Strings.toUTF8Bytes(seed);
                BlobContainer testContainer = blobStore().blobContainer(basePath().add(testBlobPrefix(seed)));
                String blobName = "master.dat";
                BytesArray bytes = new BytesArray(testBytes);
                try (InputStream stream = bytes.streamInput()) {
                    testContainer.writeBlobAtomic(blobName, stream, bytes.length(), true);
                }
                return seed;
            }
        } catch (IOException exp) {
            throw new RepositoryVerificationException(metadata.name(), "path " + basePath() + " is not accessible on master node", exp);
        }
    }

    @Override
    public void endVerification(String seed) {
        if (isReadOnly()) {
            throw new UnsupportedOperationException("shouldn't be called");
        }
        try {
            blobStore().delete(basePath().add(testBlobPrefix(seed)));
        } catch (IOException exp) {
            throw new RepositoryVerificationException(metadata.name(), "cannot delete test data at " + basePath(), exp);
        }
    }

    @Override
    public RepositoryData getRepositoryData() {
        try {
            final long indexGen = latestIndexBlobId();
            final String snapshotsIndexBlobName = INDEX_FILE_PREFIX + Long.toString(indexGen);
            RepositoryData repositoryData;
            try (InputStream blob = snapshotsBlobContainer.readBlob(snapshotsIndexBlobName)) {
                BytesStreamOutput out = new BytesStreamOutput();
                Streams.copy(blob, out);
                try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, out.bytes(), XContentType.JSON)) {
                    repositoryData = RepositoryData.snapshotsFromXContent(parser, indexGen);
                } catch (NotXContentException e) {
                    logger.warn("[{}] index blob is not valid x-content [{} bytes]", snapshotsIndexBlobName, out.bytes().length());
                    throw e;
                }
            }
            try (InputStream blob = snapshotsBlobContainer.readBlob(INCOMPATIBLE_SNAPSHOTS_BLOB)) {
                BytesStreamOutput out = new BytesStreamOutput();
                Streams.copy(blob, out);
                try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, out.bytes(), XContentType.JSON)) {
                    repositoryData = repositoryData.incompatibleSnapshotsFromXContent(parser);
                }
            } catch (NoSuchFileException e) {
                if (isReadOnly()) {
                    logger.debug("[{}] Incompatible snapshots blob [{}] does not exist, the likely " + "reason is that there are no incompatible snapshots in the repository", metadata.name(), INCOMPATIBLE_SNAPSHOTS_BLOB);
                } else {
                    writeIncompatibleSnapshots(RepositoryData.EMPTY);
                }
            }
            return repositoryData;
        } catch (NoSuchFileException ex) {
            return RepositoryData.EMPTY;
        } catch (IOException ioe) {
            throw new RepositoryException(metadata.name(), "could not read repository data from index blob", ioe);
        }
    }

    public static String testBlobPrefix(String seed) {
        return TESTS_FILE + seed;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    BlobContainer blobContainer() {
        return snapshotsBlobContainer;
    }

    protected void writeIndexGen(final RepositoryData repositoryData, final long repositoryStateId) throws IOException {
        assert isReadOnly() == false;
        final long currentGen = latestIndexBlobId();
        if (repositoryStateId != SnapshotsInProgress.UNDEFINED_REPOSITORY_STATE_ID && currentGen != repositoryStateId) {
            throw new RepositoryException(metadata.name(), "concurrent modification of the index-N file, expected current generation [" + repositoryStateId + "], actual current generation [" + currentGen + "] - possibly due to simultaneous snapshot deletion requests");
        }
        final long newGen = currentGen + 1;
        final BytesReference snapshotsBytes;
        try (BytesStreamOutput bStream = new BytesStreamOutput()) {
            try (StreamOutput stream = new OutputStreamStreamOutput(bStream)) {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, stream);
                repositoryData.snapshotsToXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.close();
            }
            snapshotsBytes = bStream.bytes();
        }
        final String indexBlob = INDEX_FILE_PREFIX + Long.toString(newGen);
        logger.debug("Repository [{}] writing new index generational blob [{}]", metadata.name(), indexBlob);
        writeAtomic(indexBlob, snapshotsBytes, true);
        if (isReadOnly() == false && newGen - 2 >= 0) {
            final String oldSnapshotIndexFile = INDEX_FILE_PREFIX + Long.toString(newGen - 2);
            snapshotsBlobContainer.deleteBlobIgnoringIfNotExists(oldSnapshotIndexFile);
        }
        final BytesReference genBytes;
        try (BytesStreamOutput bStream = new BytesStreamOutput()) {
            bStream.writeLong(newGen);
            genBytes = bStream.bytes();
        }
        logger.debug("Repository [{}] updating index.latest with generation [{}]", metadata.name(), newGen);
        writeAtomic(INDEX_LATEST_BLOB, genBytes, false);
    }

    void writeIncompatibleSnapshots(RepositoryData repositoryData) throws IOException {
        assert isReadOnly() == false;
        final BytesReference bytes;
        try (BytesStreamOutput bStream = new BytesStreamOutput()) {
            try (StreamOutput stream = new OutputStreamStreamOutput(bStream)) {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, stream);
                repositoryData.incompatibleSnapshotsToXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.close();
            }
            bytes = bStream.bytes();
        }
        writeAtomic(INCOMPATIBLE_SNAPSHOTS_BLOB, bytes, false);
    }

    long latestIndexBlobId() throws IOException {
        try {
            return listBlobsToGetLatestIndexId();
        } catch (UnsupportedOperationException e) {
            try {
                return readSnapshotIndexLatestBlob();
            } catch (NoSuchFileException nsfe) {
                return RepositoryData.EMPTY_REPO_GEN;
            }
        }
    }

    long readSnapshotIndexLatestBlob() throws IOException {
        try (InputStream blob = snapshotsBlobContainer.readBlob(INDEX_LATEST_BLOB)) {
            BytesStreamOutput out = new BytesStreamOutput();
            Streams.copy(blob, out);
            return Numbers.bytesToLong(out.bytes().toBytesRef());
        }
    }

    private long listBlobsToGetLatestIndexId() throws IOException {
        Map<String, BlobMetaData> blobs = snapshotsBlobContainer.listBlobsByPrefix(INDEX_FILE_PREFIX);
        long latest = RepositoryData.EMPTY_REPO_GEN;
        if (blobs.isEmpty()) {
            return latest;
        }
        for (final BlobMetaData blobMetaData : blobs.values()) {
            final String blobName = blobMetaData.name();
            try {
                final long curr = Long.parseLong(blobName.substring(INDEX_FILE_PREFIX.length()));
                latest = Math.max(latest, curr);
            } catch (NumberFormatException nfe) {
                logger.debug("[{}] Unknown blob in the repository: {}", metadata.name(), blobName);
            }
        }
        return latest;
    }

    private void writeAtomic(final String blobName, final BytesReference bytesRef, boolean failIfAlreadyExists) throws IOException {
        try (InputStream stream = bytesRef.streamInput()) {
            snapshotsBlobContainer.writeBlobAtomic(blobName, stream, bytesRef.length(), failIfAlreadyExists);
        }
    }

    @Override
    public void snapshotShard(IndexShard shard, SnapshotId snapshotId, IndexId indexId, IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus) {
        SnapshotContext snapshotContext = new SnapshotContext(shard, snapshotId, indexId, snapshotStatus, System.currentTimeMillis());
        try {
            snapshotContext.snapshot(snapshotIndexCommit);
        } catch (Exception e) {
            snapshotStatus.moveToFailed(System.currentTimeMillis(), ExceptionsHelper.detailedMessage(e));
            if (e instanceof IndexShardSnapshotFailedException) {
                throw (IndexShardSnapshotFailedException) e;
            } else {
                throw new IndexShardSnapshotFailedException(shard.shardId(), e);
            }
        }
    }

    @Override
    public void restoreShard(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId, RecoveryState recoveryState) {
        final RestoreContext snapshotContext = new RestoreContext(shard, snapshotId, version, indexId, snapshotShardId, recoveryState);
        try {
            snapshotContext.restore();
        } catch (Exception e) {
            throw new IndexShardRestoreFailedException(shard.shardId(), "failed to restore snapshot [" + snapshotId + "]", e);
        }
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
        Context context = new Context(snapshotId, version, indexId, shardId);
        BlobStoreIndexShardSnapshot snapshot = context.loadSnapshot();
        return IndexShardSnapshotStatus.newDone(snapshot.startTime(), snapshot.time(), snapshot.incrementalFileCount(), snapshot.totalFileCount(), snapshot.incrementalSize(), snapshot.totalSize());
    }

    @Override
    public void verify(String seed, DiscoveryNode localNode) {
        BlobContainer testBlobContainer = blobStore().blobContainer(basePath().add(testBlobPrefix(seed)));
        if (testBlobContainer.blobExists("master.dat")) {
            try {
                BytesArray bytes = new BytesArray(seed);
                try (InputStream stream = bytes.streamInput()) {
                    testBlobContainer.writeBlob("data-" + localNode.getId() + ".dat", stream, bytes.length(), true);
                }
            } catch (IOException exp) {
                throw new RepositoryVerificationException(metadata.name(), "store location [" + blobStore() + "] is not accessible on the node [" + localNode + "]", exp);
            }
        } else {
            throw new RepositoryVerificationException(metadata.name(), "a file written by master to the store [" + blobStore() + "] cannot be accessed on the node [" + localNode + "]. " + "This might indicate that the store [" + blobStore() + "] is not shared between this node and the master node or " + "that permissions on the store don't allow reading files written by the master node");
        }
    }

    private void delete(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
        Context context = new Context(snapshotId, version, indexId, shardId, shardId);
        context.delete();
    }

    @Override
    public String toString() {
        return "BlobStoreRepository[" + "[" + metadata.name() + "], [" + blobStore() + ']' + ']';
    }

    BlobStoreFormat<BlobStoreIndexShardSnapshot> indexShardSnapshotFormat(Version version) {
        return indexShardSnapshotFormat;
    }

    private class Context {

        protected final SnapshotId snapshotId;

        protected final ShardId shardId;

        protected final BlobContainer blobContainer;

        protected final Version version;

        Context(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
            this(snapshotId, version, indexId, shardId, shardId);
        }

        Context(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId, ShardId snapshotShardId) {
            this.snapshotId = snapshotId;
            this.version = version;
            this.shardId = shardId;
            blobContainer = blobStore().blobContainer(basePath().add("indices").add(indexId.getId()).add(Integer.toString(snapshotShardId.getId())));
        }

        public void delete() {
            final Map<String, BlobMetaData> blobs;
            try {
                blobs = blobContainer.listBlobs();
            } catch (IOException e) {
                throw new IndexShardSnapshotException(shardId, "Failed to list content of gateway", e);
            }
            Tuple<BlobStoreIndexShardSnapshots, Integer> tuple = buildBlobStoreIndexShardSnapshots(blobs);
            BlobStoreIndexShardSnapshots snapshots = tuple.v1();
            int fileListGeneration = tuple.v2();
            try {
                indexShardSnapshotFormat(version).delete(blobContainer, snapshotId.getUUID());
            } catch (IOException e) {
                logger.debug("[{}] [{}] failed to delete shard snapshot file", shardId, snapshotId);
            }
            List<SnapshotFiles> newSnapshotsList = new ArrayList<>();
            for (SnapshotFiles point : snapshots) {
                if (!point.snapshot().equals(snapshotId.getName())) {
                    newSnapshotsList.add(point);
                }
            }
            finalize(newSnapshotsList, fileListGeneration + 1, blobs, "snapshot deletion [" + snapshotId + "]");
        }

        BlobStoreIndexShardSnapshot loadSnapshot() {
            try {
                return indexShardSnapshotFormat(version).read(blobContainer, snapshotId.getUUID());
            } catch (IOException ex) {
                throw new SnapshotException(metadata.name(), snapshotId, "failed to read shard snapshot file for " + shardId, ex);
            }
        }

        protected void finalize(final List<SnapshotFiles> snapshots, final int fileListGeneration, final Map<String, BlobMetaData> blobs, final String reason) {
            final String indexGeneration = Integer.toString(fileListGeneration);
            final String currentIndexGen = indexShardSnapshotsFormat.blobName(indexGeneration);
            final BlobStoreIndexShardSnapshots updatedSnapshots = new BlobStoreIndexShardSnapshots(snapshots);
            try {
                for (final String blobName : blobs.keySet()) {
                    if (FsBlobContainer.isTempBlobName(blobName)) {
                        try {
                            blobContainer.deleteBlobIgnoringIfNotExists(blobName);
                        } catch (IOException e) {
                            logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to delete index blob [{}] during finalization", snapshotId, shardId, blobName), e);
                            throw e;
                        }
                    }
                }
                if (snapshots.size() > 0) {
                    indexShardSnapshotsFormat.writeAtomic(updatedSnapshots, blobContainer, indexGeneration);
                }
                for (final String blobName : blobs.keySet()) {
                    if (blobName.startsWith(SNAPSHOT_INDEX_PREFIX)) {
                        try {
                            blobContainer.deleteBlobIgnoringIfNotExists(blobName);
                        } catch (IOException e) {
                            logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to delete index blob [{}] during finalization", snapshotId, shardId, blobName), e);
                            throw e;
                        }
                    }
                }
                for (final String blobName : blobs.keySet()) {
                    if (blobName.startsWith(DATA_BLOB_PREFIX) && (updatedSnapshots.findNameFile(canonicalName(blobName)) == null)) {
                        try {
                            blobContainer.deleteBlobIgnoringIfNotExists(blobName);
                        } catch (IOException e) {
                            logger.warn(() -> new ParameterizedMessage("[{}][{}] failed to delete data blob [{}] during finalization", snapshotId, shardId, blobName), e);
                        }
                    }
                }
            } catch (IOException e) {
                String message = "Failed to finalize " + reason + " with shard index [" + currentIndexGen + "]";
                throw new IndexShardSnapshotFailedException(shardId, message, e);
            }
        }

        protected String fileNameFromGeneration(long generation) {
            return DATA_BLOB_PREFIX + Long.toString(generation, Character.MAX_RADIX);
        }

        protected long findLatestFileNameGeneration(Map<String, BlobMetaData> blobs) {
            long generation = -1;
            for (String name : blobs.keySet()) {
                if (!name.startsWith(DATA_BLOB_PREFIX)) {
                    continue;
                }
                name = canonicalName(name);
                try {
                    long currentGen = Long.parseLong(name.substring(DATA_BLOB_PREFIX.length()), Character.MAX_RADIX);
                    if (currentGen > generation) {
                        generation = currentGen;
                    }
                } catch (NumberFormatException e) {
                    logger.warn("file [{}] does not conform to the '{}' schema", name, DATA_BLOB_PREFIX);
                }
            }
            return generation;
        }

        protected Tuple<BlobStoreIndexShardSnapshots, Integer> buildBlobStoreIndexShardSnapshots(Map<String, BlobMetaData> blobs) {
            int latest = -1;
            Set<String> blobKeys = blobs.keySet();
            for (String name : blobKeys) {
                if (name.startsWith(SNAPSHOT_INDEX_PREFIX)) {
                    try {
                        int gen = Integer.parseInt(name.substring(SNAPSHOT_INDEX_PREFIX.length()));
                        if (gen > latest) {
                            latest = gen;
                        }
                    } catch (NumberFormatException ex) {
                        logger.warn("failed to parse index file name [{}]", name);
                    }
                }
            }
            if (latest >= 0) {
                try {
                    final BlobStoreIndexShardSnapshots shardSnapshots = indexShardSnapshotsFormat.read(blobContainer, Integer.toString(latest));
                    return new Tuple<>(shardSnapshots, latest);
                } catch (IOException e) {
                    final String file = SNAPSHOT_INDEX_PREFIX + latest;
                    logger.warn(() -> new ParameterizedMessage("failed to read index file [{}]", file), e);
                }
            } else if (blobKeys.isEmpty() == false) {
                logger.debug("Could not find a readable index-N file in a non-empty shard snapshot directory [{}]", blobContainer.path());
            }
            List<SnapshotFiles> snapshots = new ArrayList<>();
            for (String name : blobKeys) {
                try {
                    BlobStoreIndexShardSnapshot snapshot = null;
                    if (name.startsWith(SNAPSHOT_PREFIX)) {
                        snapshot = indexShardSnapshotFormat.readBlob(blobContainer, name);
                    }
                    if (snapshot != null) {
                        snapshots.add(new SnapshotFiles(snapshot.snapshot(), snapshot.indexFiles()));
                    }
                } catch (IOException e) {
                    logger.warn(() -> new ParameterizedMessage("failed to read commit point [{}]", name), e);
                }
            }
            return new Tuple<>(new BlobStoreIndexShardSnapshots(snapshots), -1);
        }
    }

    private class SnapshotContext extends Context {

        private final Store store;

        private final IndexShardSnapshotStatus snapshotStatus;

        private final long startTime;

        SnapshotContext(IndexShard shard, SnapshotId snapshotId, IndexId indexId, IndexShardSnapshotStatus snapshotStatus, long startTime) {
            super(snapshotId, Version.CURRENT, indexId, shard.shardId());
            this.snapshotStatus = snapshotStatus;
            this.store = shard.store();
            this.startTime = startTime;
        }

        public void snapshot(final IndexCommit snapshotIndexCommit) {
            logger.debug("[{}] [{}] snapshot to [{}] ...", shardId, snapshotId, metadata.name());
            final Map<String, BlobMetaData> blobs;
            try {
                blobs = blobContainer.listBlobs();
            } catch (IOException e) {
                throw new IndexShardSnapshotFailedException(shardId, "failed to list blobs", e);
            }
            long generation = findLatestFileNameGeneration(blobs);
            Tuple<BlobStoreIndexShardSnapshots, Integer> tuple = buildBlobStoreIndexShardSnapshots(blobs);
            BlobStoreIndexShardSnapshots snapshots = tuple.v1();
            int fileListGeneration = tuple.v2();
            if (snapshots.snapshots().stream().anyMatch(sf -> sf.snapshot().equals(snapshotId.getName()))) {
                throw new IndexShardSnapshotFailedException(shardId, "Duplicate snapshot name [" + snapshotId.getName() + "] detected, aborting");
            }
            final List<BlobStoreIndexShardSnapshot.FileInfo> indexCommitPointFiles = new ArrayList<>();
            store.incRef();
            int indexIncrementalFileCount = 0;
            int indexTotalNumberOfFiles = 0;
            long indexIncrementalSize = 0;
            long indexTotalFileCount = 0;
            try {
                ArrayList<BlobStoreIndexShardSnapshot.FileInfo> filesToSnapshot = new ArrayList<>();
                final Store.MetadataSnapshot metadata;
                final Collection<String> fileNames;
                try {
                    logger.trace("[{}] [{}] Loading store metadata using index commit [{}]", shardId, snapshotId, snapshotIndexCommit);
                    metadata = store.getMetadata(snapshotIndexCommit);
                    fileNames = snapshotIndexCommit.getFileNames();
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "Failed to get store file metadata", e);
                }
                for (String fileName : fileNames) {
                    if (snapshotStatus.isAborted()) {
                        logger.debug("[{}] [{}] Aborted on the file [{}], exiting", shardId, snapshotId, fileName);
                        throw new IndexShardSnapshotFailedException(shardId, "Aborted");
                    }
                    logger.trace("[{}] [{}] Processing [{}]", shardId, snapshotId, fileName);
                    final StoreFileMetaData md = metadata.get(fileName);
                    BlobStoreIndexShardSnapshot.FileInfo existingFileInfo = null;
                    List<BlobStoreIndexShardSnapshot.FileInfo> filesInfo = snapshots.findPhysicalIndexFiles(fileName);
                    if (filesInfo != null) {
                        for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : filesInfo) {
                            try {
                                maybeRecalculateMetadataHash(blobContainer, fileInfo, metadata);
                            } catch (Exception e) {
                                logger.warn(() -> new ParameterizedMessage("{} Can't calculate hash from blob for file [{}] [{}]", shardId, fileInfo.physicalName(), fileInfo.metadata()), e);
                            }
                            if (fileInfo.isSame(md) && snapshotFileExistsInBlobs(fileInfo, blobs)) {
                                existingFileInfo = fileInfo;
                                break;
                            }
                        }
                    }
                    indexTotalFileCount += md.length();
                    indexTotalNumberOfFiles++;
                    if (existingFileInfo == null) {
                        indexIncrementalFileCount++;
                        indexIncrementalSize += md.length();
                        BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo = new BlobStoreIndexShardSnapshot.FileInfo(fileNameFromGeneration(++generation), md, chunkSize());
                        indexCommitPointFiles.add(snapshotFileInfo);
                        filesToSnapshot.add(snapshotFileInfo);
                    } else {
                        indexCommitPointFiles.add(existingFileInfo);
                    }
                }
                snapshotStatus.moveToStarted(startTime, indexIncrementalFileCount, indexTotalNumberOfFiles, indexIncrementalSize, indexTotalFileCount);
                for (BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo : filesToSnapshot) {
                    try {
                        snapshotFile(snapshotFileInfo);
                    } catch (IOException e) {
                        throw new IndexShardSnapshotFailedException(shardId, "Failed to perform snapshot (index files)", e);
                    }
                }
            } finally {
                store.decRef();
            }
            final IndexShardSnapshotStatus.Copy lastSnapshotStatus = snapshotStatus.moveToFinalize(snapshotIndexCommit.getGeneration());
            final BlobStoreIndexShardSnapshot snapshot = new BlobStoreIndexShardSnapshot(snapshotId.getName(), lastSnapshotStatus.getIndexVersion(), indexCommitPointFiles, lastSnapshotStatus.getStartTime(), System.currentTimeMillis() - lastSnapshotStatus.getStartTime(), lastSnapshotStatus.getIncrementalFileCount(), lastSnapshotStatus.getIncrementalSize());
            logger.trace("[{}] [{}] writing shard snapshot file", shardId, snapshotId);
            try {
                indexShardSnapshotFormat.write(snapshot, blobContainer, snapshotId.getUUID());
            } catch (IOException e) {
                throw new IndexShardSnapshotFailedException(shardId, "Failed to write commit point", e);
            }
            List<SnapshotFiles> newSnapshotsList = new ArrayList<>();
            newSnapshotsList.add(new SnapshotFiles(snapshot.snapshot(), snapshot.indexFiles()));
            for (SnapshotFiles point : snapshots) {
                newSnapshotsList.add(point);
            }
            finalize(newSnapshotsList, fileListGeneration + 1, blobs, "snapshot creation [" + snapshotId + "]");
            snapshotStatus.moveToDone(System.currentTimeMillis());
        }

        private void snapshotFile(final BlobStoreIndexShardSnapshot.FileInfo fileInfo) throws IOException {
            final String file = fileInfo.physicalName();
            try (IndexInput indexInput = store.openVerifyingInput(file, IOContext.READONCE, fileInfo.metadata())) {
                for (int i = 0; i < fileInfo.numberOfParts(); i++) {
                    final long partBytes = fileInfo.partBytes(i);
                    final InputStreamIndexInput inputStreamIndexInput = new InputStreamIndexInput(indexInput, partBytes);
                    InputStream inputStream = inputStreamIndexInput;
                    if (snapshotRateLimiter != null) {
                        inputStream = new RateLimitingInputStream(inputStreamIndexInput, snapshotRateLimiter, snapshotRateLimitingTimeInNanos::inc);
                    }
                    inputStream = new AbortableInputStream(inputStream, fileInfo.physicalName());
                    blobContainer.writeBlob(fileInfo.partName(i), inputStream, partBytes, true);
                }
                Store.verify(indexInput);
                snapshotStatus.addProcessedFile(fileInfo.length());
            } catch (Exception t) {
                failStoreIfCorrupted(t);
                snapshotStatus.addProcessedFile(0);
                throw t;
            }
        }

        private void failStoreIfCorrupted(Exception e) {
            if (e instanceof CorruptIndexException || e instanceof IndexFormatTooOldException || e instanceof IndexFormatTooNewException) {
                try {
                    store.markStoreCorrupted((IOException) e);
                } catch (IOException inner) {
                    inner.addSuppressed(e);
                    logger.warn("store cannot be marked as corrupted", inner);
                }
            }
        }

        private boolean snapshotFileExistsInBlobs(BlobStoreIndexShardSnapshot.FileInfo fileInfo, Map<String, BlobMetaData> blobs) {
            BlobMetaData blobMetaData = blobs.get(fileInfo.name());
            if (blobMetaData != null) {
                return blobMetaData.length() == fileInfo.length();
            } else if (blobs.containsKey(fileInfo.partName(0))) {
                int part = 0;
                long totalSize = 0;
                while (true) {
                    blobMetaData = blobs.get(fileInfo.partName(part++));
                    if (blobMetaData == null) {
                        break;
                    }
                    totalSize += blobMetaData.length();
                }
                return totalSize == fileInfo.length();
            }
            return false;
        }

        private class AbortableInputStream extends FilterInputStream {

            private final String fileName;

            AbortableInputStream(InputStream delegate, String fileName) {
                super(delegate);
                this.fileName = fileName;
            }

            @Override
            public int read() throws IOException {
                checkAborted();
                return in.read();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                checkAborted();
                return in.read(b, off, len);
            }

            private void checkAborted() {
                if (snapshotStatus.isAborted()) {
                    logger.debug("[{}] [{}] Aborted on the file [{}], exiting", shardId, snapshotId, fileName);
                    throw new IndexShardSnapshotFailedException(shardId, "Aborted");
                }
            }
        }
    }

    private static void maybeRecalculateMetadataHash(final BlobContainer blobContainer, final BlobStoreIndexShardSnapshot.FileInfo fileInfo, Store.MetadataSnapshot snapshot) throws Exception {
        final StoreFileMetaData metadata;
        if (fileInfo != null && (metadata = snapshot.get(fileInfo.physicalName())) != null) {
            if (metadata.hash().length > 0 && fileInfo.metadata().hash().length == 0) {
                try (InputStream stream = new PartSliceStream(blobContainer, fileInfo)) {
                    BytesRefBuilder builder = new BytesRefBuilder();
                    Store.MetadataSnapshot.hashFile(builder, stream, fileInfo.length());
                    BytesRef hash = fileInfo.metadata().hash();
                    assert hash.length == 0;
                    hash.bytes = builder.bytes();
                    hash.offset = 0;
                    hash.length = builder.length();
                }
            }
        }
    }

    private static final class PartSliceStream extends SlicedInputStream {

        private final BlobContainer container;

        private final BlobStoreIndexShardSnapshot.FileInfo info;

        PartSliceStream(BlobContainer container, BlobStoreIndexShardSnapshot.FileInfo info) {
            super(info.numberOfParts());
            this.info = info;
            this.container = container;
        }

        @Override
        protected InputStream openSlice(long slice) throws IOException {
            return container.readBlob(info.partName(slice));
        }
    }

    private class RestoreContext extends Context {

        private final IndexShard targetShard;

        private final RecoveryState recoveryState;

        RestoreContext(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId, RecoveryState recoveryState) {
            super(snapshotId, version, indexId, shard.shardId(), snapshotShardId);
            this.recoveryState = recoveryState;
            this.targetShard = shard;
        }

        public void restore() throws IOException {
            final Store store = targetShard.store();
            store.incRef();
            try {
                logger.debug("[{}] [{}] restoring to [{}] ...", snapshotId, metadata.name(), shardId);
                BlobStoreIndexShardSnapshot snapshot = loadSnapshot();
                if (snapshot.indexFiles().size() == 1 && snapshot.indexFiles().get(0).physicalName().startsWith("segments_") && snapshot.indexFiles().get(0).hasUnknownChecksum()) {
                    IndexWriter writer = new IndexWriter(store.directory(), new IndexWriterConfig(null).setOpenMode(IndexWriterConfig.OpenMode.CREATE).setCommitOnClose(true));
                    writer.close();
                    return;
                }
                SnapshotFiles snapshotFiles = new SnapshotFiles(snapshot.snapshot(), snapshot.indexFiles());
                Store.MetadataSnapshot recoveryTargetMetadata;
                try {
                    recoveryTargetMetadata = targetShard.snapshotStoreMetadata();
                } catch (IndexNotFoundException e) {
                    logger.trace("[{}] [{}] restoring from to an empty shard", shardId, snapshotId);
                    recoveryTargetMetadata = Store.MetadataSnapshot.EMPTY;
                } catch (IOException e) {
                    logger.warn(() -> new ParameterizedMessage("{} Can't read metadata from store, will not reuse any local file while restoring", shardId), e);
                    recoveryTargetMetadata = Store.MetadataSnapshot.EMPTY;
                }
                final List<BlobStoreIndexShardSnapshot.FileInfo> filesToRecover = new ArrayList<>();
                final Map<String, StoreFileMetaData> snapshotMetaData = new HashMap<>();
                final Map<String, BlobStoreIndexShardSnapshot.FileInfo> fileInfos = new HashMap<>();
                for (final BlobStoreIndexShardSnapshot.FileInfo fileInfo : snapshot.indexFiles()) {
                    try {
                        maybeRecalculateMetadataHash(blobContainer, fileInfo, recoveryTargetMetadata);
                    } catch (Exception e) {
                        logger.warn(() -> new ParameterizedMessage("{} Can't calculate hash from blog for file [{}] [{}]", shardId, fileInfo.physicalName(), fileInfo.metadata()), e);
                    }
                    snapshotMetaData.put(fileInfo.metadata().name(), fileInfo.metadata());
                    fileInfos.put(fileInfo.metadata().name(), fileInfo);
                }
                final Store.MetadataSnapshot sourceMetaData = new Store.MetadataSnapshot(unmodifiableMap(snapshotMetaData), emptyMap(), 0);
                final StoreFileMetaData restoredSegmentsFile = sourceMetaData.getSegmentsFile();
                if (restoredSegmentsFile == null) {
                    throw new IndexShardRestoreFailedException(shardId, "Snapshot has no segments file");
                }
                final Store.RecoveryDiff diff = sourceMetaData.recoveryDiff(recoveryTargetMetadata);
                for (StoreFileMetaData md : diff.identical) {
                    BlobStoreIndexShardSnapshot.FileInfo fileInfo = fileInfos.get(md.name());
                    recoveryState.getIndex().addFileDetail(fileInfo.name(), fileInfo.length(), true);
                    if (logger.isTraceEnabled()) {
                        logger.trace("[{}] [{}] not_recovering [{}] from [{}], exists in local store and is same", shardId, snapshotId, fileInfo.physicalName(), fileInfo.name());
                    }
                }
                for (StoreFileMetaData md : Iterables.concat(diff.different, diff.missing)) {
                    BlobStoreIndexShardSnapshot.FileInfo fileInfo = fileInfos.get(md.name());
                    filesToRecover.add(fileInfo);
                    recoveryState.getIndex().addFileDetail(fileInfo.name(), fileInfo.length(), false);
                    if (logger.isTraceEnabled()) {
                        if (md == null) {
                            logger.trace("[{}] [{}] recovering [{}] from [{}], does not exists in local store", shardId, snapshotId, fileInfo.physicalName(), fileInfo.name());
                        } else {
                            logger.trace("[{}] [{}] recovering [{}] from [{}], exists in local store but is different", shardId, snapshotId, fileInfo.physicalName(), fileInfo.name());
                        }
                    }
                }
                if (filesToRecover.isEmpty()) {
                    logger.trace("no files to recover, all exists within the local store");
                }
                try {
                    final List<String> deleteIfExistFiles = Arrays.asList(store.directory().listAll());
                    for (final BlobStoreIndexShardSnapshot.FileInfo fileToRecover : filesToRecover) {
                        final String physicalName = fileToRecover.physicalName();
                        if (deleteIfExistFiles.contains(physicalName)) {
                            logger.trace("[{}] [{}] deleting pre-existing file [{}]", shardId, snapshotId, physicalName);
                            store.directory().deleteFile(physicalName);
                        }
                        logger.trace("[{}] [{}] restoring file [{}]", shardId, snapshotId, fileToRecover.name());
                        restoreFile(fileToRecover, store);
                    }
                } catch (IOException ex) {
                    throw new IndexShardRestoreFailedException(shardId, "Failed to recover index", ex);
                }
                final SegmentInfos segmentCommitInfos;
                try {
                    segmentCommitInfos = Lucene.pruneUnreferencedFiles(restoredSegmentsFile.name(), store.directory());
                } catch (IOException e) {
                    throw new IndexShardRestoreFailedException(shardId, "Failed to fetch index version after copying it over", e);
                }
                recoveryState.getIndex().updateVersion(segmentCommitInfos.getVersion());
                try {
                    for (String storeFile : store.directory().listAll()) {
                        if (Store.isAutogenerated(storeFile) || snapshotFiles.containPhysicalIndexFile(storeFile)) {
                            continue;
                        }
                        try {
                            store.deleteQuiet("restore", storeFile);
                            store.directory().deleteFile(storeFile);
                        } catch (IOException e) {
                            logger.warn("[{}] failed to delete file [{}] during snapshot cleanup", snapshotId, storeFile);
                        }
                    }
                } catch (IOException e) {
                    logger.warn("[{}] failed to list directory - some of files might not be deleted", snapshotId);
                }
            } finally {
                store.decRef();
            }
        }

        private void restoreFile(final BlobStoreIndexShardSnapshot.FileInfo fileInfo, final Store store) throws IOException {
            boolean success = false;
            try (InputStream partSliceStream = new PartSliceStream(blobContainer, fileInfo)) {
                final InputStream stream;
                if (restoreRateLimiter == null) {
                    stream = partSliceStream;
                } else {
                    stream = new RateLimitingInputStream(partSliceStream, restoreRateLimiter, restoreRateLimitingTimeInNanos::inc);
                }
                try (IndexOutput indexOutput = store.createVerifyingOutput(fileInfo.physicalName(), fileInfo.metadata(), IOContext.DEFAULT)) {
                    final byte[] buffer = new byte[BUFFER_SIZE];
                    int length;
                    while ((length = stream.read(buffer)) > 0) {
                        indexOutput.writeBytes(buffer, 0, length);
                        recoveryState.getIndex().addRecoveredBytesToFile(fileInfo.name(), length);
                    }
                    Store.verify(indexOutput);
                    indexOutput.close();
                    store.directory().sync(Collections.singleton(fileInfo.physicalName()));
                    success = true;
                } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                    try {
                        store.markStoreCorrupted(ex);
                    } catch (IOException e) {
                        logger.warn("store cannot be marked as corrupted", e);
                    }
                    throw ex;
                } finally {
                    if (success == false) {
                        store.deleteQuiet(fileInfo.physicalName());
                    }
                }
            }
        }
    }
}