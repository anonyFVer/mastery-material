package org.elasticsearch.index.store;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Version;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.RefCounted;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.CombinedDeletionPolicy;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class Store extends AbstractIndexShardComponent implements Closeable, RefCounted {

    static final String CODEC = "store";

    static final int VERSION_WRITE_THROWABLE = 2;

    static final int VERSION_STACK_TRACE = 1;

    static final int VERSION_START = 0;

    static final int VERSION = VERSION_WRITE_THROWABLE;

    static final String CORRUPTED = "corrupted_";

    public static final Setting<TimeValue> INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING = Setting.timeSetting("index.store.stats_refresh_interval", TimeValue.timeValueSeconds(10), Property.IndexScope);

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final StoreDirectory directory;

    private final ReentrantReadWriteLock metadataLock = new ReentrantReadWriteLock();

    private final ShardLock shardLock;

    private final OnClose onClose;

    private final AbstractRefCounted refCounter = new AbstractRefCounted("store") {

        @Override
        protected void closeInternal() {
            Store.this.closeInternal();
        }
    };

    public Store(ShardId shardId, IndexSettings indexSettings, DirectoryService directoryService, ShardLock shardLock) throws IOException {
        this(shardId, indexSettings, directoryService, shardLock, OnClose.EMPTY);
    }

    public Store(ShardId shardId, IndexSettings indexSettings, DirectoryService directoryService, ShardLock shardLock, OnClose onClose) throws IOException {
        super(shardId, indexSettings);
        final Settings settings = indexSettings.getSettings();
        Directory dir = directoryService.newDirectory();
        final TimeValue refreshInterval = indexSettings.getValue(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING);
        logger.debug("store stats are refreshed with refresh_interval [{}]", refreshInterval);
        ByteSizeCachingDirectory sizeCachingDir = new ByteSizeCachingDirectory(dir, refreshInterval);
        this.directory = new StoreDirectory(sizeCachingDir, Loggers.getLogger("index.store.deletes", settings, shardId));
        this.shardLock = shardLock;
        this.onClose = onClose;
        assert onClose != null;
        assert shardLock != null;
        assert shardLock.getShardId().equals(shardId);
    }

    public Directory directory() {
        ensureOpen();
        return directory;
    }

    public SegmentInfos readLastCommittedSegmentsInfo() throws IOException {
        failIfCorrupted();
        try {
            return readSegmentsInfo(null, directory());
        } catch (CorruptIndexException ex) {
            markStoreCorrupted(ex);
            throw ex;
        }
    }

    private static SegmentInfos readSegmentsInfo(IndexCommit commit, Directory directory) throws IOException {
        assert commit == null || commit.getDirectory() == directory;
        try {
            return commit == null ? Lucene.readSegmentInfos(directory) : Lucene.readSegmentInfos(commit);
        } catch (EOFException eof) {
            throw new CorruptIndexException("Read past EOF while reading segment infos", "commit(" + commit + ")", eof);
        } catch (IOException exception) {
            throw exception;
        } catch (Exception ex) {
            throw new CorruptIndexException("Hit unexpected exception while reading segment infos", "commit(" + commit + ")", ex);
        }
    }

    public static SequenceNumbers.CommitInfo loadSeqNoInfo(final IndexCommit commit) throws IOException {
        final Map<String, String> userData = commit.getUserData();
        return SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userData.entrySet());
    }

    final void ensureOpen() {
        if (this.refCounter.refCount() <= 0) {
            throw new AlreadyClosedException("store is already closed");
        }
    }

    public MetadataSnapshot getMetadata(IndexCommit commit) throws IOException {
        return getMetadata(commit, false);
    }

    public MetadataSnapshot getMetadata(IndexCommit commit, boolean lockDirectory) throws IOException {
        ensureOpen();
        failIfCorrupted();
        assert lockDirectory ? commit == null : true : "IW lock should not be obtained if there is a commit point available";
        java.util.concurrent.locks.Lock lock = lockDirectory ? metadataLock.writeLock() : metadataLock.readLock();
        lock.lock();
        try (Closeable ignored = lockDirectory ? directory.obtainLock(IndexWriter.WRITE_LOCK_NAME) : () -> {
        }) {
            return new MetadataSnapshot(commit, directory, logger);
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            markStoreCorrupted(ex);
            throw ex;
        } finally {
            lock.unlock();
        }
    }

    public void renameTempFilesSafe(Map<String, String> tempFileMap) throws IOException {
        final Map.Entry<String, String>[] entries = tempFileMap.entrySet().toArray(new Map.Entry[tempFileMap.size()]);
        ArrayUtil.timSort(entries, new Comparator<Map.Entry<String, String>>() {

            @Override
            public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2) {
                String left = o1.getValue();
                String right = o2.getValue();
                if (left.startsWith(IndexFileNames.SEGMENTS) || right.startsWith(IndexFileNames.SEGMENTS)) {
                    if (left.startsWith(IndexFileNames.SEGMENTS) == false) {
                        return -1;
                    } else if (right.startsWith(IndexFileNames.SEGMENTS) == false) {
                        return 1;
                    }
                }
                return left.compareTo(right);
            }
        });
        metadataLock.writeLock().lock();
        try (Lock writeLock = directory().obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            for (Map.Entry<String, String> entry : entries) {
                String tempFile = entry.getKey();
                String origFile = entry.getValue();
                try {
                    directory.deleteFile(origFile);
                } catch (FileNotFoundException | NoSuchFileException e) {
                } catch (Exception ex) {
                    logger.debug(() -> new ParameterizedMessage("failed to delete file [{}]", origFile), ex);
                }
                directory.rename(tempFile, origFile);
                final String remove = tempFileMap.remove(tempFile);
                assert remove != null;
            }
            directory.syncMetaData();
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    public CheckIndex.Status checkIndex(PrintStream out) throws IOException {
        metadataLock.writeLock().lock();
        try (CheckIndex checkIndex = new CheckIndex(directory)) {
            checkIndex.setInfoStream(out);
            return checkIndex.checkIndex();
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    public void exorciseIndex(CheckIndex.Status status) throws IOException {
        metadataLock.writeLock().lock();
        try (CheckIndex checkIndex = new CheckIndex(directory)) {
            checkIndex.exorciseIndex(status);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    public StoreStats stats() throws IOException {
        ensureOpen();
        return new StoreStats(directory.estimateSize());
    }

    @Override
    public final void incRef() {
        refCounter.incRef();
    }

    @Override
    public final boolean tryIncRef() {
        return refCounter.tryIncRef();
    }

    @Override
    public final void decRef() {
        refCounter.decRef();
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            decRef();
            logger.debug("store reference count on close: {}", refCounter.refCount());
        }
    }

    private void closeInternal() {
        try {
            try {
                directory.innerClose();
            } finally {
                onClose.accept(shardLock);
            }
        } catch (IOException e) {
            logger.debug("failed to close directory", e);
        } finally {
            IOUtils.closeWhileHandlingException(shardLock);
        }
    }

    public static MetadataSnapshot readMetadataSnapshot(Path indexLocation, ShardId shardId, NodeEnvironment.ShardLocker shardLocker, Logger logger) throws IOException {
        try (ShardLock lock = shardLocker.lock(shardId, TimeUnit.SECONDS.toMillis(5));
            Directory dir = new SimpleFSDirectory(indexLocation)) {
            failIfCorrupted(dir, shardId);
            return new MetadataSnapshot(null, dir, logger);
        } catch (IndexNotFoundException ex) {
        } catch (FileNotFoundException | NoSuchFileException ex) {
            logger.info("Failed to open / find files while reading metadata snapshot");
        } catch (ShardLockObtainFailedException ex) {
            logger.info(() -> new ParameterizedMessage("{}: failed to obtain shard lock", shardId), ex);
        }
        return MetadataSnapshot.EMPTY;
    }

    public static boolean canOpenIndex(Logger logger, Path indexLocation, ShardId shardId, NodeEnvironment.ShardLocker shardLocker) throws IOException {
        try {
            tryOpenIndex(indexLocation, shardId, shardLocker, logger);
        } catch (Exception ex) {
            logger.trace(() -> new ParameterizedMessage("Can't open index for path [{}]", indexLocation), ex);
            return false;
        }
        return true;
    }

    public static void tryOpenIndex(Path indexLocation, ShardId shardId, NodeEnvironment.ShardLocker shardLocker, Logger logger) throws IOException, ShardLockObtainFailedException {
        try (ShardLock lock = shardLocker.lock(shardId, TimeUnit.SECONDS.toMillis(5));
            Directory dir = new SimpleFSDirectory(indexLocation)) {
            failIfCorrupted(dir, shardId);
            SegmentInfos segInfo = Lucene.readSegmentInfos(dir);
            logger.trace("{} loaded segment info [{}]", shardId, segInfo);
        }
    }

    public IndexOutput createVerifyingOutput(String fileName, final StoreFileMetaData metadata, final IOContext context) throws IOException {
        IndexOutput output = directory().createOutput(fileName, context);
        boolean success = false;
        try {
            assert metadata.writtenBy() != null;
            output = new LuceneVerifyingIndexOutput(metadata, output);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(output);
            }
        }
        return output;
    }

    public static void verify(IndexOutput output) throws IOException {
        if (output instanceof VerifyingIndexOutput) {
            ((VerifyingIndexOutput) output).verify();
        }
    }

    public IndexInput openVerifyingInput(String filename, IOContext context, StoreFileMetaData metadata) throws IOException {
        assert metadata.writtenBy() != null;
        return new VerifyingIndexInput(directory().openInput(filename, context));
    }

    public static void verify(IndexInput input) throws IOException {
        if (input instanceof VerifyingIndexInput) {
            ((VerifyingIndexInput) input).verify();
        }
    }

    public boolean checkIntegrityNoException(StoreFileMetaData md) {
        return checkIntegrityNoException(md, directory());
    }

    public static boolean checkIntegrityNoException(StoreFileMetaData md, Directory directory) {
        try {
            checkIntegrity(md, directory);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static void checkIntegrity(final StoreFileMetaData md, final Directory directory) throws IOException {
        try (IndexInput input = directory.openInput(md.name(), IOContext.READONCE)) {
            if (input.length() != md.length()) {
                throw new CorruptIndexException("expected length=" + md.length() + " != actual length: " + input.length() + " : file truncated?", input);
            }
            String checksum = Store.digestToString(CodecUtil.checksumEntireFile(input));
            if (!checksum.equals(md.checksum())) {
                throw new CorruptIndexException("inconsistent metadata: lucene checksum=" + checksum + ", metadata checksum=" + md.checksum(), input);
            }
        }
    }

    public boolean isMarkedCorrupted() throws IOException {
        ensureOpen();
        final String[] files = directory().listAll();
        for (String file : files) {
            if (file.startsWith(CORRUPTED)) {
                return true;
            }
        }
        return false;
    }

    public void removeCorruptionMarker() throws IOException {
        ensureOpen();
        final Directory directory = directory();
        IOException firstException = null;
        final String[] files = directory.listAll();
        for (String file : files) {
            if (file.startsWith(CORRUPTED)) {
                try {
                    directory.deleteFile(file);
                } catch (IOException ex) {
                    if (firstException == null) {
                        firstException = ex;
                    } else {
                        firstException.addSuppressed(ex);
                    }
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    public void failIfCorrupted() throws IOException {
        ensureOpen();
        failIfCorrupted(directory, shardId);
    }

    private static void failIfCorrupted(Directory directory, ShardId shardId) throws IOException {
        final String[] files = directory.listAll();
        List<CorruptIndexException> ex = new ArrayList<>();
        for (String file : files) {
            if (file.startsWith(CORRUPTED)) {
                try (ChecksumIndexInput input = directory.openChecksumInput(file, IOContext.READONCE)) {
                    int version = CodecUtil.checkHeader(input, CODEC, VERSION_START, VERSION);
                    if (version == VERSION_WRITE_THROWABLE) {
                        final int size = input.readVInt();
                        final byte[] buffer = new byte[size];
                        input.readBytes(buffer, 0, buffer.length);
                        StreamInput in = StreamInput.wrap(buffer);
                        Exception t = in.readException();
                        if (t instanceof CorruptIndexException) {
                            ex.add((CorruptIndexException) t);
                        } else {
                            ex.add(new CorruptIndexException(t.getMessage(), "preexisting_corruption", t));
                        }
                    } else {
                        assert version == VERSION_START || version == VERSION_STACK_TRACE;
                        String msg = input.readString();
                        StringBuilder builder = new StringBuilder(shardId.toString());
                        builder.append(" Preexisting corrupted index [");
                        builder.append(file).append("] caused by: ");
                        builder.append(msg);
                        if (version == VERSION_STACK_TRACE) {
                            builder.append(System.lineSeparator());
                            builder.append(input.readString());
                        }
                        ex.add(new CorruptIndexException(builder.toString(), "preexisting_corruption"));
                    }
                    CodecUtil.checkFooter(input);
                }
            }
        }
        if (ex.isEmpty() == false) {
            ExceptionsHelper.rethrowAndSuppress(ex);
        }
    }

    public void cleanupAndVerify(String reason, MetadataSnapshot sourceMetaData) throws IOException {
        metadataLock.writeLock().lock();
        try (Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
            for (String existingFile : directory.listAll()) {
                if (Store.isAutogenerated(existingFile) || sourceMetaData.contains(existingFile)) {
                    continue;
                }
                try {
                    directory.deleteFile(reason, existingFile);
                } catch (IOException ex) {
                    if (existingFile.startsWith(IndexFileNames.SEGMENTS) || existingFile.equals(IndexFileNames.OLD_SEGMENTS_GEN) || existingFile.startsWith(CORRUPTED)) {
                        throw new IllegalStateException("Can't delete " + existingFile + " - cleanup failed", ex);
                    }
                    logger.debug(() -> new ParameterizedMessage("failed to delete file [{}]", existingFile), ex);
                }
            }
            directory.syncMetaData();
            final Store.MetadataSnapshot metadataOrEmpty = getMetadata(null);
            verifyAfterCleanup(sourceMetaData, metadataOrEmpty);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    final void verifyAfterCleanup(MetadataSnapshot sourceMetaData, MetadataSnapshot targetMetaData) {
        final RecoveryDiff recoveryDiff = targetMetaData.recoveryDiff(sourceMetaData);
        if (recoveryDiff.identical.size() != recoveryDiff.size()) {
            if (recoveryDiff.missing.isEmpty()) {
                for (StoreFileMetaData meta : recoveryDiff.different) {
                    StoreFileMetaData local = targetMetaData.get(meta.name());
                    StoreFileMetaData remote = sourceMetaData.get(meta.name());
                    if (local.isSame(remote) == false) {
                        logger.debug("Files are different on the recovery target: {} ", recoveryDiff);
                        throw new IllegalStateException("local version: " + local + " is different from remote version after recovery: " + remote, null);
                    }
                }
            } else {
                logger.debug("Files are missing on the recovery target: {} ", recoveryDiff);
                throw new IllegalStateException("Files are missing on the recovery target: [different=" + recoveryDiff.different + ", missing=" + recoveryDiff.missing + ']', null);
            }
        }
    }

    public int refCount() {
        return refCounter.refCount();
    }

    static final class StoreDirectory extends FilterDirectory {

        private final Logger deletesLogger;

        StoreDirectory(ByteSizeCachingDirectory delegateDirectory, Logger deletesLogger) {
            super(delegateDirectory);
            this.deletesLogger = deletesLogger;
        }

        long estimateSize() throws IOException {
            return ((ByteSizeCachingDirectory) getDelegate()).estimateSizeInBytes();
        }

        @Override
        public void close() {
            assert false : "Nobody should close this directory except of the Store itself";
        }

        public void deleteFile(String msg, String name) throws IOException {
            deletesLogger.trace("{}: delete file {}", msg, name);
            super.deleteFile(name);
        }

        @Override
        public void deleteFile(String name) throws IOException {
            deleteFile("StoreDirectory.deleteFile", name);
        }

        private void innerClose() throws IOException {
            super.close();
        }

        @Override
        public String toString() {
            return "store(" + in.toString() + ")";
        }
    }

    public static final class MetadataSnapshot implements Iterable<StoreFileMetaData>, Writeable {

        private final Map<String, StoreFileMetaData> metadata;

        public static final MetadataSnapshot EMPTY = new MetadataSnapshot();

        private final Map<String, String> commitUserData;

        private final long numDocs;

        public MetadataSnapshot(Map<String, StoreFileMetaData> metadata, Map<String, String> commitUserData, long numDocs) {
            this.metadata = metadata;
            this.commitUserData = commitUserData;
            this.numDocs = numDocs;
        }

        MetadataSnapshot() {
            metadata = emptyMap();
            commitUserData = emptyMap();
            numDocs = 0;
        }

        MetadataSnapshot(IndexCommit commit, Directory directory, Logger logger) throws IOException {
            LoadedMetadata loadedMetadata = loadMetadata(commit, directory, logger);
            metadata = loadedMetadata.fileMetadata;
            commitUserData = loadedMetadata.userData;
            numDocs = loadedMetadata.numDocs;
            assert metadata.isEmpty() || numSegmentFiles() == 1 : "numSegmentFiles: " + numSegmentFiles();
        }

        public MetadataSnapshot(StreamInput in) throws IOException {
            final int size = in.readVInt();
            Map<String, StoreFileMetaData> metadata = new HashMap<>();
            for (int i = 0; i < size; i++) {
                StoreFileMetaData meta = new StoreFileMetaData(in);
                metadata.put(meta.name(), meta);
            }
            Map<String, String> commitUserData = new HashMap<>();
            int num = in.readVInt();
            for (int i = num; i > 0; i--) {
                commitUserData.put(in.readString(), in.readString());
            }
            this.metadata = unmodifiableMap(metadata);
            this.commitUserData = unmodifiableMap(commitUserData);
            this.numDocs = in.readLong();
            assert metadata.isEmpty() || numSegmentFiles() == 1 : "numSegmentFiles: " + numSegmentFiles();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.metadata.size());
            for (StoreFileMetaData meta : this) {
                meta.writeTo(out);
            }
            out.writeVInt(commitUserData.size());
            for (Map.Entry<String, String> entry : commitUserData.entrySet()) {
                out.writeString(entry.getKey());
                out.writeString(entry.getValue());
            }
            out.writeLong(numDocs);
        }

        public long getNumDocs() {
            return numDocs;
        }

        static class LoadedMetadata {

            final Map<String, StoreFileMetaData> fileMetadata;

            final Map<String, String> userData;

            final long numDocs;

            LoadedMetadata(Map<String, StoreFileMetaData> fileMetadata, Map<String, String> userData, long numDocs) {
                this.fileMetadata = fileMetadata;
                this.userData = userData;
                this.numDocs = numDocs;
            }
        }

        static LoadedMetadata loadMetadata(IndexCommit commit, Directory directory, Logger logger) throws IOException {
            long numDocs;
            Map<String, StoreFileMetaData> builder = new HashMap<>();
            Map<String, String> commitUserDataBuilder = new HashMap<>();
            try {
                final SegmentInfos segmentCommitInfos = Store.readSegmentsInfo(commit, directory);
                numDocs = Lucene.getNumDocs(segmentCommitInfos);
                commitUserDataBuilder.putAll(segmentCommitInfos.getUserData());
                Version maxVersion = segmentCommitInfos.getMinSegmentLuceneVersion();
                for (SegmentCommitInfo info : segmentCommitInfos) {
                    final Version version = info.info.getVersion();
                    if (version == null) {
                        throw new IllegalArgumentException("expected valid version value: " + info.info.toString());
                    }
                    if (version.onOrAfter(maxVersion)) {
                        maxVersion = version;
                    }
                    for (String file : info.files()) {
                        checksumFromLuceneFile(directory, file, builder, logger, version, SEGMENT_INFO_EXTENSION.equals(IndexFileNames.getExtension(file)));
                    }
                }
                if (maxVersion == null) {
                    maxVersion = org.elasticsearch.Version.CURRENT.minimumIndexCompatibilityVersion().luceneVersion;
                }
                final String segmentsFile = segmentCommitInfos.getSegmentsFileName();
                checksumFromLuceneFile(directory, segmentsFile, builder, logger, maxVersion, true);
            } catch (CorruptIndexException | IndexNotFoundException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                throw ex;
            } catch (Exception ex) {
                try {
                    logger.warn(() -> new ParameterizedMessage("failed to build store metadata. checking segment info integrity (with commit [{}])", commit == null ? "no" : "yes"), ex);
                    Lucene.checkSegmentInfoIntegrity(directory);
                } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException cex) {
                    cex.addSuppressed(ex);
                    throw cex;
                } catch (Exception inner) {
                    inner.addSuppressed(ex);
                    throw inner;
                }
                throw ex;
            }
            return new LoadedMetadata(unmodifiableMap(builder), unmodifiableMap(commitUserDataBuilder), numDocs);
        }

        private static void checksumFromLuceneFile(Directory directory, String file, Map<String, StoreFileMetaData> builder, Logger logger, Version version, boolean readFileAsHash) throws IOException {
            final String checksum;
            final BytesRefBuilder fileHash = new BytesRefBuilder();
            try (IndexInput in = directory.openInput(file, IOContext.READONCE)) {
                final long length;
                try {
                    length = in.length();
                    if (length < CodecUtil.footerLength()) {
                        throw new CorruptIndexException("Can't retrieve checksum from file: " + file + " file length must be >= " + CodecUtil.footerLength() + " but was: " + in.length(), in);
                    }
                    if (readFileAsHash) {
                        final VerifyingIndexInput verifyingIndexInput = new VerifyingIndexInput(in);
                        hashFile(fileHash, new InputStreamIndexInput(verifyingIndexInput, length), length);
                        checksum = digestToString(verifyingIndexInput.verify());
                    } else {
                        checksum = digestToString(CodecUtil.retrieveChecksum(in));
                    }
                } catch (Exception ex) {
                    logger.debug(() -> new ParameterizedMessage("Can retrieve checksum from file [{}]", file), ex);
                    throw ex;
                }
                builder.put(file, new StoreFileMetaData(file, length, checksum, version, fileHash.get()));
            }
        }

        public static void hashFile(BytesRefBuilder fileHash, InputStream in, long size) throws IOException {
            final int len = (int) Math.min(1024 * 1024, size);
            fileHash.grow(len);
            fileHash.setLength(len);
            final int readBytes = Streams.readFully(in, fileHash.bytes(), 0, len);
            assert readBytes == len : Integer.toString(readBytes) + " != " + Integer.toString(len);
            assert fileHash.length() == len : Integer.toString(fileHash.length()) + " != " + Integer.toString(len);
        }

        @Override
        public Iterator<StoreFileMetaData> iterator() {
            return metadata.values().iterator();
        }

        public StoreFileMetaData get(String name) {
            return metadata.get(name);
        }

        public Map<String, StoreFileMetaData> asMap() {
            return metadata;
        }

        private static final String DEL_FILE_EXTENSION = "del";

        private static final String LIV_FILE_EXTENSION = "liv";

        private static final String FIELD_INFOS_FILE_EXTENSION = "fnm";

        private static final String SEGMENT_INFO_EXTENSION = "si";

        public RecoveryDiff recoveryDiff(MetadataSnapshot recoveryTargetSnapshot) {
            final List<StoreFileMetaData> identical = new ArrayList<>();
            final List<StoreFileMetaData> different = new ArrayList<>();
            final List<StoreFileMetaData> missing = new ArrayList<>();
            final Map<String, List<StoreFileMetaData>> perSegment = new HashMap<>();
            final List<StoreFileMetaData> perCommitStoreFiles = new ArrayList<>();
            for (StoreFileMetaData meta : this) {
                if (IndexFileNames.OLD_SEGMENTS_GEN.equals(meta.name())) {
                    continue;
                }
                final String segmentId = IndexFileNames.parseSegmentName(meta.name());
                final String extension = IndexFileNames.getExtension(meta.name());
                if (IndexFileNames.SEGMENTS.equals(segmentId) || DEL_FILE_EXTENSION.equals(extension) || LIV_FILE_EXTENSION.equals(extension)) {
                    perCommitStoreFiles.add(meta);
                } else {
                    List<StoreFileMetaData> perSegStoreFiles = perSegment.get(segmentId);
                    if (perSegStoreFiles == null) {
                        perSegStoreFiles = new ArrayList<>();
                        perSegment.put(segmentId, perSegStoreFiles);
                    }
                    perSegStoreFiles.add(meta);
                }
            }
            final ArrayList<StoreFileMetaData> identicalFiles = new ArrayList<>();
            for (List<StoreFileMetaData> segmentFiles : Iterables.concat(perSegment.values(), Collections.singleton(perCommitStoreFiles))) {
                identicalFiles.clear();
                boolean consistent = true;
                for (StoreFileMetaData meta : segmentFiles) {
                    StoreFileMetaData storeFileMetaData = recoveryTargetSnapshot.get(meta.name());
                    if (storeFileMetaData == null) {
                        consistent = false;
                        missing.add(meta);
                    } else if (storeFileMetaData.isSame(meta) == false) {
                        consistent = false;
                        different.add(meta);
                    } else {
                        identicalFiles.add(meta);
                    }
                }
                if (consistent) {
                    identical.addAll(identicalFiles);
                } else {
                    different.addAll(identicalFiles);
                }
            }
            RecoveryDiff recoveryDiff = new RecoveryDiff(Collections.unmodifiableList(identical), Collections.unmodifiableList(different), Collections.unmodifiableList(missing));
            assert recoveryDiff.size() == this.metadata.size() - (metadata.containsKey(IndexFileNames.OLD_SEGMENTS_GEN) ? 1 : 0) : "some files are missing recoveryDiff size: [" + recoveryDiff.size() + "] metadata size: [" + this.metadata.size() + "] contains  segments.gen: [" + metadata.containsKey(IndexFileNames.OLD_SEGMENTS_GEN) + "]";
            return recoveryDiff;
        }

        public int size() {
            return metadata.size();
        }

        public Map<String, String> getCommitUserData() {
            return commitUserData;
        }

        public String getHistoryUUID() {
            return commitUserData.get(Engine.HISTORY_UUID_KEY);
        }

        public String getTranslogUUID() {
            return commitUserData.get(Translog.TRANSLOG_UUID_KEY);
        }

        public boolean contains(String existingFile) {
            return metadata.containsKey(existingFile);
        }

        public StoreFileMetaData getSegmentsFile() {
            for (StoreFileMetaData file : this) {
                if (file.name().startsWith(IndexFileNames.SEGMENTS)) {
                    return file;
                }
            }
            assert metadata.isEmpty();
            return null;
        }

        private int numSegmentFiles() {
            int count = 0;
            for (StoreFileMetaData file : this) {
                if (file.name().startsWith(IndexFileNames.SEGMENTS)) {
                    count++;
                }
            }
            return count;
        }

        public String getSyncId() {
            return commitUserData.get(Engine.SYNC_COMMIT_ID);
        }
    }

    public static final class RecoveryDiff {

        public final List<StoreFileMetaData> identical;

        public final List<StoreFileMetaData> different;

        public final List<StoreFileMetaData> missing;

        RecoveryDiff(List<StoreFileMetaData> identical, List<StoreFileMetaData> different, List<StoreFileMetaData> missing) {
            this.identical = identical;
            this.different = different;
            this.missing = missing;
        }

        public int size() {
            return identical.size() + different.size() + missing.size();
        }

        @Override
        public String toString() {
            return "RecoveryDiff{" + "identical=" + identical + ", different=" + different + ", missing=" + missing + '}';
        }
    }

    public static boolean isAutogenerated(String name) {
        return IndexWriter.WRITE_LOCK_NAME.equals(name);
    }

    public static String digestToString(long digest) {
        return Long.toString(digest, Character.MAX_RADIX);
    }

    static class LuceneVerifyingIndexOutput extends VerifyingIndexOutput {

        private final StoreFileMetaData metadata;

        private long writtenBytes;

        private final long checksumPosition;

        private String actualChecksum;

        private final byte[] footerChecksum = new byte[8];

        LuceneVerifyingIndexOutput(StoreFileMetaData metadata, IndexOutput out) {
            super(out);
            this.metadata = metadata;
            checksumPosition = metadata.length() - 8;
        }

        @Override
        public void verify() throws IOException {
            String footerDigest = null;
            if (metadata.checksum().equals(actualChecksum) && writtenBytes == metadata.length()) {
                ByteArrayIndexInput indexInput = new ByteArrayIndexInput("checksum", this.footerChecksum);
                footerDigest = digestToString(indexInput.readLong());
                if (metadata.checksum().equals(footerDigest)) {
                    return;
                }
            }
            throw new CorruptIndexException("verification failed (hardware problem?) : expected=" + metadata.checksum() + " actual=" + actualChecksum + " footer=" + footerDigest + " writtenLength=" + writtenBytes + " expectedLength=" + metadata.length() + " (resource=" + metadata.toString() + ")", "VerifyingIndexOutput(" + metadata.name() + ")");
        }

        @Override
        public void writeByte(byte b) throws IOException {
            final long writtenBytes = this.writtenBytes++;
            if (writtenBytes >= checksumPosition) {
                if (writtenBytes == checksumPosition) {
                    readAndCompareChecksum();
                }
                final int index = Math.toIntExact(writtenBytes - checksumPosition);
                if (index < footerChecksum.length) {
                    footerChecksum[index] = b;
                    if (index == footerChecksum.length - 1) {
                        verify();
                    }
                } else {
                    verify();
                    throw new AssertionError("write past EOF expected length: " + metadata.length() + " writtenBytes: " + writtenBytes);
                }
            }
            out.writeByte(b);
        }

        private void readAndCompareChecksum() throws IOException {
            actualChecksum = digestToString(getChecksum());
            if (!metadata.checksum().equals(actualChecksum)) {
                throw new CorruptIndexException("checksum failed (hardware problem?) : expected=" + metadata.checksum() + " actual=" + actualChecksum + " (resource=" + metadata.toString() + ")", "VerifyingIndexOutput(" + metadata.name() + ")");
            }
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            if (writtenBytes + length > checksumPosition) {
                for (int i = 0; i < length; i++) {
                    writeByte(b[offset + i]);
                }
            } else {
                out.writeBytes(b, offset, length);
                writtenBytes += length;
            }
        }
    }

    static class VerifyingIndexInput extends ChecksumIndexInput {

        private final IndexInput input;

        private final Checksum digest;

        private final long checksumPosition;

        private final byte[] checksum = new byte[8];

        private long verifiedPosition = 0;

        VerifyingIndexInput(IndexInput input) {
            this(input, new BufferedChecksum(new CRC32()));
        }

        VerifyingIndexInput(IndexInput input, Checksum digest) {
            super("VerifyingIndexInput(" + input + ")");
            this.input = input;
            this.digest = digest;
            checksumPosition = input.length() - 8;
        }

        @Override
        public byte readByte() throws IOException {
            long pos = input.getFilePointer();
            final byte b = input.readByte();
            pos++;
            if (pos > verifiedPosition) {
                if (pos <= checksumPosition) {
                    digest.update(b);
                } else {
                    checksum[(int) (pos - checksumPosition - 1)] = b;
                }
                verifiedPosition = pos;
            }
            return b;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            long pos = input.getFilePointer();
            input.readBytes(b, offset, len);
            if (pos + len > verifiedPosition) {
                int alreadyVerified = (int) Math.max(0, verifiedPosition - pos);
                if (pos < checksumPosition) {
                    if (pos + len < checksumPosition) {
                        digest.update(b, offset + alreadyVerified, len - alreadyVerified);
                    } else {
                        int checksumOffset = (int) (checksumPosition - pos);
                        if (checksumOffset - alreadyVerified > 0) {
                            digest.update(b, offset + alreadyVerified, checksumOffset - alreadyVerified);
                        }
                        System.arraycopy(b, offset + checksumOffset, checksum, 0, len - checksumOffset);
                    }
                } else {
                    assert pos - checksumPosition < 8;
                    System.arraycopy(b, offset, checksum, (int) (pos - checksumPosition), len);
                }
                verifiedPosition = pos + len;
            }
        }

        @Override
        public long getChecksum() {
            return digest.getValue();
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos < verifiedPosition) {
                input.seek(pos);
            } else {
                if (verifiedPosition > getFilePointer()) {
                    input.seek(verifiedPosition);
                    skipBytes(pos - verifiedPosition);
                } else {
                    skipBytes(pos - getFilePointer());
                }
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

        @Override
        public long getFilePointer() {
            return input.getFilePointer();
        }

        @Override
        public long length() {
            return input.length();
        }

        @Override
        public IndexInput clone() {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            throw new UnsupportedOperationException();
        }

        public long getStoredChecksum() {
            return new ByteArrayDataInput(checksum).readLong();
        }

        public long verify() throws CorruptIndexException {
            long storedChecksum = getStoredChecksum();
            if (getChecksum() == storedChecksum) {
                return storedChecksum;
            }
            throw new CorruptIndexException("verification failed : calculated=" + Store.digestToString(getChecksum()) + " stored=" + Store.digestToString(storedChecksum), this);
        }
    }

    public void deleteQuiet(String... files) {
        ensureOpen();
        StoreDirectory directory = this.directory;
        for (String file : files) {
            try {
                directory.deleteFile("Store.deleteQuiet", file);
            } catch (Exception ex) {
            }
        }
    }

    public void markStoreCorrupted(IOException exception) throws IOException {
        ensureOpen();
        if (!isMarkedCorrupted()) {
            String uuid = CORRUPTED + UUIDs.randomBase64UUID();
            try (IndexOutput output = this.directory().createOutput(uuid, IOContext.DEFAULT)) {
                CodecUtil.writeHeader(output, CODEC, VERSION);
                BytesStreamOutput out = new BytesStreamOutput();
                out.writeException(exception);
                BytesReference bytes = out.bytes();
                output.writeVInt(bytes.length());
                BytesRef ref = bytes.toBytesRef();
                output.writeBytes(ref.bytes, ref.offset, ref.length);
                CodecUtil.writeFooter(output);
            } catch (IOException ex) {
                logger.warn("Can't mark store as corrupted", ex);
            }
            directory().sync(Collections.singleton(uuid));
        }
    }

    public interface OnClose extends Consumer<ShardLock> {

        OnClose EMPTY = new OnClose() {

            @Override
            public void accept(ShardLock Lock) {
            }
        };
    }

    public void createEmpty() throws IOException {
        metadataLock.writeLock().lock();
        try (IndexWriter writer = newIndexWriter(IndexWriterConfig.OpenMode.CREATE, directory, null)) {
            final Map<String, String> map = new HashMap<>();
            map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
            map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            map.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(SequenceNumbers.NO_OPS_PERFORMED));
            map.put(InternalEngine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, "-1");
            updateCommitData(writer, map);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    public void bootstrapNewHistory() throws IOException {
        metadataLock.writeLock().lock();
        try (IndexWriter writer = newIndexWriter(IndexWriterConfig.OpenMode.APPEND, directory, null)) {
            final Map<String, String> userData = getUserData(writer);
            final long maxSeqNo = Long.parseLong(userData.get(SequenceNumbers.MAX_SEQ_NO));
            final Map<String, String> map = new HashMap<>();
            map.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
            map.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(maxSeqNo));
            updateCommitData(writer, map);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    public void associateIndexWithNewTranslog(final String translogUUID) throws IOException {
        metadataLock.writeLock().lock();
        try (IndexWriter writer = newIndexWriter(IndexWriterConfig.OpenMode.APPEND, directory, null)) {
            if (translogUUID.equals(getUserData(writer).get(Translog.TRANSLOG_UUID_KEY))) {
                throw new IllegalArgumentException("a new translog uuid can't be equal to existing one. got [" + translogUUID + "]");
            }
            final Map<String, String> map = new HashMap<>();
            map.put(Translog.TRANSLOG_GENERATION_KEY, "1");
            map.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
            updateCommitData(writer, map);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    public void ensureIndexHasHistoryUUID() throws IOException {
        metadataLock.writeLock().lock();
        try (IndexWriter writer = newIndexWriter(IndexWriterConfig.OpenMode.APPEND, directory, null)) {
            final Map<String, String> userData = getUserData(writer);
            if (userData.containsKey(Engine.HISTORY_UUID_KEY) == false) {
                updateCommitData(writer, Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()));
            }
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    public void trimUnsafeCommits(final long lastSyncedGlobalCheckpoint, final long minRetainedTranslogGen, final org.elasticsearch.Version indexVersionCreated) throws IOException {
        metadataLock.writeLock().lock();
        try {
            final List<IndexCommit> existingCommits = DirectoryReader.listCommits(directory);
            if (existingCommits.isEmpty()) {
                throw new IllegalArgumentException("No index found to trim");
            }
            final String translogUUID = existingCommits.get(existingCommits.size() - 1).getUserData().get(Translog.TRANSLOG_UUID_KEY);
            final IndexCommit startingIndexCommit;
            if (indexVersionCreated.before(org.elasticsearch.Version.V_6_2_0)) {
                final List<IndexCommit> recoverableCommits = new ArrayList<>();
                for (IndexCommit commit : existingCommits) {
                    if (minRetainedTranslogGen <= Long.parseLong(commit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY))) {
                        recoverableCommits.add(commit);
                    }
                }
                assert recoverableCommits.isEmpty() == false : "No commit point with translog found; " + "commits [" + existingCommits + "], minRetainedTranslogGen [" + minRetainedTranslogGen + "]";
                startingIndexCommit = CombinedDeletionPolicy.findSafeCommitPoint(recoverableCommits, lastSyncedGlobalCheckpoint);
            } else {
                startingIndexCommit = CombinedDeletionPolicy.findSafeCommitPoint(existingCommits, lastSyncedGlobalCheckpoint);
            }
            if (translogUUID.equals(startingIndexCommit.getUserData().get(Translog.TRANSLOG_UUID_KEY)) == false) {
                throw new IllegalStateException("starting commit translog uuid [" + startingIndexCommit.getUserData().get(Translog.TRANSLOG_UUID_KEY) + "] is not equal to last commit's translog uuid [" + translogUUID + "]");
            }
            if (startingIndexCommit.equals(existingCommits.get(existingCommits.size() - 1)) == false) {
                try (IndexWriter writer = newIndexWriter(IndexWriterConfig.OpenMode.APPEND, directory, startingIndexCommit)) {
                    writer.setLiveCommitData(startingIndexCommit.getUserData().entrySet());
                    writer.commit();
                }
            }
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    private void updateCommitData(IndexWriter writer, Map<String, String> keysToUpdate) throws IOException {
        final Map<String, String> userData = getUserData(writer);
        userData.putAll(keysToUpdate);
        writer.setLiveCommitData(userData.entrySet());
        writer.commit();
    }

    private Map<String, String> getUserData(IndexWriter writer) {
        final Map<String, String> userData = new HashMap<>();
        writer.getLiveCommitData().forEach(e -> userData.put(e.getKey(), e.getValue()));
        return userData;
    }

    private static IndexWriter newIndexWriter(final IndexWriterConfig.OpenMode openMode, final Directory dir, final IndexCommit commit) throws IOException {
        assert openMode == IndexWriterConfig.OpenMode.APPEND || commit == null : "can't specify create flag with a commit";
        IndexWriterConfig iwc = new IndexWriterConfig(null).setSoftDeletesField(Lucene.SOFT_DELETES_FIELD).setCommitOnClose(false).setIndexCommit(commit).setMergePolicy(NoMergePolicy.INSTANCE).setOpenMode(openMode);
        return new IndexWriter(dir, iwc);
    }
}