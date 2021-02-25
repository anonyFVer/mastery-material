package org.elasticsearch.index.translog;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class Translog extends AbstractIndexShardComponent implements IndexShardComponent, Closeable {

    public static final String TRANSLOG_GENERATION_KEY = "translog_generation";

    public static final String TRANSLOG_UUID_KEY = "translog_uuid";

    public static final String TRANSLOG_FILE_PREFIX = "translog-";

    public static final String TRANSLOG_FILE_SUFFIX = ".tlog";

    public static final String CHECKPOINT_SUFFIX = ".ckp";

    public static final String CHECKPOINT_FILE_NAME = "translog" + CHECKPOINT_SUFFIX;

    static final Pattern PARSE_STRICT_ID_PATTERN = Pattern.compile("^" + TRANSLOG_FILE_PREFIX + "(\\d+)(\\.tlog)$");

    public static final int DEFAULT_HEADER_SIZE_IN_BYTES = TranslogHeader.headerSizeInBytes(UUIDs.randomBase64UUID());

    private final List<TranslogReader> readers = new ArrayList<>();

    private BigArrays bigArrays;

    protected final ReleasableLock readLock;

    protected final ReleasableLock writeLock;

    private final Path location;

    private TranslogWriter current;

    private final AtomicBoolean closed = new AtomicBoolean();

    private final TranslogConfig config;

    private final LongSupplier globalCheckpointSupplier;

    private final LongSupplier primaryTermSupplier;

    private final String translogUUID;

    private final TranslogDeletionPolicy deletionPolicy;

    public Translog(final TranslogConfig config, final String translogUUID, TranslogDeletionPolicy deletionPolicy, final LongSupplier globalCheckpointSupplier, final LongSupplier primaryTermSupplier) throws IOException {
        super(config.getShardId(), config.getIndexSettings());
        this.config = config;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.primaryTermSupplier = primaryTermSupplier;
        this.deletionPolicy = deletionPolicy;
        this.translogUUID = translogUUID;
        bigArrays = config.getBigArrays();
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());
        this.location = config.getTranslogPath();
        Files.createDirectories(this.location);
        try {
            final Checkpoint checkpoint = readCheckpoint(location);
            final Path nextTranslogFile = location.resolve(getFilename(checkpoint.generation + 1));
            final Path currentCheckpointFile = location.resolve(getCommitCheckpointFileName(checkpoint.generation));
            assert Files.exists(nextTranslogFile) == false || Files.size(nextTranslogFile) <= TranslogHeader.headerSizeInBytes(translogUUID) : "unexpected translog file: [" + nextTranslogFile + "]";
            if (Files.exists(currentCheckpointFile) && Files.deleteIfExists(nextTranslogFile)) {
                logger.warn("deleted previously created, but not yet committed, next generation [{}]. This can happen due to a tragic exception when creating a new generation", nextTranslogFile.getFileName());
            }
            this.readers.addAll(recoverFromFiles(checkpoint));
            if (readers.isEmpty()) {
                throw new IllegalStateException("at least one reader must be recovered");
            }
            boolean success = false;
            current = null;
            try {
                current = createWriter(checkpoint.generation + 1, getMinFileGeneration(), checkpoint.globalCheckpoint);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(readers);
                }
            }
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(current);
            IOUtils.closeWhileHandlingException(readers);
            throw e;
        }
    }

    private ArrayList<TranslogReader> recoverFromFiles(Checkpoint checkpoint) throws IOException {
        boolean success = false;
        ArrayList<TranslogReader> foundTranslogs = new ArrayList<>();
        final Path tempFile = Files.createTempFile(location, TRANSLOG_FILE_PREFIX, TRANSLOG_FILE_SUFFIX);
        boolean tempFileRenamed = false;
        try (ReleasableLock lock = writeLock.acquire()) {
            logger.debug("open uncommitted translog checkpoint {}", checkpoint);
            final long minGenerationToRecoverFrom;
            if (checkpoint.minTranslogGeneration < 0) {
                final Version indexVersionCreated = indexSettings().getIndexVersionCreated();
                assert indexVersionCreated.before(Version.V_6_0_0_beta1) : "no minTranslogGeneration in checkpoint, but index was created with version [" + indexVersionCreated + "]";
                minGenerationToRecoverFrom = deletionPolicy.getMinTranslogGenerationForRecovery();
            } else {
                minGenerationToRecoverFrom = checkpoint.minTranslogGeneration;
            }
            final String checkpointTranslogFile = getFilename(checkpoint.generation);
            foundTranslogs.add(openReader(location.resolve(checkpointTranslogFile), checkpoint));
            for (long i = checkpoint.generation - 1; i >= minGenerationToRecoverFrom; i--) {
                Path committedTranslogFile = location.resolve(getFilename(i));
                if (Files.exists(committedTranslogFile) == false) {
                    throw new IllegalStateException("translog file doesn't exist with generation: " + i + " recovering from: " + minGenerationToRecoverFrom + " checkpoint: " + checkpoint.generation + " - translog ids must be consecutive");
                }
                final TranslogReader reader = openReader(committedTranslogFile, Checkpoint.read(location.resolve(getCommitCheckpointFileName(i))));
                assert reader.getPrimaryTerm() <= primaryTermSupplier.getAsLong() : "Primary terms go backwards; current term [" + primaryTermSupplier.getAsLong() + "]" + "translog path [ " + committedTranslogFile + ", existing term [" + reader.getPrimaryTerm() + "]";
                foundTranslogs.add(reader);
                logger.debug("recovered local translog from checkpoint {}", checkpoint);
            }
            Collections.reverse(foundTranslogs);
            IOUtils.deleteFilesIgnoringExceptions(location.resolve(getFilename(minGenerationToRecoverFrom - 1)), location.resolve(getCommitCheckpointFileName(minGenerationToRecoverFrom - 1)));
            Path commitCheckpoint = location.resolve(getCommitCheckpointFileName(checkpoint.generation));
            if (Files.exists(commitCheckpoint)) {
                Checkpoint checkpointFromDisk = Checkpoint.read(commitCheckpoint);
                if (checkpoint.equals(checkpointFromDisk) == false) {
                    throw new IllegalStateException("Checkpoint file " + commitCheckpoint.getFileName() + " already exists but has corrupted content expected: " + checkpoint + " but got: " + checkpointFromDisk);
                }
            } else {
                Files.copy(location.resolve(CHECKPOINT_FILE_NAME), tempFile, StandardCopyOption.REPLACE_EXISTING);
                IOUtils.fsync(tempFile, false);
                Files.move(tempFile, commitCheckpoint, StandardCopyOption.ATOMIC_MOVE);
                tempFileRenamed = true;
                IOUtils.fsync(commitCheckpoint.getParent(), true);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(foundTranslogs);
            }
            if (tempFileRenamed == false) {
                try {
                    Files.delete(tempFile);
                } catch (IOException ex) {
                    logger.warn(() -> new ParameterizedMessage("failed to delete temp file {}", tempFile), ex);
                }
            }
        }
        return foundTranslogs;
    }

    TranslogReader openReader(Path path, Checkpoint checkpoint) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            assert Translog.parseIdFromFileName(path) == checkpoint.generation : "expected generation: " + Translog.parseIdFromFileName(path) + " but got: " + checkpoint.generation;
            TranslogReader reader = TranslogReader.open(channel, path, checkpoint, translogUUID);
            channel = null;
            return reader;
        } finally {
            IOUtils.close(channel);
        }
    }

    public static long parseIdFromFileName(Path translogFile) {
        final String fileName = translogFile.getFileName().toString();
        final Matcher matcher = PARSE_STRICT_ID_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            try {
                return Long.parseLong(matcher.group(1));
            } catch (NumberFormatException e) {
                throw new IllegalStateException("number formatting issue in a file that passed PARSE_STRICT_ID_PATTERN: " + fileName + "]", e);
            }
        }
        throw new IllegalArgumentException("can't parse id from file: " + fileName);
    }

    public boolean isOpen() {
        return closed.get() == false;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try (ReleasableLock lock = writeLock.acquire()) {
                try {
                    current.sync();
                } finally {
                    closeFilesIfNoPendingRetentionLocks();
                }
            } finally {
                logger.debug("translog closed");
            }
        }
    }

    public Path location() {
        return location;
    }

    public long currentFileGeneration() {
        try (ReleasableLock ignored = readLock.acquire()) {
            return current.getGeneration();
        }
    }

    public long getMinFileGeneration() {
        try (ReleasableLock ignored = readLock.acquire()) {
            if (readers.isEmpty()) {
                return current.getGeneration();
            } else {
                assert readers.stream().map(TranslogReader::getGeneration).min(Long::compareTo).get().equals(readers.get(0).getGeneration()) : "the first translog isn't the one with the minimum generation:" + readers;
                return readers.get(0).getGeneration();
            }
        }
    }

    public int totalOperations() {
        return totalOperationsByMinGen(-1);
    }

    public long sizeInBytes() {
        return sizeInBytesByMinGen(-1);
    }

    long earliestLastModifiedAge() {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return findEarliestLastModifiedAge(System.currentTimeMillis(), readers, current);
        } catch (IOException e) {
            throw new TranslogException(shardId, "Unable to get the earliest last modified time for the transaction log");
        }
    }

    static long findEarliestLastModifiedAge(long currentTime, Iterable<TranslogReader> readers, TranslogWriter writer) throws IOException {
        long earliestTime = currentTime;
        for (BaseTranslogReader r : readers) {
            earliestTime = Math.min(r.getLastModifiedTime(), earliestTime);
        }
        return Math.max(0, currentTime - Math.min(earliestTime, writer.getLastModifiedTime()));
    }

    public int totalOperationsByMinGen(long minGeneration) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return Stream.concat(readers.stream(), Stream.of(current)).filter(r -> r.getGeneration() >= minGeneration).mapToInt(BaseTranslogReader::totalOperations).sum();
        }
    }

    public int estimateTotalOperationsFromMinSeq(long minSeqNo) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return readersAboveMinSeqNo(minSeqNo).mapToInt(BaseTranslogReader::totalOperations).sum();
        }
    }

    public long sizeInBytesByMinGen(long minGeneration) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return Stream.concat(readers.stream(), Stream.of(current)).filter(r -> r.getGeneration() >= minGeneration).mapToLong(BaseTranslogReader::sizeInBytes).sum();
        }
    }

    TranslogWriter createWriter(long fileGeneration) throws IOException {
        final TranslogWriter writer = createWriter(fileGeneration, getMinFileGeneration(), globalCheckpointSupplier.getAsLong());
        assert writer.sizeInBytes() == DEFAULT_HEADER_SIZE_IN_BYTES : "Mismatch translog header size; " + "empty translog size [" + writer.sizeInBytes() + ", header size [" + DEFAULT_HEADER_SIZE_IN_BYTES + "]";
        return writer;
    }

    TranslogWriter createWriter(long fileGeneration, long initialMinTranslogGen, long initialGlobalCheckpoint) throws IOException {
        final TranslogWriter newFile;
        try {
            newFile = TranslogWriter.create(shardId, translogUUID, fileGeneration, location.resolve(getFilename(fileGeneration)), getChannelFactory(), config.getBufferSize(), initialMinTranslogGen, initialGlobalCheckpoint, globalCheckpointSupplier, this::getMinFileGeneration, primaryTermSupplier.getAsLong());
        } catch (final IOException e) {
            throw new TranslogException(shardId, "failed to create new translog file", e);
        }
        return newFile;
    }

    public Location add(final Operation operation) throws IOException {
        final ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(bigArrays);
        try {
            final long start = out.position();
            out.skip(Integer.BYTES);
            writeOperationNoSize(new BufferedChecksumStreamOutput(out), operation);
            final long end = out.position();
            final int operationSize = (int) (end - Integer.BYTES - start);
            out.seek(start);
            out.writeInt(operationSize);
            out.seek(end);
            final ReleasablePagedBytesReference bytes = out.bytes();
            try (ReleasableLock ignored = readLock.acquire()) {
                ensureOpen();
                if (operation.primaryTerm() > current.getPrimaryTerm()) {
                    throw new IllegalArgumentException("Operation term is newer than the current term;" + "current term[" + current.getPrimaryTerm() + "], operation term[" + operation + "]");
                }
                return current.add(bytes, operation.seqNo());
            }
        } catch (final AlreadyClosedException | IOException ex) {
            closeOnTragicEvent(ex);
            throw ex;
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw new TranslogException(shardId, "Failed to write operation [" + operation + "]", ex);
        } finally {
            Releasables.close(out);
        }
    }

    public boolean shouldRollGeneration() {
        final long size = this.current.sizeInBytes();
        final long threshold = this.indexSettings.getGenerationThresholdSize().getBytes();
        return size > threshold;
    }

    public Location getLastWriteLocation() {
        try (ReleasableLock lock = readLock.acquire()) {
            return new Location(current.generation, current.sizeInBytes() - 1, Integer.MAX_VALUE);
        }
    }

    public long getLastSyncedGlobalCheckpoint() {
        try (ReleasableLock ignored = readLock.acquire()) {
            return current.getLastSyncedCheckpoint().globalCheckpoint;
        }
    }

    public Snapshot newSnapshot() throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            return newSnapshotFromGen(getMinFileGeneration());
        }
    }

    public Snapshot newSnapshotFromGen(long minGeneration) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            if (minGeneration < getMinFileGeneration()) {
                throw new IllegalArgumentException("requested snapshot generation [" + minGeneration + "] is not available. " + "Min referenced generation is [" + getMinFileGeneration() + "]");
            }
            TranslogSnapshot[] snapshots = Stream.concat(readers.stream(), Stream.of(current)).filter(reader -> reader.getGeneration() >= minGeneration).map(BaseTranslogReader::newSnapshot).toArray(TranslogSnapshot[]::new);
            return newMultiSnapshot(snapshots);
        }
    }

    public Operation readOperation(Location location) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            if (location.generation < getMinFileGeneration()) {
                return null;
            }
            if (current.generation == location.generation) {
                return current.read(location);
            } else {
                for (int i = readers.size() - 1; i >= 0; i--) {
                    TranslogReader translogReader = readers.get(i);
                    if (translogReader.generation == location.generation) {
                        return translogReader.read(location);
                    }
                }
            }
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
        return null;
    }

    public Snapshot newSnapshotFromMinSeqNo(long minSeqNo) throws IOException {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            TranslogSnapshot[] snapshots = readersAboveMinSeqNo(minSeqNo).map(BaseTranslogReader::newSnapshot).toArray(TranslogSnapshot[]::new);
            return newMultiSnapshot(snapshots);
        }
    }

    private Snapshot newMultiSnapshot(TranslogSnapshot[] snapshots) throws IOException {
        final Closeable onClose;
        if (snapshots.length == 0) {
            onClose = () -> {
            };
        } else {
            assert Arrays.stream(snapshots).map(BaseTranslogReader::getGeneration).min(Long::compareTo).get() == snapshots[0].generation : "first reader generation of " + snapshots + " is not the smallest";
            onClose = acquireTranslogGenFromDeletionPolicy(snapshots[0].generation);
        }
        boolean success = false;
        try {
            Snapshot result = new MultiSnapshot(snapshots, onClose);
            success = true;
            return result;
        } finally {
            if (success == false) {
                onClose.close();
            }
        }
    }

    private Stream<? extends BaseTranslogReader> readersAboveMinSeqNo(long minSeqNo) {
        assert readLock.isHeldByCurrentThread() || writeLock.isHeldByCurrentThread() : "callers of readersAboveMinSeqNo must hold a lock: readLock [" + readLock.isHeldByCurrentThread() + "], writeLock [" + readLock.isHeldByCurrentThread() + "]";
        return Stream.concat(readers.stream(), Stream.of(current)).filter(reader -> {
            final long maxSeqNo = reader.getCheckpoint().maxSeqNo;
            return maxSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO || maxSeqNo >= minSeqNo;
        });
    }

    public Closeable acquireRetentionLock() {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            final long viewGen = getMinFileGeneration();
            return acquireTranslogGenFromDeletionPolicy(viewGen);
        }
    }

    private Closeable acquireTranslogGenFromDeletionPolicy(long viewGen) {
        Releasable toClose = deletionPolicy.acquireTranslogGen(viewGen);
        return () -> {
            try {
                toClose.close();
            } finally {
                trimUnreferencedReaders();
                closeFilesIfNoPendingRetentionLocks();
            }
        };
    }

    public void sync() throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (closed.get() == false) {
                current.sync();
            }
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
    }

    public boolean syncNeeded() {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.syncNeeded();
        }
    }

    public static String getFilename(long generation) {
        return TRANSLOG_FILE_PREFIX + generation + TRANSLOG_FILE_SUFFIX;
    }

    static String getCommitCheckpointFileName(long generation) {
        return TRANSLOG_FILE_PREFIX + generation + CHECKPOINT_SUFFIX;
    }

    public void trimOperations(long belowTerm, long aboveSeqNo) throws IOException {
        assert aboveSeqNo >= SequenceNumbers.NO_OPS_PERFORMED : "aboveSeqNo has to a valid sequence number";
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            if (current.getPrimaryTerm() < belowTerm) {
                throw new IllegalArgumentException("Trimming the translog can only be done for terms lower than the current one. " + "Trim requested for term [ " + belowTerm + " ] , current is [ " + current.getPrimaryTerm() + " ]");
            }
            assert current.assertNoSeqAbove(belowTerm, aboveSeqNo);
            final List<TranslogReader> newReaders = new ArrayList<>(readers.size());
            try {
                for (TranslogReader reader : readers) {
                    final TranslogReader newReader = reader.getPrimaryTerm() < belowTerm ? reader.closeIntoTrimmedReader(aboveSeqNo, getChannelFactory()) : reader;
                    newReaders.add(newReader);
                }
            } catch (IOException e) {
                IOUtils.closeWhileHandlingException(newReaders);
                close();
                throw e;
            }
            this.readers.clear();
            this.readers.addAll(newReaders);
        }
    }

    public boolean ensureSynced(Location location) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (location.generation == current.getGeneration()) {
                ensureOpen();
                return current.syncUpTo(location.translogLocation + location.size);
            }
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
        return false;
    }

    public boolean ensureSynced(Stream<Location> locations) throws IOException {
        final Optional<Location> max = locations.max(Location::compareTo);
        if (max.isPresent()) {
            return ensureSynced(max.get());
        } else {
            return false;
        }
    }

    private void closeOnTragicEvent(final Exception ex) {
        assert readLock.isHeldByCurrentThread() == false : Thread.currentThread().getName();
        if (current.getTragicException() != null) {
            try {
                close();
            } catch (final AlreadyClosedException inner) {
            } catch (final Exception inner) {
                assert ex != inner.getCause();
                ex.addSuppressed(inner);
            }
        }
    }

    public TranslogStats stats() {
        try (ReleasableLock lock = readLock.acquire()) {
            final long uncommittedGen = deletionPolicy.getTranslogGenerationOfLastCommit();
            return new TranslogStats(totalOperations(), sizeInBytes(), totalOperationsByMinGen(uncommittedGen), sizeInBytesByMinGen(uncommittedGen), earliestLastModifiedAge());
        }
    }

    public TranslogConfig getConfig() {
        return config;
    }

    public TranslogDeletionPolicy getDeletionPolicy() {
        return deletionPolicy;
    }

    public static class Location implements Comparable<Location> {

        public final long generation;

        public final long translogLocation;

        public final int size;

        public Location(long generation, long translogLocation, int size) {
            this.generation = generation;
            this.translogLocation = translogLocation;
            this.size = size;
        }

        public String toString() {
            return "[generation: " + generation + ", location: " + translogLocation + ", size: " + size + "]";
        }

        @Override
        public int compareTo(Location o) {
            if (generation == o.generation) {
                return Long.compare(translogLocation, o.translogLocation);
            }
            return Long.compare(generation, o.generation);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Location location = (Location) o;
            if (generation != location.generation) {
                return false;
            }
            if (translogLocation != location.translogLocation) {
                return false;
            }
            return size == location.size;
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(generation);
            result = 31 * result + Long.hashCode(translogLocation);
            result = 31 * result + size;
            return result;
        }
    }

    public interface Snapshot extends Closeable {

        int totalOperations();

        default int skippedOperations() {
            return 0;
        }

        default int overriddenOperations() {
            return 0;
        }

        Translog.Operation next() throws IOException;
    }

    public interface Operation {

        enum Type {

            @Deprecated
            CREATE((byte) 1), INDEX((byte) 2), DELETE((byte) 3), NO_OP((byte) 4);

            private final byte id;

            Type(byte id) {
                this.id = id;
            }

            public byte id() {
                return this.id;
            }

            public static Type fromId(byte id) {
                switch(id) {
                    case 1:
                        return CREATE;
                    case 2:
                        return INDEX;
                    case 3:
                        return DELETE;
                    case 4:
                        return NO_OP;
                    default:
                        throw new IllegalArgumentException("no type mapped for [" + id + "]");
                }
            }
        }

        Type opType();

        long estimateSize();

        Source getSource();

        long seqNo();

        long primaryTerm();

        static Operation readOperation(final StreamInput input) throws IOException {
            final Translog.Operation.Type type = Translog.Operation.Type.fromId(input.readByte());
            switch(type) {
                case CREATE:
                case INDEX:
                    return new Index(input);
                case DELETE:
                    return new Delete(input);
                case NO_OP:
                    return new NoOp(input);
                default:
                    throw new AssertionError("no case for [" + type + "]");
            }
        }

        static void writeOperation(final StreamOutput output, final Operation operation) throws IOException {
            output.writeByte(operation.opType().id());
            switch(operation.opType()) {
                case CREATE:
                case INDEX:
                    ((Index) operation).write(output);
                    break;
                case DELETE:
                    ((Delete) operation).write(output);
                    break;
                case NO_OP:
                    ((NoOp) operation).write(output);
                    break;
                default:
                    throw new AssertionError("no case for [" + operation.opType() + "]");
            }
        }
    }

    public static class Source {

        public final BytesReference source;

        public final String routing;

        public Source(BytesReference source, String routing) {
            this.source = source;
            this.routing = routing;
        }
    }

    public static class Index implements Operation {

        public static final int FORMAT_6_0 = 8;

        public static final int FORMAT_NO_PARENT = FORMAT_6_0 + 1;

        public static final int SERIALIZATION_FORMAT = FORMAT_NO_PARENT;

        private final String id;

        private final long autoGeneratedIdTimestamp;

        private final String type;

        private final long seqNo;

        private final long primaryTerm;

        private final long version;

        private final VersionType versionType;

        private final BytesReference source;

        private final String routing;

        private Index(final StreamInput in) throws IOException {
            final int format = in.readVInt();
            assert format >= FORMAT_6_0 : "format was: " + format;
            id = in.readString();
            type = in.readString();
            source = in.readBytesReference();
            routing = in.readOptionalString();
            if (format < FORMAT_NO_PARENT) {
                in.readOptionalString();
            }
            this.version = in.readLong();
            this.versionType = VersionType.fromValue(in.readByte());
            assert versionType.validateVersionForWrites(this.version) : "invalid version for writes: " + this.version;
            this.autoGeneratedIdTimestamp = in.readLong();
            seqNo = in.readLong();
            primaryTerm = in.readLong();
        }

        public Index(Engine.Index index, Engine.IndexResult indexResult) {
            this.id = index.id();
            this.type = index.type();
            this.source = index.source();
            this.routing = index.routing();
            this.seqNo = indexResult.getSeqNo();
            this.primaryTerm = index.primaryTerm();
            this.version = indexResult.getVersion();
            this.versionType = index.versionType();
            this.autoGeneratedIdTimestamp = index.getAutoGeneratedIdTimestamp();
        }

        public Index(String type, String id, long seqNo, long primaryTerm, byte[] source) {
            this(type, id, seqNo, primaryTerm, Versions.MATCH_ANY, VersionType.INTERNAL, source, null, -1);
        }

        public Index(String type, String id, long seqNo, long primaryTerm, long version, VersionType versionType, byte[] source, String routing, long autoGeneratedIdTimestamp) {
            this.type = type;
            this.id = id;
            this.source = new BytesArray(source);
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
            this.versionType = versionType;
            this.routing = routing;
            this.autoGeneratedIdTimestamp = autoGeneratedIdTimestamp;
        }

        @Override
        public Type opType() {
            return Type.INDEX;
        }

        @Override
        public long estimateSize() {
            return ((id.length() + type.length()) * 2) + source.length() + 12;
        }

        public String type() {
            return this.type;
        }

        public String id() {
            return this.id;
        }

        public String routing() {
            return this.routing;
        }

        public BytesReference source() {
            return this.source;
        }

        @Override
        public long seqNo() {
            return seqNo;
        }

        @Override
        public long primaryTerm() {
            return primaryTerm;
        }

        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return versionType;
        }

        @Override
        public Source getSource() {
            return new Source(source, routing);
        }

        private void write(final StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(id);
            out.writeString(type);
            out.writeBytesReference(source);
            out.writeOptionalString(routing);
            out.writeLong(version);
            out.writeByte(versionType.getValue());
            out.writeLong(autoGeneratedIdTimestamp);
            out.writeLong(seqNo);
            out.writeLong(primaryTerm);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Index index = (Index) o;
            if (version != index.version || seqNo != index.seqNo || primaryTerm != index.primaryTerm || id.equals(index.id) == false || type.equals(index.type) == false || versionType != index.versionType || autoGeneratedIdTimestamp != index.autoGeneratedIdTimestamp || source.equals(index.source) == false) {
                return false;
            }
            if (routing != null ? !routing.equals(index.routing) : index.routing != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + Long.hashCode(seqNo);
            result = 31 * result + Long.hashCode(primaryTerm);
            result = 31 * result + Long.hashCode(version);
            result = 31 * result + versionType.hashCode();
            result = 31 * result + source.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + Long.hashCode(autoGeneratedIdTimestamp);
            return result;
        }

        @Override
        public String toString() {
            return "Index{" + "id='" + id + '\'' + ", type='" + type + '\'' + ", seqNo=" + seqNo + ", primaryTerm=" + primaryTerm + '}';
        }

        public long getAutoGeneratedIdTimestamp() {
            return autoGeneratedIdTimestamp;
        }
    }

    public static class Delete implements Operation {

        private static final int FORMAT_6_0 = 4;

        public static final int SERIALIZATION_FORMAT = FORMAT_6_0;

        private final String type, id;

        private final Term uid;

        private final long seqNo;

        private final long primaryTerm;

        private final long version;

        private final VersionType versionType;

        private Delete(final StreamInput in) throws IOException {
            final int format = in.readVInt();
            assert format >= FORMAT_6_0 : "format was: " + format;
            type = in.readString();
            id = in.readString();
            uid = new Term(in.readString(), in.readBytesRef());
            this.version = in.readLong();
            this.versionType = VersionType.fromValue(in.readByte());
            assert versionType.validateVersionForWrites(this.version);
            seqNo = in.readLong();
            primaryTerm = in.readLong();
        }

        public Delete(Engine.Delete delete, Engine.DeleteResult deleteResult) {
            this(delete.type(), delete.id(), delete.uid(), deleteResult.getSeqNo(), delete.primaryTerm(), deleteResult.getVersion(), delete.versionType());
        }

        public Delete(String type, String id, long seqNo, long primaryTerm, Term uid) {
            this(type, id, uid, seqNo, primaryTerm, Versions.MATCH_ANY, VersionType.INTERNAL);
        }

        public Delete(String type, String id, Term uid, long seqNo, long primaryTerm, long version, VersionType versionType) {
            this.type = Objects.requireNonNull(type);
            this.id = Objects.requireNonNull(id);
            this.uid = uid;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.version = version;
            this.versionType = versionType;
        }

        @Override
        public Type opType() {
            return Type.DELETE;
        }

        @Override
        public long estimateSize() {
            return ((uid.field().length() + uid.text().length()) * 2) + 20;
        }

        public String type() {
            return type;
        }

        public String id() {
            return id;
        }

        public Term uid() {
            return this.uid;
        }

        @Override
        public long seqNo() {
            return seqNo;
        }

        @Override
        public long primaryTerm() {
            return primaryTerm;
        }

        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return this.versionType;
        }

        @Override
        public Source getSource() {
            throw new IllegalStateException("trying to read doc source from delete operation");
        }

        private void write(final StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(type);
            out.writeString(id);
            out.writeString(uid.field());
            out.writeBytesRef(uid.bytes());
            out.writeLong(version);
            out.writeByte(versionType.getValue());
            out.writeLong(seqNo);
            out.writeLong(primaryTerm);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Delete delete = (Delete) o;
            return version == delete.version && seqNo == delete.seqNo && primaryTerm == delete.primaryTerm && uid.equals(delete.uid) && versionType == delete.versionType;
        }

        @Override
        public int hashCode() {
            int result = uid.hashCode();
            result = 31 * result + Long.hashCode(seqNo);
            result = 31 * result + Long.hashCode(primaryTerm);
            result = 31 * result + Long.hashCode(version);
            result = 31 * result + versionType.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Delete{" + "uid=" + uid + ", seqNo=" + seqNo + ", primaryTerm=" + primaryTerm + '}';
        }
    }

    public static class NoOp implements Operation {

        private final long seqNo;

        private final long primaryTerm;

        private final String reason;

        @Override
        public long seqNo() {
            return seqNo;
        }

        @Override
        public long primaryTerm() {
            return primaryTerm;
        }

        public String reason() {
            return reason;
        }

        private NoOp(final StreamInput in) throws IOException {
            seqNo = in.readLong();
            primaryTerm = in.readLong();
            reason = in.readString();
        }

        public NoOp(final long seqNo, final long primaryTerm, final String reason) {
            assert seqNo > SequenceNumbers.NO_OPS_PERFORMED;
            assert primaryTerm >= 0;
            assert reason != null;
            this.seqNo = seqNo;
            this.primaryTerm = primaryTerm;
            this.reason = reason;
        }

        private void write(final StreamOutput out) throws IOException {
            out.writeLong(seqNo);
            out.writeLong(primaryTerm);
            out.writeString(reason);
        }

        @Override
        public Type opType() {
            return Type.NO_OP;
        }

        @Override
        public long estimateSize() {
            return 2 * reason.length() + 2 * Long.BYTES;
        }

        @Override
        public Source getSource() {
            throw new UnsupportedOperationException("source does not exist for a no-op");
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final NoOp that = (NoOp) obj;
            return seqNo == that.seqNo && primaryTerm == that.primaryTerm && reason.equals(that.reason);
        }

        @Override
        public int hashCode() {
            return 31 * 31 * 31 + 31 * 31 * Long.hashCode(seqNo) + 31 * Long.hashCode(primaryTerm) + reason().hashCode();
        }

        @Override
        public String toString() {
            return "NoOp{" + "seqNo=" + seqNo + ", primaryTerm=" + primaryTerm + ", reason='" + reason + '\'' + '}';
        }
    }

    public enum Durability {

        ASYNC, REQUEST
    }

    static void verifyChecksum(BufferedChecksumStreamInput in) throws IOException {
        long expectedChecksum = in.getChecksum();
        long readChecksum = Integer.toUnsignedLong(in.readInt());
        if (readChecksum != expectedChecksum) {
            throw new TranslogCorruptedException("translog stream is corrupted, expected: 0x" + Long.toHexString(expectedChecksum) + ", got: 0x" + Long.toHexString(readChecksum));
        }
    }

    public static List<Operation> readOperations(StreamInput input) throws IOException {
        ArrayList<Operation> operations = new ArrayList<>();
        int numOps = input.readInt();
        final BufferedChecksumStreamInput checksumStreamInput = new BufferedChecksumStreamInput(input);
        for (int i = 0; i < numOps; i++) {
            operations.add(readOperation(checksumStreamInput));
        }
        return operations;
    }

    static Translog.Operation readOperation(BufferedChecksumStreamInput in) throws IOException {
        final Translog.Operation operation;
        try {
            final int opSize = in.readInt();
            if (opSize < 4) {
                throw new TranslogCorruptedException("operation size must be at least 4 but was: " + opSize);
            }
            in.resetDigest();
            if (in.markSupported()) {
                in.mark(opSize);
                in.skip(opSize - 4);
                verifyChecksum(in);
                in.reset();
            }
            operation = Translog.Operation.readOperation(in);
            verifyChecksum(in);
        } catch (TranslogCorruptedException e) {
            throw e;
        } catch (EOFException e) {
            throw new TruncatedTranslogException("reached premature end of file, translog is truncated", e);
        }
        return operation;
    }

    public static void writeOperations(StreamOutput outStream, List<Operation> toWrite) throws IOException {
        final ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(BigArrays.NON_RECYCLING_INSTANCE);
        try {
            outStream.writeInt(toWrite.size());
            final BufferedChecksumStreamOutput checksumStreamOutput = new BufferedChecksumStreamOutput(out);
            for (Operation op : toWrite) {
                out.reset();
                final long start = out.position();
                out.skip(Integer.BYTES);
                writeOperationNoSize(checksumStreamOutput, op);
                long end = out.position();
                int operationSize = (int) (out.position() - Integer.BYTES - start);
                out.seek(start);
                out.writeInt(operationSize);
                out.seek(end);
                ReleasablePagedBytesReference bytes = out.bytes();
                bytes.writeTo(outStream);
            }
        } finally {
            Releasables.close(out);
        }
    }

    public static void writeOperationNoSize(BufferedChecksumStreamOutput out, Translog.Operation op) throws IOException {
        out.resetDigest();
        Translog.Operation.writeOperation(out, op);
        long checksum = out.getChecksum();
        out.writeInt((int) checksum);
    }

    public TranslogGeneration getMinGenerationForSeqNo(final long seqNo) {
        try (ReleasableLock ignored = readLock.acquire()) {
            long minTranslogFileGeneration = this.currentFileGeneration();
            for (final TranslogReader reader : readers) {
                if (seqNo <= reader.getCheckpoint().maxSeqNo) {
                    minTranslogFileGeneration = Math.min(minTranslogFileGeneration, reader.getGeneration());
                }
            }
            return new TranslogGeneration(translogUUID, minTranslogFileGeneration);
        }
    }

    public void rollGeneration() throws IOException {
        try (Releasable ignored = writeLock.acquire()) {
            try {
                final TranslogReader reader = current.closeIntoReader();
                readers.add(reader);
                final Path checkpoint = location.resolve(CHECKPOINT_FILE_NAME);
                assert Checkpoint.read(checkpoint).generation == current.getGeneration();
                final Path generationCheckpoint = location.resolve(getCommitCheckpointFileName(current.getGeneration()));
                Files.copy(checkpoint, generationCheckpoint);
                IOUtils.fsync(generationCheckpoint, false);
                IOUtils.fsync(generationCheckpoint.getParent(), true);
                current = createWriter(current.getGeneration() + 1);
                logger.trace("current translog set to [{}]", current.getGeneration());
            } catch (final Exception e) {
                IOUtils.closeWhileHandlingException(this);
                throw e;
            }
        }
    }

    public void trimUnreferencedReaders() throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            if (closed.get()) {
                return;
            }
            long minReferencedGen = deletionPolicy.minTranslogGenRequired(readers, current);
            assert minReferencedGen >= getMinFileGeneration() : "deletion policy requires a minReferenceGen of [" + minReferencedGen + "] but the lowest gen available is [" + getMinFileGeneration() + "]";
            assert minReferencedGen <= currentFileGeneration() : "deletion policy requires a minReferenceGen of [" + minReferencedGen + "] which is higher than the current generation [" + currentFileGeneration() + "]";
            for (Iterator<TranslogReader> iterator = readers.iterator(); iterator.hasNext(); ) {
                TranslogReader reader = iterator.next();
                if (reader.getGeneration() >= minReferencedGen) {
                    break;
                }
                iterator.remove();
                IOUtils.closeWhileHandlingException(reader);
                final Path translogPath = reader.path();
                logger.trace("delete translog file [{}], not referenced and not current anymore", translogPath);
                current.sync();
                deleteReaderFiles(reader);
            }
            assert readers.isEmpty() == false || current.generation == minReferencedGen : "all readers were cleaned but the minReferenceGen [" + minReferencedGen + "] is not the current writer's gen [" + current.generation + "]";
        } catch (final Exception ex) {
            closeOnTragicEvent(ex);
            throw ex;
        }
    }

    void deleteReaderFiles(TranslogReader reader) {
        IOUtils.deleteFilesIgnoringExceptions(reader.path(), reader.path().resolveSibling(getCommitCheckpointFileName(reader.getGeneration())));
    }

    void closeFilesIfNoPendingRetentionLocks() throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            if (closed.get() && deletionPolicy.pendingTranslogRefCount() == 0) {
                logger.trace("closing files. translog is closed and there are no pending retention locks");
                ArrayList<Closeable> toClose = new ArrayList<>(readers);
                toClose.add(current);
                IOUtils.close(toClose);
            }
        }
    }

    public static final class TranslogGeneration {

        public final String translogUUID;

        public final long translogFileGeneration;

        public TranslogGeneration(String translogUUID, long translogFileGeneration) {
            this.translogUUID = translogUUID;
            this.translogFileGeneration = translogFileGeneration;
        }
    }

    public TranslogGeneration getGeneration() {
        try (ReleasableLock lock = writeLock.acquire()) {
            return new TranslogGeneration(translogUUID, currentFileGeneration());
        }
    }

    public boolean isCurrent(TranslogGeneration generation) {
        try (ReleasableLock lock = writeLock.acquire()) {
            if (generation != null) {
                if (generation.translogUUID.equals(translogUUID) == false) {
                    throw new IllegalArgumentException("commit belongs to a different translog: " + generation.translogUUID + " vs. " + translogUUID);
                }
                return generation.translogFileGeneration == currentFileGeneration();
            }
        }
        return false;
    }

    long getFirstOperationPosition() {
        return current.getFirstOperationOffset();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new AlreadyClosedException("translog is already closed", current.getTragicException());
        }
    }

    ChannelFactory getChannelFactory() {
        return FileChannel::open;
    }

    public Exception getTragicException() {
        return current.getTragicException();
    }

    static Checkpoint readCheckpoint(final Path location) throws IOException {
        return Checkpoint.read(location.resolve(CHECKPOINT_FILE_NAME));
    }

    public static long readGlobalCheckpoint(final Path location, final String expectedTranslogUUID) throws IOException {
        final Checkpoint checkpoint = readCheckpoint(location, expectedTranslogUUID);
        return checkpoint.globalCheckpoint;
    }

    private static Checkpoint readCheckpoint(Path location, String expectedTranslogUUID) throws IOException {
        final Checkpoint checkpoint = readCheckpoint(location);
        final Path translogFile = location.resolve(getFilename(checkpoint.generation));
        try (FileChannel channel = FileChannel.open(translogFile, StandardOpenOption.READ)) {
            TranslogHeader.read(expectedTranslogUUID, translogFile, channel);
        } catch (TranslogCorruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new TranslogCorruptedException("Translog at [" + location + "] is corrupted", ex);
        }
        return checkpoint;
    }

    public static long readMinTranslogGeneration(final Path location, final String expectedTranslogUUID) throws IOException {
        final Checkpoint checkpoint = readCheckpoint(location, expectedTranslogUUID);
        return checkpoint.minTranslogGeneration;
    }

    public String getTranslogUUID() {
        return translogUUID;
    }

    TranslogWriter getCurrent() {
        return current;
    }

    List<TranslogReader> getReaders() {
        return readers;
    }

    public static String createEmptyTranslog(final Path location, final long initialGlobalCheckpoint, final ShardId shardId, final long primaryTerm) throws IOException {
        final ChannelFactory channelFactory = FileChannel::open;
        return createEmptyTranslog(location, initialGlobalCheckpoint, shardId, channelFactory, primaryTerm);
    }

    static String createEmptyTranslog(Path location, long initialGlobalCheckpoint, ShardId shardId, ChannelFactory channelFactory, long primaryTerm) throws IOException {
        IOUtils.rm(location);
        Files.createDirectories(location);
        final Checkpoint checkpoint = Checkpoint.emptyTranslogCheckpoint(0, 1, initialGlobalCheckpoint, 1);
        final Path checkpointFile = location.resolve(CHECKPOINT_FILE_NAME);
        Checkpoint.write(channelFactory, checkpointFile, checkpoint, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        IOUtils.fsync(checkpointFile, false);
        final String translogUUID = UUIDs.randomBase64UUID();
        TranslogWriter writer = TranslogWriter.create(shardId, translogUUID, 1, location.resolve(getFilename(1)), channelFactory, new ByteSizeValue(10), 1, initialGlobalCheckpoint, () -> {
            throw new UnsupportedOperationException();
        }, () -> {
            throw new UnsupportedOperationException();
        }, primaryTerm);
        writer.close();
        return translogUUID;
    }
}