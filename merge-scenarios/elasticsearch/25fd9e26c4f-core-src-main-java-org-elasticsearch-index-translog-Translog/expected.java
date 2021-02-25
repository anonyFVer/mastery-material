package org.elasticsearch.index.translog;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TwoPhaseCommit;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardComponent;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Translog extends AbstractIndexShardComponent implements IndexShardComponent, Closeable, TwoPhaseCommit {

    public static final String TRANSLOG_GENERATION_KEY = "translog_generation";

    public static final String TRANSLOG_UUID_KEY = "translog_uuid";

    public static final String TRANSLOG_FILE_PREFIX = "translog-";

    public static final String TRANSLOG_FILE_SUFFIX = ".tlog";

    public static final String CHECKPOINT_SUFFIX = ".ckp";

    public static final String CHECKPOINT_FILE_NAME = "translog" + CHECKPOINT_SUFFIX;

    static final Pattern PARSE_STRICT_ID_PATTERN = Pattern.compile("^" + TRANSLOG_FILE_PREFIX + "(\\d+)(\\.tlog)$");

    private final List<TranslogReader> readers = new ArrayList<>();

    private final Set<View> outstandingViews = ConcurrentCollections.newConcurrentSet();

    private BigArrays bigArrays;

    protected final ReleasableLock readLock;

    protected final ReleasableLock writeLock;

    private final Path location;

    private TranslogWriter current;

    private static final long NOT_SET_GENERATION = -1;

    private volatile long currentCommittingGeneration = NOT_SET_GENERATION;

    private volatile long lastCommittedTranslogFileGeneration = NOT_SET_GENERATION;

    private final AtomicBoolean closed = new AtomicBoolean();

    private final TranslogConfig config;

    private final String translogUUID;

    public Translog(TranslogConfig config, TranslogGeneration translogGeneration) throws IOException {
        super(config.getShardId(), config.getIndexSettings());
        this.config = config;
        if (translogGeneration == null || translogGeneration.translogUUID == null) {
            translogUUID = UUIDs.randomBase64UUID();
        } else {
            translogUUID = translogGeneration.translogUUID;
        }
        bigArrays = config.getBigArrays();
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());
        this.location = config.getTranslogPath();
        Files.createDirectories(this.location);
        try {
            if (translogGeneration != null) {
                final Checkpoint checkpoint = readCheckpoint();
                final Path nextTranslogFile = location.resolve(getFilename(checkpoint.generation + 1));
                final Path currentCheckpointFile = location.resolve(getCommitCheckpointFileName(checkpoint.generation));
                assert Files.exists(nextTranslogFile) == false || Files.size(nextTranslogFile) <= TranslogWriter.getHeaderLength(translogUUID) : "unexpected translog file: [" + nextTranslogFile + "]";
                if (Files.exists(currentCheckpointFile) && Files.deleteIfExists(nextTranslogFile)) {
                    logger.warn("deleted previously created, but not yet committed, next generation [{}]. This can happen due to a tragic exception when creating a new generation", nextTranslogFile.getFileName());
                }
                this.readers.addAll(recoverFromFiles(translogGeneration, checkpoint));
                if (readers.isEmpty()) {
                    throw new IllegalStateException("at least one reader must be recovered");
                }
                boolean success = false;
                try {
                    current = createWriter(checkpoint.generation + 1);
                    this.lastCommittedTranslogFileGeneration = translogGeneration.translogFileGeneration;
                    success = true;
                } finally {
                    if (success == false) {
                        IOUtils.closeWhileHandlingException(readers);
                    }
                }
            } else {
                IOUtils.rm(location);
                logger.debug("wipe translog location - creating new translog");
                Files.createDirectories(location);
                final long generation = 1;
                Checkpoint checkpoint = new Checkpoint(0, 0, generation);
                final Path checkpointFile = location.resolve(CHECKPOINT_FILE_NAME);
                Checkpoint.write(getChannelFactory(), checkpointFile, checkpoint, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
                IOUtils.fsync(checkpointFile, false);
                current = createWriter(generation);
                this.lastCommittedTranslogFileGeneration = NOT_SET_GENERATION;
            }
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(current);
            IOUtils.closeWhileHandlingException(readers);
            throw e;
        }
    }

    private ArrayList<TranslogReader> recoverFromFiles(TranslogGeneration translogGeneration, Checkpoint checkpoint) throws IOException {
        boolean success = false;
        ArrayList<TranslogReader> foundTranslogs = new ArrayList<>();
        final Path tempFile = Files.createTempFile(location, TRANSLOG_FILE_PREFIX, TRANSLOG_FILE_SUFFIX);
        boolean tempFileRenamed = false;
        try (ReleasableLock lock = writeLock.acquire()) {
            logger.debug("open uncommitted translog checkpoint {}", checkpoint);
            final String checkpointTranslogFile = getFilename(checkpoint.generation);
            for (long i = translogGeneration.translogFileGeneration; i < checkpoint.generation; i++) {
                Path committedTranslogFile = location.resolve(getFilename(i));
                if (Files.exists(committedTranslogFile) == false) {
                    throw new IllegalStateException("translog file doesn't exist with generation: " + i + " lastCommitted: " + lastCommittedTranslogFileGeneration + " checkpoint: " + checkpoint.generation + " - translog ids must be consecutive");
                }
                final TranslogReader reader = openReader(committedTranslogFile, Checkpoint.read(location.resolve(getCommitCheckpointFileName(i))));
                foundTranslogs.add(reader);
                logger.debug("recovered local translog from checkpoint {}", checkpoint);
            }
            foundTranslogs.add(openReader(location.resolve(checkpointTranslogFile), checkpoint));
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
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to delete temp file {}", tempFile), ex);
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
                    closeFilesIfNoPendingViews();
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
        try (ReleasableLock lock = readLock.acquire()) {
            return current.getGeneration();
        }
    }

    public int totalOperations() {
        return totalOperations(lastCommittedTranslogFileGeneration);
    }

    public long sizeInBytes() {
        return sizeInBytes(lastCommittedTranslogFileGeneration);
    }

    private int totalOperations(long minGeneration) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return Stream.concat(readers.stream(), Stream.of(current)).filter(r -> r.getGeneration() >= minGeneration).mapToInt(BaseTranslogReader::totalOperations).sum();
        }
    }

    private long sizeInBytes(long minGeneration) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            return Stream.concat(readers.stream(), Stream.of(current)).filter(r -> r.getGeneration() >= minGeneration).mapToLong(BaseTranslogReader::sizeInBytes).sum();
        }
    }

    TranslogWriter createWriter(long fileGeneration) throws IOException {
        TranslogWriter newFile;
        try {
            newFile = TranslogWriter.create(shardId, translogUUID, fileGeneration, location.resolve(getFilename(fileGeneration)), getChannelFactory(), config.getBufferSize());
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to create new translog file", e);
        }
        return newFile;
    }

    public Location add(Operation operation) throws IOException {
        final ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(bigArrays);
        try {
            final BufferedChecksumStreamOutput checksumStreamOutput = new BufferedChecksumStreamOutput(out);
            final long start = out.position();
            out.skip(Integer.BYTES);
            writeOperationNoSize(checksumStreamOutput, operation);
            final long end = out.position();
            final int operationSize = (int) (end - Integer.BYTES - start);
            out.seek(start);
            out.writeInt(operationSize);
            out.seek(end);
            final ReleasablePagedBytesReference bytes = out.bytes();
            try (ReleasableLock lock = readLock.acquire()) {
                ensureOpen();
                Location location = current.add(bytes);
                return location;
            }
        } catch (AlreadyClosedException | IOException ex) {
            try {
                closeOnTragicEvent(ex);
            } catch (Exception inner) {
                ex.addSuppressed(inner);
            }
            throw ex;
        } catch (Exception e) {
            try {
                closeOnTragicEvent(e);
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
            throw new TranslogException(shardId, "Failed to write operation [" + operation + "]", e);
        } finally {
            Releasables.close(out.bytes());
        }
    }

    public Location getLastWriteLocation() {
        try (ReleasableLock lock = readLock.acquire()) {
            return new Location(current.generation, current.sizeInBytes() - 1, Integer.MAX_VALUE);
        }
    }

    public Snapshot newSnapshot() {
        return createSnapshot(Long.MIN_VALUE);
    }

    private Snapshot createSnapshot(long minGeneration) {
        try (ReleasableLock ignored = readLock.acquire()) {
            ensureOpen();
            Snapshot[] snapshots = Stream.concat(readers.stream(), Stream.of(current)).filter(reader -> reader.getGeneration() >= minGeneration).map(BaseTranslogReader::newSnapshot).toArray(Snapshot[]::new);
            return new MultiSnapshot(snapshots);
        }
    }

    public Translog.View newView() {
        try (ReleasableLock lock = readLock.acquire()) {
            ensureOpen();
            View view = new View(lastCommittedTranslogFileGeneration);
            outstandingViews.add(view);
            return view;
        }
    }

    public void sync() throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (closed.get() == false) {
                current.sync();
            }
        } catch (Exception ex) {
            try {
                closeOnTragicEvent(ex);
            } catch (Exception inner) {
                ex.addSuppressed(inner);
            }
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

    public boolean ensureSynced(Location location) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (location.generation == current.getGeneration()) {
                ensureOpen();
                return current.syncUpTo(location.translogLocation + location.size);
            }
        } catch (Exception ex) {
            try {
                closeOnTragicEvent(ex);
            } catch (Exception inner) {
                ex.addSuppressed(inner);
            }
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

    private void closeOnTragicEvent(Exception ex) {
        if (current.getTragicException() != null) {
            try {
                close();
            } catch (AlreadyClosedException inner) {
            } catch (Exception inner) {
                assert (ex != inner.getCause());
                ex.addSuppressed(inner);
            }
        }
    }

    public TranslogStats stats() {
        try (ReleasableLock lock = readLock.acquire()) {
            return new TranslogStats(totalOperations(), sizeInBytes());
        }
    }

    public TranslogConfig getConfig() {
        return config;
    }

    public class View implements Closeable {

        AtomicBoolean closed = new AtomicBoolean();

        final long minGeneration;

        View(long minGeneration) {
            this.minGeneration = minGeneration;
        }

        public long minTranslogGeneration() {
            return minGeneration;
        }

        public int totalOperations() {
            return Translog.this.totalOperations(minGeneration);
        }

        public long sizeInBytes() {
            return Translog.this.sizeInBytes(minGeneration);
        }

        public Snapshot snapshot() {
            ensureOpen();
            return Translog.this.createSnapshot(minGeneration);
        }

        void ensureOpen() {
            if (closed.get()) {
                throw new AlreadyClosedException("View is already closed");
            }
        }

        @Override
        public void close() throws IOException {
            if (closed.getAndSet(true) == false) {
                logger.trace("closing view starting at translog [{}]", minTranslogGeneration());
                boolean removed = outstandingViews.remove(this);
                assert removed : "View was never set but was supposed to be removed";
                trimUnreferencedReaders();
                closeFilesIfNoPendingViews();
            }
        }
    }

    public static class Location implements Comparable<Location> {

        public final long generation;

        public final long translogLocation;

        public final int size;

        Location(long generation, long translogLocation, int size) {
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

    public interface Snapshot {

        int totalOperations();

        Translog.Operation next() throws IOException;
    }

    public interface Operation extends Writeable {

        enum Type {

            @Deprecated
            CREATE((byte) 1), INDEX((byte) 2), DELETE((byte) 3);

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
                    default:
                        throw new IllegalArgumentException("No type mapped for [" + id + "]");
                }
            }
        }

        Type opType();

        long estimateSize();

        Source getSource();

        static Operation readType(StreamInput input) throws IOException {
            Translog.Operation.Type type = Translog.Operation.Type.fromId(input.readByte());
            switch(type) {
                case CREATE:
                    return new Index(input);
                case DELETE:
                    return new Translog.Delete(input);
                case INDEX:
                    return new Index(input);
                default:
                    throw new IOException("No type for [" + type + "]");
            }
        }

        static void writeType(Translog.Operation operation, StreamOutput output) throws IOException {
            output.writeByte(operation.opType().id());
            operation.writeTo(output);
        }
    }

    public static class Source {

        public final BytesReference source;

        public final String routing;

        public final String parent;

        public final long timestamp;

        public final long ttl;

        public Source(BytesReference source, String routing, String parent, long timestamp, long ttl) {
            this.source = source;
            this.routing = routing;
            this.parent = parent;
            this.timestamp = timestamp;
            this.ttl = ttl;
        }
    }

    public static class Index implements Operation {

        public static final int FORMAT_2x = 6;

        public static final int FORMAT_AUTO_GENERATED_IDS = 7;

        public static final int FORMAT_SEQ_NO = FORMAT_AUTO_GENERATED_IDS + 1;

        public static final int SERIALIZATION_FORMAT = FORMAT_SEQ_NO + 1;

        private final String id;

        private final long autoGeneratedIdTimestamp;

        private final String type;

        private long seqNo = SequenceNumbersService.UNASSIGNED_SEQ_NO;

        private final long version;

        private final VersionType versionType;

        private final BytesReference source;

        private final String routing;

        private final String parent;

        private final long timestamp;

        private final long ttl;

        public Index(StreamInput in) throws IOException {
            final int format = in.readVInt();
            assert format >= FORMAT_2x : "format was: " + format;
            id = in.readString();
            type = in.readString();
            source = in.readBytesReference();
            routing = in.readOptionalString();
            parent = in.readOptionalString();
            this.version = in.readLong();
            this.timestamp = in.readLong();
            this.ttl = in.readLong();
            this.versionType = VersionType.fromValue(in.readByte());
            assert versionType.validateVersionForWrites(this.version);
            if (format >= FORMAT_AUTO_GENERATED_IDS) {
                this.autoGeneratedIdTimestamp = in.readLong();
            } else {
                this.autoGeneratedIdTimestamp = IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP;
            }
            if (format >= FORMAT_SEQ_NO) {
                seqNo = in.readVLong();
            }
        }

        public Index(Engine.Index index) {
            this.id = index.id();
            this.type = index.type();
            this.source = index.source();
            this.routing = index.routing();
            this.parent = index.parent();
            this.seqNo = index.seqNo();
            this.version = index.version();
            this.timestamp = index.timestamp();
            this.ttl = index.ttl();
            this.versionType = index.versionType();
            this.autoGeneratedIdTimestamp = index.getAutoGeneratedIdTimestamp();
        }

        public Index(String type, String id, byte[] source) {
            this.type = type;
            this.id = id;
            this.source = new BytesArray(source);
            this.seqNo = 0;
            version = Versions.MATCH_ANY;
            versionType = VersionType.INTERNAL;
            routing = null;
            parent = null;
            timestamp = 0;
            ttl = 0;
            autoGeneratedIdTimestamp = -1;
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

        public String parent() {
            return this.parent;
        }

        public long timestamp() {
            return this.timestamp;
        }

        public long ttl() {
            return this.ttl;
        }

        public BytesReference source() {
            return this.source;
        }

        public long seqNo() {
            return seqNo;
        }

        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return versionType;
        }

        @Override
        public Source getSource() {
            return new Source(source, routing, parent, timestamp, ttl);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(id);
            out.writeString(type);
            out.writeBytesReference(source);
            out.writeOptionalString(routing);
            out.writeOptionalString(parent);
            out.writeLong(version);
            out.writeLong(timestamp);
            out.writeLong(ttl);
            out.writeByte(versionType.getValue());
            out.writeLong(autoGeneratedIdTimestamp);
            out.writeVLong(seqNo);
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
            if (version != index.version || seqNo != index.seqNo || timestamp != index.timestamp || ttl != index.ttl || id.equals(index.id) == false || type.equals(index.type) == false || versionType != index.versionType || autoGeneratedIdTimestamp != index.autoGeneratedIdTimestamp || source.equals(index.source) == false) {
                return false;
            }
            if (routing != null ? !routing.equals(index.routing) : index.routing != null) {
                return false;
            }
            return !(parent != null ? !parent.equals(index.parent) : index.parent != null);
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + Long.hashCode(seqNo);
            result = 31 * result + Long.hashCode(version);
            result = 31 * result + versionType.hashCode();
            result = 31 * result + source.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + (parent != null ? parent.hashCode() : 0);
            result = 31 * result + Long.hashCode(timestamp);
            result = 31 * result + Long.hashCode(autoGeneratedIdTimestamp);
            result = 31 * result + Long.hashCode(ttl);
            return result;
        }

        @Override
        public String toString() {
            return "Index{" + "id='" + id + '\'' + ", type='" + type + '\'' + '}';
        }

        public long getAutoGeneratedIdTimestamp() {
            return autoGeneratedIdTimestamp;
        }
    }

    public static class Delete implements Operation {

        public static final int SERIALIZATION_FORMAT = 3;

        private Term uid;

        private long seqNo = SequenceNumbersService.UNASSIGNED_SEQ_NO;

        private long version = Versions.MATCH_ANY;

        private VersionType versionType = VersionType.INTERNAL;

        public Delete(StreamInput in) throws IOException {
            final int format = in.readVInt();
            assert format >= SERIALIZATION_FORMAT - 1 : "format was: " + format;
            uid = new Term(in.readString(), in.readString());
            this.version = in.readLong();
            this.versionType = VersionType.fromValue(in.readByte());
            assert versionType.validateVersionForWrites(this.version);
            if (format >= 3) {
                seqNo = in.readZLong();
            }
        }

        public Delete(Engine.Delete delete) {
            this(delete.uid(), delete.seqNo(), delete.version(), delete.versionType());
        }

        public Delete(Term uid) {
            this(uid, SequenceNumbersService.UNASSIGNED_SEQ_NO, Versions.MATCH_ANY, VersionType.INTERNAL);
        }

        public Delete(Term uid, long seqNo, long version, VersionType versionType) {
            this.uid = uid;
            this.version = version;
            this.versionType = versionType;
            this.seqNo = seqNo;
        }

        @Override
        public Type opType() {
            return Type.DELETE;
        }

        @Override
        public long estimateSize() {
            return ((uid.field().length() + uid.text().length()) * 2) + 20;
        }

        public Term uid() {
            return this.uid;
        }

        public long seqNo() {
            return seqNo;
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(uid.field());
            out.writeString(uid.text());
            out.writeLong(version);
            out.writeByte(versionType.getValue());
            out.writeZLong(seqNo);
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
            return version == delete.version && seqNo == delete.seqNo && uid.equals(delete.uid) && versionType == delete.versionType;
        }

        @Override
        public int hashCode() {
            int result = uid.hashCode();
            result = 31 * result + Long.hashCode(seqNo);
            result = 31 * result + Long.hashCode(version);
            result = 31 * result + versionType.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Delete{" + "uid=" + uid + '}';
        }
    }

    public enum Durability {

        ASYNC, REQUEST
    }

    private static void verifyChecksum(BufferedChecksumStreamInput in) throws IOException {
        long expectedChecksum = in.getChecksum();
        long readChecksum = in.readInt() & 0xFFFF_FFFFL;
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
            operation = Translog.Operation.readType(in);
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
            Releasables.close(out.bytes());
        }
    }

    public static void writeOperationNoSize(BufferedChecksumStreamOutput out, Translog.Operation op) throws IOException {
        out.resetDigest();
        Translog.Operation.writeType(op, out);
        long checksum = out.getChecksum();
        out.writeInt((int) checksum);
    }

    @Override
    public long prepareCommit() throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            if (currentCommittingGeneration != NOT_SET_GENERATION) {
                throw new IllegalStateException("already committing a translog with generation: " + currentCommittingGeneration);
            }
            currentCommittingGeneration = current.getGeneration();
            TranslogReader currentCommittingTranslog = current.closeIntoReader();
            readers.add(currentCommittingTranslog);
            Path checkpoint = location.resolve(CHECKPOINT_FILE_NAME);
            assert Checkpoint.read(checkpoint).generation == currentCommittingTranslog.getGeneration();
            Path commitCheckpoint = location.resolve(getCommitCheckpointFileName(currentCommittingTranslog.getGeneration()));
            Files.copy(checkpoint, commitCheckpoint);
            IOUtils.fsync(commitCheckpoint, false);
            IOUtils.fsync(commitCheckpoint.getParent(), true);
            current = createWriter(current.getGeneration() + 1);
            logger.trace("current translog set to [{}]", current.getGeneration());
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(this);
            throw e;
        }
        return 0L;
    }

    @Override
    public long commit() throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            ensureOpen();
            if (currentCommittingGeneration == NOT_SET_GENERATION) {
                prepareCommit();
            }
            assert currentCommittingGeneration != NOT_SET_GENERATION;
            assert readers.stream().filter(r -> r.getGeneration() == currentCommittingGeneration).findFirst().isPresent() : "reader list doesn't contain committing generation [" + currentCommittingGeneration + "]";
            lastCommittedTranslogFileGeneration = current.getGeneration();
            currentCommittingGeneration = NOT_SET_GENERATION;
            trimUnreferencedReaders();
        }
        return 0;
    }

    void trimUnreferencedReaders() {
        try (ReleasableLock ignored = writeLock.acquire()) {
            if (closed.get()) {
                return;
            }
            long minReferencedGen = outstandingViews.stream().mapToLong(View::minTranslogGeneration).min().orElse(Long.MAX_VALUE);
            minReferencedGen = Math.min(lastCommittedTranslogFileGeneration, minReferencedGen);
            final long finalMinReferencedGen = minReferencedGen;
            List<TranslogReader> unreferenced = readers.stream().filter(r -> r.getGeneration() < finalMinReferencedGen).collect(Collectors.toList());
            for (final TranslogReader unreferencedReader : unreferenced) {
                Path translogPath = unreferencedReader.path();
                logger.trace("delete translog file - not referenced and not current anymore {}", translogPath);
                IOUtils.closeWhileHandlingException(unreferencedReader);
                IOUtils.deleteFilesIgnoringExceptions(translogPath, translogPath.resolveSibling(getCommitCheckpointFileName(unreferencedReader.getGeneration())));
            }
            readers.removeAll(unreferenced);
        }
    }

    void closeFilesIfNoPendingViews() throws IOException {
        try (ReleasableLock ignored = writeLock.acquire()) {
            if (closed.get() && outstandingViews.isEmpty()) {
                logger.trace("closing files. translog is closed and there are no pending views");
                ArrayList<Closeable> toClose = new ArrayList<>(readers);
                toClose.add(current);
                IOUtils.close(toClose);
            }
        }
    }

    @Override
    public void rollback() throws IOException {
        ensureOpen();
        close();
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

    int getNumOpenViews() {
        return outstandingViews.size();
    }

    ChannelFactory getChannelFactory() {
        return FileChannel::open;
    }

    public Exception getTragicException() {
        return current.getTragicException();
    }

    final Checkpoint readCheckpoint() throws IOException {
        return Checkpoint.read(location.resolve(CHECKPOINT_FILE_NAME));
    }

    public String getTranslogUUID() {
        return translogUUID;
    }
}