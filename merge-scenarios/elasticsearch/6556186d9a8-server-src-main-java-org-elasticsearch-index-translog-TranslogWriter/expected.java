package org.elasticsearch.index.translog;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.Assertions;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

public class TranslogWriter extends BaseTranslogReader implements Closeable {

    private final ShardId shardId;

    private final ChannelFactory channelFactory;

    private volatile Checkpoint lastSyncedCheckpoint;

    private volatile int operationCounter;

    private volatile Exception tragedy;

    private final OutputStream outputStream;

    private volatile long totalOffset;

    private volatile long minSeqNo;

    private volatile long maxSeqNo;

    private final LongSupplier globalCheckpointSupplier;

    private final LongSupplier minTranslogGenerationSupplier;

    protected final AtomicBoolean closed = new AtomicBoolean(false);

    private final Object syncLock = new Object();

    private final Map<Long, Tuple<BytesReference, Exception>> seenSequenceNumbers;

    private TranslogWriter(final ChannelFactory channelFactory, final ShardId shardId, final Checkpoint initialCheckpoint, final FileChannel channel, final Path path, final ByteSizeValue bufferSize, final LongSupplier globalCheckpointSupplier, LongSupplier minTranslogGenerationSupplier, TranslogHeader header) throws IOException {
        super(initialCheckpoint.generation, channel, path, header);
        assert initialCheckpoint.offset == channel.position() : "initial checkpoint offset [" + initialCheckpoint.offset + "] is different than current channel position [" + channel.position() + "]";
        this.shardId = shardId;
        this.channelFactory = channelFactory;
        this.minTranslogGenerationSupplier = minTranslogGenerationSupplier;
        this.outputStream = new BufferedChannelOutputStream(java.nio.channels.Channels.newOutputStream(channel), bufferSize.bytesAsInt());
        this.lastSyncedCheckpoint = initialCheckpoint;
        this.totalOffset = initialCheckpoint.offset;
        assert initialCheckpoint.minSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.minSeqNo;
        this.minSeqNo = initialCheckpoint.minSeqNo;
        assert initialCheckpoint.maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.maxSeqNo;
        this.maxSeqNo = initialCheckpoint.maxSeqNo;
        assert initialCheckpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : initialCheckpoint.trimmedAboveSeqNo;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.seenSequenceNumbers = Assertions.ENABLED ? new HashMap<>() : null;
    }

    public static TranslogWriter create(ShardId shardId, String translogUUID, long fileGeneration, Path file, ChannelFactory channelFactory, ByteSizeValue bufferSize, final long initialMinTranslogGen, long initialGlobalCheckpoint, final LongSupplier globalCheckpointSupplier, final LongSupplier minTranslogGenerationSupplier, final long primaryTerm) throws IOException {
        final FileChannel channel = channelFactory.open(file);
        try {
            final TranslogHeader header = new TranslogHeader(translogUUID, primaryTerm);
            header.write(channel);
            final Checkpoint checkpoint = Checkpoint.emptyTranslogCheckpoint(header.sizeInBytes(), fileGeneration, initialGlobalCheckpoint, initialMinTranslogGen);
            writeCheckpoint(channelFactory, file.getParent(), checkpoint);
            final LongSupplier writerGlobalCheckpointSupplier;
            if (Assertions.ENABLED) {
                writerGlobalCheckpointSupplier = () -> {
                    long gcp = globalCheckpointSupplier.getAsLong();
                    assert gcp >= initialGlobalCheckpoint : "global checkpoint [" + gcp + "] lower than initial gcp [" + initialGlobalCheckpoint + "]";
                    return gcp;
                };
            } else {
                writerGlobalCheckpointSupplier = globalCheckpointSupplier;
            }
            return new TranslogWriter(channelFactory, shardId, checkpoint, channel, file, bufferSize, writerGlobalCheckpointSupplier, minTranslogGenerationSupplier, header);
        } catch (Exception exception) {
            IOUtils.closeWhileHandlingException(channel);
            throw exception;
        }
    }

    public Exception getTragicException() {
        return tragedy;
    }

    private synchronized void closeWithTragicEvent(final Exception ex) {
        assert ex != null;
        if (tragedy == null) {
            tragedy = ex;
        } else if (tragedy != ex) {
            tragedy.addSuppressed(ex);
        }
        try {
            close();
        } catch (final IOException | RuntimeException e) {
            ex.addSuppressed(e);
        }
    }

    public synchronized Translog.Location add(final BytesReference data, final long seqNo) throws IOException {
        ensureOpen();
        final long offset = totalOffset;
        try {
            data.writeTo(outputStream);
        } catch (final Exception ex) {
            closeWithTragicEvent(ex);
            throw ex;
        }
        totalOffset += data.length();
        if (minSeqNo == SequenceNumbers.NO_OPS_PERFORMED) {
            assert operationCounter == 0;
        }
        if (maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED) {
            assert operationCounter == 0;
        }
        minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
        maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);
        operationCounter++;
        assert assertNoSeqNumberConflict(seqNo, data);
        return new Translog.Location(generation, offset, data.length());
    }

    private synchronized boolean assertNoSeqNumberConflict(long seqNo, BytesReference data) throws IOException {
        if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
        } else if (seenSequenceNumbers.containsKey(seqNo)) {
            final Tuple<BytesReference, Exception> previous = seenSequenceNumbers.get(seqNo);
            if (previous.v1().equals(data) == false) {
                Translog.Operation newOp = Translog.readOperation(new BufferedChecksumStreamInput(data.streamInput(), "assertion"));
                Translog.Operation prvOp = Translog.readOperation(new BufferedChecksumStreamInput(previous.v1().streamInput(), "assertion"));
                final boolean sameOp;
                if (newOp instanceof Translog.Index && prvOp instanceof Translog.Index) {
                    final Translog.Index o1 = (Translog.Index) prvOp;
                    final Translog.Index o2 = (Translog.Index) newOp;
                    sameOp = Objects.equals(o1.id(), o2.id()) && Objects.equals(o1.type(), o2.type()) && Objects.equals(o1.source(), o2.source()) && Objects.equals(o1.routing(), o2.routing()) && o1.primaryTerm() == o2.primaryTerm() && o1.seqNo() == o2.seqNo() && o1.version() == o2.version();
                } else if (newOp instanceof Translog.Delete && prvOp instanceof Translog.Delete) {
                    final Translog.Delete o1 = (Translog.Delete) newOp;
                    final Translog.Delete o2 = (Translog.Delete) prvOp;
                    sameOp = Objects.equals(o1.id(), o2.id()) && Objects.equals(o1.type(), o2.type()) && o1.primaryTerm() == o2.primaryTerm() && o1.seqNo() == o2.seqNo() && o1.version() == o2.version();
                } else {
                    sameOp = false;
                }
                if (sameOp == false) {
                    throw new AssertionError("seqNo [" + seqNo + "] was processed twice in generation [" + generation + "], with different data. " + "prvOp [" + prvOp + "], newOp [" + newOp + "]", previous.v2());
                }
            }
        } else {
            seenSequenceNumbers.put(seqNo, new Tuple<>(new BytesArray(data.toBytesRef(), true), new RuntimeException("stack capture previous op")));
        }
        return true;
    }

    synchronized boolean assertNoSeqAbove(long belowTerm, long aboveSeqNo) {
        seenSequenceNumbers.entrySet().stream().filter(e -> e.getKey().longValue() > aboveSeqNo).forEach(e -> {
            final Translog.Operation op;
            try {
                op = Translog.readOperation(new BufferedChecksumStreamInput(e.getValue().v1().streamInput(), "assertion"));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            long seqNo = op.seqNo();
            long primaryTerm = op.primaryTerm();
            if (primaryTerm < belowTerm) {
                throw new AssertionError("current should not have any operations with seq#:primaryTerm [" + seqNo + ":" + primaryTerm + "] > " + aboveSeqNo + ":" + belowTerm);
            }
        });
        return true;
    }

    public void sync() throws IOException {
        syncUpTo(Long.MAX_VALUE);
    }

    public boolean syncNeeded() {
        return totalOffset != lastSyncedCheckpoint.offset || globalCheckpointSupplier.getAsLong() != lastSyncedCheckpoint.globalCheckpoint || minTranslogGenerationSupplier.getAsLong() != lastSyncedCheckpoint.minTranslogGeneration;
    }

    @Override
    public int totalOperations() {
        return operationCounter;
    }

    @Override
    synchronized Checkpoint getCheckpoint() {
        return new Checkpoint(totalOffset, operationCounter, generation, minSeqNo, maxSeqNo, globalCheckpointSupplier.getAsLong(), minTranslogGenerationSupplier.getAsLong(), SequenceNumbers.UNASSIGNED_SEQ_NO);
    }

    @Override
    public long sizeInBytes() {
        return totalOffset;
    }

    public TranslogReader closeIntoReader() throws IOException {
        synchronized (syncLock) {
            synchronized (this) {
                try {
                    sync();
                } catch (final Exception ex) {
                    closeWithTragicEvent(ex);
                    throw ex;
                }
                if (closed.compareAndSet(false, true)) {
                    return new TranslogReader(getLastSyncedCheckpoint(), channel, path, header);
                } else {
                    throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed (path [" + path + "]", tragedy);
                }
            }
        }
    }

    @Override
    public TranslogSnapshot newSnapshot() {
        synchronized (syncLock) {
            synchronized (this) {
                ensureOpen();
                try {
                    sync();
                } catch (IOException e) {
                    throw new TranslogException(shardId, "exception while syncing before creating a snapshot", e);
                }
                return super.newSnapshot();
            }
        }
    }

    private long getWrittenOffset() throws IOException {
        return channel.position();
    }

    public boolean syncUpTo(long offset) throws IOException {
        if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
            synchronized (syncLock) {
                if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
                    final Checkpoint checkpointToSync;
                    synchronized (this) {
                        ensureOpen();
                        try {
                            outputStream.flush();
                            checkpointToSync = getCheckpoint();
                        } catch (final Exception ex) {
                            closeWithTragicEvent(ex);
                            throw ex;
                        }
                    }
                    try {
                        channel.force(false);
                        writeCheckpoint(channelFactory, path.getParent(), checkpointToSync);
                    } catch (final Exception ex) {
                        closeWithTragicEvent(ex);
                        throw ex;
                    }
                    assert lastSyncedCheckpoint.offset <= checkpointToSync.offset : "illegal state: " + lastSyncedCheckpoint.offset + " <= " + checkpointToSync.offset;
                    lastSyncedCheckpoint = checkpointToSync;
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    protected void readBytes(ByteBuffer targetBuffer, long position) throws IOException {
        try {
            if (position + targetBuffer.remaining() > getWrittenOffset()) {
                synchronized (this) {
                    if (position + targetBuffer.remaining() > getWrittenOffset()) {
                        outputStream.flush();
                    }
                }
            }
        } catch (final Exception ex) {
            closeWithTragicEvent(ex);
            throw ex;
        }
        Channels.readFromFileChannelWithEofException(channel, position, targetBuffer);
    }

    private static void writeCheckpoint(final ChannelFactory channelFactory, final Path translogFile, final Checkpoint checkpoint) throws IOException {
        Checkpoint.write(channelFactory, translogFile.resolve(Translog.CHECKPOINT_FILE_NAME), checkpoint, StandardOpenOption.WRITE);
    }

    Checkpoint getLastSyncedCheckpoint() {
        return lastSyncedCheckpoint;
    }

    protected final void ensureOpen() {
        if (isClosed()) {
            throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed", tragedy);
        }
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            channel.close();
        }
    }

    protected final boolean isClosed() {
        return closed.get();
    }

    private final class BufferedChannelOutputStream extends BufferedOutputStream {

        BufferedChannelOutputStream(OutputStream out, int size) throws IOException {
            super(out, size);
        }

        @Override
        public synchronized void flush() throws IOException {
            if (count > 0) {
                try {
                    ensureOpen();
                    super.flush();
                } catch (final Exception ex) {
                    closeWithTragicEvent(ex);
                    throw ex;
                }
            }
        }

        @Override
        public void close() throws IOException {
            throw new IllegalStateException("never close this stream");
        }
    }
}