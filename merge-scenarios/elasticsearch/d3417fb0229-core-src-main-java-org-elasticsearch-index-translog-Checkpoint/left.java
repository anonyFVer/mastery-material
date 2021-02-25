package org.elasticsearch.index.translog;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;

class Checkpoint {

    final long offset;

    final int numOps;

    final long generation;

    final long globalCheckpoint;

    private static final int INITIAL_VERSION = 1;

    private static final int CURRENT_VERSION = 2;

    private static final String CHECKPOINT_CODEC = "ckp";

    static final int FILE_SIZE = CodecUtil.headerLength(CHECKPOINT_CODEC) + Integer.BYTES + Long.BYTES + Long.BYTES + Long.BYTES + CodecUtil.footerLength();

    static final int V1_FILE_SIZE = CodecUtil.headerLength(CHECKPOINT_CODEC) + Integer.BYTES + Long.BYTES + Long.BYTES + CodecUtil.footerLength();

    static final int LEGACY_NON_CHECKSUMMED_FILE_LENGTH = Integer.BYTES + Long.BYTES + Long.BYTES;

    Checkpoint(long offset, int numOps, long generation, long globalCheckpoint) {
        this.offset = offset;
        this.numOps = numOps;
        this.generation = generation;
        this.globalCheckpoint = globalCheckpoint;
    }

    private void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        out.writeInt(numOps);
        out.writeLong(generation);
        out.writeLong(globalCheckpoint);
    }

    static Checkpoint readChecksummedV2(DataInput in) throws IOException {
        return new Checkpoint(in.readLong(), in.readInt(), in.readLong(), in.readLong());
    }

    static Checkpoint readChecksummedV1(DataInput in) throws IOException {
        return new Checkpoint(in.readLong(), in.readInt(), in.readLong(), SequenceNumbersService.UNASSIGNED_SEQ_NO);
    }

    static Checkpoint readNonChecksummed(DataInput in) throws IOException {
        return new Checkpoint(in.readLong(), in.readInt(), in.readLong(), SequenceNumbersService.UNASSIGNED_SEQ_NO);
    }

    @Override
    public String toString() {
        return "Checkpoint{" + "offset=" + offset + ", numOps=" + numOps + ", translogFileGeneration=" + generation + ", globalCheckpoint=" + globalCheckpoint + '}';
    }

    public static Checkpoint read(Path path) throws IOException {
        try (Directory dir = new SimpleFSDirectory(path.getParent())) {
            try (final IndexInput indexInput = dir.openInput(path.getFileName().toString(), IOContext.DEFAULT)) {
                if (indexInput.length() == LEGACY_NON_CHECKSUMMED_FILE_LENGTH) {
                    return Checkpoint.readNonChecksummed(indexInput);
                } else {
                    CodecUtil.checksumEntireFile(indexInput);
                    final int fileVersion = CodecUtil.checkHeader(indexInput, CHECKPOINT_CODEC, INITIAL_VERSION, CURRENT_VERSION);
                    if (fileVersion == INITIAL_VERSION) {
                        assert indexInput.length() == V1_FILE_SIZE;
                        return Checkpoint.readChecksummedV1(indexInput);
                    } else {
                        assert fileVersion == CURRENT_VERSION;
                        assert indexInput.length() == FILE_SIZE;
                        return Checkpoint.readChecksummedV2(indexInput);
                    }
                }
            }
        }
    }

    public static void write(ChannelFactory factory, Path checkpointFile, Checkpoint checkpoint, OpenOption... options) throws IOException {
        final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream(FILE_SIZE) {

            @Override
            public synchronized byte[] toByteArray() {
                return buf;
            }
        };
        final String resourceDesc = "checkpoint(path=\"" + checkpointFile + "\", gen=" + checkpoint + ")";
        try (final OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(resourceDesc, checkpointFile.toString(), byteOutputStream, FILE_SIZE)) {
            CodecUtil.writeHeader(indexOutput, CHECKPOINT_CODEC, CURRENT_VERSION);
            checkpoint.write(indexOutput);
            CodecUtil.writeFooter(indexOutput);
            assert indexOutput.getFilePointer() == FILE_SIZE : "get you numbers straight; bytes written: " + indexOutput.getFilePointer() + ", buffer size: " + FILE_SIZE;
            assert indexOutput.getFilePointer() < 512 : "checkpoint files have to be smaller than 512 bytes for atomic writes; size: " + indexOutput.getFilePointer();
        }
        try (FileChannel channel = factory.open(checkpointFile, options)) {
            Channels.writeToChannel(byteOutputStream.toByteArray(), channel);
            channel.force(false);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Checkpoint that = (Checkpoint) o;
        if (offset != that.offset) {
            return false;
        }
        if (numOps != that.numOps) {
            return false;
        }
        return generation == that.generation;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + numOps;
        result = 31 * result + Long.hashCode(generation);
        return result;
    }
}