package io.netty.handler.codec.spdy;

import static io.netty.handler.codec.spdy.SpdyCodecUtil.*;
import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBuffers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.Channels;
import io.netty.handler.codec.frame.FrameDecoder;
import io.netty.handler.codec.frame.TooLongFrameException;

public class SpdyFrameDecoder extends FrameDecoder {

    private final int spdyVersion;

    private final int maxChunkSize;

    private final int maxHeaderSize;

    private final SpdyHeaderBlockDecompressor headerBlockDecompressor;

    private State state;

    private SpdySettingsFrame spdySettingsFrame;

    private SpdyHeaderBlock spdyHeaderBlock;

    private byte flags;

    private int length;

    private int version;

    private int type;

    private int streamID;

    private int headerSize;

    private int numHeaders;

    private ChannelBuffer decompressed;

    private static enum State {

        READ_COMMON_HEADER,
        READ_CONTROL_FRAME,
        READ_SETTINGS_FRAME,
        READ_HEADER_BLOCK_FRAME,
        READ_HEADER_BLOCK,
        READ_DATA_FRAME,
        DISCARD_FRAME,
        FRAME_ERROR
    }

    public SpdyFrameDecoder(int version) {
        this(version, 8192, 16384);
    }

    public SpdyFrameDecoder(int version, int maxChunkSize, int maxHeaderSize) {
        super(false);
        if (version < SPDY_MIN_VERSION || version > SPDY_MAX_VERSION) {
            throw new IllegalArgumentException("unsupported version: " + version);
        }
        if (maxChunkSize <= 0) {
            throw new IllegalArgumentException("maxChunkSize must be a positive integer: " + maxChunkSize);
        }
        if (maxHeaderSize <= 0) {
            throw new IllegalArgumentException("maxHeaderSize must be a positive integer: " + maxHeaderSize);
        }
        spdyVersion = version;
        this.maxChunkSize = maxChunkSize;
        this.maxHeaderSize = maxHeaderSize;
        headerBlockDecompressor = SpdyHeaderBlockDecompressor.newInstance(version);
        state = State.READ_COMMON_HEADER;
    }

    @Override
    protected Object decodeLast(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        try {
            Object frame = decode(ctx, channel, buffer);
            return frame;
        } finally {
            headerBlockDecompressor.end();
        }
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        switch(state) {
            case READ_COMMON_HEADER:
                state = readCommonHeader(buffer);
                if (state == State.FRAME_ERROR) {
                    if (version != spdyVersion) {
                        fireProtocolException(ctx, "Unsupported version: " + version);
                    } else {
                        fireInvalidControlFrameException(ctx);
                    }
                }
                if (length == 0) {
                    if (state == State.READ_DATA_FRAME) {
                        if (streamID == 0) {
                            state = State.FRAME_ERROR;
                            fireProtocolException(ctx, "Received invalid data frame");
                            return null;
                        }
                        SpdyDataFrame spdyDataFrame = new DefaultSpdyDataFrame(streamID);
                        spdyDataFrame.setLast((flags & SPDY_DATA_FLAG_FIN) != 0);
                        state = State.READ_COMMON_HEADER;
                        return spdyDataFrame;
                    }
                    state = State.READ_COMMON_HEADER;
                }
                return null;
            case READ_CONTROL_FRAME:
                try {
                    Object frame = readControlFrame(buffer);
                    if (frame != null) {
                        state = State.READ_COMMON_HEADER;
                    }
                    return frame;
                } catch (IllegalArgumentException e) {
                    state = State.FRAME_ERROR;
                    fireInvalidControlFrameException(ctx);
                }
                return null;
            case READ_SETTINGS_FRAME:
                if (spdySettingsFrame == null) {
                    if (buffer.readableBytes() < 4) {
                        return null;
                    }
                    int numEntries = getUnsignedInt(buffer, buffer.readerIndex());
                    buffer.skipBytes(4);
                    length -= 4;
                    if ((length & 0x07) != 0 || length >> 3 != numEntries) {
                        state = State.FRAME_ERROR;
                        fireInvalidControlFrameException(ctx);
                        return null;
                    }
                    spdySettingsFrame = new DefaultSpdySettingsFrame();
                    boolean clear = (flags & SPDY_SETTINGS_CLEAR) != 0;
                    spdySettingsFrame.setClearPreviouslyPersistedSettings(clear);
                }
                int readableEntries = Math.min(buffer.readableBytes() >> 3, length >> 3);
                for (int i = 0; i < readableEntries; i++) {
                    int ID;
                    byte ID_flags;
                    if (version < 3) {
                        ID = buffer.readByte() & 0xFF | (buffer.readByte() & 0xFF) << 8 | (buffer.readByte() & 0xFF) << 16;
                        ID_flags = buffer.readByte();
                    } else {
                        ID_flags = buffer.readByte();
                        ID = getUnsignedMedium(buffer, buffer.readerIndex());
                        buffer.skipBytes(3);
                    }
                    int value = getSignedInt(buffer, buffer.readerIndex());
                    buffer.skipBytes(4);
                    if (ID == 0) {
                        state = State.FRAME_ERROR;
                        spdySettingsFrame = null;
                        fireInvalidControlFrameException(ctx);
                        return null;
                    }
                    if (!spdySettingsFrame.isSet(ID)) {
                        boolean persistVal = (ID_flags & SPDY_SETTINGS_PERSIST_VALUE) != 0;
                        boolean persisted = (ID_flags & SPDY_SETTINGS_PERSISTED) != 0;
                        spdySettingsFrame.setValue(ID, value, persistVal, persisted);
                    }
                }
                length -= 8 * readableEntries;
                if (length == 0) {
                    state = State.READ_COMMON_HEADER;
                    Object frame = spdySettingsFrame;
                    spdySettingsFrame = null;
                    return frame;
                }
                return null;
            case READ_HEADER_BLOCK_FRAME:
                try {
                    spdyHeaderBlock = readHeaderBlockFrame(buffer);
                    if (spdyHeaderBlock != null) {
                        if (length == 0) {
                            state = State.READ_COMMON_HEADER;
                            Object frame = spdyHeaderBlock;
                            spdyHeaderBlock = null;
                            return frame;
                        }
                        state = State.READ_HEADER_BLOCK;
                    }
                    return null;
                } catch (IllegalArgumentException e) {
                    state = State.FRAME_ERROR;
                    fireInvalidControlFrameException(ctx);
                    return null;
                }
            case READ_HEADER_BLOCK:
                int compressedBytes = Math.min(buffer.readableBytes(), length);
                length -= compressedBytes;
                try {
                    decodeHeaderBlock(buffer.readSlice(compressedBytes));
                } catch (Exception e) {
                    state = State.FRAME_ERROR;
                    spdyHeaderBlock = null;
                    decompressed = null;
                    Channels.fireExceptionCaught(ctx, e);
                    return null;
                }
                if (spdyHeaderBlock != null && spdyHeaderBlock.isInvalid()) {
                    Object frame = spdyHeaderBlock;
                    spdyHeaderBlock = null;
                    decompressed = null;
                    if (length == 0) {
                        state = State.READ_COMMON_HEADER;
                    }
                    return frame;
                }
                if (length == 0) {
                    Object frame = spdyHeaderBlock;
                    spdyHeaderBlock = null;
                    state = State.READ_COMMON_HEADER;
                    return frame;
                }
                return null;
            case READ_DATA_FRAME:
                if (streamID == 0) {
                    state = State.FRAME_ERROR;
                    fireProtocolException(ctx, "Received invalid data frame");
                    return null;
                }
                int dataLength = Math.min(maxChunkSize, length);
                if (buffer.readableBytes() < dataLength) {
                    return null;
                }
                SpdyDataFrame spdyDataFrame = new DefaultSpdyDataFrame(streamID);
                spdyDataFrame.setData(buffer.readBytes(dataLength));
                length -= dataLength;
                if (length == 0) {
                    spdyDataFrame.setLast((flags & SPDY_DATA_FLAG_FIN) != 0);
                    state = State.READ_COMMON_HEADER;
                }
                return spdyDataFrame;
            case DISCARD_FRAME:
                int numBytes = Math.min(buffer.readableBytes(), length);
                buffer.skipBytes(numBytes);
                length -= numBytes;
                if (length == 0) {
                    state = State.READ_COMMON_HEADER;
                }
                return null;
            case FRAME_ERROR:
                buffer.skipBytes(buffer.readableBytes());
                return null;
            default:
                throw new Error("Shouldn't reach here.");
        }
    }

    private State readCommonHeader(ChannelBuffer buffer) {
        if (buffer.readableBytes() < SPDY_HEADER_SIZE) {
            return State.READ_COMMON_HEADER;
        }
        int frameOffset = buffer.readerIndex();
        int flagsOffset = frameOffset + SPDY_HEADER_FLAGS_OFFSET;
        int lengthOffset = frameOffset + SPDY_HEADER_LENGTH_OFFSET;
        buffer.skipBytes(SPDY_HEADER_SIZE);
        boolean control = (buffer.getByte(frameOffset) & 0x80) != 0;
        flags = buffer.getByte(flagsOffset);
        length = getUnsignedMedium(buffer, lengthOffset);
        if (control) {
            version = getUnsignedShort(buffer, frameOffset) & 0x7FFF;
            int typeOffset = frameOffset + SPDY_HEADER_TYPE_OFFSET;
            type = getUnsignedShort(buffer, typeOffset);
            if (version != spdyVersion || !isValidControlFrameHeader()) {
                return State.FRAME_ERROR;
            }
            State nextState;
            if (willGenerateControlFrame()) {
                switch(type) {
                    case SPDY_SYN_STREAM_FRAME:
                    case SPDY_SYN_REPLY_FRAME:
                    case SPDY_HEADERS_FRAME:
                        nextState = State.READ_HEADER_BLOCK_FRAME;
                        break;
                    case SPDY_SETTINGS_FRAME:
                        nextState = State.READ_SETTINGS_FRAME;
                        break;
                    default:
                        nextState = State.READ_CONTROL_FRAME;
                }
            } else if (length != 0) {
                nextState = State.DISCARD_FRAME;
            } else {
                nextState = State.READ_COMMON_HEADER;
            }
            return nextState;
        } else {
            streamID = getUnsignedInt(buffer, frameOffset);
            return State.READ_DATA_FRAME;
        }
    }

    private Object readControlFrame(ChannelBuffer buffer) {
        int streamID;
        int statusCode;
        switch(type) {
            case SPDY_RST_STREAM_FRAME:
                if (buffer.readableBytes() < 8) {
                    return null;
                }
                streamID = getUnsignedInt(buffer, buffer.readerIndex());
                statusCode = getSignedInt(buffer, buffer.readerIndex() + 4);
                buffer.skipBytes(8);
                return new DefaultSpdyRstStreamFrame(streamID, statusCode);
            case SPDY_PING_FRAME:
                if (buffer.readableBytes() < 4) {
                    return null;
                }
                int ID = getSignedInt(buffer, buffer.readerIndex());
                buffer.skipBytes(4);
                return new DefaultSpdyPingFrame(ID);
            case SPDY_GOAWAY_FRAME:
                int minLength = version < 3 ? 4 : 8;
                if (buffer.readableBytes() < minLength) {
                    return null;
                }
                int lastGoodStreamID = getUnsignedInt(buffer, buffer.readerIndex());
                buffer.skipBytes(4);
                if (version < 3) {
                    return new DefaultSpdyGoAwayFrame(lastGoodStreamID);
                }
                statusCode = getSignedInt(buffer, buffer.readerIndex());
                buffer.skipBytes(4);
                return new DefaultSpdyGoAwayFrame(lastGoodStreamID, statusCode);
            case SPDY_WINDOW_UPDATE_FRAME:
                if (buffer.readableBytes() < 8) {
                    return null;
                }
                streamID = getUnsignedInt(buffer, buffer.readerIndex());
                int deltaWindowSize = getUnsignedInt(buffer, buffer.readerIndex() + 4);
                buffer.skipBytes(8);
                return new DefaultSpdyWindowUpdateFrame(streamID, deltaWindowSize);
            default:
                throw new Error("Shouldn't reach here.");
        }
    }

    private SpdyHeaderBlock readHeaderBlockFrame(ChannelBuffer buffer) {
        int minLength;
        int streamID;
        switch(type) {
            case SPDY_SYN_STREAM_FRAME:
                minLength = version < 3 ? 12 : 10;
                if (buffer.readableBytes() < minLength) {
                    return null;
                }
                int offset = buffer.readerIndex();
                streamID = getUnsignedInt(buffer, offset);
                int associatedToStreamID = getUnsignedInt(buffer, offset + 4);
                byte priority = (byte) (buffer.getByte(offset + 8) >> 5 & 0x07);
                if (version < 3) {
                    priority >>= 1;
                }
                buffer.skipBytes(10);
                length -= 10;
                if (version < 3 && length == 2 && buffer.getShort(buffer.readerIndex()) == 0) {
                    buffer.skipBytes(2);
                    length = 0;
                }
                SpdySynStreamFrame spdySynStreamFrame = new DefaultSpdySynStreamFrame(streamID, associatedToStreamID, priority);
                spdySynStreamFrame.setLast((flags & SPDY_FLAG_FIN) != 0);
                spdySynStreamFrame.setUnidirectional((flags & SPDY_FLAG_UNIDIRECTIONAL) != 0);
                return spdySynStreamFrame;
            case SPDY_SYN_REPLY_FRAME:
                minLength = version < 3 ? 8 : 4;
                if (buffer.readableBytes() < minLength) {
                    return null;
                }
                streamID = getUnsignedInt(buffer, buffer.readerIndex());
                buffer.skipBytes(4);
                length -= 4;
                if (version < 3) {
                    buffer.skipBytes(2);
                    length -= 2;
                }
                if (version < 3 && length == 2 && buffer.getShort(buffer.readerIndex()) == 0) {
                    buffer.skipBytes(2);
                    length = 0;
                }
                SpdySynReplyFrame spdySynReplyFrame = new DefaultSpdySynReplyFrame(streamID);
                spdySynReplyFrame.setLast((flags & SPDY_FLAG_FIN) != 0);
                return spdySynReplyFrame;
            case SPDY_HEADERS_FRAME:
                if (buffer.readableBytes() < 4) {
                    return null;
                }
                if (version < 3 && length > 4 && buffer.readableBytes() < 8) {
                    return null;
                }
                streamID = getUnsignedInt(buffer, buffer.readerIndex());
                buffer.skipBytes(4);
                length -= 4;
                if (version < 3 && length != 0) {
                    buffer.skipBytes(2);
                    length -= 2;
                }
                if (version < 3 && length == 2 && buffer.getShort(buffer.readerIndex()) == 0) {
                    buffer.skipBytes(2);
                    length = 0;
                }
                SpdyHeadersFrame spdyHeadersFrame = new DefaultSpdyHeadersFrame(streamID);
                spdyHeadersFrame.setLast((flags & SPDY_FLAG_FIN) != 0);
                return spdyHeadersFrame;
            default:
                throw new Error("Shouldn't reach here.");
        }
    }

    private boolean ensureBytes(int bytes) throws Exception {
        if (decompressed.readableBytes() >= bytes) {
            return true;
        }
        headerBlockDecompressor.decode(decompressed);
        return decompressed.readableBytes() >= bytes;
    }

    private int readLengthField() {
        if (version < 3) {
            return decompressed.readUnsignedShort();
        } else {
            return decompressed.readInt();
        }
    }

    private void decodeHeaderBlock(ChannelBuffer buffer) throws Exception {
        if (decompressed == null) {
            headerSize = 0;
            numHeaders = -1;
            decompressed = ChannelBuffers.dynamicBuffer(8192);
        }
        headerBlockDecompressor.setInput(buffer);
        headerBlockDecompressor.decode(decompressed);
        if (spdyHeaderBlock == null) {
            decompressed = null;
            return;
        }
        int lengthFieldSize = version < 3 ? 2 : 4;
        if (numHeaders == -1) {
            if (decompressed.readableBytes() < lengthFieldSize) {
                return;
            }
            numHeaders = readLengthField();
            if (numHeaders < 0) {
                spdyHeaderBlock.setInvalid();
                return;
            }
        }
        while (numHeaders > 0) {
            int headerSize = this.headerSize;
            decompressed.markReaderIndex();
            if (!ensureBytes(lengthFieldSize)) {
                decompressed.resetReaderIndex();
                decompressed.discardReadBytes();
                return;
            }
            int nameLength = readLengthField();
            if (nameLength <= 0) {
                spdyHeaderBlock.setInvalid();
                return;
            }
            headerSize += nameLength;
            if (headerSize > maxHeaderSize) {
                throw new TooLongFrameException("Header block exceeds " + maxHeaderSize);
            }
            if (!ensureBytes(nameLength)) {
                decompressed.resetReaderIndex();
                decompressed.discardReadBytes();
                return;
            }
            byte[] nameBytes = new byte[nameLength];
            decompressed.readBytes(nameBytes);
            String name = new String(nameBytes, "UTF-8");
            if (spdyHeaderBlock.containsHeader(name)) {
                spdyHeaderBlock.setInvalid();
                return;
            }
            if (!ensureBytes(lengthFieldSize)) {
                decompressed.resetReaderIndex();
                decompressed.discardReadBytes();
                return;
            }
            int valueLength = readLengthField();
            if (valueLength <= 0) {
                spdyHeaderBlock.setInvalid();
                return;
            }
            headerSize += valueLength;
            if (headerSize > maxHeaderSize) {
                throw new TooLongFrameException("Header block exceeds " + maxHeaderSize);
            }
            if (!ensureBytes(valueLength)) {
                decompressed.resetReaderIndex();
                decompressed.discardReadBytes();
                return;
            }
            byte[] valueBytes = new byte[valueLength];
            decompressed.readBytes(valueBytes);
            int index = 0;
            int offset = 0;
            while (index < valueLength) {
                while (index < valueBytes.length && valueBytes[index] != (byte) 0) {
                    index++;
                }
                if (index < valueBytes.length && valueBytes[index + 1] == (byte) 0) {
                    spdyHeaderBlock.setInvalid();
                    return;
                }
                String value = new String(valueBytes, offset, index - offset, "UTF-8");
                try {
                    spdyHeaderBlock.addHeader(name, value);
                } catch (IllegalArgumentException e) {
                    spdyHeaderBlock.setInvalid();
                    return;
                }
                index++;
                offset = index;
            }
            numHeaders--;
            this.headerSize = headerSize;
        }
        decompressed = null;
    }

    private boolean isValidControlFrameHeader() {
        switch(type) {
            case SPDY_SYN_STREAM_FRAME:
                return version < 3 ? length >= 12 : length >= 10;
            case SPDY_SYN_REPLY_FRAME:
                return version < 3 ? length >= 8 : length >= 4;
            case SPDY_RST_STREAM_FRAME:
                return flags == 0 && length == 8;
            case SPDY_SETTINGS_FRAME:
                return length >= 4;
            case SPDY_NOOP_FRAME:
                return length == 0;
            case SPDY_PING_FRAME:
                return length == 4;
            case SPDY_GOAWAY_FRAME:
                return version < 3 ? length == 4 : length == 8;
            case SPDY_HEADERS_FRAME:
                if (version < 3) {
                    return length == 4 || length >= 8;
                } else {
                    return length >= 4;
                }
            case SPDY_WINDOW_UPDATE_FRAME:
                return length == 8;
            case SPDY_CREDENTIAL_FRAME:
            default:
                return true;
        }
    }

    private boolean willGenerateControlFrame() {
        switch(type) {
            case SPDY_SYN_STREAM_FRAME:
            case SPDY_SYN_REPLY_FRAME:
            case SPDY_RST_STREAM_FRAME:
            case SPDY_SETTINGS_FRAME:
            case SPDY_PING_FRAME:
            case SPDY_GOAWAY_FRAME:
            case SPDY_HEADERS_FRAME:
            case SPDY_WINDOW_UPDATE_FRAME:
                return true;
            case SPDY_NOOP_FRAME:
            case SPDY_CREDENTIAL_FRAME:
            default:
                return false;
        }
    }

    private void fireInvalidControlFrameException(ChannelHandlerContext ctx) {
        String message = "Received invalid control frame";
        switch(type) {
            case SPDY_SYN_STREAM_FRAME:
                message = "Received invalid SYN_STREAM control frame";
                break;
            case SPDY_SYN_REPLY_FRAME:
                message = "Received invalid SYN_REPLY control frame";
                break;
            case SPDY_RST_STREAM_FRAME:
                message = "Received invalid RST_STREAM control frame";
                break;
            case SPDY_SETTINGS_FRAME:
                message = "Received invalid SETTINGS control frame";
                break;
            case SPDY_NOOP_FRAME:
                message = "Received invalid NOOP control frame";
                break;
            case SPDY_PING_FRAME:
                message = "Received invalid PING control frame";
                break;
            case SPDY_GOAWAY_FRAME:
                message = "Received invalid GOAWAY control frame";
                break;
            case SPDY_HEADERS_FRAME:
                message = "Received invalid HEADERS control frame";
                break;
            case SPDY_WINDOW_UPDATE_FRAME:
                message = "Received invalid WINDOW_UPDATE control frame";
                break;
            case SPDY_CREDENTIAL_FRAME:
                message = "Received invalid CREDENTIAL control frame";
                break;
        }
        fireProtocolException(ctx, message);
    }

    private static void fireProtocolException(ChannelHandlerContext ctx, String message) {
        Channels.fireExceptionCaught(ctx, new SpdyProtocolException(message));
    }
}