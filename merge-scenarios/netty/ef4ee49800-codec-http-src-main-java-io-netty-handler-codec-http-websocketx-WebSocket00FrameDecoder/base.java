package io.netty.handler.codec.http.websocketx;

import io.netty.buffer.ChannelBuffer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.frame.TooLongFrameException;
import io.netty.handler.codec.replay.ReplayingDecoder;
import io.netty.handler.codec.replay.VoidEnum;

public class WebSocket00FrameDecoder extends ReplayingDecoder<VoidEnum> {

    private static final int DEFAULT_MAX_FRAME_SIZE = 16384;

    private final int maxFrameSize;

    private boolean receivedClosingHandshake;

    public WebSocket00FrameDecoder() {
        this(DEFAULT_MAX_FRAME_SIZE);
    }

    public WebSocket00FrameDecoder(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, VoidEnum state) throws Exception {
        if (receivedClosingHandshake) {
            buffer.skipBytes(actualReadableBytes());
            return null;
        }
        byte type = buffer.readByte();
        if ((type & 0x80) == 0x80) {
            return decodeBinaryFrame(type, buffer);
        } else {
            return decodeTextFrame(buffer);
        }
    }

    private WebSocketFrame decodeBinaryFrame(byte type, ChannelBuffer buffer) throws TooLongFrameException {
        long frameSize = 0;
        int lengthFieldSize = 0;
        byte b;
        do {
            b = buffer.readByte();
            frameSize <<= 7;
            frameSize |= b & 0x7f;
            if (frameSize > maxFrameSize) {
                throw new TooLongFrameException();
            }
            lengthFieldSize++;
            if (lengthFieldSize > 8) {
                throw new TooLongFrameException();
            }
        } while ((b & 0x80) == 0x80);
        if (type == (byte) 0xFF && frameSize == 0) {
            receivedClosingHandshake = true;
            return new CloseWebSocketFrame();
        }
        return new BinaryWebSocketFrame(buffer.readBytes((int) frameSize));
    }

    private WebSocketFrame decodeTextFrame(ChannelBuffer buffer) throws TooLongFrameException {
        int ridx = buffer.readerIndex();
        int rbytes = actualReadableBytes();
        int delimPos = buffer.indexOf(ridx, ridx + rbytes, (byte) 0xFF);
        if (delimPos == -1) {
            if (rbytes > maxFrameSize) {
                throw new TooLongFrameException();
            } else {
                return null;
            }
        }
        int frameSize = delimPos - ridx;
        if (frameSize > maxFrameSize) {
            throw new TooLongFrameException();
        }
        ChannelBuffer binaryData = buffer.readBytes(frameSize);
        buffer.skipBytes(1);
        int ffDelimPos = binaryData.indexOf(binaryData.readerIndex(), binaryData.writerIndex(), (byte) 0xFF);
        if (ffDelimPos >= 0) {
            throw new IllegalArgumentException("a text frame should not contain 0xFF.");
        }
        return new TextWebSocketFrame(binaryData);
    }
}