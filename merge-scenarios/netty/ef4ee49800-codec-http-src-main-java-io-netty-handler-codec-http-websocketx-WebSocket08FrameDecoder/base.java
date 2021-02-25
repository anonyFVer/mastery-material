package io.netty.handler.codec.http.websocketx;

import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBuffers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.frame.CorruptedFrameException;
import io.netty.handler.codec.frame.TooLongFrameException;
import io.netty.handler.codec.replay.ReplayingDecoder;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

public class WebSocket08FrameDecoder extends ReplayingDecoder<WebSocket08FrameDecoder.State> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(WebSocket08FrameDecoder.class);

    private static final byte OPCODE_CONT = 0x0;

    private static final byte OPCODE_TEXT = 0x1;

    private static final byte OPCODE_BINARY = 0x2;

    private static final byte OPCODE_CLOSE = 0x8;

    private static final byte OPCODE_PING = 0x9;

    private static final byte OPCODE_PONG = 0xA;

    private UTF8Output fragmentedFramesText;

    private int fragmentedFramesCount;

    private boolean frameFinalFlag;

    private int frameRsv;

    private int frameOpcode;

    private long framePayloadLength;

    private ChannelBuffer framePayload;

    private int framePayloadBytesRead;

    private ChannelBuffer maskingKey;

    private final boolean allowExtensions;

    private final boolean maskedPayload;

    private boolean receivedClosingHandshake;

    public enum State {

        FRAME_START, MASKING_KEY, PAYLOAD, CORRUPT
    }

    public WebSocket08FrameDecoder(boolean maskedPayload, boolean allowExtensions) {
        super(State.FRAME_START);
        this.maskedPayload = maskedPayload;
        this.allowExtensions = allowExtensions;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, State state) throws Exception {
        if (receivedClosingHandshake) {
            buffer.skipBytes(actualReadableBytes());
            return null;
        }
        switch(state) {
            case FRAME_START:
                framePayloadBytesRead = 0;
                framePayloadLength = -1;
                framePayload = null;
                byte b = buffer.readByte();
                frameFinalFlag = (b & 0x80) != 0;
                frameRsv = (b & 0x70) >> 4;
                frameOpcode = b & 0x0F;
                if (logger.isDebugEnabled()) {
                    logger.debug("Decoding WebSocket Frame opCode=" + frameOpcode);
                }
                b = buffer.readByte();
                boolean frameMasked = (b & 0x80) != 0;
                int framePayloadLen1 = b & 0x7F;
                if (frameRsv != 0 && !allowExtensions) {
                    protocolViolation(channel, "RSV != 0 and no extension negotiated, RSV:" + frameRsv);
                    return null;
                }
                if (maskedPayload && !frameMasked) {
                    protocolViolation(channel, "unmasked client to server frame");
                    return null;
                }
                if (frameOpcode > 7) {
                    if (!frameFinalFlag) {
                        protocolViolation(channel, "fragmented control frame");
                        return null;
                    }
                    if (framePayloadLen1 > 125) {
                        protocolViolation(channel, "control frame with payload length > 125 octets");
                        return null;
                    }
                    if (!(frameOpcode == OPCODE_CLOSE || frameOpcode == OPCODE_PING || frameOpcode == OPCODE_PONG)) {
                        protocolViolation(channel, "control frame using reserved opcode " + frameOpcode);
                        return null;
                    }
                    if (frameOpcode == 8 && framePayloadLen1 == 1) {
                        protocolViolation(channel, "received close control frame with payload len 1");
                        return null;
                    }
                } else {
                    if (!(frameOpcode == OPCODE_CONT || frameOpcode == OPCODE_TEXT || frameOpcode == OPCODE_BINARY)) {
                        protocolViolation(channel, "data frame using reserved opcode " + frameOpcode);
                        return null;
                    }
                    if (fragmentedFramesCount == 0 && frameOpcode == OPCODE_CONT) {
                        protocolViolation(channel, "received continuation data frame outside fragmented message");
                        return null;
                    }
                    if (fragmentedFramesCount != 0 && frameOpcode != OPCODE_CONT && frameOpcode != OPCODE_PING) {
                        protocolViolation(channel, "received non-continuation data frame while inside fragmented message");
                        return null;
                    }
                }
                if (framePayloadLen1 == 126) {
                    framePayloadLength = buffer.readUnsignedShort();
                    if (framePayloadLength < 126) {
                        protocolViolation(channel, "invalid data frame length (not using minimal length encoding)");
                        return null;
                    }
                } else if (framePayloadLen1 == 127) {
                    framePayloadLength = buffer.readLong();
                    if (framePayloadLength < 65536) {
                        protocolViolation(channel, "invalid data frame length (not using minimal length encoding)");
                        return null;
                    }
                } else {
                    framePayloadLength = framePayloadLen1;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Decoding WebSocket Frame length=" + framePayloadLength);
                }
                checkpoint(State.MASKING_KEY);
            case MASKING_KEY:
                if (maskedPayload) {
                    maskingKey = buffer.readBytes(4);
                }
                checkpoint(State.PAYLOAD);
            case PAYLOAD:
                int rbytes = actualReadableBytes();
                ChannelBuffer payloadBuffer = null;
                int willHaveReadByteCount = framePayloadBytesRead + rbytes;
                if (willHaveReadByteCount == framePayloadLength) {
                    payloadBuffer = buffer.readBytes(rbytes);
                } else if (willHaveReadByteCount < framePayloadLength) {
                    payloadBuffer = buffer.readBytes(rbytes);
                    if (framePayload == null) {
                        framePayload = channel.getConfig().getBufferFactory().getBuffer(toFrameLength(framePayloadLength));
                    }
                    framePayload.writeBytes(payloadBuffer);
                    framePayloadBytesRead += rbytes;
                    return null;
                } else if (willHaveReadByteCount > framePayloadLength) {
                    payloadBuffer = buffer.readBytes(toFrameLength(framePayloadLength - framePayloadBytesRead));
                }
                checkpoint(State.FRAME_START);
                if (framePayload == null) {
                    framePayload = payloadBuffer;
                } else {
                    framePayload.writeBytes(payloadBuffer);
                }
                if (maskedPayload) {
                    unmask(framePayload);
                }
                if (frameOpcode == OPCODE_PING) {
                    return new PingWebSocketFrame(frameFinalFlag, frameRsv, framePayload);
                } else if (frameOpcode == OPCODE_PONG) {
                    return new PongWebSocketFrame(frameFinalFlag, frameRsv, framePayload);
                } else if (frameOpcode == OPCODE_CLOSE) {
                    receivedClosingHandshake = true;
                    return new CloseWebSocketFrame(frameFinalFlag, frameRsv);
                }
                String aggregatedText = null;
                if (frameFinalFlag) {
                    if (frameOpcode != OPCODE_PING) {
                        fragmentedFramesCount = 0;
                        if (frameOpcode == OPCODE_TEXT || fragmentedFramesText != null) {
                            checkUTF8String(channel, framePayload.array());
                            aggregatedText = fragmentedFramesText.toString();
                            fragmentedFramesText = null;
                        }
                    }
                } else {
                    if (fragmentedFramesCount == 0) {
                        fragmentedFramesText = null;
                        if (frameOpcode == OPCODE_TEXT) {
                            checkUTF8String(channel, framePayload.array());
                        }
                    } else {
                        if (fragmentedFramesText != null) {
                            checkUTF8String(channel, framePayload.array());
                        }
                    }
                    fragmentedFramesCount++;
                }
                if (frameOpcode == OPCODE_TEXT) {
                    return new TextWebSocketFrame(frameFinalFlag, frameRsv, framePayload);
                } else if (frameOpcode == OPCODE_BINARY) {
                    return new BinaryWebSocketFrame(frameFinalFlag, frameRsv, framePayload);
                } else if (frameOpcode == OPCODE_CONT) {
                    return new ContinuationWebSocketFrame(frameFinalFlag, frameRsv, framePayload, aggregatedText);
                } else {
                    throw new UnsupportedOperationException("Cannot decode web socket frame with opcode: " + frameOpcode);
                }
            case CORRUPT:
                buffer.readByte();
                return null;
            default:
                throw new Error("Shouldn't reach here.");
        }
    }

    private void unmask(ChannelBuffer frame) {
        byte[] bytes = frame.array();
        for (int i = 0; i < bytes.length; i++) {
            frame.setByte(i, frame.getByte(i) ^ maskingKey.getByte(i % 4));
        }
    }

    private void protocolViolation(Channel channel, String reason) throws CorruptedFrameException {
        checkpoint(State.CORRUPT);
        if (channel.isConnected()) {
            channel.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            channel.close().awaitUninterruptibly();
        }
        throw new CorruptedFrameException(reason);
    }

    private int toFrameLength(long l) throws TooLongFrameException {
        if (l > Integer.MAX_VALUE) {
            throw new TooLongFrameException("Length:" + l);
        } else {
            return (int) l;
        }
    }

    private void checkUTF8String(Channel channel, byte[] bytes) throws CorruptedFrameException {
        try {
            if (fragmentedFramesText == null) {
                fragmentedFramesText = new UTF8Output(bytes);
            } else {
                fragmentedFramesText.write(bytes);
            }
        } catch (UTF8Exception ex) {
            protocolViolation(channel, "invalid UTF-8 bytes");
        }
    }
}