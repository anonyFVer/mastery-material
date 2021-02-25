package io.netty.handler.codec.http.websocketx;

import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBuffers;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInboundHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

public class WebSocket08FrameDecoder extends ReplayingDecoder<WebSocketFrame, WebSocket08FrameDecoder.State> {

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
    public WebSocketFrame decode(ChannelInboundHandlerContext<Byte> ctx, ChannelBuffer in) throws Exception {
        if (receivedClosingHandshake) {
            in.skipBytes(actualReadableBytes());
            return null;
        }
        switch(state()) {
            case FRAME_START:
                framePayloadBytesRead = 0;
                framePayloadLength = -1;
                framePayload = null;
                byte b = in.readByte();
                frameFinalFlag = (b & 0x80) != 0;
                frameRsv = (b & 0x70) >> 4;
                frameOpcode = b & 0x0F;
                if (logger.isDebugEnabled()) {
                    logger.debug("Decoding WebSocket Frame opCode=" + frameOpcode);
                }
                b = in.readByte();
                boolean frameMasked = (b & 0x80) != 0;
                int framePayloadLen1 = b & 0x7F;
                if (frameRsv != 0 && !allowExtensions) {
                    protocolViolation(ctx, "RSV != 0 and no extension negotiated, RSV:" + frameRsv);
                    return null;
                }
                if (maskedPayload && !frameMasked) {
                    protocolViolation(ctx, "unmasked client to server frame");
                    return null;
                }
                if (frameOpcode > 7) {
                    if (!frameFinalFlag) {
                        protocolViolation(ctx, "fragmented control frame");
                        return null;
                    }
                    if (framePayloadLen1 > 125) {
                        protocolViolation(ctx, "control frame with payload length > 125 octets");
                        return null;
                    }
                    if (!(frameOpcode == OPCODE_CLOSE || frameOpcode == OPCODE_PING || frameOpcode == OPCODE_PONG)) {
                        protocolViolation(ctx, "control frame using reserved opcode " + frameOpcode);
                        return null;
                    }
                    if (frameOpcode == 8 && framePayloadLen1 == 1) {
                        protocolViolation(ctx, "received close control frame with payload len 1");
                        return null;
                    }
                } else {
                    if (!(frameOpcode == OPCODE_CONT || frameOpcode == OPCODE_TEXT || frameOpcode == OPCODE_BINARY)) {
                        protocolViolation(ctx, "data frame using reserved opcode " + frameOpcode);
                        return null;
                    }
                    if (fragmentedFramesCount == 0 && frameOpcode == OPCODE_CONT) {
                        protocolViolation(ctx, "received continuation data frame outside fragmented message");
                        return null;
                    }
                    if (fragmentedFramesCount != 0 && frameOpcode != OPCODE_CONT && frameOpcode != OPCODE_PING) {
                        protocolViolation(ctx, "received non-continuation data frame while inside fragmented message");
                        return null;
                    }
                }
                if (framePayloadLen1 == 126) {
                    framePayloadLength = in.readUnsignedShort();
                    if (framePayloadLength < 126) {
                        protocolViolation(ctx, "invalid data frame length (not using minimal length encoding)");
                        return null;
                    }
                } else if (framePayloadLen1 == 127) {
                    framePayloadLength = in.readLong();
                    if (framePayloadLength < 65536) {
                        protocolViolation(ctx, "invalid data frame length (not using minimal length encoding)");
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
                    maskingKey = in.readBytes(4);
                }
                checkpoint(State.PAYLOAD);
            case PAYLOAD:
                int rbytes = actualReadableBytes();
                ChannelBuffer payloadBuffer = null;
                int willHaveReadByteCount = framePayloadBytesRead + rbytes;
                if (willHaveReadByteCount == framePayloadLength) {
                    payloadBuffer = in.readBytes(rbytes);
                } else if (willHaveReadByteCount < framePayloadLength) {
                    payloadBuffer = in.readBytes(rbytes);
                    if (framePayload == null) {
                        framePayload = ChannelBuffers.buffer(toFrameLength(framePayloadLength));
                    }
                    framePayload.writeBytes(payloadBuffer);
                    framePayloadBytesRead += rbytes;
                    return null;
                } else if (willHaveReadByteCount > framePayloadLength) {
                    payloadBuffer = in.readBytes(toFrameLength(framePayloadLength - framePayloadBytesRead));
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
                    checkCloseFrameBody(ctx, framePayload);
                    receivedClosingHandshake = true;
                    return new CloseWebSocketFrame(frameFinalFlag, frameRsv, framePayload);
                }
                String aggregatedText = null;
                if (frameFinalFlag) {
                    if (frameOpcode != OPCODE_PING) {
                        fragmentedFramesCount = 0;
                        if (frameOpcode == OPCODE_TEXT || fragmentedFramesText != null) {
                            checkUTF8String(ctx, framePayload.array());
                            aggregatedText = fragmentedFramesText.toString();
                            fragmentedFramesText = null;
                        }
                    }
                } else {
                    if (fragmentedFramesCount == 0) {
                        fragmentedFramesText = null;
                        if (frameOpcode == OPCODE_TEXT) {
                            checkUTF8String(ctx, framePayload.array());
                        }
                    } else {
                        if (fragmentedFramesText != null) {
                            checkUTF8String(ctx, framePayload.array());
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
                in.readByte();
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

    private void protocolViolation(ChannelInboundHandlerContext<Byte> ctx, String reason) throws CorruptedFrameException {
        checkpoint(State.CORRUPT);
        if (ctx.channel().isActive()) {
            ctx.flush().addListener(ChannelFutureListener.CLOSE);
        }
        throw new CorruptedFrameException(reason);
    }

    private static int toFrameLength(long l) throws TooLongFrameException {
        if (l > Integer.MAX_VALUE) {
            throw new TooLongFrameException("Length:" + l);
        } else {
            return (int) l;
        }
    }

    private void checkUTF8String(ChannelInboundHandlerContext<Byte> ctx, byte[] bytes) throws CorruptedFrameException {
        try {
            if (fragmentedFramesText == null) {
                fragmentedFramesText = new UTF8Output(bytes);
            } else {
                fragmentedFramesText.write(bytes);
            }
        } catch (UTF8Exception ex) {
            protocolViolation(ctx, "invalid UTF-8 bytes");
        }
    }

    protected void checkCloseFrameBody(ChannelInboundHandlerContext<Byte> ctx, ChannelBuffer buffer) throws CorruptedFrameException {
        if (buffer == null || buffer.capacity() == 0) {
            return;
        }
        if (buffer.capacity() == 1) {
            protocolViolation(ctx, "Invalid close frame body");
        }
        int idx = buffer.readerIndex();
        buffer.readerIndex(0);
        int statusCode = buffer.readShort();
        if (statusCode >= 0 && statusCode <= 999 || statusCode >= 1004 && statusCode <= 1006 || statusCode >= 1012 && statusCode <= 2999) {
            protocolViolation(ctx, "Invalid close frame status code: " + statusCode);
        }
        if (buffer.readableBytes() > 0) {
            byte[] b = new byte[buffer.readableBytes()];
            buffer.readBytes(b);
            try {
                new UTF8Output(b);
            } catch (UTF8Exception ex) {
                protocolViolation(ctx, "Invalid close frame reason text. Invalid UTF-8 bytes");
            }
        }
        buffer.readerIndex(idx);
    }
}