package org.jboss.netty.handler.codec.frame;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;

public class LengthFieldBasedFrameDecoder extends FrameDecoder {

    private final int maxFrameLength;

    private final int lengthFieldOffset;

    private final int lengthFieldLength;

    private final int lengthFieldEndOffset;

    private final int lengthAdjustment;

    private final int initialBytesToStrip;

    private final boolean lengthFieldIncludedInFrameLength;

    private boolean discardingTooLongFrame;

    private long tooLongFrameLength;

    private long bytesToDiscard;

    private boolean failImmediatelyOnTooLongFrame = false;

    public LengthFieldBasedFrameDecoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength) {
        this(maxFrameLength, lengthFieldOffset, lengthFieldLength, 0, 0, false);
    }

    public LengthFieldBasedFrameDecoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength, int lengthAdjustment, int initialBytesToStrip) {
        this(maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToStrip, false);
    }

    public LengthFieldBasedFrameDecoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength, int lengthAdjustment, int initialBytesToStrip, boolean lengthFieldIncludedInFrameLength) {
        if (maxFrameLength <= 0) {
            throw new IllegalArgumentException("maxFrameLength must be a positive integer: " + maxFrameLength);
        }
        if (lengthFieldOffset < 0) {
            throw new IllegalArgumentException("lengthFieldOffset must be a non-negative integer: " + lengthFieldOffset);
        }
        if (initialBytesToStrip < 0) {
            throw new IllegalArgumentException("initialBytesToStrip must be a non-negative integer: " + initialBytesToStrip);
        }
        if (lengthFieldLength != 1 && lengthFieldLength != 2 && lengthFieldLength != 3 && lengthFieldLength != 4 && lengthFieldLength != 8) {
            throw new IllegalArgumentException("lengthFieldLength must be either 1, 2, 3, 4, or 8: " + lengthFieldLength);
        }
        if (lengthFieldOffset > maxFrameLength - lengthFieldLength) {
            throw new IllegalArgumentException("maxFrameLength (" + maxFrameLength + ") " + "must be equal to or greater than " + "lengthFieldOffset (" + lengthFieldOffset + ") + " + "lengthFieldLength (" + lengthFieldLength + ").");
        }
        this.maxFrameLength = maxFrameLength;
        this.lengthFieldOffset = lengthFieldOffset;
        this.lengthFieldLength = lengthFieldLength;
        this.lengthAdjustment = lengthAdjustment;
        lengthFieldEndOffset = lengthFieldOffset + lengthFieldLength;
        this.initialBytesToStrip = initialBytesToStrip;
        this.lengthFieldIncludedInFrameLength = lengthFieldIncludedInFrameLength;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        if (discardingTooLongFrame) {
            long bytesToDiscard = this.bytesToDiscard;
            int localBytesToDiscard = (int) Math.min(bytesToDiscard, buffer.readableBytes());
            buffer.skipBytes(localBytesToDiscard);
            bytesToDiscard -= localBytesToDiscard;
            this.bytesToDiscard = bytesToDiscard;
            failIfNecessary(ctx, false);
            return null;
        }
        if (buffer.readableBytes() < lengthFieldEndOffset) {
            return null;
        }
        int actualLengthFieldOffset = buffer.readerIndex() + lengthFieldOffset;
        long frameLength;
        switch(lengthFieldLength) {
            case 1:
                frameLength = buffer.getUnsignedByte(actualLengthFieldOffset);
                break;
            case 2:
                frameLength = buffer.getUnsignedShort(actualLengthFieldOffset);
                break;
            case 3:
                frameLength = buffer.getUnsignedMedium(actualLengthFieldOffset);
                break;
            case 4:
                frameLength = buffer.getUnsignedInt(actualLengthFieldOffset);
                break;
            case 8:
                frameLength = buffer.getLong(actualLengthFieldOffset);
                break;
            default:
                throw new Error("should not reach here");
        }
        if (frameLength < 0) {
            buffer.skipBytes(lengthFieldEndOffset);
            throw new CorruptedFrameException("negative pre-adjustment length field: " + frameLength);
        }
        if (!lengthFieldIncludedInFrameLength) {
            frameLength += lengthAdjustment + lengthFieldEndOffset;
        }
        if (frameLength < lengthFieldEndOffset) {
            buffer.skipBytes(lengthFieldEndOffset);
            throw new CorruptedFrameException("Adjusted frame length (" + frameLength + ") is less " + "than lengthFieldEndOffset: " + lengthFieldEndOffset);
        }
        if (frameLength > maxFrameLength) {
            discardingTooLongFrame = true;
            tooLongFrameLength = frameLength;
            bytesToDiscard = frameLength - buffer.readableBytes();
            buffer.skipBytes(buffer.readableBytes());
            failIfNecessary(ctx, true);
            return null;
        }
        int frameLengthInt = (int) frameLength;
        if (buffer.readableBytes() < frameLengthInt) {
            return null;
        }
        if (initialBytesToStrip > frameLengthInt) {
            buffer.skipBytes(frameLengthInt);
            throw new CorruptedFrameException("Adjusted frame length (" + frameLength + ") is less " + "than initialBytesToStrip: " + initialBytesToStrip);
        }
        buffer.skipBytes(initialBytesToStrip);
        int readerIndex = buffer.readerIndex();
        int actualFrameLength = frameLengthInt - initialBytesToStrip;
        ChannelBuffer frame = extractFrame(buffer, readerIndex, actualFrameLength);
        buffer.readerIndex(readerIndex + actualFrameLength);
        return frame;
    }

    private void failIfNecessary(ChannelHandlerContext ctx, boolean firstDetectionOfTooLongFrame) {
        if (bytesToDiscard == 0) {
            long tooLongFrameLength = this.tooLongFrameLength;
            this.tooLongFrameLength = 0;
            discardingTooLongFrame = false;
            if ((!failImmediatelyOnTooLongFrame) || (failImmediatelyOnTooLongFrame && firstDetectionOfTooLongFrame)) {
                fail(ctx, tooLongFrameLength);
            }
        } else {
            if (failImmediatelyOnTooLongFrame && firstDetectionOfTooLongFrame) {
                fail(ctx, this.tooLongFrameLength);
            }
        }
    }

    protected ChannelBuffer extractFrame(ChannelBuffer buffer, int index, int length) {
        ChannelBuffer frame = buffer.factory().getBuffer(length);
        frame.writeBytes(buffer, index, length);
        return frame;
    }

    public LengthFieldBasedFrameDecoder setFailImmediatelyOnTooLongFrame(boolean failImmediatelyOnTooLongFrame) {
        this.failImmediatelyOnTooLongFrame = failImmediatelyOnTooLongFrame;
        return this;
    }

    private void fail(ChannelHandlerContext ctx, long frameLength) {
        if (frameLength > 0) {
            Channels.fireExceptionCaught(ctx.getChannel(), new TooLongFrameException("Adjusted frame length exceeds " + maxFrameLength + ": " + frameLength + " - discarded"));
        } else {
            Channels.fireExceptionCaught(ctx.getChannel(), new TooLongFrameException("Adjusted frame length exceeds " + maxFrameLength + " - discarding"));
        }
    }
}