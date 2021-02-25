package io.netty.channel.embedded;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ChannelBufType;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;

public class EmbeddedByteChannel extends AbstractEmbeddedChannel {

    public EmbeddedByteChannel(ChannelHandler... handlers) {
        super(Unpooled.dynamicBuffer(), handlers);
    }

    @Override
    public ChannelBufType bufferType() {
        return ChannelBufType.BYTE;
    }

    public ByteBuf inboundBuffer() {
        return pipeline().inboundByteBuffer();
    }

    public ByteBuf lastOutboundBuffer() {
        return (ByteBuf) lastOutboundBuffer;
    }

    public ByteBuf readOutbound() {
        if (!lastOutboundBuffer().readable()) {
            return null;
        }
        try {
            return lastOutboundBuffer().readBytes(lastOutboundBuffer().readableBytes());
        } finally {
            lastOutboundBuffer().clear();
        }
    }

    public boolean writeInbound(ByteBuf data) {
        inboundBuffer().writeBytes(data);
        pipeline().fireInboundBufferUpdated();
        checkException();
        return lastInboundByteBuffer().readable() || !lastInboundMessageBuffer().isEmpty();
    }

    public boolean writeOutbound(Object msg) {
        write(msg);
        checkException();
        return lastOutboundBuffer().readable();
    }

    public boolean finish() {
        close();
        checkException();
        return lastInboundByteBuffer().readable() || !lastInboundMessageBuffer().isEmpty() || lastOutboundBuffer().readable();
    }

    @Override
    protected void doFlushByteBuffer(ByteBuf buf) throws Exception {
        if (!lastOutboundBuffer().readable()) {
            lastOutboundBuffer().discardReadBytes();
        }
        lastOutboundBuffer().writeBytes(buf);
    }
}