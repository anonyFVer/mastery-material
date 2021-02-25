package io.netty.channel.embedded;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ChannelBufType;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelMetadata;

public class EmbeddedByteChannel extends AbstractEmbeddedChannel {

    private static final ChannelMetadata METADATA = new ChannelMetadata(ChannelBufType.BYTE, false);

    public EmbeddedByteChannel(ChannelHandler... handlers) {
        super(Unpooled.dynamicBuffer(), handlers);
    }

    @Override
    public ChannelMetadata metadata() {
        return METADATA;
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