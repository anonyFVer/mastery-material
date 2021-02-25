package io.netty.channel.embedded;

import io.netty.buffer.ChannelBufType;
import io.netty.buffer.MessageBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;

public class EmbeddedMessageChannel extends AbstractEmbeddedChannel {

    public EmbeddedMessageChannel(ChannelHandler... handlers) {
        super(Unpooled.messageBuffer(), handlers);
    }

    @Override
    public ChannelBufType bufferType() {
        return ChannelBufType.MESSAGE;
    }

    public MessageBuf<Object> inboundBuffer() {
        return pipeline().inboundMessageBuffer();
    }

    @SuppressWarnings("unchecked")
    public MessageBuf<Object> lastOutboundBuffer() {
        return (MessageBuf<Object>) lastOutboundBuffer;
    }

    public Object readOutbound() {
        return lastOutboundBuffer().poll();
    }

    public boolean writeInbound(Object msg) {
        inboundBuffer().add(msg);
        pipeline().fireInboundBufferUpdated();
        checkException();
        return lastInboundByteBuffer().readable() || !lastInboundMessageBuffer().isEmpty();
    }

    public boolean writeOutbound(Object msg) {
        write(msg);
        checkException();
        return !lastOutboundBuffer().isEmpty();
    }

    public boolean finish() {
        close();
        checkException();
        return lastInboundByteBuffer().readable() || !lastInboundMessageBuffer().isEmpty() || !lastOutboundBuffer().isEmpty();
    }

    @Override
    protected void doFlushMessageBuffer(MessageBuf<Object> buf) throws Exception {
        buf.drainTo(lastOutboundBuffer());
    }
}