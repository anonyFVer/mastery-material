package io.netty.channel.socket.nio;

import io.netty.buffer.ChannelBufType;
import io.netty.buffer.MessageBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import java.io.IOException;
import java.nio.channels.SelectableChannel;

abstract class AbstractNioMessageChannel extends AbstractNioChannel {

    protected AbstractNioMessageChannel(Channel parent, Integer id, SelectableChannel ch, int defaultInterestOps) {
        super(parent, id, ch, defaultInterestOps);
    }

    @Override
    public ChannelBufType bufferType() {
        return ChannelBufType.MESSAGE;
    }

    @Override
    protected Unsafe newUnsafe() {
        return new NioMessageUnsafe();
    }

    private class NioMessageUnsafe extends AbstractNioUnsafe {

        @Override
        public void read() {
            assert eventLoop().inEventLoop();
            final ChannelPipeline pipeline = pipeline();
            final MessageBuf<Object> msgBuf = pipeline.inboundMessageBuffer();
            boolean closed = false;
            boolean read = false;
            try {
                for (; ; ) {
                    int localReadAmount = doReadMessages(msgBuf);
                    if (localReadAmount > 0) {
                        read = true;
                    } else if (localReadAmount == 0) {
                        break;
                    } else if (localReadAmount < 0) {
                        closed = true;
                        break;
                    }
                }
            } catch (Throwable t) {
                if (read) {
                    read = false;
                    pipeline.fireInboundBufferUpdated();
                }
                pipeline().fireExceptionCaught(t);
                if (t instanceof IOException) {
                    close(voidFuture());
                }
            } finally {
                if (read) {
                    pipeline.fireInboundBufferUpdated();
                }
                if (closed && isOpen()) {
                    close(voidFuture());
                }
            }
        }
    }

    @Override
    protected void doFlushMessageBuffer(MessageBuf<Object> buf) throws Exception {
        final int writeSpinCount = config().getWriteSpinCount() - 1;
        while (!buf.isEmpty()) {
            boolean wrote = false;
            for (int i = writeSpinCount; i >= 0; i--) {
                int localFlushedAmount = doWriteMessages(buf, i == 0);
                if (localFlushedAmount > 0) {
                    wrote = true;
                    break;
                }
            }
            if (!wrote) {
                break;
            }
        }
    }

    protected abstract int doReadMessages(MessageBuf<Object> buf) throws Exception;

    protected abstract int doWriteMessages(MessageBuf<Object> buf, boolean lastSpin) throws Exception;
}