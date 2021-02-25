package io.netty.channel.socket.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ChannelBufType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

abstract class AbstractNioByteChannel extends AbstractNioChannel {

    protected AbstractNioByteChannel(Channel parent, Integer id, SelectableChannel ch) {
        super(parent, id, ch, SelectionKey.OP_READ);
    }

    @Override
    public ChannelBufType bufferType() {
        return ChannelBufType.BYTE;
    }

    @Override
    protected Unsafe newUnsafe() {
        return new NioByteUnsafe();
    }

    private class NioByteUnsafe extends AbstractNioUnsafe {

        @Override
        public void read() {
            assert eventLoop().inEventLoop();
            final ChannelPipeline pipeline = pipeline();
            final ByteBuf byteBuf = pipeline.inboundByteBuffer();
            boolean closed = false;
            boolean read = false;
            try {
                expandReadBuffer(byteBuf);
                for (; ; ) {
                    int localReadAmount = doReadBytes(byteBuf);
                    if (localReadAmount > 0) {
                        read = true;
                    } else if (localReadAmount < 0) {
                        closed = true;
                        break;
                    }
                    if (!expandReadBuffer(byteBuf)) {
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
    protected void doFlushByteBuffer(ByteBuf buf) throws Exception {
        if (!buf.readable()) {
            buf.clear();
            return;
        }
        for (int i = config().getWriteSpinCount() - 1; i >= 0; i--) {
            int localFlushedAmount = doWriteBytes(buf, i == 0);
            if (localFlushedAmount > 0) {
                break;
            }
            if (!buf.readable()) {
                buf.clear();
                break;
            }
        }
    }

    protected abstract int doReadBytes(ByteBuf buf) throws Exception;

    protected abstract int doWriteBytes(ByteBuf buf, boolean lastSpin) throws Exception;

    private static boolean expandReadBuffer(ByteBuf byteBuf) {
        if (!byteBuf.writable()) {
            byteBuf.ensureWritableBytes(4096);
            return true;
        }
        return false;
    }
}