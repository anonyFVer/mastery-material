package io.netty.channel.socket.oio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ChannelBufType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import java.io.IOException;

abstract class AbstractOioByteChannel extends AbstractOioChannel {

    protected AbstractOioByteChannel(Channel parent, Integer id) {
        super(parent, id);
    }

    @Override
    public ChannelBufType bufferType() {
        return ChannelBufType.BYTE;
    }

    @Override
    protected Unsafe newUnsafe() {
        return new OioByteUnsafe();
    }

    private class OioByteUnsafe extends AbstractOioUnsafe {

        @Override
        public void read() {
            assert eventLoop().inEventLoop();
            final ChannelPipeline pipeline = pipeline();
            final ByteBuf byteBuf = pipeline.inboundByteBuffer();
            boolean closed = false;
            boolean read = false;
            try {
                expandReadBuffer(byteBuf);
                int localReadAmount = doReadBytes(byteBuf);
                if (localReadAmount > 0) {
                    read = true;
                } else if (localReadAmount < 0) {
                    closed = true;
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

        private void expandReadBuffer(ByteBuf byteBuf) {
            int available = available();
            if (available > 0) {
                byteBuf.ensureWritableBytes(available);
            } else if (!byteBuf.writable()) {
                byteBuf.ensureWritableBytes(4096);
            }
        }
    }

    @Override
    protected void doFlushByteBuffer(ByteBuf buf) throws Exception {
        while (buf.readable()) {
            doWriteBytes(buf);
        }
        buf.clear();
    }

    protected abstract int available();

    protected abstract int doReadBytes(ByteBuf buf) throws Exception;

    protected abstract void doWriteBytes(ByteBuf buf) throws Exception;
}