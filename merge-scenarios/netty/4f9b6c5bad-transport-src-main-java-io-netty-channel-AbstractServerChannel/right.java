package io.netty.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ChannelBufType;
import io.netty.buffer.MessageBuf;
import java.net.SocketAddress;

public abstract class AbstractServerChannel extends AbstractChannel implements ServerChannel {

    private static final ChannelMetadata METADATA = new ChannelMetadata(ChannelBufType.MESSAGE, false);

    protected AbstractServerChannel(Integer id) {
        super(null, id);
    }

    @Override
    public ByteBuf outboundByteBuffer() {
        throw new NoSuchBufferException();
    }

    @Override
    public MessageBuf<Object> outboundMessageBuffer() {
        throw new NoSuchBufferException();
    }

    @Override
    public ChannelMetadata metadata() {
        return METADATA;
    }

    @Override
    public SocketAddress remoteAddress() {
        return null;
    }

    @Override
    protected Unsafe newUnsafe() {
        return new DefaultServerUnsafe();
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return null;
    }

    @Override
    protected void doDisconnect() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doFlushByteBuffer(ByteBuf buf) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doFlushMessageBuffer(MessageBuf<Object> buf) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean isFlushPending() {
        return false;
    }

    protected class DefaultServerUnsafe extends AbstractUnsafe {

        @Override
        public void flush(final ChannelFuture future) {
            if (eventLoop().inEventLoop()) {
                reject(future);
            } else {
                eventLoop().execute(new Runnable() {

                    @Override
                    public void run() {
                        flush(future);
                    }
                });
            }
        }

        @Override
        public void connect(final SocketAddress remoteAddress, final SocketAddress localAddress, final ChannelFuture future) {
            if (eventLoop().inEventLoop()) {
                reject(future);
            } else {
                eventLoop().execute(new Runnable() {

                    @Override
                    public void run() {
                        connect(remoteAddress, localAddress, future);
                    }
                });
            }
        }

        private void reject(ChannelFuture future) {
            Exception cause = new UnsupportedOperationException();
            future.setFailure(cause);
            pipeline().fireExceptionCaught(cause);
        }
    }
}