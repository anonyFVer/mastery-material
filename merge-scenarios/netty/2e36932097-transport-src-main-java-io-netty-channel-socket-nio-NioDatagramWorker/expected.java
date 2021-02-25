package io.netty.channel.socket.nio;

import static io.netty.channel.Channels.fireChannelDisconnected;
import static io.netty.channel.Channels.fireExceptionCaught;
import static io.netty.channel.Channels.fireMessageReceived;
import static io.netty.channel.Channels.succeededFuture;
import io.netty.buffer.ChannelBufferFactory;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ReceiveBufferSizePredictor;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.Executor;

class NioDatagramWorker extends AbstractNioWorker {

    NioDatagramWorker(final Executor executor) {
        super(executor);
    }

    @Override
    protected boolean read(final SelectionKey key) {
        final NioDatagramChannel channel = (NioDatagramChannel) key.attachment();
        ReceiveBufferSizePredictor predictor = channel.getConfig().getReceiveBufferSizePredictor();
        final ChannelBufferFactory bufferFactory = channel.getConfig().getBufferFactory();
        final DatagramChannel nioChannel = (DatagramChannel) key.channel();
        final ByteBuffer byteBuffer = ByteBuffer.allocate(predictor.nextReceiveBufferSize()).order(bufferFactory.getDefaultOrder());
        boolean failure = true;
        SocketAddress remoteAddress = null;
        try {
            remoteAddress = nioChannel.receive(byteBuffer);
            failure = false;
        } catch (ClosedChannelException e) {
        } catch (Throwable t) {
            fireExceptionCaught(channel, t);
        }
        if (remoteAddress != null) {
            byteBuffer.flip();
            int readBytes = byteBuffer.remaining();
            if (readBytes > 0) {
                predictor.previousReceiveBufferSize(readBytes);
                fireMessageReceived(channel, bufferFactory.getBuffer(byteBuffer), remoteAddress);
            }
        }
        if (failure) {
            key.cancel();
            close(channel, succeededFuture(channel));
            return false;
        }
        return true;
    }

    @Override
    protected boolean scheduleWriteIfNecessary(final AbstractNioChannel<?> channel) {
        final Thread workerThread = thread;
        if (workerThread == null || Thread.currentThread() != workerThread) {
            if (channel.writeTaskInTaskQueue.compareAndSet(false, true)) {
                boolean offered = writeTaskQueue.offer(channel.writeTask);
                assert offered;
            }
            final Selector selector = this.selector;
            if (selector != null) {
                if (wakenUp.compareAndSet(false, true)) {
                    selector.wakeup();
                }
            }
            return true;
        }
        return false;
    }

    static void disconnect(NioDatagramChannel channel, ChannelFuture future) {
        boolean connected = channel.isConnected();
        try {
            channel.getDatagramChannel().disconnect();
            future.setSuccess();
            if (connected) {
                fireChannelDisconnected(channel);
            }
        } catch (Throwable t) {
            future.setFailure(t);
            fireExceptionCaught(channel, t);
        }
    }

    @Override
    protected Runnable createRegisterTask(AbstractNioChannel<?> channel, ChannelFuture future) {
        return new ChannelRegistionTask((NioDatagramChannel) channel, future);
    }

    private final class ChannelRegistionTask implements Runnable {

        private final NioDatagramChannel channel;

        private final ChannelFuture future;

        ChannelRegistionTask(final NioDatagramChannel channel, final ChannelFuture future) {
            this.channel = channel;
            this.future = future;
        }

        @Override
        public void run() {
            final SocketAddress localAddress = channel.getLocalAddress();
            if (localAddress == null) {
                if (future != null) {
                    future.setFailure(new ClosedChannelException());
                }
                close(channel, succeededFuture(channel));
                return;
            }
            try {
                synchronized (channel.interestOpsLock) {
                    channel.getDatagramChannel().register(selector, channel.getRawInterestOps(), channel);
                }
                if (future != null) {
                    future.setSuccess();
                }
            } catch (final ClosedChannelException e) {
                if (future != null) {
                    future.setFailure(e);
                }
                close(channel, succeededFuture(channel));
                throw new ChannelException("Failed to register a socket to the selector.", e);
            }
        }
    }
}