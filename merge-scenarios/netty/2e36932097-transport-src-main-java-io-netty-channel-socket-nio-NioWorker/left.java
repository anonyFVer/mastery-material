package io.netty.channel.socket.nio;

import static io.netty.channel.Channels.fireChannelBound;
import static io.netty.channel.Channels.fireChannelConnected;
import static io.netty.channel.Channels.fireExceptionCaught;
import static io.netty.channel.Channels.fireMessageReceived;
import static io.netty.channel.Channels.succeededFuture;
import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBufferFactory;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ReceiveBufferSizePredictor;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;

class NioWorker extends AbstractNioWorker {

    private final SocketReceiveBufferPool recvBufferPool = new SocketReceiveBufferPool();

    NioWorker(Executor executor) {
        super(executor);
    }

    @Override
    protected boolean read(SelectionKey k) {
        final SocketChannel ch = (SocketChannel) k.channel();
        final NioSocketChannel channel = (NioSocketChannel) k.attachment();
        final ReceiveBufferSizePredictor predictor = channel.getConfig().getReceiveBufferSizePredictor();
        final int predictedRecvBufSize = predictor.nextReceiveBufferSize();
        int ret = 0;
        int readBytes = 0;
        boolean failure = true;
        ByteBuffer bb = recvBufferPool.acquire(predictedRecvBufSize);
        try {
            while ((ret = ch.read(bb)) > 0) {
                readBytes += ret;
                if (!bb.hasRemaining()) {
                    break;
                }
            }
            failure = false;
        } catch (ClosedChannelException e) {
        } catch (Throwable t) {
            fireExceptionCaught(channel, t);
        }
        if (readBytes > 0) {
            bb.flip();
            final ChannelBufferFactory bufferFactory = channel.getConfig().getBufferFactory();
            final ChannelBuffer buffer = bufferFactory.getBuffer(readBytes);
            buffer.setBytes(0, bb);
            buffer.writerIndex(readBytes);
            recvBufferPool.release(bb);
            predictor.previousReceiveBufferSize(readBytes);
            fireMessageReceived(channel, buffer);
        } else {
            recvBufferPool.release(bb);
        }
        if (ret < 0 || failure) {
            k.cancel();
            close(channel, succeededFuture(channel));
            return false;
        }
        return true;
    }

    @Override
    protected boolean scheduleWriteIfNecessary(final AbstractNioChannel<?> channel) {
        final Thread currentThread = Thread.currentThread();
        final Thread workerThread = thread;
        if (currentThread != workerThread) {
            if (channel.writeTaskInTaskQueue.compareAndSet(false, true)) {
                boolean offered = writeTaskQueue.offer(channel.writeTask);
                assert offered;
            }
            if (!(channel instanceof NioAcceptedSocketChannel) || ((NioAcceptedSocketChannel) channel).bossThread != currentThread) {
                final Selector workerSelector = selector;
                if (workerSelector != null) {
                    if (wakenUp.compareAndSet(false, true)) {
                        workerSelector.wakeup();
                    }
                }
            } else {
            }
            return true;
        }
        return false;
    }

    @Override
    protected Runnable createRegisterTask(AbstractNioChannel<?> channel, ChannelFuture future) {
        boolean server = !(channel instanceof NioClientSocketChannel);
        return new RegisterTask((NioSocketChannel) channel, future, server);
    }

    private final class RegisterTask implements Runnable {

        private final NioSocketChannel channel;

        private final ChannelFuture future;

        private final boolean server;

        RegisterTask(NioSocketChannel channel, ChannelFuture future, boolean server) {
            this.channel = channel;
            this.future = future;
            this.server = server;
        }

        @Override
        public void run() {
            SocketAddress localAddress = channel.getLocalAddress();
            SocketAddress remoteAddress = channel.getRemoteAddress();
            if (localAddress == null || remoteAddress == null) {
                if (future != null) {
                    future.setFailure(new ClosedChannelException());
                }
                close(channel, succeededFuture(channel));
                return;
            }
            try {
                if (server) {
                    channel.channel.configureBlocking(false);
                }
                synchronized (channel.interestOpsLock) {
                    channel.channel.register(selector, channel.getRawInterestOps(), channel);
                }
                if (future != null) {
                    channel.setConnected();
                    future.setSuccess();
                }
            } catch (IOException e) {
                if (future != null) {
                    future.setFailure(e);
                }
                close(channel, succeededFuture(channel));
                if (!(e instanceof ClosedChannelException)) {
                    throw new ChannelException("Failed to register a socket to the selector.", e);
                }
            }
            if (server || !((NioClientSocketChannel) channel).boundManually) {
                fireChannelBound(channel, localAddress);
            }
            fireChannelConnected(channel, remoteAddress);
        }
    }
}