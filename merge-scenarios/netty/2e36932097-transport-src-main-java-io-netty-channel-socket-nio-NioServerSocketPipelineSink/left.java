package io.netty.channel.socket.nio;

import static io.netty.channel.Channels.*;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import io.netty.channel.AbstractChannelSink;
import io.netty.channel.Channel;
import io.netty.channel.ChannelEvent;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelState;
import io.netty.channel.ChannelStateEvent;
import io.netty.channel.MessageEvent;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import io.netty.util.internal.DeadLockProofWorker;

class NioServerSocketPipelineSink extends AbstractChannelSink {

    static final InternalLogger logger = InternalLoggerFactory.getInstance(NioServerSocketPipelineSink.class);

    private final NioWorker[] workers;

    private final AtomicInteger workerIndex = new AtomicInteger();

    NioServerSocketPipelineSink(Executor workerExecutor, int workerCount) {
        workers = new NioWorker[workerCount];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new NioWorker(workerExecutor);
        }
    }

    @Override
    public void eventSunk(ChannelPipeline pipeline, ChannelEvent e) throws Exception {
        Channel channel = e.getChannel();
        if (channel instanceof NioServerSocketChannel) {
            handleServerSocket(e);
        } else if (channel instanceof NioSocketChannel) {
            handleAcceptedSocket(e);
        }
    }

    private void handleServerSocket(ChannelEvent e) {
        if (!(e instanceof ChannelStateEvent)) {
            return;
        }
        ChannelStateEvent event = (ChannelStateEvent) e;
        NioServerSocketChannel channel = (NioServerSocketChannel) event.getChannel();
        ChannelFuture future = event.getFuture();
        ChannelState state = event.getState();
        Object value = event.getValue();
        switch(state) {
            case OPEN:
                if (Boolean.FALSE.equals(value)) {
                    close(channel, future);
                }
                break;
            case BOUND:
                if (value != null) {
                    bind(channel, future, (SocketAddress) value);
                } else {
                    close(channel, future);
                }
                break;
        }
    }

    private void handleAcceptedSocket(ChannelEvent e) {
        if (e instanceof ChannelStateEvent) {
            ChannelStateEvent event = (ChannelStateEvent) e;
            NioSocketChannel channel = (NioSocketChannel) event.getChannel();
            ChannelFuture future = event.getFuture();
            ChannelState state = event.getState();
            Object value = event.getValue();
            switch(state) {
                case OPEN:
                    if (Boolean.FALSE.equals(value)) {
                        channel.worker.close(channel, future);
                    }
                    break;
                case BOUND:
                case CONNECTED:
                    if (value == null) {
                        channel.worker.close(channel, future);
                    }
                    break;
                case INTEREST_OPS:
                    channel.worker.setInterestOps(channel, future, ((Integer) value).intValue());
                    break;
            }
        } else if (e instanceof MessageEvent) {
            MessageEvent event = (MessageEvent) e;
            NioSocketChannel channel = (NioSocketChannel) event.getChannel();
            boolean offered = channel.writeBufferQueue.offer(event);
            assert offered;
            channel.worker.writeFromUserCode(channel);
        }
    }

    private void bind(NioServerSocketChannel channel, ChannelFuture future, SocketAddress localAddress) {
        boolean bound = false;
        boolean bossStarted = false;
        try {
            channel.socket.socket().bind(localAddress, channel.getConfig().getBacklog());
            bound = true;
            future.setSuccess();
            fireChannelBound(channel, channel.getLocalAddress());
            Executor bossExecutor = ((NioServerSocketChannelFactory) channel.getFactory()).bossExecutor;
            DeadLockProofWorker.start(bossExecutor, new Boss(channel));
            bossStarted = true;
        } catch (Throwable t) {
            future.setFailure(t);
            fireExceptionCaught(channel, t);
        } finally {
            if (!bossStarted && bound) {
                close(channel, future);
            }
        }
    }

    private void close(NioServerSocketChannel channel, ChannelFuture future) {
        boolean bound = channel.isBound();
        try {
            if (channel.socket.isOpen()) {
                channel.socket.close();
                Selector selector = channel.selector;
                if (selector != null) {
                    selector.wakeup();
                }
            }
            channel.shutdownLock.lock();
            try {
                if (channel.setClosed()) {
                    future.setSuccess();
                    if (bound) {
                        fireChannelUnbound(channel);
                    }
                    fireChannelClosed(channel);
                } else {
                    future.setSuccess();
                }
            } finally {
                channel.shutdownLock.unlock();
            }
        } catch (Throwable t) {
            future.setFailure(t);
            fireExceptionCaught(channel, t);
        }
    }

    NioWorker nextWorker() {
        return workers[Math.abs(workerIndex.getAndIncrement() % workers.length)];
    }

    private final class Boss implements Runnable {

        private final Selector selector;

        private final NioServerSocketChannel channel;

        Boss(NioServerSocketChannel channel) throws IOException {
            this.channel = channel;
            selector = Selector.open();
            boolean registered = false;
            try {
                channel.socket.register(selector, SelectionKey.OP_ACCEPT);
                registered = true;
            } finally {
                if (!registered) {
                    closeSelector();
                }
            }
            channel.selector = selector;
        }

        @Override
        public void run() {
            final Thread currentThread = Thread.currentThread();
            channel.shutdownLock.lock();
            try {
                for (; ; ) {
                    try {
                        if (selector.select(1000) > 0) {
                            selector.selectedKeys().clear();
                        }
                        SocketChannel acceptedSocket = channel.socket.accept();
                        if (acceptedSocket != null) {
                            registerAcceptedChannel(acceptedSocket, currentThread);
                        }
                    } catch (SocketTimeoutException e) {
                    } catch (CancelledKeyException e) {
                    } catch (ClosedSelectorException e) {
                    } catch (ClosedChannelException e) {
                        break;
                    } catch (Throwable e) {
                        logger.warn("Failed to accept a connection.", e);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e1) {
                        }
                    }
                }
            } finally {
                channel.shutdownLock.unlock();
                closeSelector();
            }
        }

        private void registerAcceptedChannel(SocketChannel acceptedSocket, Thread currentThread) {
            try {
                ChannelPipeline pipeline = channel.getConfig().getPipelineFactory().getPipeline();
                NioWorker worker = nextWorker();
                worker.register(NioAcceptedSocketChannel.create(channel.getFactory(), pipeline, channel, NioServerSocketPipelineSink.this, acceptedSocket, worker, currentThread), null);
            } catch (Exception e) {
                logger.warn("Failed to initialize an accepted socket.", e);
                try {
                    acceptedSocket.close();
                } catch (IOException e2) {
                    logger.warn("Failed to close a partially accepted socket.", e2);
                }
            }
        }

        private void closeSelector() {
            channel.selector = null;
            try {
                selector.close();
            } catch (Exception e) {
                logger.warn("Failed to close a selector.", e);
            }
        }
    }
}