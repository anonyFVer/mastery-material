package io.netty.channel.sctp;

import static io.netty.channel.Channels.*;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import com.sun.nio.sctp.SctpChannel;
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

class SctpServerPipelineSink extends AbstractChannelSink {

    static final InternalLogger logger = InternalLoggerFactory.getInstance(SctpServerPipelineSink.class);

    private final SctpWorker[] workers;

    private final AtomicInteger workerIndex = new AtomicInteger();

    SctpServerPipelineSink(Executor workerExecutor, int workerCount) {
        workers = new SctpWorker[workerCount];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new SctpWorker(workerExecutor);
        }
    }

    @Override
    public void eventSunk(ChannelPipeline pipeline, ChannelEvent e) throws Exception {
        Channel channel = e.getChannel();
        if (channel instanceof SctpServerChannelImpl) {
            handleServerSocket(e);
        } else if (channel instanceof SctpChannelImpl) {
            handleAcceptedSocket(e);
        }
    }

    private void handleServerSocket(ChannelEvent e) {
        if (!(e instanceof ChannelStateEvent)) {
            return;
        }
        ChannelStateEvent event = (ChannelStateEvent) e;
        SctpServerChannelImpl channel = (SctpServerChannelImpl) event.getChannel();
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
            SctpChannelImpl channel = (SctpChannelImpl) event.getChannel();
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
                    channel.worker.setInterestOps(channel, future, (Integer) value);
                    break;
            }
        } else if (e instanceof MessageEvent) {
            MessageEvent event = (MessageEvent) e;
            SctpChannelImpl channel = (SctpChannelImpl) event.getChannel();
            boolean offered = channel.writeBuffer.offer(event);
            assert offered;
            channel.worker.writeFromUserCode(channel);
        }
    }

    private void bind(SctpServerChannelImpl channel, ChannelFuture future, SocketAddress localAddress) {
        boolean bound = false;
        boolean bossStarted = false;
        try {
            channel.serverChannel.bind(localAddress, channel.getConfig().getBacklog());
            bound = true;
            channel.setBound();
            future.setSuccess();
            fireChannelBound(channel, channel.getLocalAddress());
            Executor bossExecutor = ((SctpServerSocketChannelFactory) channel.getFactory()).bossExecutor;
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

    private void close(SctpServerChannelImpl channel, ChannelFuture future) {
        boolean bound = channel.isBound();
        try {
            if (channel.serverChannel.isOpen()) {
                channel.serverChannel.close();
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

    SctpWorker nextWorker() {
        return workers[Math.abs(workerIndex.getAndIncrement() % workers.length)];
    }

    private final class Boss implements Runnable {

        private final Selector selector;

        private final SctpServerChannelImpl channel;

        Boss(SctpServerChannelImpl channel) throws IOException {
            this.channel = channel;
            selector = Selector.open();
            boolean registered = false;
            try {
                channel.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
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
                        if (selector.select(500) > 0) {
                            selector.selectedKeys().clear();
                        }
                        SctpChannel acceptedSocket = channel.serverChannel.accept();
                        if (acceptedSocket != null) {
                            registerAcceptedChannel(acceptedSocket, currentThread);
                        }
                    } catch (SocketTimeoutException e) {
                    } catch (CancelledKeyException e) {
                    } catch (ClosedSelectorException e) {
                    } catch (ClosedChannelException e) {
                        break;
                    } catch (Throwable e) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Failed to accept a connection.", e);
                        }
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

        private void registerAcceptedChannel(SctpChannel acceptedSocket, Thread currentThread) {
            try {
                ChannelPipeline pipeline = channel.getConfig().getPipelineFactory().getPipeline();
                SctpWorker worker = nextWorker();
                worker.register(new SctpAcceptedChannel(channel.getFactory(), pipeline, channel, SctpServerPipelineSink.this, acceptedSocket, worker, currentThread), null);
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to initialize an accepted socket.", e);
                }
                try {
                    acceptedSocket.close();
                } catch (IOException e2) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to close a partially accepted socket.", e2);
                    }
                }
            }
        }

        private void closeSelector() {
            channel.selector = null;
            try {
                selector.close();
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to close a selector.", e);
                }
            }
        }
    }
}