package io.netty.channel.socket.nio;

import static io.netty.channel.Channels.*;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import io.netty.channel.AbstractChannelSink;
import io.netty.channel.ChannelEvent;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelState;
import io.netty.channel.ChannelStateEvent;
import io.netty.channel.MessageEvent;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import io.netty.util.internal.DeadLockProofWorker;
import io.netty.util.internal.QueueFactory;

class NioClientSocketPipelineSink extends AbstractChannelSink {

    static final InternalLogger logger = InternalLoggerFactory.getInstance(NioClientSocketPipelineSink.class);

    final Executor bossExecutor;

    private final Boss[] bosses;

    private final NioWorker[] workers;

    private final AtomicInteger bossIndex = new AtomicInteger();

    private final AtomicInteger workerIndex = new AtomicInteger();

    NioClientSocketPipelineSink(Executor bossExecutor, Executor workerExecutor, int bossCount, int workerCount) {
        this.bossExecutor = bossExecutor;
        bosses = new Boss[bossCount];
        for (int i = 0; i < bosses.length; i++) {
            bosses[i] = new Boss();
        }
        workers = new NioWorker[workerCount];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new NioWorker(workerExecutor);
        }
    }

    @Override
    public void eventSunk(ChannelPipeline pipeline, ChannelEvent e) throws Exception {
        if (e instanceof ChannelStateEvent) {
            ChannelStateEvent event = (ChannelStateEvent) e;
            NioClientSocketChannel channel = (NioClientSocketChannel) event.getChannel();
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
                    if (value != null) {
                        bind(channel, future, (SocketAddress) value);
                    } else {
                        channel.worker.close(channel, future);
                    }
                    break;
                case CONNECTED:
                    if (value != null) {
                        connect(channel, future, (SocketAddress) value);
                    } else {
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

    private void bind(NioClientSocketChannel channel, ChannelFuture future, SocketAddress localAddress) {
        try {
            channel.channel.socket().bind(localAddress);
            channel.boundManually = true;
            channel.setBound();
            future.setSuccess();
            fireChannelBound(channel, channel.getLocalAddress());
        } catch (Throwable t) {
            future.setFailure(t);
            fireExceptionCaught(channel, t);
        }
    }

    private void connect(final NioClientSocketChannel channel, final ChannelFuture cf, SocketAddress remoteAddress) {
        try {
            if (channel.channel.connect(remoteAddress)) {
                channel.worker.register(channel, cf);
            } else {
                channel.getCloseFuture().addListener(new ChannelFutureListener() {

                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        if (!cf.isDone()) {
                            cf.setFailure(new ClosedChannelException());
                        }
                    }
                });
                cf.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
                channel.connectFuture = cf;
                nextBoss().register(channel);
            }
        } catch (Throwable t) {
            cf.setFailure(t);
            fireExceptionCaught(channel, t);
            channel.worker.close(channel, succeededFuture(channel));
        }
    }

    NioWorker nextWorker() {
        return workers[Math.abs(workerIndex.getAndIncrement() % workers.length)];
    }

    Boss nextBoss() {
        return bosses[Math.abs(bossIndex.getAndIncrement() % bosses.length)];
    }

    private final class Boss implements Runnable {

        volatile Selector selector;

        private boolean started;

        private final AtomicBoolean wakenUp = new AtomicBoolean();

        private final Object startStopLock = new Object();

        private final Queue<Runnable> registerTaskQueue = QueueFactory.createQueue(Runnable.class);

        Boss() {
        }

        void register(NioClientSocketChannel channel) {
            Runnable registerTask = new RegisterTask(this, channel);
            Selector selector;
            synchronized (startStopLock) {
                if (!started) {
                    try {
                        this.selector = selector = Selector.open();
                    } catch (Throwable t) {
                        throw new ChannelException("Failed to create a selector.", t);
                    }
                    boolean success = false;
                    try {
                        DeadLockProofWorker.start(bossExecutor, this);
                        success = true;
                    } finally {
                        if (!success) {
                            try {
                                selector.close();
                            } catch (Throwable t) {
                                logger.warn("Failed to close a selector.", t);
                            }
                            this.selector = selector = null;
                        }
                    }
                } else {
                    selector = this.selector;
                }
                assert selector != null && selector.isOpen();
                started = true;
                boolean offered = registerTaskQueue.offer(registerTask);
                assert offered;
            }
            if (wakenUp.compareAndSet(false, true)) {
                selector.wakeup();
            }
        }

        @Override
        public void run() {
            boolean shutdown = false;
            Selector selector = this.selector;
            long lastConnectTimeoutCheckTimeNanos = System.nanoTime();
            for (; ; ) {
                wakenUp.set(false);
                try {
                    int selectedKeyCount = selector.select(500);
                    if (wakenUp.get()) {
                        selector.wakeup();
                    }
                    processRegisterTaskQueue();
                    if (selectedKeyCount > 0) {
                        processSelectedKeys(selector.selectedKeys());
                    }
                    long currentTimeNanos = System.nanoTime();
                    if (currentTimeNanos - lastConnectTimeoutCheckTimeNanos >= 500 * 1000000L) {
                        lastConnectTimeoutCheckTimeNanos = currentTimeNanos;
                        processConnectTimeout(selector.keys(), currentTimeNanos);
                    }
                    if (selector.keys().isEmpty()) {
                        if (shutdown || bossExecutor instanceof ExecutorService && ((ExecutorService) bossExecutor).isShutdown()) {
                            synchronized (startStopLock) {
                                if (registerTaskQueue.isEmpty() && selector.keys().isEmpty()) {
                                    started = false;
                                    try {
                                        selector.close();
                                    } catch (IOException e) {
                                        logger.warn("Failed to close a selector.", e);
                                    } finally {
                                        this.selector = null;
                                    }
                                    break;
                                } else {
                                    shutdown = false;
                                }
                            }
                        } else {
                            shutdown = true;
                        }
                    } else {
                        shutdown = false;
                    }
                } catch (Throwable t) {
                    logger.warn("Unexpected exception in the selector loop.", t);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }

        private void processRegisterTaskQueue() {
            for (; ; ) {
                final Runnable task = registerTaskQueue.poll();
                if (task == null) {
                    break;
                }
                task.run();
            }
        }

        private void processSelectedKeys(Set<SelectionKey> selectedKeys) {
            for (Iterator<SelectionKey> i = selectedKeys.iterator(); i.hasNext(); ) {
                SelectionKey k = i.next();
                i.remove();
                if (!k.isValid()) {
                    close(k);
                    continue;
                }
                if (k.isConnectable()) {
                    connect(k);
                }
            }
        }

        private void processConnectTimeout(Set<SelectionKey> keys, long currentTimeNanos) {
            ConnectException cause = null;
            for (SelectionKey k : keys) {
                if (!k.isValid()) {
                    continue;
                }
                NioClientSocketChannel ch = (NioClientSocketChannel) k.attachment();
                if (ch.connectDeadlineNanos > 0 && currentTimeNanos >= ch.connectDeadlineNanos) {
                    if (cause == null) {
                        cause = new ConnectException("connection timed out");
                    }
                    ch.connectFuture.setFailure(cause);
                    fireExceptionCaught(ch, cause);
                    ch.worker.close(ch, succeededFuture(ch));
                }
            }
        }

        private void connect(SelectionKey k) {
            NioClientSocketChannel ch = (NioClientSocketChannel) k.attachment();
            try {
                if (ch.channel.finishConnect()) {
                    k.cancel();
                    ch.worker.register(ch, ch.connectFuture);
                }
            } catch (Throwable t) {
                ch.connectFuture.setFailure(t);
                fireExceptionCaught(ch, t);
                k.cancel();
                ch.worker.close(ch, succeededFuture(ch));
            }
        }

        private void close(SelectionKey k) {
            NioClientSocketChannel ch = (NioClientSocketChannel) k.attachment();
            ch.worker.close(ch, succeededFuture(ch));
        }
    }

    private static final class RegisterTask implements Runnable {

        private final Boss boss;

        private final NioClientSocketChannel channel;

        RegisterTask(Boss boss, NioClientSocketChannel channel) {
            this.boss = boss;
            this.channel = channel;
        }

        @Override
        public void run() {
            try {
                channel.channel.register(boss.selector, SelectionKey.OP_CONNECT, channel);
            } catch (ClosedChannelException e) {
                channel.worker.close(channel, succeededFuture(channel));
            }
            int connectTimeout = channel.getConfig().getConnectTimeoutMillis();
            if (connectTimeout > 0) {
                channel.connectDeadlineNanos = System.nanoTime() + connectTimeout * 1000000L;
            }
        }
    }
}