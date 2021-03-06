package io.netty.channel.socket.nio;

import static io.netty.channel.Channels.*;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBufferFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.MessageEvent;
import io.netty.channel.ReceiveBufferSizePredictor;
import io.netty.channel.socket.nio.SocketSendBufferPool.SendBuffer;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import io.netty.util.internal.DeadLockProofWorker;
import io.netty.util.internal.QueueFactory;

class NioWorker implements Runnable {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(NioWorker.class);

    private static final int CONSTRAINT_LEVEL = NioProviderMetadata.CONSTRAINT_LEVEL;

    static final int CLEANUP_INTERVAL = 256;

    private final Executor executor;

    private boolean started;

    private volatile Thread thread;

    volatile Selector selector;

    private final AtomicBoolean wakenUp = new AtomicBoolean();

    private final ReadWriteLock selectorGuard = new ReentrantReadWriteLock();

    private final Object startStopLock = new Object();

    private final Queue<Runnable> registerTaskQueue = QueueFactory.createQueue(Runnable.class);

    private final Queue<Runnable> writeTaskQueue = QueueFactory.createQueue(Runnable.class);

    private volatile int cancelledKeys;

    private final SocketReceiveBufferPool recvBufferPool = new SocketReceiveBufferPool();

    private final SocketSendBufferPool sendBufferPool = new SocketSendBufferPool();

    NioWorker(Executor executor) {
        this.executor = executor;
    }

    void register(NioSocketChannel channel, ChannelFuture future) {
        boolean server = !(channel instanceof NioClientSocketChannel);
        Runnable registerTask = new RegisterTask(channel, future, server);
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
                    DeadLockProofWorker.start(executor, this);
                    success = true;
                } finally {
                    if (!success) {
                        try {
                            selector.close();
                        } catch (Throwable t) {
                            if (logger.isWarnEnabled()) {
                                logger.warn("Failed to close a selector.", t);
                            }
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
        thread = Thread.currentThread();
        boolean shutdown = false;
        Selector selector = this.selector;
        for (; ; ) {
            wakenUp.set(false);
            if (CONSTRAINT_LEVEL != 0) {
                selectorGuard.writeLock().lock();
                selectorGuard.writeLock().unlock();
            }
            try {
                SelectorUtil.select(selector);
                if (wakenUp.get()) {
                    selector.wakeup();
                }
                cancelledKeys = 0;
                processRegisterTaskQueue();
                processWriteTaskQueue();
                processSelectedKeys(selector.selectedKeys());
                if (selector.keys().isEmpty()) {
                    if (shutdown || executor instanceof ExecutorService && ((ExecutorService) executor).isShutdown()) {
                        synchronized (startStopLock) {
                            if (registerTaskQueue.isEmpty() && selector.keys().isEmpty()) {
                                started = false;
                                try {
                                    selector.close();
                                } catch (IOException e) {
                                    if (logger.isWarnEnabled()) {
                                        logger.warn("Failed to close a selector.", e);
                                    }
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
                if (logger.isWarnEnabled()) {
                    logger.warn("Unexpected exception in the selector loop.", t);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    private void processRegisterTaskQueue() throws IOException {
        for (; ; ) {
            final Runnable task = registerTaskQueue.poll();
            if (task == null) {
                break;
            }
            task.run();
            cleanUpCancelledKeys();
        }
    }

    private void processWriteTaskQueue() throws IOException {
        for (; ; ) {
            final Runnable task = writeTaskQueue.poll();
            if (task == null) {
                break;
            }
            task.run();
            cleanUpCancelledKeys();
        }
    }

    private void processSelectedKeys(Set<SelectionKey> selectedKeys) throws IOException {
        for (Iterator<SelectionKey> i = selectedKeys.iterator(); i.hasNext(); ) {
            SelectionKey k = i.next();
            i.remove();
            try {
                int readyOps = k.readyOps();
                if ((readyOps & SelectionKey.OP_READ) != 0 || readyOps == 0) {
                    if (!read(k)) {
                        continue;
                    }
                }
                if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                    writeFromSelectorLoop(k);
                }
            } catch (CancelledKeyException e) {
                close(k);
            }
            if (cleanUpCancelledKeys()) {
                break;
            }
        }
    }

    private boolean cleanUpCancelledKeys() throws IOException {
        if (cancelledKeys >= CLEANUP_INTERVAL) {
            cancelledKeys = 0;
            selector.selectNow();
            return true;
        }
        return false;
    }

    private boolean read(SelectionKey k) {
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

    private void close(SelectionKey k) {
        NioSocketChannel ch = (NioSocketChannel) k.attachment();
        close(ch, succeededFuture(ch));
    }

    void writeFromUserCode(final NioSocketChannel channel) {
        if (!channel.isConnected()) {
            cleanUpWriteBuffer(channel);
            return;
        }
        if (scheduleWriteIfNecessary(channel)) {
            return;
        }
        if (channel.writeSuspended) {
            return;
        }
        if (channel.inWriteNowLoop) {
            return;
        }
        write0(channel);
    }

    void writeFromTaskLoop(final NioSocketChannel ch) {
        if (!ch.writeSuspended) {
            write0(ch);
        }
    }

    void writeFromSelectorLoop(final SelectionKey k) {
        NioSocketChannel ch = (NioSocketChannel) k.attachment();
        ch.writeSuspended = false;
        write0(ch);
    }

    private boolean scheduleWriteIfNecessary(final NioSocketChannel channel) {
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

    private void write0(NioSocketChannel channel) {
        boolean open = true;
        boolean addOpWrite = false;
        boolean removeOpWrite = false;
        long writtenBytes = 0;
        final SocketSendBufferPool sendBufferPool = this.sendBufferPool;
        final SocketChannel ch = channel.socket;
        final Queue<MessageEvent> writeBuffer = channel.writeBuffer;
        final int writeSpinCount = channel.getConfig().getWriteSpinCount();
        synchronized (channel.writeLock) {
            channel.inWriteNowLoop = true;
            for (; ; ) {
                MessageEvent evt = channel.currentWriteEvent;
                SendBuffer buf;
                if (evt == null) {
                    if ((channel.currentWriteEvent = evt = writeBuffer.poll()) == null) {
                        removeOpWrite = true;
                        channel.writeSuspended = false;
                        break;
                    }
                    channel.currentWriteBuffer = buf = sendBufferPool.acquire(evt.getMessage());
                } else {
                    buf = channel.currentWriteBuffer;
                }
                ChannelFuture future = evt.getFuture();
                try {
                    long localWrittenBytes = 0;
                    for (int i = writeSpinCount; i > 0; i--) {
                        localWrittenBytes = buf.transferTo(ch);
                        if (localWrittenBytes != 0) {
                            writtenBytes += localWrittenBytes;
                            break;
                        }
                        if (buf.finished()) {
                            break;
                        }
                    }
                    if (buf.finished()) {
                        buf.release();
                        channel.currentWriteEvent = null;
                        channel.currentWriteBuffer = null;
                        evt = null;
                        buf = null;
                        future.setSuccess();
                    } else {
                        addOpWrite = true;
                        channel.writeSuspended = true;
                        if (localWrittenBytes > 0) {
                            future.setProgress(localWrittenBytes, buf.writtenBytes(), buf.totalBytes());
                        }
                        break;
                    }
                } catch (AsynchronousCloseException e) {
                } catch (Throwable t) {
                    if (buf != null) {
                        buf.release();
                    }
                    channel.currentWriteEvent = null;
                    channel.currentWriteBuffer = null;
                    buf = null;
                    evt = null;
                    future.setFailure(t);
                    fireExceptionCaught(channel, t);
                    if (t instanceof IOException) {
                        open = false;
                        close(channel, succeededFuture(channel));
                    }
                }
            }
            channel.inWriteNowLoop = false;
            if (open) {
                if (addOpWrite) {
                    setOpWrite(channel);
                } else if (removeOpWrite) {
                    clearOpWrite(channel);
                }
            }
        }
        fireWriteComplete(channel, writtenBytes);
    }

    private void setOpWrite(NioSocketChannel channel) {
        Selector selector = this.selector;
        SelectionKey key = channel.socket.keyFor(selector);
        if (key == null) {
            return;
        }
        if (!key.isValid()) {
            close(key);
            return;
        }
        synchronized (channel.interestOpsLock) {
            int interestOps = channel.getRawInterestOps();
            if ((interestOps & SelectionKey.OP_WRITE) == 0) {
                interestOps |= SelectionKey.OP_WRITE;
                key.interestOps(interestOps);
                channel.setRawInterestOpsNow(interestOps);
            }
        }
    }

    private void clearOpWrite(NioSocketChannel channel) {
        Selector selector = this.selector;
        SelectionKey key = channel.socket.keyFor(selector);
        if (key == null) {
            return;
        }
        if (!key.isValid()) {
            close(key);
            return;
        }
        synchronized (channel.interestOpsLock) {
            int interestOps = channel.getRawInterestOps();
            if ((interestOps & SelectionKey.OP_WRITE) != 0) {
                interestOps &= ~SelectionKey.OP_WRITE;
                key.interestOps(interestOps);
                channel.setRawInterestOpsNow(interestOps);
            }
        }
    }

    void close(NioSocketChannel channel, ChannelFuture future) {
        boolean connected = channel.isConnected();
        boolean bound = channel.isBound();
        try {
            channel.socket.close();
            cancelledKeys++;
            if (channel.setClosed()) {
                future.setSuccess();
                if (connected) {
                    fireChannelDisconnected(channel);
                }
                if (bound) {
                    fireChannelUnbound(channel);
                }
                cleanUpWriteBuffer(channel);
                fireChannelClosed(channel);
            } else {
                future.setSuccess();
            }
        } catch (Throwable t) {
            future.setFailure(t);
            fireExceptionCaught(channel, t);
        }
    }

    private void cleanUpWriteBuffer(NioSocketChannel channel) {
        Exception cause = null;
        boolean fireExceptionCaught = false;
        synchronized (channel.writeLock) {
            MessageEvent evt = channel.currentWriteEvent;
            if (evt != null) {
                if (channel.isOpen()) {
                    cause = new NotYetConnectedException();
                } else {
                    cause = new ClosedChannelException();
                }
                ChannelFuture future = evt.getFuture();
                channel.currentWriteBuffer.release();
                channel.currentWriteBuffer = null;
                channel.currentWriteEvent = null;
                evt = null;
                future.setFailure(cause);
                fireExceptionCaught = true;
            }
            Queue<MessageEvent> writeBuffer = channel.writeBuffer;
            if (!writeBuffer.isEmpty()) {
                if (cause == null) {
                    if (channel.isOpen()) {
                        cause = new NotYetConnectedException();
                    } else {
                        cause = new ClosedChannelException();
                    }
                }
                for (; ; ) {
                    evt = writeBuffer.poll();
                    if (evt == null) {
                        break;
                    }
                    evt.getFuture().setFailure(cause);
                    fireExceptionCaught = true;
                }
            }
        }
        if (fireExceptionCaught) {
            fireExceptionCaught(channel, cause);
        }
    }

    void setInterestOps(NioSocketChannel channel, ChannelFuture future, int interestOps) {
        boolean changed = false;
        try {
            synchronized (channel.interestOpsLock) {
                Selector selector = this.selector;
                SelectionKey key = channel.socket.keyFor(selector);
                if (key == null || selector == null) {
                    channel.setRawInterestOpsNow(interestOps);
                    return;
                }
                interestOps &= ~Channel.OP_WRITE;
                interestOps |= channel.getRawInterestOps() & Channel.OP_WRITE;
                switch(CONSTRAINT_LEVEL) {
                    case 0:
                        if (channel.getRawInterestOps() != interestOps) {
                            key.interestOps(interestOps);
                            if (Thread.currentThread() != thread && wakenUp.compareAndSet(false, true)) {
                                selector.wakeup();
                            }
                            changed = true;
                        }
                        break;
                    case 1:
                    case 2:
                        if (channel.getRawInterestOps() != interestOps) {
                            if (Thread.currentThread() == thread) {
                                key.interestOps(interestOps);
                                changed = true;
                            } else {
                                selectorGuard.readLock().lock();
                                try {
                                    if (wakenUp.compareAndSet(false, true)) {
                                        selector.wakeup();
                                    }
                                    key.interestOps(interestOps);
                                    changed = true;
                                } finally {
                                    selectorGuard.readLock().unlock();
                                }
                            }
                        }
                        break;
                    default:
                        throw new Error();
                }
                if (changed) {
                    channel.setRawInterestOpsNow(interestOps);
                }
            }
            future.setSuccess();
            if (changed) {
                fireChannelInterestChanged(channel);
            }
        } catch (CancelledKeyException e) {
            ClosedChannelException cce = new ClosedChannelException();
            future.setFailure(cce);
            fireExceptionCaught(channel, cce);
        } catch (Throwable t) {
            future.setFailure(t);
            fireExceptionCaught(channel, t);
        }
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
                    channel.socket.configureBlocking(false);
                }
                synchronized (channel.interestOpsLock) {
                    channel.socket.register(selector, channel.getRawInterestOps(), channel);
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