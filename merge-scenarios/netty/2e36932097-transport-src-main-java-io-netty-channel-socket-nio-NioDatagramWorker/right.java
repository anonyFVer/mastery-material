package io.netty.channel.socket.nio;

import static io.netty.channel.Channels.*;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.NotYetBoundException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import io.netty.buffer.ChannelBufferFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.MessageEvent;
import io.netty.channel.ReceiveBufferSizePredictor;
import io.netty.channel.socket.nio.SocketSendBufferPool.SendBuffer;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import io.netty.util.internal.QueueFactory;

class NioDatagramWorker implements Runnable {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(NioDatagramWorker.class);

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

    private final SocketSendBufferPool sendBufferPool = new SocketSendBufferPool();

    NioDatagramWorker(final Executor executor) {
        this.executor = executor;
    }

    void register(final NioDatagramChannel channel, final ChannelFuture future) {
        final Runnable channelRegTask = new ChannelRegistionTask(channel, future);
        Selector selector;
        synchronized (startStopLock) {
            if (!started) {
                try {
                    this.selector = selector = Selector.open();
                } catch (final Throwable t) {
                    throw new ChannelException("Failed to create a selector.", t);
                }
                boolean success = false;
                try {
                    executor.execute(this);
                    success = true;
                } finally {
                    if (!success) {
                        try {
                            selector.close();
                        } catch (final Throwable t) {
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
            boolean offered = registerTaskQueue.offer(channelRegTask);
            assert offered;
        }
        if (wakenUp.compareAndSet(false, true)) {
            selector.wakeup();
        }
    }

    @Override
    public void run() {
        thread = Thread.currentThread();
        final Selector selector = this.selector;
        boolean shutdown = false;
        for (; ; ) {
            wakenUp.set(false);
            if (NioProviderMetadata.CONSTRAINT_LEVEL != 0) {
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

    private void processSelectedKeys(final Set<SelectionKey> selectedKeys) throws IOException {
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
        if (cancelledKeys >= NioWorker.CLEANUP_INTERVAL) {
            cancelledKeys = 0;
            selector.selectNow();
            return true;
        }
        return false;
    }

    private boolean read(final SelectionKey key) {
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

    private void close(SelectionKey k) {
        final NioDatagramChannel ch = (NioDatagramChannel) k.attachment();
        close(ch, succeededFuture(ch));
    }

    void writeFromUserCode(final NioDatagramChannel channel) {
        if (!channel.isBound()) {
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

    void writeFromTaskLoop(final NioDatagramChannel ch) {
        if (!ch.writeSuspended) {
            write0(ch);
        }
    }

    void writeFromSelectorLoop(final SelectionKey k) {
        NioDatagramChannel ch = (NioDatagramChannel) k.attachment();
        ch.writeSuspended = false;
        write0(ch);
    }

    private boolean scheduleWriteIfNecessary(final NioDatagramChannel channel) {
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

    private void write0(final NioDatagramChannel channel) {
        boolean addOpWrite = false;
        boolean removeOpWrite = false;
        long writtenBytes = 0;
        final SocketSendBufferPool sendBufferPool = this.sendBufferPool;
        final DatagramChannel ch = channel.getDatagramChannel();
        final Queue<MessageEvent> writeBuffer = channel.writeBufferQueue;
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
                try {
                    long localWrittenBytes = 0;
                    SocketAddress raddr = evt.getRemoteAddress();
                    if (raddr == null) {
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
                    } else {
                        for (int i = writeSpinCount; i > 0; i--) {
                            localWrittenBytes = buf.transferTo(ch, raddr);
                            if (localWrittenBytes != 0) {
                                writtenBytes += localWrittenBytes;
                                break;
                            }
                            if (buf.finished()) {
                                break;
                            }
                        }
                    }
                    if (localWrittenBytes > 0 || buf.finished()) {
                        buf.release();
                        ChannelFuture future = evt.getFuture();
                        channel.currentWriteEvent = null;
                        channel.currentWriteBuffer = null;
                        evt = null;
                        buf = null;
                        future.setSuccess();
                    } else {
                        addOpWrite = true;
                        channel.writeSuspended = true;
                        break;
                    }
                } catch (final AsynchronousCloseException e) {
                } catch (final Throwable t) {
                    buf.release();
                    ChannelFuture future = evt.getFuture();
                    channel.currentWriteEvent = null;
                    channel.currentWriteBuffer = null;
                    buf = null;
                    evt = null;
                    future.setFailure(t);
                    fireExceptionCaught(channel, t);
                }
            }
            channel.inWriteNowLoop = false;
            if (addOpWrite) {
                setOpWrite(channel);
            } else if (removeOpWrite) {
                clearOpWrite(channel);
            }
        }
        fireWriteComplete(channel, writtenBytes);
    }

    private void setOpWrite(final NioDatagramChannel channel) {
        Selector selector = this.selector;
        SelectionKey key = channel.getDatagramChannel().keyFor(selector);
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

    private void clearOpWrite(NioDatagramChannel channel) {
        Selector selector = this.selector;
        SelectionKey key = channel.getDatagramChannel().keyFor(selector);
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

    void close(final NioDatagramChannel channel, final ChannelFuture future) {
        boolean connected = channel.isConnected();
        boolean bound = channel.isBound();
        try {
            channel.getDatagramChannel().close();
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

    private void cleanUpWriteBuffer(final NioDatagramChannel channel) {
        Exception cause = null;
        boolean fireExceptionCaught = false;
        synchronized (channel.writeLock) {
            MessageEvent evt = channel.currentWriteEvent;
            if (evt != null) {
                if (channel.isOpen()) {
                    cause = new NotYetBoundException();
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
            Queue<MessageEvent> writeBuffer = channel.writeBufferQueue;
            if (!writeBuffer.isEmpty()) {
                if (cause == null) {
                    if (channel.isOpen()) {
                        cause = new NotYetBoundException();
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

    void setInterestOps(final NioDatagramChannel channel, ChannelFuture future, int interestOps) {
        boolean changed = false;
        try {
            synchronized (channel.interestOpsLock) {
                final Selector selector = this.selector;
                final SelectionKey key = channel.getDatagramChannel().keyFor(selector);
                if (key == null || selector == null) {
                    channel.setRawInterestOpsNow(interestOps);
                    return;
                }
                interestOps &= ~Channel.OP_WRITE;
                interestOps |= channel.getRawInterestOps() & Channel.OP_WRITE;
                switch(NioProviderMetadata.CONSTRAINT_LEVEL) {
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
        } catch (final CancelledKeyException e) {
            ClosedChannelException cce = new ClosedChannelException();
            future.setFailure(cce);
            fireExceptionCaught(channel, cce);
        } catch (final Throwable t) {
            future.setFailure(t);
            fireExceptionCaught(channel, t);
        }
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