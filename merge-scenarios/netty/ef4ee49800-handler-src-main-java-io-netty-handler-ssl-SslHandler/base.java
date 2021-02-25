package io.netty.handler.ssl;

import static io.netty.channel.Channels.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBuffers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDownstreamHandler;
import io.netty.channel.ChannelEvent;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelStateEvent;
import io.netty.channel.Channels;
import io.netty.channel.DownstreamMessageEvent;
import io.netty.channel.ExceptionEvent;
import io.netty.channel.LifeCycleAwareChannelHandler;
import io.netty.channel.MessageEvent;
import io.netty.handler.codec.frame.FrameDecoder;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import io.netty.util.internal.NonReentrantLock;
import io.netty.util.internal.QueueFactory;

public class SslHandler extends FrameDecoder implements ChannelDownstreamHandler, LifeCycleAwareChannelHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(SslHandler.class);

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private static final Pattern IGNORABLE_ERROR_MESSAGE = Pattern.compile("^.*(?:connection.*reset|connection.*closed|broken.*pipe).*$", Pattern.CASE_INSENSITIVE);

    private static SslBufferPool defaultBufferPool;

    public static synchronized SslBufferPool getDefaultBufferPool() {
        if (defaultBufferPool == null) {
            defaultBufferPool = new SslBufferPool();
        }
        return defaultBufferPool;
    }

    private volatile ChannelHandlerContext ctx;

    private final SSLEngine engine;

    private final SslBufferPool bufferPool;

    private final Executor delegatedTaskExecutor;

    private final boolean startTls;

    private volatile boolean enableRenegotiation = true;

    final Object handshakeLock = new Object();

    private boolean handshaking;

    private volatile boolean handshaken;

    private volatile ChannelFuture handshakeFuture;

    private final AtomicBoolean sentFirstMessage = new AtomicBoolean();

    private final AtomicBoolean sentCloseNotify = new AtomicBoolean();

    int ignoreClosedChannelException;

    final Object ignoreClosedChannelExceptionLock = new Object();

    private final Queue<PendingWrite> pendingUnencryptedWrites = new LinkedList<PendingWrite>();

    private final Queue<MessageEvent> pendingEncryptedWrites = QueueFactory.createQueue(MessageEvent.class);

    private final NonReentrantLock pendingEncryptedWritesLock = new NonReentrantLock();

    private volatile boolean issueHandshake;

    private static final ChannelFutureListener HANDSHAKE_LISTENER = new ChannelFutureListener() {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (!future.isSuccess()) {
                Channels.fireExceptionCaught(future.getChannel(), future.getCause());
            }
        }
    };

    public SslHandler(SSLEngine engine) {
        this(engine, getDefaultBufferPool(), ImmediateExecutor.INSTANCE);
    }

    public SslHandler(SSLEngine engine, SslBufferPool bufferPool) {
        this(engine, bufferPool, ImmediateExecutor.INSTANCE);
    }

    public SslHandler(SSLEngine engine, boolean startTls) {
        this(engine, getDefaultBufferPool(), startTls);
    }

    public SslHandler(SSLEngine engine, SslBufferPool bufferPool, boolean startTls) {
        this(engine, bufferPool, startTls, ImmediateExecutor.INSTANCE);
    }

    public SslHandler(SSLEngine engine, Executor delegatedTaskExecutor) {
        this(engine, getDefaultBufferPool(), delegatedTaskExecutor);
    }

    public SslHandler(SSLEngine engine, SslBufferPool bufferPool, Executor delegatedTaskExecutor) {
        this(engine, bufferPool, false, delegatedTaskExecutor);
    }

    public SslHandler(SSLEngine engine, boolean startTls, Executor delegatedTaskExecutor) {
        this(engine, getDefaultBufferPool(), startTls, delegatedTaskExecutor);
    }

    public SslHandler(SSLEngine engine, SslBufferPool bufferPool, boolean startTls, Executor delegatedTaskExecutor) {
        if (engine == null) {
            throw new NullPointerException("engine");
        }
        if (bufferPool == null) {
            throw new NullPointerException("bufferPool");
        }
        if (delegatedTaskExecutor == null) {
            throw new NullPointerException("delegatedTaskExecutor");
        }
        this.engine = engine;
        this.bufferPool = bufferPool;
        this.delegatedTaskExecutor = delegatedTaskExecutor;
        this.startTls = startTls;
    }

    public SSLEngine getEngine() {
        return engine;
    }

    public ChannelFuture handshake() {
        if (handshaken && !isEnableRenegotiation()) {
            throw new IllegalStateException("renegotiation disabled");
        }
        ChannelHandlerContext ctx = this.ctx;
        Channel channel = ctx.getChannel();
        ChannelFuture handshakeFuture;
        Exception exception = null;
        synchronized (handshakeLock) {
            if (handshaking) {
                return this.handshakeFuture;
            } else {
                handshaking = true;
                try {
                    engine.beginHandshake();
                    runDelegatedTasks();
                    handshakeFuture = this.handshakeFuture = future(channel);
                } catch (Exception e) {
                    handshakeFuture = this.handshakeFuture = failedFuture(channel, e);
                    exception = e;
                }
            }
        }
        if (exception == null) {
            try {
                wrapNonAppData(ctx, channel);
            } catch (SSLException e) {
                fireExceptionCaught(ctx, e);
                handshakeFuture.setFailure(e);
            }
        } else {
            fireExceptionCaught(ctx, exception);
        }
        return handshakeFuture;
    }

    public ChannelFuture close() {
        ChannelHandlerContext ctx = this.ctx;
        Channel channel = ctx.getChannel();
        try {
            engine.closeOutbound();
            return wrapNonAppData(ctx, channel);
        } catch (SSLException e) {
            fireExceptionCaught(ctx, e);
            return failedFuture(channel, e);
        }
    }

    public boolean isEnableRenegotiation() {
        return enableRenegotiation;
    }

    public void setEnableRenegotiation(boolean enableRenegotiation) {
        this.enableRenegotiation = enableRenegotiation;
    }

    public void setIssueHandshake(boolean issueHandshake) {
        this.issueHandshake = issueHandshake;
    }

    public boolean isIssueHandshake() {
        return issueHandshake;
    }

    @Override
    public void handleDownstream(final ChannelHandlerContext context, final ChannelEvent evt) throws Exception {
        if (evt instanceof ChannelStateEvent) {
            ChannelStateEvent e = (ChannelStateEvent) evt;
            switch(e.getState()) {
                case OPEN:
                case CONNECTED:
                case BOUND:
                    if (Boolean.FALSE.equals(e.getValue()) || e.getValue() == null) {
                        closeOutboundAndChannel(context, e);
                        return;
                    }
            }
        }
        if (!(evt instanceof MessageEvent)) {
            context.sendDownstream(evt);
            return;
        }
        MessageEvent e = (MessageEvent) evt;
        if (!(e.getMessage() instanceof ChannelBuffer)) {
            context.sendDownstream(evt);
            return;
        }
        if (startTls && sentFirstMessage.compareAndSet(false, true)) {
            context.sendDownstream(evt);
            return;
        }
        ChannelBuffer msg = (ChannelBuffer) e.getMessage();
        PendingWrite pendingWrite;
        if (msg.readable()) {
            pendingWrite = new PendingWrite(evt.getFuture(), msg.toByteBuffer(msg.readerIndex(), msg.readableBytes()));
        } else {
            pendingWrite = new PendingWrite(evt.getFuture(), null);
        }
        synchronized (pendingUnencryptedWrites) {
            boolean offered = pendingUnencryptedWrites.offer(pendingWrite);
            assert offered;
        }
        wrap(context, evt.getChannel());
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        synchronized (handshakeLock) {
            if (handshaking) {
                handshakeFuture.setFailure(new ClosedChannelException());
            }
        }
        try {
            super.channelDisconnected(ctx, e);
        } finally {
            unwrap(ctx, e.getChannel(), ChannelBuffers.EMPTY_BUFFER, 0, 0);
            engine.closeOutbound();
            if (!sentCloseNotify.get() && handshaken) {
                try {
                    engine.closeInbound();
                } catch (SSLException ex) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to clean up SSLEngine.", ex);
                    }
                }
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
            if (cause instanceof ClosedChannelException) {
                synchronized (ignoreClosedChannelExceptionLock) {
                    if (ignoreClosedChannelException > 0) {
                        ignoreClosedChannelException--;
                        if (logger.isDebugEnabled()) {
                            logger.debug("Swallowing an exception raised while " + "writing non-app data", cause);
                        }
                        return;
                    }
                }
            } else if (engine.isOutboundDone()) {
                String message = String.valueOf(cause.getMessage()).toLowerCase();
                if (IGNORABLE_ERROR_MESSAGE.matcher(message).matches()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Swallowing a 'connection reset by peer / " + "broken pipe' error occurred while writing " + "'closure_notify'", cause);
                    }
                    Channels.close(ctx, succeededFuture(e.getChannel()));
                    return;
                }
            }
        }
        ctx.sendUpstream(e);
    }

    @Override
    protected Object decode(final ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        if (buffer.readableBytes() < 5) {
            return null;
        }
        int packetLength = 0;
        boolean tls;
        switch(buffer.getUnsignedByte(buffer.readerIndex())) {
            case 20:
            case 21:
            case 22:
            case 23:
                tls = true;
                break;
            default:
                tls = false;
        }
        if (tls) {
            int majorVersion = buffer.getUnsignedByte(buffer.readerIndex() + 1);
            if (majorVersion == 3) {
                packetLength = (getShort(buffer, buffer.readerIndex() + 3) & 0xFFFF) + 5;
                if (packetLength <= 5) {
                    tls = false;
                }
            } else {
                tls = false;
            }
        }
        if (!tls) {
            boolean sslv2 = true;
            int headerLength = (buffer.getUnsignedByte(buffer.readerIndex()) & 0x80) != 0 ? 2 : 3;
            int majorVersion = buffer.getUnsignedByte(buffer.readerIndex() + headerLength + 1);
            if (majorVersion == 2 || majorVersion == 3) {
                if (headerLength == 2) {
                    packetLength = (getShort(buffer, buffer.readerIndex()) & 0x7FFF) + 2;
                } else {
                    packetLength = (getShort(buffer, buffer.readerIndex()) & 0x3FFF) + 3;
                }
                if (packetLength <= headerLength) {
                    sslv2 = false;
                }
            } else {
                sslv2 = false;
            }
            if (!sslv2) {
                SSLException e = new SSLException("not an SSL/TLS record: " + ChannelBuffers.hexDump(buffer));
                buffer.skipBytes(buffer.readableBytes());
                throw e;
            }
        }
        assert packetLength > 0;
        if (buffer.readableBytes() < packetLength) {
            return null;
        }
        final int packetOffset = buffer.readerIndex();
        buffer.skipBytes(packetLength);
        return unwrap(ctx, channel, buffer, packetOffset, packetLength);
    }

    private static short getShort(ChannelBuffer buf, int offset) {
        return (short) (buf.getByte(offset) << 8 | buf.getByte(offset + 1) & 0xFF);
    }

    private ChannelFuture wrap(ChannelHandlerContext context, Channel channel) throws SSLException {
        ChannelFuture future = null;
        ChannelBuffer msg;
        ByteBuffer outNetBuf = bufferPool.acquire();
        boolean success = true;
        boolean offered = false;
        boolean needsUnwrap = false;
        try {
            loop: for (; ; ) {
                synchronized (pendingUnencryptedWrites) {
                    PendingWrite pendingWrite = pendingUnencryptedWrites.peek();
                    if (pendingWrite == null) {
                        break;
                    }
                    ByteBuffer outAppBuf = pendingWrite.outAppBuf;
                    if (outAppBuf == null) {
                        pendingUnencryptedWrites.remove();
                        offerEncryptedWriteRequest(new DownstreamMessageEvent(channel, pendingWrite.future, ChannelBuffers.EMPTY_BUFFER, channel.getRemoteAddress()));
                        offered = true;
                    } else {
                        SSLEngineResult result = null;
                        try {
                            synchronized (handshakeLock) {
                                result = engine.wrap(outAppBuf, outNetBuf);
                            }
                        } finally {
                            if (!outAppBuf.hasRemaining()) {
                                pendingUnencryptedWrites.remove();
                            }
                        }
                        if (result.bytesProduced() > 0) {
                            outNetBuf.flip();
                            msg = ChannelBuffers.buffer(outNetBuf.remaining());
                            msg.writeBytes(outNetBuf.array(), 0, msg.capacity());
                            outNetBuf.clear();
                            if (pendingWrite.outAppBuf.hasRemaining()) {
                                future = succeededFuture(channel);
                            } else {
                                future = pendingWrite.future;
                            }
                            MessageEvent encryptedWrite = new DownstreamMessageEvent(channel, future, msg, channel.getRemoteAddress());
                            offerEncryptedWriteRequest(encryptedWrite);
                            offered = true;
                        } else if (result.getStatus() == Status.CLOSED) {
                            success = false;
                            break;
                        } else {
                            final HandshakeStatus handshakeStatus = result.getHandshakeStatus();
                            handleRenegotiation(handshakeStatus);
                            switch(handshakeStatus) {
                                case NEED_WRAP:
                                    if (outAppBuf.hasRemaining()) {
                                        break;
                                    } else {
                                        break loop;
                                    }
                                case NEED_UNWRAP:
                                    needsUnwrap = true;
                                    break loop;
                                case NEED_TASK:
                                    runDelegatedTasks();
                                    break;
                                case FINISHED:
                                case NOT_HANDSHAKING:
                                    if (handshakeStatus == HandshakeStatus.FINISHED) {
                                        setHandshakeSuccess(channel);
                                    }
                                    if (result.getStatus() == Status.CLOSED) {
                                        success = false;
                                    }
                                    break loop;
                                default:
                                    throw new IllegalStateException("Unknown handshake status: " + handshakeStatus);
                            }
                        }
                    }
                }
            }
        } catch (SSLException e) {
            success = false;
            setHandshakeFailure(channel, e);
            throw e;
        } finally {
            bufferPool.release(outNetBuf);
            if (offered) {
                flushPendingEncryptedWrites(context);
            }
            if (!success) {
                IllegalStateException cause = new IllegalStateException("SSLEngine already closed");
                for (; ; ) {
                    PendingWrite pendingWrite;
                    synchronized (pendingUnencryptedWrites) {
                        pendingWrite = pendingUnencryptedWrites.poll();
                        if (pendingWrite == null) {
                            break;
                        }
                    }
                    pendingWrite.future.setFailure(cause);
                }
            }
        }
        if (needsUnwrap) {
            unwrap(context, channel, ChannelBuffers.EMPTY_BUFFER, 0, 0);
        }
        if (future == null) {
            future = succeededFuture(channel);
        }
        return future;
    }

    private void offerEncryptedWriteRequest(MessageEvent encryptedWrite) {
        final boolean locked = pendingEncryptedWritesLock.tryLock();
        try {
            pendingEncryptedWrites.offer(encryptedWrite);
        } finally {
            if (locked) {
                pendingEncryptedWritesLock.unlock();
            }
        }
    }

    private void flushPendingEncryptedWrites(ChannelHandlerContext ctx) {
        if (!pendingEncryptedWritesLock.tryLock()) {
            return;
        }
        try {
            MessageEvent e;
            while ((e = pendingEncryptedWrites.poll()) != null) {
                ctx.sendDownstream(e);
            }
        } finally {
            pendingEncryptedWritesLock.unlock();
        }
    }

    private ChannelFuture wrapNonAppData(ChannelHandlerContext ctx, Channel channel) throws SSLException {
        ChannelFuture future = null;
        ByteBuffer outNetBuf = bufferPool.acquire();
        SSLEngineResult result;
        try {
            for (; ; ) {
                synchronized (handshakeLock) {
                    result = engine.wrap(EMPTY_BUFFER, outNetBuf);
                }
                if (result.bytesProduced() > 0) {
                    outNetBuf.flip();
                    ChannelBuffer msg = ChannelBuffers.buffer(outNetBuf.remaining());
                    msg.writeBytes(outNetBuf.array(), 0, msg.capacity());
                    outNetBuf.clear();
                    future = future(channel);
                    future.addListener(new ChannelFutureListener() {

                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (future.getCause() instanceof ClosedChannelException) {
                                synchronized (ignoreClosedChannelExceptionLock) {
                                    ignoreClosedChannelException++;
                                }
                            }
                        }
                    });
                    write(ctx, future, msg);
                }
                final HandshakeStatus handshakeStatus = result.getHandshakeStatus();
                handleRenegotiation(handshakeStatus);
                switch(handshakeStatus) {
                    case FINISHED:
                        setHandshakeSuccess(channel);
                        runDelegatedTasks();
                        break;
                    case NEED_TASK:
                        runDelegatedTasks();
                        break;
                    case NEED_UNWRAP:
                        if (!Thread.holdsLock(handshakeLock)) {
                            unwrap(ctx, channel, ChannelBuffers.EMPTY_BUFFER, 0, 0);
                        }
                        break;
                    case NOT_HANDSHAKING:
                    case NEED_WRAP:
                        break;
                    default:
                        throw new IllegalStateException("Unexpected handshake status: " + handshakeStatus);
                }
                if (result.bytesProduced() == 0) {
                    break;
                }
            }
        } catch (SSLException e) {
            setHandshakeFailure(channel, e);
            throw e;
        } finally {
            bufferPool.release(outNetBuf);
        }
        if (future == null) {
            future = succeededFuture(channel);
        }
        return future;
    }

    private ChannelBuffer unwrap(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, int offset, int length) throws SSLException {
        ByteBuffer inNetBuf = buffer.toByteBuffer(offset, length);
        ByteBuffer outAppBuf = bufferPool.acquire();
        try {
            boolean needsWrap = false;
            loop: for (; ; ) {
                SSLEngineResult result;
                boolean needsHandshake = false;
                synchronized (handshakeLock) {
                    if (!handshaken && !handshaking && !engine.getUseClientMode() && !engine.isInboundDone() && !engine.isOutboundDone()) {
                        needsHandshake = true;
                    }
                }
                if (needsHandshake) {
                    handshake();
                }
                synchronized (handshakeLock) {
                    result = engine.unwrap(inNetBuf, outAppBuf);
                }
                final HandshakeStatus handshakeStatus = result.getHandshakeStatus();
                handleRenegotiation(handshakeStatus);
                switch(handshakeStatus) {
                    case NEED_UNWRAP:
                        if (inNetBuf.hasRemaining() && !engine.isInboundDone()) {
                            break;
                        } else {
                            break loop;
                        }
                    case NEED_WRAP:
                        wrapNonAppData(ctx, channel);
                        break;
                    case NEED_TASK:
                        runDelegatedTasks();
                        break;
                    case FINISHED:
                        setHandshakeSuccess(channel);
                        needsWrap = true;
                        break loop;
                    case NOT_HANDSHAKING:
                        needsWrap = true;
                        break loop;
                    default:
                        throw new IllegalStateException("Unknown handshake status: " + handshakeStatus);
                }
            }
            if (needsWrap) {
                if (!Thread.holdsLock(handshakeLock) && !pendingEncryptedWritesLock.isHeldByCurrentThread()) {
                    wrap(ctx, channel);
                }
            }
            outAppBuf.flip();
            if (outAppBuf.hasRemaining()) {
                ChannelBuffer frame = ctx.getChannel().getConfig().getBufferFactory().getBuffer(outAppBuf.remaining());
                frame.writeBytes(outAppBuf.array(), 0, frame.capacity());
                return frame;
            } else {
                return null;
            }
        } catch (SSLException e) {
            setHandshakeFailure(channel, e);
            throw e;
        } finally {
            bufferPool.release(outAppBuf);
        }
    }

    private void handleRenegotiation(HandshakeStatus handshakeStatus) {
        if (handshakeStatus == HandshakeStatus.NOT_HANDSHAKING || handshakeStatus == HandshakeStatus.FINISHED) {
            return;
        }
        if (!handshaken) {
            return;
        }
        final boolean renegotiate;
        synchronized (handshakeLock) {
            if (handshaking) {
                return;
            }
            if (engine.isInboundDone() || engine.isOutboundDone()) {
                return;
            }
            if (isEnableRenegotiation()) {
                renegotiate = true;
            } else {
                renegotiate = false;
                handshaking = true;
            }
        }
        if (renegotiate) {
            handshake();
        } else {
            fireExceptionCaught(ctx, new SSLException("renegotiation attempted by peer; " + "closing the connection"));
            Channels.close(ctx, succeededFuture(ctx.getChannel()));
        }
    }

    private void runDelegatedTasks() {
        for (; ; ) {
            final Runnable task;
            synchronized (handshakeLock) {
                task = engine.getDelegatedTask();
            }
            if (task == null) {
                break;
            }
            delegatedTaskExecutor.execute(new Runnable() {

                @Override
                public void run() {
                    synchronized (handshakeLock) {
                        task.run();
                    }
                }
            });
        }
    }

    private void setHandshakeSuccess(Channel channel) {
        synchronized (handshakeLock) {
            handshaking = false;
            handshaken = true;
            if (handshakeFuture == null) {
                handshakeFuture = future(channel);
            }
        }
        handshakeFuture.setSuccess();
    }

    private void setHandshakeFailure(Channel channel, SSLException cause) {
        synchronized (handshakeLock) {
            if (!handshaking) {
                return;
            }
            handshaking = false;
            handshaken = false;
            if (handshakeFuture == null) {
                handshakeFuture = future(channel);
            }
            engine.closeOutbound();
            try {
                engine.closeInbound();
            } catch (SSLException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("SSLEngine.closeInbound() raised an exception after " + "a handshake failure.", e);
                }
            }
        }
        handshakeFuture.setFailure(cause);
    }

    private void closeOutboundAndChannel(final ChannelHandlerContext context, final ChannelStateEvent e) {
        if (!e.getChannel().isConnected()) {
            context.sendDownstream(e);
            return;
        }
        boolean success = false;
        try {
            try {
                unwrap(context, e.getChannel(), ChannelBuffers.EMPTY_BUFFER, 0, 0);
            } catch (SSLException ex) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Failed to unwrap before sending a close_notify message", ex);
                }
            }
            if (!engine.isInboundDone()) {
                if (sentCloseNotify.compareAndSet(false, true)) {
                    engine.closeOutbound();
                    try {
                        ChannelFuture closeNotifyFuture = wrapNonAppData(context, e.getChannel());
                        closeNotifyFuture.addListener(new ClosingChannelFutureListener(context, e));
                        success = true;
                    } catch (SSLException ex) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Failed to encode a close_notify message", ex);
                        }
                    }
                }
            } else {
                success = true;
            }
        } finally {
            if (!success) {
                context.sendDownstream(e);
            }
        }
    }

    private static final class PendingWrite {

        final ChannelFuture future;

        final ByteBuffer outAppBuf;

        PendingWrite(ChannelFuture future, ByteBuffer outAppBuf) {
            this.future = future;
            this.outAppBuf = outAppBuf;
        }
    }

    private static final class ClosingChannelFutureListener implements ChannelFutureListener {

        private final ChannelHandlerContext context;

        private final ChannelStateEvent e;

        ClosingChannelFutureListener(ChannelHandlerContext context, ChannelStateEvent e) {
            this.context = context;
            this.e = e;
        }

        @Override
        public void operationComplete(ChannelFuture closeNotifyFuture) throws Exception {
            if (!(closeNotifyFuture.getCause() instanceof ClosedChannelException)) {
                Channels.close(context, e.getFuture());
            } else {
                e.getFuture().setSuccess();
            }
        }
    }

    @Override
    public void beforeAdd(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    @Override
    public void afterAdd(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void beforeRemove(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void afterRemove(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        if (issueHandshake) {
            handshake().addListener(HANDSHAKE_LISTENER);
        }
        super.channelConnected(ctx, e);
    }
}