package io.netty.handler.stream;

import static io.netty.channel.Channels.*;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import io.netty.buffer.ChannelBuffers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDownstreamHandler;
import io.netty.channel.ChannelEvent;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelStateEvent;
import io.netty.channel.ChannelUpstreamHandler;
import io.netty.channel.Channels;
import io.netty.channel.LifeCycleAwareChannelHandler;
import io.netty.channel.MessageEvent;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import io.netty.util.internal.QueueFactory;

public class ChunkedWriteHandler implements ChannelUpstreamHandler, ChannelDownstreamHandler, LifeCycleAwareChannelHandler {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ChunkedWriteHandler.class);

    private final Queue<MessageEvent> queue = QueueFactory.createQueue(MessageEvent.class);

    private volatile ChannelHandlerContext ctx;

    private final AtomicBoolean flush = new AtomicBoolean(false);

    private MessageEvent currentEvent;

    public void resumeTransfer() {
        ChannelHandlerContext ctx = this.ctx;
        if (ctx == null) {
            return;
        }
        try {
            flush(ctx, false);
        } catch (Exception e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Unexpected exception while sending chunks.", e);
            }
        }
    }

    @Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (!(e instanceof MessageEvent)) {
            ctx.sendDownstream(e);
            return;
        }
        boolean offered = queue.offer((MessageEvent) e);
        assert offered;
        final Channel channel = ctx.getChannel();
        if (channel.isWritable() || !channel.isConnected()) {
            this.ctx = ctx;
            flush(ctx, false);
        }
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (e instanceof ChannelStateEvent) {
            ChannelStateEvent cse = (ChannelStateEvent) e;
            switch(cse.getState()) {
                case INTEREST_OPS:
                    flush(ctx, true);
                    break;
                case OPEN:
                    if (!Boolean.TRUE.equals(cse.getValue())) {
                        discard(ctx, true);
                    }
                    break;
            }
        }
        ctx.sendUpstream(e);
    }

    private void discard(ChannelHandlerContext ctx, boolean fireNow) {
        ClosedChannelException cause = null;
        for (; ; ) {
            MessageEvent currentEvent = this.currentEvent;
            if (this.currentEvent == null) {
                currentEvent = queue.poll();
            } else {
                this.currentEvent = null;
            }
            if (currentEvent == null) {
                break;
            }
            Object m = currentEvent.getMessage();
            if (m instanceof ChunkedInput) {
                closeInput((ChunkedInput) m);
            }
            if (cause == null) {
                cause = new ClosedChannelException();
            }
            currentEvent.getFuture().setFailure(cause);
            currentEvent = null;
        }
        if (cause != null) {
            if (fireNow) {
                Channels.fireExceptionCaught(ctx.getChannel(), cause);
            } else {
                Channels.fireExceptionCaughtLater(ctx.getChannel(), cause);
            }
        }
    }

    private void flush(ChannelHandlerContext ctx, boolean fireNow) throws Exception {
        boolean acquired = false;
        final Channel channel = ctx.getChannel();
        if (acquired = flush.compareAndSet(false, true)) {
            try {
                if (!channel.isConnected()) {
                    discard(ctx, fireNow);
                    return;
                }
                while (channel.isWritable()) {
                    if (currentEvent == null) {
                        currentEvent = queue.poll();
                    }
                    if (currentEvent == null) {
                        break;
                    }
                    if (currentEvent.getFuture().isDone()) {
                        currentEvent = null;
                    } else {
                        final MessageEvent currentEvent = this.currentEvent;
                        Object m = currentEvent.getMessage();
                        if (m instanceof ChunkedInput) {
                            final ChunkedInput chunks = (ChunkedInput) m;
                            Object chunk;
                            boolean endOfInput;
                            boolean suspend;
                            try {
                                chunk = chunks.nextChunk();
                                endOfInput = chunks.isEndOfInput();
                                if (chunk == null) {
                                    chunk = ChannelBuffers.EMPTY_BUFFER;
                                    suspend = !endOfInput;
                                } else {
                                    suspend = false;
                                }
                            } catch (Throwable t) {
                                this.currentEvent = null;
                                currentEvent.getFuture().setFailure(t);
                                if (fireNow) {
                                    fireExceptionCaught(ctx, t);
                                } else {
                                    fireExceptionCaughtLater(ctx, t);
                                }
                                closeInput(chunks);
                                break;
                            }
                            if (suspend) {
                                break;
                            } else {
                                ChannelFuture writeFuture;
                                if (endOfInput) {
                                    this.currentEvent = null;
                                    writeFuture = currentEvent.getFuture();
                                    writeFuture.addListener(new ChannelFutureListener() {

                                        public void operationComplete(ChannelFuture future) throws Exception {
                                            closeInput(chunks);
                                        }
                                    });
                                } else {
                                    writeFuture = future(channel);
                                    writeFuture.addListener(new ChannelFutureListener() {

                                        public void operationComplete(ChannelFuture future) throws Exception {
                                            if (!future.isSuccess()) {
                                                currentEvent.getFuture().setFailure(future.getCause());
                                                closeInput((ChunkedInput) currentEvent.getMessage());
                                            }
                                        }
                                    });
                                }
                                Channels.write(ctx, writeFuture, chunk, currentEvent.getRemoteAddress());
                            }
                        } else {
                            this.currentEvent = null;
                            ctx.sendDownstream(currentEvent);
                        }
                    }
                    if (!channel.isConnected()) {
                        discard(ctx, fireNow);
                        return;
                    }
                }
            } finally {
                flush.set(false);
            }
        }
        if (acquired && (!channel.isConnected() || (channel.isWritable() && !queue.isEmpty()))) {
            flush(ctx, fireNow);
        }
    }

    static void closeInput(ChunkedInput chunks) {
        try {
            chunks.close();
        } catch (Throwable t) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to close a chunked input.", t);
            }
        }
    }

    @Override
    public void beforeAdd(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void afterAdd(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void beforeRemove(ChannelHandlerContext ctx) throws Exception {
        flush(ctx, false);
    }

    @Override
    public void afterRemove(ChannelHandlerContext ctx) throws Exception {
        Throwable cause = null;
        boolean fireExceptionCaught = false;
        for (; ; ) {
            MessageEvent currentEvent = this.currentEvent;
            if (this.currentEvent == null) {
                currentEvent = queue.poll();
            } else {
                this.currentEvent = null;
            }
            if (currentEvent == null) {
                break;
            }
            Object m = currentEvent.getMessage();
            if (m instanceof ChunkedInput) {
                closeInput((ChunkedInput) m);
            }
            if (cause == null) {
                cause = new IOException("Unable to flush event, discarding");
            }
            currentEvent.getFuture().setFailure(cause);
            fireExceptionCaught = true;
            currentEvent = null;
        }
        if (fireExceptionCaught) {
            Channels.fireExceptionCaughtLater(ctx.getChannel(), cause);
        }
    }
}