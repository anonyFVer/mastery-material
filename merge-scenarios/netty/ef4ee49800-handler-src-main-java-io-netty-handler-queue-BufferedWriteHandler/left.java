package io.netty.handler.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBuffers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelStateEvent;
import io.netty.channel.Channels;
import io.netty.channel.MessageEvent;
import io.netty.channel.SimpleChannelHandler;
import io.netty.channel.socket.nio.NioSocketChannelConfig;
import io.netty.util.HashedWheelTimer;
import io.netty.util.internal.QueueFactory;

public class BufferedWriteHandler extends SimpleChannelHandler {

    private final Queue<MessageEvent> queue;

    private final boolean consolidateOnFlush;

    private volatile ChannelHandlerContext ctx;

    public BufferedWriteHandler() {
        this(false);
    }

    public BufferedWriteHandler(Queue<MessageEvent> queue) {
        this(queue, false);
    }

    public BufferedWriteHandler(boolean consolidateOnFlush) {
        this(QueueFactory.createQueue(MessageEvent.class), consolidateOnFlush);
    }

    public BufferedWriteHandler(Queue<MessageEvent> queue, boolean consolidateOnFlush) {
        if (queue == null) {
            throw new NullPointerException("queue");
        }
        this.queue = queue;
        this.consolidateOnFlush = consolidateOnFlush;
    }

    public boolean isConsolidateOnFlush() {
        return consolidateOnFlush;
    }

    protected Queue<MessageEvent> getQueue() {
        return queue;
    }

    public void flush() {
        flush(consolidateOnFlush);
    }

    public void flush(boolean consolidateOnFlush) {
        final ChannelHandlerContext ctx = this.ctx;
        if (ctx == null) {
            return;
        }
        final Queue<MessageEvent> queue = getQueue();
        if (consolidateOnFlush) {
            if (queue.isEmpty()) {
                return;
            }
            List<MessageEvent> pendingWrites = new ArrayList<MessageEvent>();
            synchronized (this) {
                for (; ; ) {
                    MessageEvent e = queue.poll();
                    if (e == null) {
                        break;
                    }
                    if (!(e.getMessage() instanceof ChannelBuffer)) {
                        if ((pendingWrites = consolidatedWrite(pendingWrites)) == null) {
                            pendingWrites = new ArrayList<MessageEvent>();
                        }
                        ctx.sendDownstream(e);
                    } else {
                        pendingWrites.add(e);
                    }
                }
                consolidatedWrite(pendingWrites);
            }
        } else {
            synchronized (this) {
                for (; ; ) {
                    MessageEvent e = queue.poll();
                    if (e == null) {
                        break;
                    }
                    ctx.sendDownstream(e);
                }
            }
        }
    }

    private List<MessageEvent> consolidatedWrite(final List<MessageEvent> pendingWrites) {
        final int size = pendingWrites.size();
        if (size == 1) {
            ctx.sendDownstream(pendingWrites.remove(0));
            return pendingWrites;
        } else if (size == 0) {
            return pendingWrites;
        }
        ChannelBuffer[] data = new ChannelBuffer[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = (ChannelBuffer) pendingWrites.get(i).getMessage();
        }
        ChannelBuffer composite = ChannelBuffers.wrappedBuffer(data);
        ChannelFuture future = Channels.future(ctx.channel());
        future.addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    for (MessageEvent e : pendingWrites) {
                        e.getFuture().setSuccess();
                    }
                } else {
                    Throwable cause = future.cause();
                    for (MessageEvent e : pendingWrites) {
                        e.getFuture().setFailure(cause);
                    }
                }
            }
        });
        Channels.write(ctx, future, composite);
        return null;
    }

    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if (this.ctx == null) {
            this.ctx = ctx;
        } else {
            assert this.ctx == ctx;
        }
        getQueue().add(e);
    }

    @Override
    public void disconnectRequested(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        try {
            flush(consolidateOnFlush);
        } finally {
            ctx.sendDownstream(e);
        }
    }

    @Override
    public void closeRequested(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        try {
            flush(consolidateOnFlush);
        } finally {
            ctx.sendDownstream(e);
        }
    }
}