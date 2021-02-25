package io.netty.handler.traffic;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import io.netty.buffer.ChannelBuffer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelEvent;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelState;
import io.netty.channel.ChannelStateEvent;
import io.netty.channel.MessageEvent;
import io.netty.channel.SimpleChannelHandler;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import io.netty.util.DefaultObjectSizeEstimator;
import io.netty.util.ExternalResourceReleasable;
import io.netty.util.ObjectSizeEstimator;
import io.netty.util.internal.ExecutorUtil;

public abstract class AbstractTrafficShapingHandler extends SimpleChannelHandler implements ExternalResourceReleasable {

    static InternalLogger logger = InternalLoggerFactory.getInstance(AbstractTrafficShapingHandler.class);

    public static final long DEFAULT_CHECK_INTERVAL = 1000;

    private static final long MINIMAL_WAIT = 10;

    protected TrafficCounter trafficCounter;

    private ObjectSizeEstimator objectSizeEstimator;

    protected Executor executor;

    private long writeLimit;

    private long readLimit;

    protected long checkInterval = DEFAULT_CHECK_INTERVAL;

    final AtomicBoolean release = new AtomicBoolean(false);

    private void init(ObjectSizeEstimator newObjectSizeEstimator, Executor newExecutor, long newWriteLimit, long newReadLimit, long newCheckInterval) {
        objectSizeEstimator = newObjectSizeEstimator;
        executor = newExecutor;
        writeLimit = newWriteLimit;
        readLimit = newReadLimit;
        checkInterval = newCheckInterval;
    }

    void setTrafficCounter(TrafficCounter newTrafficCounter) {
        trafficCounter = newTrafficCounter;
    }

    public AbstractTrafficShapingHandler(Executor executor, long writeLimit, long readLimit, long checkInterval) {
        init(new DefaultObjectSizeEstimator(), executor, writeLimit, readLimit, checkInterval);
    }

    public AbstractTrafficShapingHandler(ObjectSizeEstimator objectSizeEstimator, Executor executor, long writeLimit, long readLimit, long checkInterval) {
        init(objectSizeEstimator, executor, writeLimit, readLimit, checkInterval);
    }

    public AbstractTrafficShapingHandler(Executor executor, long writeLimit, long readLimit) {
        init(new DefaultObjectSizeEstimator(), executor, writeLimit, readLimit, DEFAULT_CHECK_INTERVAL);
    }

    public AbstractTrafficShapingHandler(ObjectSizeEstimator objectSizeEstimator, Executor executor, long writeLimit, long readLimit) {
        init(objectSizeEstimator, executor, writeLimit, readLimit, DEFAULT_CHECK_INTERVAL);
    }

    public AbstractTrafficShapingHandler(Executor executor) {
        init(new DefaultObjectSizeEstimator(), executor, 0, 0, DEFAULT_CHECK_INTERVAL);
    }

    public AbstractTrafficShapingHandler(ObjectSizeEstimator objectSizeEstimator, Executor executor) {
        init(objectSizeEstimator, executor, 0, 0, DEFAULT_CHECK_INTERVAL);
    }

    public AbstractTrafficShapingHandler(Executor executor, long checkInterval) {
        init(new DefaultObjectSizeEstimator(), executor, 0, 0, checkInterval);
    }

    public AbstractTrafficShapingHandler(ObjectSizeEstimator objectSizeEstimator, Executor executor, long checkInterval) {
        init(objectSizeEstimator, executor, 0, 0, checkInterval);
    }

    public void configure(long newWriteLimit, long newReadLimit, long newCheckInterval) {
        this.configure(newWriteLimit, newReadLimit);
        this.configure(newCheckInterval);
    }

    public void configure(long newWriteLimit, long newReadLimit) {
        writeLimit = newWriteLimit;
        readLimit = newReadLimit;
        if (trafficCounter != null) {
            trafficCounter.resetAccounting(System.currentTimeMillis() + 1);
        }
    }

    public void configure(long newCheckInterval) {
        checkInterval = newCheckInterval;
        if (trafficCounter != null) {
            trafficCounter.configure(checkInterval);
        }
    }

    protected void doAccounting(TrafficCounter counter) {
    }

    private class ReopenRead implements Runnable {

        private ChannelHandlerContext ctx;

        private long timeToWait;

        protected ReopenRead(ChannelHandlerContext ctx, long timeToWait) {
            this.ctx = ctx;
            this.timeToWait = timeToWait;
        }

        @Override
        public void run() {
            try {
                if (release.get()) {
                    return;
                }
                Thread.sleep(timeToWait);
            } catch (InterruptedException e) {
                return;
            }
            if (ctx != null && ctx.getChannel() != null && ctx.getChannel().isConnected()) {
                ctx.setAttachment(null);
                ctx.getChannel().setReadable(true);
            }
        }
    }

    private long getTimeToWait(long limit, long bytes, long lastTime, long curtime) {
        long interval = curtime - lastTime;
        if (interval == 0) {
            return 0;
        }
        return ((bytes * 1000 / limit - interval) / 10) * 10;
    }

    @Override
    public void messageReceived(ChannelHandlerContext arg0, MessageEvent arg1) throws Exception {
        try {
            long curtime = System.currentTimeMillis();
            long size = objectSizeEstimator.estimateSize(arg1.getMessage());
            if (trafficCounter != null) {
                trafficCounter.bytesRecvFlowControl(arg0, size);
                if (readLimit == 0) {
                    return;
                }
                long wait = getTimeToWait(readLimit, trafficCounter.getCurrentReadBytes(), trafficCounter.getLastTime(), curtime);
                if (wait > MINIMAL_WAIT) {
                    Channel channel = arg0.getChannel();
                    if (channel != null && channel.isConnected()) {
                        if (executor == null) {
                            if (release.get()) {
                                return;
                            }
                            Thread.sleep(wait);
                            return;
                        }
                        if (arg0.getAttachment() == null) {
                            arg0.setAttachment(Boolean.TRUE);
                            channel.setReadable(false);
                            executor.execute(new ReopenRead(arg0, wait));
                        } else {
                            if (release.get()) {
                                return;
                            }
                            Thread.sleep(wait);
                        }
                    } else {
                        if (release.get()) {
                            return;
                        }
                        Thread.sleep(wait);
                    }
                }
            }
        } finally {
            super.messageReceived(arg0, arg1);
        }
    }

    @Override
    public void writeRequested(ChannelHandlerContext arg0, MessageEvent arg1) throws Exception {
        try {
            long curtime = System.currentTimeMillis();
            long size = objectSizeEstimator.estimateSize(arg1.getMessage());
            if (trafficCounter != null) {
                trafficCounter.bytesWriteFlowControl(size);
                if (writeLimit == 0) {
                    return;
                }
                long wait = getTimeToWait(writeLimit, trafficCounter.getCurrentWrittenBytes(), trafficCounter.getLastTime(), curtime);
                if (wait > MINIMAL_WAIT) {
                    if (release.get()) {
                        return;
                    }
                    Thread.sleep(wait);
                }
            }
        } finally {
            super.writeRequested(arg0, arg1);
        }
    }

    @Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (e instanceof ChannelStateEvent) {
            ChannelStateEvent cse = (ChannelStateEvent) e;
            if (cse.getState() == ChannelState.INTEREST_OPS && (((Integer) cse.getValue()).intValue() & Channel.OP_READ) != 0) {
                boolean readSuspended = ctx.getAttachment() != null;
                if (readSuspended) {
                    e.getFuture().setSuccess();
                    return;
                }
            }
        }
        super.handleDownstream(ctx, e);
    }

    public TrafficCounter getTrafficCounter() {
        return trafficCounter;
    }

    @Override
    public void releaseExternalResources() {
        if (trafficCounter != null) {
            trafficCounter.stop();
        }
        release.set(true);
        ExecutorUtil.terminate(executor);
    }

    @Override
    public String toString() {
        return "TrafficShaping with Write Limit: " + writeLimit + " Read Limit: " + readLimit + " and Counter: " + (trafficCounter != null ? trafficCounter.toString() : "none");
    }
}