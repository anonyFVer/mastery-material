package io.netty.handler.codec.spdy;

import static io.netty.handler.codec.spdy.SpdyCodecUtil.*;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicInteger;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDownstreamHandler;
import io.netty.channel.ChannelEvent;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelStateEvent;
import io.netty.channel.Channels;
import io.netty.channel.ExceptionEvent;
import io.netty.channel.MessageEvent;
import io.netty.channel.SimpleChannelUpstreamHandler;

public class SpdySessionHandler extends SimpleChannelUpstreamHandler implements ChannelDownstreamHandler {

    private static final SpdyProtocolException PROTOCOL_EXCEPTION = new SpdyProtocolException();

    private final SpdySession spdySession = new SpdySession();

    private volatile int lastGoodStreamID;

    private volatile int remoteConcurrentStreams;

    private volatile int localConcurrentStreams;

    private volatile int maxConcurrentStreams;

    private static final int DEFAULT_WINDOW_SIZE = 64 * 1024;

    private volatile int initialSendWindowSize = DEFAULT_WINDOW_SIZE;

    private volatile int initialReceiveWindowSize = DEFAULT_WINDOW_SIZE;

    private final Object flowControlLock = new Object();

    private final AtomicInteger pings = new AtomicInteger();

    private volatile boolean sentGoAwayFrame;

    private volatile boolean receivedGoAwayFrame;

    private volatile ChannelFuture closeSessionFuture;

    private final boolean server;

    private final boolean flowControl;

    public SpdySessionHandler(int version, boolean server) {
        super();
        if (version < SPDY_MIN_VERSION || version > SPDY_MAX_VERSION) {
            throw new IllegalArgumentException("unsupported version: " + version);
        }
        this.server = server;
        this.flowControl = version >= 3;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (msg instanceof SpdyDataFrame) {
            SpdyDataFrame spdyDataFrame = (SpdyDataFrame) msg;
            int streamID = spdyDataFrame.getStreamID();
            if (!spdySession.isActiveStream(streamID)) {
                if (streamID <= lastGoodStreamID) {
                    issueStreamError(ctx, e.getRemoteAddress(), streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                } else if (!sentGoAwayFrame) {
                    issueStreamError(ctx, e.getRemoteAddress(), streamID, SpdyStreamStatus.INVALID_STREAM);
                }
                return;
            }
            if (spdySession.isRemoteSideClosed(streamID)) {
                issueStreamError(ctx, e.getRemoteAddress(), streamID, SpdyStreamStatus.STREAM_ALREADY_CLOSED);
                return;
            }
            if (!isRemoteInitiatedID(streamID) && !spdySession.hasReceivedReply(streamID)) {
                issueStreamError(ctx, e.getRemoteAddress(), streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                return;
            }
            if (flowControl) {
                int deltaWindowSize = -1 * spdyDataFrame.getData().readableBytes();
                int newWindowSize = spdySession.updateReceiveWindowSize(streamID, deltaWindowSize);
                if (newWindowSize < spdySession.getReceiveWindowSizeLowerBound(streamID)) {
                    issueStreamError(ctx, e.getRemoteAddress(), streamID, SpdyStreamStatus.FLOW_CONTROL_ERROR);
                    return;
                }
                if (newWindowSize < 0) {
                    while (spdyDataFrame.getData().readableBytes() > initialReceiveWindowSize) {
                        SpdyDataFrame partialDataFrame = new DefaultSpdyDataFrame(streamID);
                        partialDataFrame.setData(spdyDataFrame.getData().readSlice(initialReceiveWindowSize));
                        Channels.fireMessageReceived(ctx, partialDataFrame, e.getRemoteAddress());
                    }
                }
                if (newWindowSize <= initialReceiveWindowSize / 2 && !spdyDataFrame.isLast()) {
                    deltaWindowSize = initialReceiveWindowSize - newWindowSize;
                    spdySession.updateReceiveWindowSize(streamID, deltaWindowSize);
                    SpdyWindowUpdateFrame spdyWindowUpdateFrame = new DefaultSpdyWindowUpdateFrame(streamID, deltaWindowSize);
                    Channels.write(ctx, Channels.future(e.getChannel()), spdyWindowUpdateFrame, e.getRemoteAddress());
                }
            }
            if (spdyDataFrame.isLast()) {
                halfCloseStream(streamID, true);
            }
        } else if (msg instanceof SpdySynStreamFrame) {
            SpdySynStreamFrame spdySynStreamFrame = (SpdySynStreamFrame) msg;
            int streamID = spdySynStreamFrame.getStreamID();
            if (spdySynStreamFrame.isInvalid() || !isRemoteInitiatedID(streamID) || spdySession.isActiveStream(streamID)) {
                issueStreamError(ctx, e.getRemoteAddress(), streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                return;
            }
            if (streamID <= lastGoodStreamID) {
                issueSessionError(ctx, e.getChannel(), e.getRemoteAddress(), SpdySessionStatus.PROTOCOL_ERROR);
                return;
            }
            byte priority = spdySynStreamFrame.getPriority();
            boolean remoteSideClosed = spdySynStreamFrame.isLast();
            boolean localSideClosed = spdySynStreamFrame.isUnidirectional();
            if (!acceptStream(streamID, priority, remoteSideClosed, localSideClosed)) {
                issueStreamError(ctx, e.getRemoteAddress(), streamID, SpdyStreamStatus.REFUSED_STREAM);
                return;
            }
        } else if (msg instanceof SpdySynReplyFrame) {
            SpdySynReplyFrame spdySynReplyFrame = (SpdySynReplyFrame) msg;
            int streamID = spdySynReplyFrame.getStreamID();
            if (spdySynReplyFrame.isInvalid() || isRemoteInitiatedID(streamID) || spdySession.isRemoteSideClosed(streamID)) {
                issueStreamError(ctx, e.getRemoteAddress(), streamID, SpdyStreamStatus.INVALID_STREAM);
                return;
            }
            if (spdySession.hasReceivedReply(streamID)) {
                issueStreamError(ctx, e.getRemoteAddress(), streamID, SpdyStreamStatus.STREAM_IN_USE);
                return;
            }
            spdySession.receivedReply(streamID);
            if (spdySynReplyFrame.isLast()) {
                halfCloseStream(streamID, true);
            }
        } else if (msg instanceof SpdyRstStreamFrame) {
            SpdyRstStreamFrame spdyRstStreamFrame = (SpdyRstStreamFrame) msg;
            removeStream(spdyRstStreamFrame.getStreamID());
        } else if (msg instanceof SpdySettingsFrame) {
            SpdySettingsFrame spdySettingsFrame = (SpdySettingsFrame) msg;
            int newConcurrentStreams = spdySettingsFrame.getValue(SpdySettingsFrame.SETTINGS_MAX_CONCURRENT_STREAMS);
            if (newConcurrentStreams >= 0) {
                updateConcurrentStreams(newConcurrentStreams, true);
            }
            if (spdySettingsFrame.isPersisted(SpdySettingsFrame.SETTINGS_INITIAL_WINDOW_SIZE)) {
                spdySettingsFrame.removeValue(SpdySettingsFrame.SETTINGS_INITIAL_WINDOW_SIZE);
            }
            spdySettingsFrame.setPersistValue(SpdySettingsFrame.SETTINGS_INITIAL_WINDOW_SIZE, false);
            if (flowControl) {
                int newInitialWindowSize = spdySettingsFrame.getValue(SpdySettingsFrame.SETTINGS_INITIAL_WINDOW_SIZE);
                if (newInitialWindowSize >= 0) {
                    updateInitialSendWindowSize(newInitialWindowSize);
                }
            }
        } else if (msg instanceof SpdyPingFrame) {
            SpdyPingFrame spdyPingFrame = (SpdyPingFrame) msg;
            if (isRemoteInitiatedID(spdyPingFrame.getID())) {
                Channels.write(ctx, Channels.future(e.getChannel()), spdyPingFrame, e.getRemoteAddress());
                return;
            }
            if (pings.get() == 0) {
                return;
            }
            pings.getAndDecrement();
        } else if (msg instanceof SpdyGoAwayFrame) {
            receivedGoAwayFrame = true;
        } else if (msg instanceof SpdyHeadersFrame) {
            SpdyHeadersFrame spdyHeadersFrame = (SpdyHeadersFrame) msg;
            int streamID = spdyHeadersFrame.getStreamID();
            if (spdyHeadersFrame.isInvalid()) {
                issueStreamError(ctx, e.getRemoteAddress(), streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                return;
            }
            if (spdySession.isRemoteSideClosed(streamID)) {
                issueStreamError(ctx, e.getRemoteAddress(), streamID, SpdyStreamStatus.INVALID_STREAM);
                return;
            }
            if (spdyHeadersFrame.isLast()) {
                halfCloseStream(streamID, true);
            }
        } else if (msg instanceof SpdyWindowUpdateFrame) {
            if (flowControl) {
                SpdyWindowUpdateFrame spdyWindowUpdateFrame = (SpdyWindowUpdateFrame) msg;
                int streamID = spdyWindowUpdateFrame.getStreamID();
                int deltaWindowSize = spdyWindowUpdateFrame.getDeltaWindowSize();
                if (spdySession.isLocalSideClosed(streamID)) {
                    return;
                }
                if (spdySession.getSendWindowSize(streamID) > Integer.MAX_VALUE - deltaWindowSize) {
                    issueStreamError(ctx, e.getRemoteAddress(), streamID, SpdyStreamStatus.FLOW_CONTROL_ERROR);
                    return;
                }
                updateSendWindowSize(ctx, streamID, deltaWindowSize);
            }
            return;
        }
        super.messageReceived(ctx, e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        Throwable cause = e.getCause();
        if (cause instanceof SpdyProtocolException) {
            issueSessionError(ctx, e.getChannel(), null, SpdySessionStatus.PROTOCOL_ERROR);
        }
        super.exceptionCaught(ctx, e);
    }

    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent evt) throws Exception {
        if (evt instanceof ChannelStateEvent) {
            ChannelStateEvent e = (ChannelStateEvent) evt;
            switch(e.getState()) {
                case OPEN:
                case CONNECTED:
                case BOUND:
                    if (Boolean.FALSE.equals(e.getValue()) || e.getValue() == null) {
                        sendGoAwayFrame(ctx, e);
                        return;
                    }
            }
        }
        if (!(evt instanceof MessageEvent)) {
            ctx.sendDownstream(evt);
            return;
        }
        MessageEvent e = (MessageEvent) evt;
        Object msg = e.getMessage();
        if (msg instanceof SpdyDataFrame) {
            SpdyDataFrame spdyDataFrame = (SpdyDataFrame) msg;
            final int streamID = spdyDataFrame.getStreamID();
            if (spdySession.isLocalSideClosed(streamID)) {
                e.getFuture().setFailure(PROTOCOL_EXCEPTION);
                return;
            }
            if (flowControl) {
                synchronized (flowControlLock) {
                    int dataLength = spdyDataFrame.getData().readableBytes();
                    int sendWindowSize = spdySession.getSendWindowSize(streamID);
                    if (sendWindowSize >= dataLength) {
                        spdySession.updateSendWindowSize(streamID, -1 * dataLength);
                        final SocketAddress remoteAddress = e.getRemoteAddress();
                        final ChannelHandlerContext context = ctx;
                        e.getFuture().addListener(new ChannelFutureListener() {

                            public void operationComplete(ChannelFuture future) throws Exception {
                                if (!future.isSuccess()) {
                                    issueStreamError(context, remoteAddress, streamID, SpdyStreamStatus.INTERNAL_ERROR);
                                }
                            }
                        });
                    } else if (sendWindowSize > 0) {
                        spdySession.updateSendWindowSize(streamID, -1 * sendWindowSize);
                        SpdyDataFrame partialDataFrame = new DefaultSpdyDataFrame(streamID);
                        partialDataFrame.setData(spdyDataFrame.getData().readSlice(sendWindowSize));
                        spdySession.putPendingWrite(streamID, e);
                        ChannelFuture writeFuture = Channels.future(e.getChannel());
                        final SocketAddress remoteAddress = e.getRemoteAddress();
                        final ChannelHandlerContext context = ctx;
                        e.getFuture().addListener(new ChannelFutureListener() {

                            public void operationComplete(ChannelFuture future) throws Exception {
                                if (!future.isSuccess()) {
                                    issueStreamError(context, remoteAddress, streamID, SpdyStreamStatus.INTERNAL_ERROR);
                                }
                            }
                        });
                        Channels.write(ctx, writeFuture, partialDataFrame, remoteAddress);
                        return;
                    } else {
                        spdySession.putPendingWrite(streamID, e);
                        return;
                    }
                }
            }
            if (spdyDataFrame.isLast()) {
                halfCloseStream(streamID, false);
            }
        } else if (msg instanceof SpdySynStreamFrame) {
            SpdySynStreamFrame spdySynStreamFrame = (SpdySynStreamFrame) msg;
            int streamID = spdySynStreamFrame.getStreamID();
            if (isRemoteInitiatedID(streamID)) {
                e.getFuture().setFailure(PROTOCOL_EXCEPTION);
                return;
            }
            byte priority = spdySynStreamFrame.getPriority();
            boolean remoteSideClosed = spdySynStreamFrame.isUnidirectional();
            boolean localSideClosed = spdySynStreamFrame.isLast();
            if (!acceptStream(streamID, priority, remoteSideClosed, localSideClosed)) {
                e.getFuture().setFailure(PROTOCOL_EXCEPTION);
                return;
            }
        } else if (msg instanceof SpdySynReplyFrame) {
            SpdySynReplyFrame spdySynReplyFrame = (SpdySynReplyFrame) msg;
            int streamID = spdySynReplyFrame.getStreamID();
            if (!isRemoteInitiatedID(streamID) || spdySession.isLocalSideClosed(streamID)) {
                e.getFuture().setFailure(PROTOCOL_EXCEPTION);
                return;
            }
            if (spdySynReplyFrame.isLast()) {
                halfCloseStream(streamID, false);
            }
        } else if (msg instanceof SpdyRstStreamFrame) {
            SpdyRstStreamFrame spdyRstStreamFrame = (SpdyRstStreamFrame) msg;
            removeStream(spdyRstStreamFrame.getStreamID());
        } else if (msg instanceof SpdySettingsFrame) {
            SpdySettingsFrame spdySettingsFrame = (SpdySettingsFrame) msg;
            int newConcurrentStreams = spdySettingsFrame.getValue(SpdySettingsFrame.SETTINGS_MAX_CONCURRENT_STREAMS);
            if (newConcurrentStreams >= 0) {
                updateConcurrentStreams(newConcurrentStreams, false);
            }
            if (spdySettingsFrame.isPersisted(SpdySettingsFrame.SETTINGS_INITIAL_WINDOW_SIZE)) {
                spdySettingsFrame.removeValue(SpdySettingsFrame.SETTINGS_INITIAL_WINDOW_SIZE);
            }
            spdySettingsFrame.setPersistValue(SpdySettingsFrame.SETTINGS_INITIAL_WINDOW_SIZE, false);
            if (flowControl) {
                int newInitialWindowSize = spdySettingsFrame.getValue(SpdySettingsFrame.SETTINGS_INITIAL_WINDOW_SIZE);
                if (newInitialWindowSize >= 0) {
                    updateInitialReceiveWindowSize(newInitialWindowSize);
                }
            }
        } else if (msg instanceof SpdyPingFrame) {
            SpdyPingFrame spdyPingFrame = (SpdyPingFrame) msg;
            if (isRemoteInitiatedID(spdyPingFrame.getID())) {
                e.getFuture().setFailure(new IllegalArgumentException("invalid PING ID: " + spdyPingFrame.getID()));
                return;
            }
            pings.getAndIncrement();
        } else if (msg instanceof SpdyGoAwayFrame) {
            e.getFuture().setFailure(PROTOCOL_EXCEPTION);
            return;
        } else if (msg instanceof SpdyHeadersFrame) {
            SpdyHeadersFrame spdyHeadersFrame = (SpdyHeadersFrame) msg;
            int streamID = spdyHeadersFrame.getStreamID();
            if (spdySession.isLocalSideClosed(streamID)) {
                e.getFuture().setFailure(PROTOCOL_EXCEPTION);
                return;
            }
            if (spdyHeadersFrame.isLast()) {
                halfCloseStream(streamID, false);
            }
        } else if (msg instanceof SpdyWindowUpdateFrame) {
            e.getFuture().setFailure(PROTOCOL_EXCEPTION);
            return;
        }
        ctx.sendDownstream(evt);
    }

    private void issueSessionError(ChannelHandlerContext ctx, Channel channel, SocketAddress remoteAddress, SpdySessionStatus status) {
        ChannelFuture future = sendGoAwayFrame(ctx, channel, remoteAddress, status);
        future.addListener(ChannelFutureListener.CLOSE);
    }

    private void issueStreamError(ChannelHandlerContext ctx, SocketAddress remoteAddress, int streamID, SpdyStreamStatus status) {
        boolean fireMessageReceived = !spdySession.isRemoteSideClosed(streamID);
        removeStream(streamID);
        SpdyRstStreamFrame spdyRstStreamFrame = new DefaultSpdyRstStreamFrame(streamID, status);
        Channels.write(ctx, Channels.future(ctx.getChannel()), spdyRstStreamFrame, remoteAddress);
        if (fireMessageReceived) {
            Channels.fireMessageReceived(ctx, spdyRstStreamFrame, remoteAddress);
        }
    }

    private boolean isRemoteInitiatedID(int ID) {
        boolean serverID = SpdyCodecUtil.isServerID(ID);
        return server && !serverID || !server && serverID;
    }

    private void updateConcurrentStreams(int newConcurrentStreams, boolean remote) {
        if (remote) {
            remoteConcurrentStreams = newConcurrentStreams;
        } else {
            localConcurrentStreams = newConcurrentStreams;
        }
        if (localConcurrentStreams == remoteConcurrentStreams) {
            maxConcurrentStreams = localConcurrentStreams;
            return;
        }
        if (localConcurrentStreams == 0) {
            maxConcurrentStreams = remoteConcurrentStreams;
            return;
        }
        if (remoteConcurrentStreams == 0) {
            maxConcurrentStreams = localConcurrentStreams;
            return;
        }
        if (localConcurrentStreams > remoteConcurrentStreams) {
            maxConcurrentStreams = remoteConcurrentStreams;
        } else {
            maxConcurrentStreams = localConcurrentStreams;
        }
    }

    private synchronized void updateInitialSendWindowSize(int newInitialWindowSize) {
        int deltaWindowSize = newInitialWindowSize - initialSendWindowSize;
        initialSendWindowSize = newInitialWindowSize;
        for (Integer StreamID : spdySession.getActiveStreams()) {
            spdySession.updateSendWindowSize(StreamID.intValue(), deltaWindowSize);
        }
    }

    private synchronized void updateInitialReceiveWindowSize(int newInitialWindowSize) {
        int deltaWindowSize = newInitialWindowSize - initialReceiveWindowSize;
        initialReceiveWindowSize = newInitialWindowSize;
        spdySession.updateAllReceiveWindowSizes(deltaWindowSize);
    }

    private synchronized boolean acceptStream(int streamID, byte priority, boolean remoteSideClosed, boolean localSideClosed) {
        if (receivedGoAwayFrame || sentGoAwayFrame) {
            return false;
        }
        int maxConcurrentStreams = this.maxConcurrentStreams;
        if (maxConcurrentStreams != 0 && spdySession.numActiveStreams() >= maxConcurrentStreams) {
            return false;
        }
        spdySession.acceptStream(streamID, priority, remoteSideClosed, localSideClosed, initialSendWindowSize, initialReceiveWindowSize);
        if (isRemoteInitiatedID(streamID)) {
            lastGoodStreamID = streamID;
        }
        return true;
    }

    private void halfCloseStream(int streamID, boolean remote) {
        if (remote) {
            spdySession.closeRemoteSide(streamID);
        } else {
            spdySession.closeLocalSide(streamID);
        }
        if (closeSessionFuture != null && spdySession.noActiveStreams()) {
            closeSessionFuture.setSuccess();
        }
    }

    private void removeStream(int streamID) {
        spdySession.removeStream(streamID);
        if (closeSessionFuture != null && spdySession.noActiveStreams()) {
            closeSessionFuture.setSuccess();
        }
    }

    private void updateSendWindowSize(ChannelHandlerContext ctx, final int streamID, int deltaWindowSize) {
        synchronized (flowControlLock) {
            int newWindowSize = spdySession.updateSendWindowSize(streamID, deltaWindowSize);
            while (newWindowSize > 0) {
                MessageEvent e = spdySession.getPendingWrite(streamID);
                if (e == null) {
                    break;
                }
                SpdyDataFrame spdyDataFrame = (SpdyDataFrame) e.getMessage();
                int dataFrameSize = spdyDataFrame.getData().readableBytes();
                if (newWindowSize >= dataFrameSize) {
                    spdySession.removePendingWrite(streamID);
                    newWindowSize = spdySession.updateSendWindowSize(streamID, -1 * dataFrameSize);
                    final SocketAddress remoteAddress = e.getRemoteAddress();
                    final ChannelHandlerContext context = ctx;
                    e.getFuture().addListener(new ChannelFutureListener() {

                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (!future.isSuccess()) {
                                issueStreamError(context, remoteAddress, streamID, SpdyStreamStatus.INTERNAL_ERROR);
                            }
                        }
                    });
                    if (spdyDataFrame.isLast()) {
                        halfCloseStream(streamID, false);
                    }
                    Channels.write(ctx, e.getFuture(), spdyDataFrame, e.getRemoteAddress());
                } else {
                    spdySession.updateSendWindowSize(streamID, -1 * newWindowSize);
                    SpdyDataFrame partialDataFrame = new DefaultSpdyDataFrame(streamID);
                    partialDataFrame.setData(spdyDataFrame.getData().readSlice(newWindowSize));
                    ChannelFuture writeFuture = Channels.future(e.getChannel());
                    final SocketAddress remoteAddress = e.getRemoteAddress();
                    final ChannelHandlerContext context = ctx;
                    e.getFuture().addListener(new ChannelFutureListener() {

                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (!future.isSuccess()) {
                                issueStreamError(context, remoteAddress, streamID, SpdyStreamStatus.INTERNAL_ERROR);
                            }
                        }
                    });
                    Channels.write(ctx, writeFuture, partialDataFrame, remoteAddress);
                    newWindowSize = 0;
                }
            }
        }
    }

    private void sendGoAwayFrame(ChannelHandlerContext ctx, ChannelStateEvent e) {
        if (!e.getChannel().isConnected()) {
            ctx.sendDownstream(e);
            return;
        }
        ChannelFuture future = sendGoAwayFrame(ctx, e.getChannel(), null, SpdySessionStatus.OK);
        if (spdySession.noActiveStreams()) {
            future.addListener(new ClosingChannelFutureListener(ctx, e));
        } else {
            closeSessionFuture = Channels.future(e.getChannel());
            closeSessionFuture.addListener(new ClosingChannelFutureListener(ctx, e));
        }
    }

    private synchronized ChannelFuture sendGoAwayFrame(ChannelHandlerContext ctx, Channel channel, SocketAddress remoteAddress, SpdySessionStatus status) {
        if (!sentGoAwayFrame) {
            sentGoAwayFrame = true;
            SpdyGoAwayFrame spdyGoAwayFrame = new DefaultSpdyGoAwayFrame(lastGoodStreamID, status);
            ChannelFuture future = Channels.future(channel);
            Channels.write(ctx, future, spdyGoAwayFrame, remoteAddress);
            return future;
        }
        return Channels.succeededFuture(channel);
    }

    private static final class ClosingChannelFutureListener implements ChannelFutureListener {

        private final ChannelHandlerContext ctx;

        private final ChannelStateEvent e;

        ClosingChannelFutureListener(ChannelHandlerContext ctx, ChannelStateEvent e) {
            this.ctx = ctx;
            this.e = e;
        }

        public void operationComplete(ChannelFuture sentGoAwayFuture) throws Exception {
            if (!(sentGoAwayFuture.getCause() instanceof ClosedChannelException)) {
                Channels.close(ctx, e.getFuture());
            } else {
                e.getFuture().setSuccess();
            }
        }
    }
}