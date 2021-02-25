package io.netty.handler.codec.spdy;

import io.netty.channel.ChannelBufferHolder;
import io.netty.channel.ChannelBufferHolders;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerContext;
import io.netty.channel.ChannelOutboundHandlerContext;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public class SpdySessionHandler extends ChannelHandlerAdapter<Object, Object> {

    private static final SpdyProtocolException PROTOCOL_EXCEPTION = new SpdyProtocolException();

    private final SpdySession spdySession = new SpdySession();

    private volatile int lastGoodStreamID;

    private volatile int remoteConcurrentStreams;

    private volatile int localConcurrentStreams;

    private volatile int maxConcurrentStreams;

    private final AtomicInteger pings = new AtomicInteger();

    private volatile boolean sentGoAwayFrame;

    private volatile boolean receivedGoAwayFrame;

    private volatile ChannelFuture closeSessionFuture;

    private final boolean server;

    public SpdySessionHandler(boolean server) {
        super();
        this.server = server;
    }

    @Override
    public ChannelBufferHolder<Object> newOutboundBuffer(ChannelOutboundHandlerContext<Object> ctx) throws Exception {
        return ChannelBufferHolders.messageBuffer();
    }

    @Override
    public ChannelBufferHolder<Object> newInboundBuffer(ChannelInboundHandlerContext<Object> ctx) throws Exception {
        return ChannelBufferHolders.messageBuffer();
    }

    @Override
    public void inboundBufferUpdated(ChannelInboundHandlerContext<Object> ctx) throws Exception {
        Queue<Object> in = ctx.in().messageBuffer();
        for (; ; ) {
            Object msg = in.poll();
            if (msg == null) {
                break;
            }
            handleInboundMessage(ctx, msg);
        }
        ctx.fireInboundBufferUpdated();
    }

    private void handleInboundMessage(ChannelInboundHandlerContext<Object> ctx, Object msg) throws Exception {
        if (msg instanceof SpdyDataFrame) {
            SpdyDataFrame spdyDataFrame = (SpdyDataFrame) msg;
            int streamID = spdyDataFrame.getStreamID();
            if (spdySession.isRemoteSideClosed(streamID)) {
                if (!sentGoAwayFrame) {
                    issueStreamError(ctx, streamID, SpdyStreamStatus.INVALID_STREAM);
                }
                return;
            }
            if (!isRemoteInitiatedID(streamID) && !spdySession.hasReceivedReply(streamID)) {
                issueStreamError(ctx, streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                return;
            }
            if (spdyDataFrame.isLast()) {
                halfCloseStream(streamID, true);
            }
        } else if (msg instanceof SpdySynStreamFrame) {
            SpdySynStreamFrame spdySynStreamFrame = (SpdySynStreamFrame) msg;
            int streamID = spdySynStreamFrame.getStreamID();
            if (spdySynStreamFrame.isInvalid() || !isRemoteInitiatedID(streamID) || spdySession.isActiveStream(streamID)) {
                issueStreamError(ctx, streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                return;
            }
            if (streamID < lastGoodStreamID) {
                issueSessionError(ctx);
                return;
            }
            boolean remoteSideClosed = spdySynStreamFrame.isLast();
            boolean localSideClosed = spdySynStreamFrame.isUnidirectional();
            if (!acceptStream(streamID, remoteSideClosed, localSideClosed)) {
                issueStreamError(ctx, streamID, SpdyStreamStatus.REFUSED_STREAM);
                return;
            }
        } else if (msg instanceof SpdySynReplyFrame) {
            SpdySynReplyFrame spdySynReplyFrame = (SpdySynReplyFrame) msg;
            int streamID = spdySynReplyFrame.getStreamID();
            if (spdySynReplyFrame.isInvalid() || isRemoteInitiatedID(streamID) || spdySession.isRemoteSideClosed(streamID)) {
                issueStreamError(ctx, streamID, SpdyStreamStatus.INVALID_STREAM);
                return;
            }
            if (spdySession.hasReceivedReply(streamID)) {
                issueStreamError(ctx, streamID, SpdyStreamStatus.PROTOCOL_ERROR);
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
            updateConcurrentStreams(spdySettingsFrame, true);
        } else if (msg instanceof SpdyPingFrame) {
            SpdyPingFrame spdyPingFrame = (SpdyPingFrame) msg;
            if (isRemoteInitiatedID(spdyPingFrame.getID())) {
                ctx.write(spdyPingFrame);
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
                issueStreamError(ctx, streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                return;
            }
            if (spdySession.isRemoteSideClosed(streamID)) {
                issueStreamError(ctx, streamID, SpdyStreamStatus.INVALID_STREAM);
                return;
            }
        }
        ctx.nextIn().messageBuffer().add(msg);
    }

    @Override
    public void disconnect(final ChannelOutboundHandlerContext<Object> ctx, final ChannelFuture future) throws Exception {
        sendGoAwayFrame(ctx).addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture f) throws Exception {
                ctx.disconnect(future);
            }
        });
    }

    @Override
    public void close(final ChannelOutboundHandlerContext<Object> ctx, final ChannelFuture future) throws Exception {
        sendGoAwayFrame(ctx).addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture f) throws Exception {
                ctx.close(future);
            }
        });
    }

    @Override
    public void flush(ChannelOutboundHandlerContext<Object> ctx, ChannelFuture future) throws Exception {
        Queue<Object> in = ctx.prevOut().messageBuffer();
        for (; ; ) {
            Object msg = in.poll();
            if (msg == null) {
                break;
            }
            handleOutboundMessage(ctx, msg);
        }
        ctx.flush(future);
    }

    private void handleOutboundMessage(ChannelOutboundHandlerContext<Object> ctx, Object msg) throws Exception {
        if (msg instanceof SpdyDataFrame) {
            SpdyDataFrame spdyDataFrame = (SpdyDataFrame) msg;
            int streamID = spdyDataFrame.getStreamID();
            if (spdySession.isLocalSideClosed(streamID)) {
                issueStreamError(ctx, streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                return;
            }
            if (spdyDataFrame.isLast()) {
                halfCloseStream(streamID, false);
            }
        } else if (msg instanceof SpdySynStreamFrame) {
            SpdySynStreamFrame spdySynStreamFrame = (SpdySynStreamFrame) msg;
            int streamID = spdySynStreamFrame.getStreamID();
            boolean remoteSideClosed = spdySynStreamFrame.isUnidirectional();
            boolean localSideClosed = spdySynStreamFrame.isLast();
            if (!acceptStream(streamID, remoteSideClosed, localSideClosed)) {
                issueStreamError(ctx, streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                return;
            }
        } else if (msg instanceof SpdySynReplyFrame) {
            SpdySynReplyFrame spdySynReplyFrame = (SpdySynReplyFrame) msg;
            int streamID = spdySynReplyFrame.getStreamID();
            if (!isRemoteInitiatedID(streamID) || spdySession.isLocalSideClosed(streamID)) {
                ctx.fireExceptionCaught(PROTOCOL_EXCEPTION);
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
            updateConcurrentStreams(spdySettingsFrame, false);
        } else if (msg instanceof SpdyPingFrame) {
            SpdyPingFrame spdyPingFrame = (SpdyPingFrame) msg;
            if (isRemoteInitiatedID(spdyPingFrame.getID())) {
                ctx.fireExceptionCaught(new IllegalArgumentException("invalid PING ID: " + spdyPingFrame.getID()));
                return;
            }
            pings.getAndIncrement();
        } else if (msg instanceof SpdyGoAwayFrame) {
            ctx.fireExceptionCaught(PROTOCOL_EXCEPTION);
            return;
        } else if (msg instanceof SpdyHeadersFrame) {
            SpdyHeadersFrame spdyHeadersFrame = (SpdyHeadersFrame) msg;
            int streamID = spdyHeadersFrame.getStreamID();
            if (spdySession.isLocalSideClosed(streamID)) {
                ctx.fireExceptionCaught(PROTOCOL_EXCEPTION);
                return;
            }
        }
        ctx.out().messageBuffer().add(msg);
    }

    private void issueSessionError(ChannelHandlerContext ctx) {
        sendGoAwayFrame(ctx).addListener(ChannelFutureListener.CLOSE);
    }

    private void issueStreamError(ChannelHandlerContext ctx, int streamID, SpdyStreamStatus status) {
        removeStream(streamID);
        SpdyRstStreamFrame spdyRstStreamFrame = new DefaultSpdyRstStreamFrame(streamID, status);
        ctx.write(spdyRstStreamFrame);
    }

    private boolean isRemoteInitiatedID(int ID) {
        boolean serverID = SpdyCodecUtil.isServerID(ID);
        return server && !serverID || !server && serverID;
    }

    private synchronized void updateConcurrentStreams(SpdySettingsFrame settings, boolean remote) {
        int newConcurrentStreams = settings.getValue(SpdySettingsFrame.SETTINGS_MAX_CONCURRENT_STREAMS);
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

    private synchronized boolean acceptStream(int streamID, boolean remoteSideClosed, boolean localSideClosed) {
        if (receivedGoAwayFrame || sentGoAwayFrame) {
            return false;
        }
        if (maxConcurrentStreams != 0 && spdySession.numActiveStreams() >= maxConcurrentStreams) {
            return false;
        }
        spdySession.acceptStream(streamID, remoteSideClosed, localSideClosed);
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

    private synchronized ChannelFuture sendGoAwayFrame(ChannelHandlerContext ctx) {
        if (!sentGoAwayFrame) {
            sentGoAwayFrame = true;
            return ctx.write(new DefaultSpdyGoAwayFrame(lastGoodStreamID));
        }
        return ctx.newSucceededFuture();
    }
}