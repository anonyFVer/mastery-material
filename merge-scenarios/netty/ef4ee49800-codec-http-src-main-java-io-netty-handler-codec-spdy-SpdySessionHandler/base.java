package io.netty.handler.codec.spdy;

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
import io.netty.channel.MessageEvent;
import io.netty.channel.SimpleChannelUpstreamHandler;

public class SpdySessionHandler extends SimpleChannelUpstreamHandler implements ChannelDownstreamHandler {

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
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (msg instanceof SpdyDataFrame) {
            SpdyDataFrame spdyDataFrame = (SpdyDataFrame) msg;
            int streamID = spdyDataFrame.getStreamID();
            if (spdySession.isRemoteSideClosed(streamID)) {
                if (!sentGoAwayFrame) {
                    issueStreamError(ctx, e, streamID, SpdyStreamStatus.INVALID_STREAM);
                }
                return;
            }
            if (!isRemoteInitiatedID(streamID) && !spdySession.hasReceivedReply(streamID)) {
                issueStreamError(ctx, e, streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                return;
            }
            if (spdyDataFrame.isLast()) {
                halfCloseStream(streamID, true);
            }
        } else if (msg instanceof SpdySynStreamFrame) {
            SpdySynStreamFrame spdySynStreamFrame = (SpdySynStreamFrame) msg;
            int streamID = spdySynStreamFrame.getStreamID();
            if (spdySynStreamFrame.isInvalid() || !isRemoteInitiatedID(streamID) || spdySession.isActiveStream(streamID)) {
                issueStreamError(ctx, e, streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                return;
            }
            if (streamID < lastGoodStreamID) {
                issueSessionError(ctx, e.getChannel(), e.getRemoteAddress());
                return;
            }
            boolean remoteSideClosed = spdySynStreamFrame.isLast();
            boolean localSideClosed = spdySynStreamFrame.isUnidirectional();
            if (!acceptStream(streamID, remoteSideClosed, localSideClosed)) {
                issueStreamError(ctx, e, streamID, SpdyStreamStatus.REFUSED_STREAM);
                return;
            }
        } else if (msg instanceof SpdySynReplyFrame) {
            SpdySynReplyFrame spdySynReplyFrame = (SpdySynReplyFrame) msg;
            int streamID = spdySynReplyFrame.getStreamID();
            if (spdySynReplyFrame.isInvalid() || isRemoteInitiatedID(streamID) || spdySession.isRemoteSideClosed(streamID)) {
                issueStreamError(ctx, e, streamID, SpdyStreamStatus.INVALID_STREAM);
                return;
            }
            if (spdySession.hasReceivedReply(streamID)) {
                issueStreamError(ctx, e, streamID, SpdyStreamStatus.PROTOCOL_ERROR);
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
                issueStreamError(ctx, e, streamID, SpdyStreamStatus.PROTOCOL_ERROR);
                return;
            }
            if (spdySession.isRemoteSideClosed(streamID)) {
                issueStreamError(ctx, e, streamID, SpdyStreamStatus.INVALID_STREAM);
                return;
            }
        }
        super.messageReceived(ctx, e);
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
            int streamID = spdyDataFrame.getStreamID();
            if (spdySession.isLocalSideClosed(streamID)) {
                e.getFuture().setFailure(PROTOCOL_EXCEPTION);
                return;
            }
            if (spdyDataFrame.isLast()) {
                halfCloseStream(streamID, false);
            }
        } else if (msg instanceof SpdySynStreamFrame) {
            SpdySynStreamFrame spdySynStreamFrame = (SpdySynStreamFrame) msg;
            boolean remoteSideClosed = spdySynStreamFrame.isUnidirectional();
            boolean localSideClosed = spdySynStreamFrame.isLast();
            if (!acceptStream(spdySynStreamFrame.getStreamID(), remoteSideClosed, localSideClosed)) {
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
            updateConcurrentStreams(spdySettingsFrame, false);
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
        }
        ctx.sendDownstream(evt);
    }

    private void issueSessionError(ChannelHandlerContext ctx, Channel channel, SocketAddress remoteAddress) {
        ChannelFuture future = sendGoAwayFrame(ctx, channel, remoteAddress);
        future.addListener(ChannelFutureListener.CLOSE);
    }

    private void issueStreamError(ChannelHandlerContext ctx, MessageEvent e, int streamID, SpdyStreamStatus status) {
        removeStream(streamID);
        SpdyRstStreamFrame spdyRstStreamFrame = new DefaultSpdyRstStreamFrame(streamID, status);
        Channels.write(ctx, Channels.future(e.getChannel()), spdyRstStreamFrame, e.getRemoteAddress());
    }

    private boolean isRemoteInitiatedID(int ID) {
        boolean serverID = SpdyCodecUtil.isServerID(ID);
        return (server && !serverID) || (!server && serverID);
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
        if ((maxConcurrentStreams != 0) && (spdySession.numActiveStreams() >= maxConcurrentStreams)) {
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
        if ((closeSessionFuture != null) && spdySession.noActiveStreams()) {
            closeSessionFuture.setSuccess();
        }
    }

    private void removeStream(int streamID) {
        spdySession.removeStream(streamID);
        if ((closeSessionFuture != null) && spdySession.noActiveStreams()) {
            closeSessionFuture.setSuccess();
        }
    }

    private void sendGoAwayFrame(ChannelHandlerContext ctx, ChannelStateEvent e) {
        if (!e.getChannel().isConnected()) {
            ctx.sendDownstream(e);
            return;
        }
        ChannelFuture future = sendGoAwayFrame(ctx, e.getChannel(), null);
        if (spdySession.noActiveStreams()) {
            future.addListener(new ClosingChannelFutureListener(ctx, e));
        } else {
            closeSessionFuture = Channels.future(e.getChannel());
            closeSessionFuture.addListener(new ClosingChannelFutureListener(ctx, e));
        }
    }

    private synchronized ChannelFuture sendGoAwayFrame(ChannelHandlerContext ctx, Channel channel, SocketAddress remoteAddress) {
        if (!sentGoAwayFrame) {
            sentGoAwayFrame = true;
            ChannelFuture future = Channels.future(channel);
            Channels.write(ctx, future, new DefaultSpdyGoAwayFrame(lastGoodStreamID));
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