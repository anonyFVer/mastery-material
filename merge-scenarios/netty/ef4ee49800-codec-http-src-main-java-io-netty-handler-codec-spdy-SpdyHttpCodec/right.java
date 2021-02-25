package io.netty.handler.codec.spdy;

import io.netty.channel.ChannelDownstreamHandler;
import io.netty.channel.ChannelEvent;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelUpstreamHandler;

public class SpdyHttpCodec implements ChannelUpstreamHandler, ChannelDownstreamHandler {

    private final SpdyHttpDecoder decoder;

    private final SpdyHttpEncoder encoder;

    public SpdyHttpCodec(int version, int maxContentLength) {
        decoder = new SpdyHttpDecoder(version, maxContentLength);
        encoder = new SpdyHttpEncoder(version);
    }

    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        decoder.handleUpstream(ctx, e);
    }

    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        encoder.handleDownstream(ctx, e);
    }
}