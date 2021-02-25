package io.netty.handler.codec.spdy;

import io.netty.channel.ChannelDownstreamHandler;
import io.netty.channel.ChannelEvent;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelUpstreamHandler;

public class SpdyFrameCodec implements ChannelUpstreamHandler, ChannelDownstreamHandler {

    private final SpdyFrameDecoder decoder;

    private final SpdyFrameEncoder encoder;

    public SpdyFrameCodec() {
        this(8192, 65536, 16384, 6, 15, 8);
    }

    public SpdyFrameCodec(int maxChunkSize, int maxFrameSize, int maxHeaderSize, int compressionLevel, int windowBits, int memLevel) {
        decoder = new SpdyFrameDecoder(maxChunkSize, maxFrameSize, maxHeaderSize);
        encoder = new SpdyFrameEncoder(compressionLevel, windowBits, memLevel);
    }

    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        decoder.handleUpstream(ctx, e);
    }

    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        encoder.handleDownstream(ctx, e);
    }
}