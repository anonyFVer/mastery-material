package io.netty.handler.codec.spdy;

import io.netty.channel.CombinedChannelHandler;

public class SpdyHttpCodec extends CombinedChannelHandler {

    public SpdyHttpCodec(int maxContentLength) {
        super(new SpdyHttpDecoder(maxContentLength), new SpdyHttpEncoder());
    }
}