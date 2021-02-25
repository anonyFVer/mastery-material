package io.netty.handler.codec.spdy;

import io.netty.channel.CombinedChannelHandler;

public class SpdyFrameCodec extends CombinedChannelHandler {

    public SpdyFrameCodec() {
        this(8192, 65536, 16384, 6, 15, 8);
    }

    public SpdyFrameCodec(int maxChunkSize, int maxFrameSize, int maxHeaderSize, int compressionLevel, int windowBits, int memLevel) {
        super(new SpdyFrameDecoder(maxChunkSize, maxFrameSize, maxHeaderSize), new SpdyFrameEncoder(compressionLevel, windowBits, memLevel));
    }
}