package io.netty.handler.codec.spdy;

import static io.netty.handler.codec.spdy.SpdyCodecUtil.*;
import java.util.List;
import java.util.Map;
import io.netty.channel.ChannelDownstreamHandler;
import io.netty.channel.ChannelEvent;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.Channels;
import io.netty.channel.MessageEvent;
import io.netty.handler.codec.http.HttpChunk;
import io.netty.handler.codec.http.HttpChunkTrailer;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;

public class SpdyHttpEncoder implements ChannelDownstreamHandler {

    private final int spdyVersion;

    private volatile int currentStreamID;

    public SpdyHttpEncoder(int version) {
        if (version < SPDY_MIN_VERSION || version > SPDY_MAX_VERSION) {
            throw new IllegalArgumentException("unsupported version: " + version);
        }
        spdyVersion = version;
    }

    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent evt) throws Exception {
        if (!(evt instanceof MessageEvent)) {
            ctx.sendDownstream(evt);
            return;
        }
        MessageEvent e = (MessageEvent) evt;
        Object msg = e.getMessage();
        if (msg instanceof HttpRequest) {
            HttpRequest httpRequest = (HttpRequest) msg;
            SpdySynStreamFrame spdySynStreamFrame = createSynStreamFrame(httpRequest);
            int streamID = spdySynStreamFrame.getStreamID();
            ChannelFuture future = getContentFuture(ctx, e, streamID, httpRequest);
            Channels.write(ctx, future, spdySynStreamFrame, e.getRemoteAddress());
        } else if (msg instanceof HttpResponse) {
            HttpResponse httpResponse = (HttpResponse) msg;
            if (httpResponse.containsHeader(SpdyHttpHeaders.Names.ASSOCIATED_TO_STREAM_ID)) {
                SpdySynStreamFrame spdySynStreamFrame = createSynStreamFrame(httpResponse);
                int streamID = spdySynStreamFrame.getStreamID();
                ChannelFuture future = getContentFuture(ctx, e, streamID, httpResponse);
                Channels.write(ctx, future, spdySynStreamFrame, e.getRemoteAddress());
            } else {
                SpdySynReplyFrame spdySynReplyFrame = createSynReplyFrame(httpResponse);
                int streamID = spdySynReplyFrame.getStreamID();
                ChannelFuture future = getContentFuture(ctx, e, streamID, httpResponse);
                Channels.write(ctx, future, spdySynReplyFrame, e.getRemoteAddress());
            }
        } else if (msg instanceof HttpChunk) {
            HttpChunk chunk = (HttpChunk) msg;
            SpdyDataFrame spdyDataFrame = new DefaultSpdyDataFrame(currentStreamID);
            spdyDataFrame.setData(chunk.getContent());
            spdyDataFrame.setLast(chunk.isLast());
            if (chunk instanceof HttpChunkTrailer) {
                HttpChunkTrailer trailer = (HttpChunkTrailer) chunk;
                List<Map.Entry<String, String>> trailers = trailer.getHeaders();
                if (trailers.isEmpty()) {
                    Channels.write(ctx, e.getFuture(), spdyDataFrame, e.getRemoteAddress());
                } else {
                    SpdyHeadersFrame spdyHeadersFrame = new DefaultSpdyHeadersFrame(currentStreamID);
                    for (Map.Entry<String, String> entry : trailers) {
                        spdyHeadersFrame.addHeader(entry.getKey(), entry.getValue());
                    }
                    ChannelFuture future = Channels.future(e.getChannel());
                    future.addListener(new SpdyFrameWriter(ctx, e, spdyDataFrame));
                    Channels.write(ctx, future, spdyHeadersFrame, e.getRemoteAddress());
                }
            } else {
                Channels.write(ctx, e.getFuture(), spdyDataFrame, e.getRemoteAddress());
            }
        } else {
            ctx.sendDownstream(evt);
        }
    }

    private ChannelFuture getContentFuture(ChannelHandlerContext ctx, MessageEvent e, int streamID, HttpMessage httpMessage) {
        if (httpMessage.getContent().readableBytes() == 0) {
            return e.getFuture();
        }
        SpdyDataFrame spdyDataFrame = new DefaultSpdyDataFrame(streamID);
        spdyDataFrame.setData(httpMessage.getContent());
        spdyDataFrame.setLast(true);
        ChannelFuture future = Channels.future(e.getChannel());
        future.addListener(new SpdyFrameWriter(ctx, e, spdyDataFrame));
        return future;
    }

    private class SpdyFrameWriter implements ChannelFutureListener {

        private final ChannelHandlerContext ctx;

        private final MessageEvent e;

        private final Object spdyFrame;

        SpdyFrameWriter(ChannelHandlerContext ctx, MessageEvent e, Object spdyFrame) {
            this.ctx = ctx;
            this.e = e;
            this.spdyFrame = spdyFrame;
        }

        public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
                Channels.write(ctx, e.getFuture(), spdyFrame, e.getRemoteAddress());
            } else if (future.isCancelled()) {
                e.getFuture().cancel();
            } else {
                e.getFuture().setFailure(future.getCause());
            }
        }
    }

    private SpdySynStreamFrame createSynStreamFrame(HttpMessage httpMessage) throws Exception {
        boolean chunked = httpMessage.isChunked();
        int streamID = SpdyHttpHeaders.getStreamID(httpMessage);
        int associatedToStreamID = SpdyHttpHeaders.getAssociatedToStreamID(httpMessage);
        byte priority = SpdyHttpHeaders.getPriority(httpMessage);
        String URL = SpdyHttpHeaders.getUrl(httpMessage);
        String scheme = SpdyHttpHeaders.getScheme(httpMessage);
        SpdyHttpHeaders.removeStreamID(httpMessage);
        SpdyHttpHeaders.removeAssociatedToStreamID(httpMessage);
        SpdyHttpHeaders.removePriority(httpMessage);
        SpdyHttpHeaders.removeUrl(httpMessage);
        SpdyHttpHeaders.removeScheme(httpMessage);
        httpMessage.removeHeader(HttpHeaders.Names.CONNECTION);
        httpMessage.removeHeader("Keep-Alive");
        httpMessage.removeHeader("Proxy-Connection");
        httpMessage.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING);
        SpdySynStreamFrame spdySynStreamFrame = new DefaultSpdySynStreamFrame(streamID, associatedToStreamID, priority);
        if (httpMessage instanceof HttpRequest) {
            HttpRequest httpRequest = (HttpRequest) httpMessage;
            SpdyHeaders.setMethod(spdyVersion, spdySynStreamFrame, httpRequest.getMethod());
            SpdyHeaders.setUrl(spdyVersion, spdySynStreamFrame, httpRequest.getUri());
            SpdyHeaders.setVersion(spdyVersion, spdySynStreamFrame, httpMessage.getProtocolVersion());
        }
        if (httpMessage instanceof HttpResponse) {
            HttpResponse httpResponse = (HttpResponse) httpMessage;
            SpdyHeaders.setStatus(spdyVersion, spdySynStreamFrame, httpResponse.getStatus());
            SpdyHeaders.setUrl(spdyVersion, spdySynStreamFrame, URL);
            spdySynStreamFrame.setUnidirectional(true);
        }
        if (spdyVersion >= 3) {
            String host = HttpHeaders.getHost(httpMessage);
            httpMessage.removeHeader(HttpHeaders.Names.HOST);
            SpdyHeaders.setHost(spdySynStreamFrame, host);
        }
        if (scheme == null) {
            scheme = "https";
        }
        SpdyHeaders.setScheme(spdyVersion, spdySynStreamFrame, scheme);
        for (Map.Entry<String, String> entry : httpMessage.getHeaders()) {
            spdySynStreamFrame.addHeader(entry.getKey(), entry.getValue());
        }
        if (chunked) {
            currentStreamID = streamID;
            spdySynStreamFrame.setLast(false);
        } else {
            spdySynStreamFrame.setLast(httpMessage.getContent().readableBytes() == 0);
        }
        return spdySynStreamFrame;
    }

    private SpdySynReplyFrame createSynReplyFrame(HttpResponse httpResponse) throws Exception {
        boolean chunked = httpResponse.isChunked();
        int streamID = SpdyHttpHeaders.getStreamID(httpResponse);
        SpdyHttpHeaders.removeStreamID(httpResponse);
        httpResponse.removeHeader(HttpHeaders.Names.CONNECTION);
        httpResponse.removeHeader("Keep-Alive");
        httpResponse.removeHeader("Proxy-Connection");
        httpResponse.removeHeader(HttpHeaders.Names.TRANSFER_ENCODING);
        SpdySynReplyFrame spdySynReplyFrame = new DefaultSpdySynReplyFrame(streamID);
        SpdyHeaders.setStatus(spdyVersion, spdySynReplyFrame, httpResponse.getStatus());
        SpdyHeaders.setVersion(spdyVersion, spdySynReplyFrame, httpResponse.getProtocolVersion());
        for (Map.Entry<String, String> entry : httpResponse.getHeaders()) {
            spdySynReplyFrame.addHeader(entry.getKey(), entry.getValue());
        }
        if (chunked) {
            currentStreamID = streamID;
            spdySynReplyFrame.setLast(false);
        } else {
            spdySynReplyFrame.setLast(httpResponse.getContent().readableBytes() == 0);
        }
        return spdySynReplyFrame;
    }
}