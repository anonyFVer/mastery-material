package io.netty.handler.codec.http.websocketx;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.Values.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBuffers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpChunkAggregator;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpHeaders.Values;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

public class WebSocketServerHandshaker00 extends WebSocketServerHandshaker {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(WebSocketServerHandshaker00.class);

    public WebSocketServerHandshaker00(String webSocketURL, String subprotocols) {
        super(WebSocketVersion.V00, webSocketURL, subprotocols);
    }

    @Override
    public ChannelFuture handshake(Channel channel, HttpRequest req) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Channel %s WS Version 00 server handshake", channel.id()));
        }
        if (!Values.UPGRADE.equalsIgnoreCase(req.getHeader(CONNECTION)) || !WEBSOCKET.equalsIgnoreCase(req.getHeader(Names.UPGRADE))) {
            throw new WebSocketHandshakeException("not a WebSocket handshake request: missing upgrade");
        }
        boolean isHixie76 = req.containsHeader(SEC_WEBSOCKET_KEY1) && req.containsHeader(SEC_WEBSOCKET_KEY2);
        HttpResponse res = new DefaultHttpResponse(HTTP_1_1, new HttpResponseStatus(101, isHixie76 ? "WebSocket Protocol Handshake" : "Web Socket Protocol Handshake"));
        res.addHeader(Names.UPGRADE, WEBSOCKET);
        res.addHeader(CONNECTION, Values.UPGRADE);
        if (isHixie76) {
            res.addHeader(SEC_WEBSOCKET_ORIGIN, req.getHeader(ORIGIN));
            res.addHeader(SEC_WEBSOCKET_LOCATION, getWebSocketUrl());
            String protocol = req.getHeader(SEC_WEBSOCKET_PROTOCOL);
            if (protocol != null) {
                res.addHeader(SEC_WEBSOCKET_PROTOCOL, selectSubprotocol(protocol));
            }
            String key1 = req.getHeader(SEC_WEBSOCKET_KEY1);
            String key2 = req.getHeader(SEC_WEBSOCKET_KEY2);
            int a = (int) (Long.parseLong(key1.replaceAll("[^0-9]", "")) / key1.replaceAll("[^ ]", "").length());
            int b = (int) (Long.parseLong(key2.replaceAll("[^0-9]", "")) / key2.replaceAll("[^ ]", "").length());
            long c = req.getContent().readLong();
            ChannelBuffer input = ChannelBuffers.buffer(16);
            input.writeInt(a);
            input.writeInt(b);
            input.writeLong(c);
            ChannelBuffer output = ChannelBuffers.wrappedBuffer(WebSocketUtil.md5(input.array()));
            res.setContent(output);
        } else {
            res.addHeader(WEBSOCKET_ORIGIN, req.getHeader(ORIGIN));
            res.addHeader(WEBSOCKET_LOCATION, getWebSocketUrl());
            String protocol = req.getHeader(WEBSOCKET_PROTOCOL);
            if (protocol != null) {
                res.addHeader(WEBSOCKET_PROTOCOL, selectSubprotocol(protocol));
            }
        }
        ChannelPipeline p = channel.pipeline();
        if (p.get(HttpChunkAggregator.class) != null) {
            p.remove(HttpChunkAggregator.class);
        }
        p.replace(HttpRequestDecoder.class, "wsdecoder", new WebSocket00FrameDecoder());
        ChannelFuture future = channel.write(res);
        p.replace(HttpResponseEncoder.class, "wsencoder", new WebSocket00FrameEncoder());
        return future;
    }

    @Override
    public ChannelFuture close(Channel channel, CloseWebSocketFrame frame) {
        return channel.write(frame);
    }
}