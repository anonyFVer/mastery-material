package io.netty.handler.codec.http.websocketx;

import static io.netty.handler.codec.http.HttpHeaders.Values.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpChunkAggregator;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import io.netty.util.CharsetUtil;

public class WebSocketServerHandshaker08 extends WebSocketServerHandshaker {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(WebSocketServerHandshaker08.class);

    public static final String WEBSOCKET_08_ACCEPT_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private final boolean allowExtensions;

    public WebSocketServerHandshaker08(String webSocketURL, String subprotocols, boolean allowExtensions) {
        super(WebSocketVersion.V08, webSocketURL, subprotocols);
        this.allowExtensions = allowExtensions;
    }

    @Override
    public ChannelFuture handshake(Channel channel, HttpRequest req) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Channel %s WS Version 8 server handshake", channel.id()));
        }
        HttpResponse res = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.SWITCHING_PROTOCOLS);
        String key = req.getHeader(Names.SEC_WEBSOCKET_KEY);
        if (key == null) {
            throw new WebSocketHandshakeException("not a WebSocket request: missing key");
        }
        String acceptSeed = key + WEBSOCKET_08_ACCEPT_GUID;
        byte[] sha1 = WebSocketUtil.sha1(acceptSeed.getBytes(CharsetUtil.US_ASCII));
        String accept = WebSocketUtil.base64(sha1);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("WS Version 8 Server Handshake key: %s. Response: %s.", key, accept));
        }
        res.setStatus(HttpResponseStatus.SWITCHING_PROTOCOLS);
        res.addHeader(Names.UPGRADE, WEBSOCKET.toLowerCase());
        res.addHeader(Names.CONNECTION, Names.UPGRADE);
        res.addHeader(Names.SEC_WEBSOCKET_ACCEPT, accept);
        String protocol = req.getHeader(Names.SEC_WEBSOCKET_PROTOCOL);
        if (protocol != null) {
            res.addHeader(Names.SEC_WEBSOCKET_PROTOCOL, selectSubprotocol(protocol));
        }
        ChannelFuture future = channel.write(res);
        ChannelPipeline p = channel.pipeline();
        if (p.get(HttpChunkAggregator.class) != null) {
            p.remove(HttpChunkAggregator.class);
        }
        p.replace(HttpRequestDecoder.class, "wsdecoder", new WebSocket08FrameDecoder(true, allowExtensions));
        p.replace(HttpResponseEncoder.class, "wsencoder", new WebSocket08FrameEncoder(false));
        return future;
    }

    @Override
    public ChannelFuture close(Channel channel, CloseWebSocketFrame frame) {
        ChannelFuture f = channel.write(frame);
        f.addListener(ChannelFutureListener.CLOSE);
        return f;
    }
}