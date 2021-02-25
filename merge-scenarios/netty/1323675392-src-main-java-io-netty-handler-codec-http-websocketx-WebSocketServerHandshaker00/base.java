package io.netty.handler.codec.http.websocketx;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.ORIGIN;
import static io.netty.handler.codec.http.HttpHeaders.Names.SEC_WEBSOCKET_KEY1;
import static io.netty.handler.codec.http.HttpHeaders.Names.SEC_WEBSOCKET_KEY2;
import static io.netty.handler.codec.http.HttpHeaders.Names.SEC_WEBSOCKET_LOCATION;
import static io.netty.handler.codec.http.HttpHeaders.Names.SEC_WEBSOCKET_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaders.Names.SEC_WEBSOCKET_PROTOCOL;
import static io.netty.handler.codec.http.HttpHeaders.Names.WEBSOCKET_LOCATION;
import static io.netty.handler.codec.http.HttpHeaders.Names.WEBSOCKET_ORIGIN;
import static io.netty.handler.codec.http.HttpHeaders.Names.WEBSOCKET_PROTOCOL;
import static io.netty.handler.codec.http.HttpHeaders.Values.WEBSOCKET;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import java.security.NoSuchAlgorithmException;
import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBuffers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpChunkAggregator;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpHeaders.Values;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

public class WebSocketServerHandshaker00 extends WebSocketServerHandshaker {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(WebSocketServerHandshaker00.class);

    public WebSocketServerHandshaker00(String webSocketURL, String subProtocols) {
        super(webSocketURL, subProtocols);
    }

    @Override
    public void performOpeningHandshake(Channel channel, HttpRequest req) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Channel %s web socket spec version 00 handshake", channel.getId()));
        }
        this.setVersion(WebSocketSpecificationVersion.V00);
        if (!Values.UPGRADE.equalsIgnoreCase(req.getHeader(CONNECTION)) || !WEBSOCKET.equalsIgnoreCase(req.getHeader(Names.UPGRADE))) {
            return;
        }
        boolean isHixie76 = req.containsHeader(SEC_WEBSOCKET_KEY1) && req.containsHeader(SEC_WEBSOCKET_KEY2);
        HttpResponse res = new DefaultHttpResponse(HTTP_1_1, new HttpResponseStatus(101, isHixie76 ? "WebSocket Protocol Handshake" : "Web Socket Protocol Handshake"));
        res.addHeader(Names.UPGRADE, WEBSOCKET);
        res.addHeader(CONNECTION, Values.UPGRADE);
        if (isHixie76) {
            res.addHeader(SEC_WEBSOCKET_ORIGIN, req.getHeader(ORIGIN));
            res.addHeader(SEC_WEBSOCKET_LOCATION, this.getWebSocketURL());
            String protocol = req.getHeader(SEC_WEBSOCKET_PROTOCOL);
            if (protocol != null) {
                res.addHeader(SEC_WEBSOCKET_PROTOCOL, selectSubProtocol(protocol));
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
            ChannelBuffer output = ChannelBuffers.wrappedBuffer(this.md5(input.array()));
            res.setContent(output);
        } else {
            res.addHeader(WEBSOCKET_ORIGIN, req.getHeader(ORIGIN));
            res.addHeader(WEBSOCKET_LOCATION, this.getWebSocketURL());
            String protocol = req.getHeader(WEBSOCKET_PROTOCOL);
            if (protocol != null) {
                res.addHeader(WEBSOCKET_PROTOCOL, selectSubProtocol(protocol));
            }
        }
        ChannelPipeline p = channel.getPipeline();
        p.remove(HttpChunkAggregator.class);
        p.replace(HttpRequestDecoder.class, "wsdecoder", new WebSocket00FrameDecoder());
        channel.write(res);
        p.replace(HttpResponseEncoder.class, "wsencoder", new WebSocket00FrameEncoder());
    }

    @Override
    public void performClosingHandshake(Channel channel, CloseWebSocketFrame frame) {
        channel.write(frame);
    }
}