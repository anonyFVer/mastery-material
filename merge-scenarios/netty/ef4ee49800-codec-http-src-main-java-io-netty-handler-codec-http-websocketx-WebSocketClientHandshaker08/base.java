package io.netty.handler.codec.http.websocketx;

import java.net.URI;
import java.util.Map;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpHeaders.Values;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import io.netty.util.CharsetUtil;

public class WebSocketClientHandshaker08 extends WebSocketClientHandshaker {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(WebSocketClientHandshaker08.class);

    public static final String MAGIC_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private String expectedChallengeResponseString;

    private static final String protocol = null;

    private final boolean allowExtensions;

    public WebSocketClientHandshaker08(URI webSocketURL, WebSocketVersion version, String subprotocol, boolean allowExtensions, Map<String, String> customHeaders) {
        super(webSocketURL, version, subprotocol, customHeaders);
        this.allowExtensions = allowExtensions;
    }

    @Override
    public ChannelFuture handshake(Channel channel) {
        URI wsURL = getWebSocketUrl();
        String path = wsURL.getPath();
        if (wsURL.getQuery() != null && wsURL.getQuery().length() > 0) {
            path = wsURL.getPath() + "?" + wsURL.getQuery();
        }
        byte[] nonce = WebSocketUtil.randomBytes(16);
        String key = WebSocketUtil.base64(nonce);
        String acceptSeed = key + MAGIC_GUID;
        byte[] sha1 = WebSocketUtil.sha1(acceptSeed.getBytes(CharsetUtil.US_ASCII));
        expectedChallengeResponseString = WebSocketUtil.base64(sha1);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("WS Version 08 Client Handshake key: %s. Expected response: %s.", key, expectedChallengeResponseString));
        }
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);
        request.addHeader(Names.UPGRADE, Values.WEBSOCKET.toLowerCase());
        request.addHeader(Names.CONNECTION, Values.UPGRADE);
        request.addHeader(Names.SEC_WEBSOCKET_KEY, key);
        request.addHeader(Names.HOST, wsURL.getHost());
        request.addHeader(Names.ORIGIN, "http://" + wsURL.getHost());
        if (protocol != null && !protocol.equals("")) {
            request.addHeader(Names.SEC_WEBSOCKET_PROTOCOL, protocol);
        }
        request.addHeader(Names.SEC_WEBSOCKET_VERSION, "8");
        if (customHeaders != null) {
            for (String header : customHeaders.keySet()) {
                request.addHeader(header, customHeaders.get(header));
            }
        }
        ChannelFuture future = channel.write(request);
        channel.getPipeline().replace(HttpRequestEncoder.class, "ws-encoder", new WebSocket08FrameEncoder(true));
        return future;
    }

    @Override
    public void finishHandshake(Channel channel, HttpResponse response) {
        final HttpResponseStatus status = HttpResponseStatus.SWITCHING_PROTOCOLS;
        if (!response.getStatus().equals(status)) {
            throw new WebSocketHandshakeException("Invalid handshake response status: " + response.getStatus());
        }
        String upgrade = response.getHeader(Names.UPGRADE);
        if (upgrade == null || !upgrade.equals(Values.WEBSOCKET.toLowerCase())) {
            throw new WebSocketHandshakeException("Invalid handshake response upgrade: " + response.getHeader(Names.UPGRADE));
        }
        String connection = response.getHeader(Names.CONNECTION);
        if (connection == null || !connection.equals(Values.UPGRADE)) {
            throw new WebSocketHandshakeException("Invalid handshake response connection: " + response.getHeader(Names.CONNECTION));
        }
        String accept = response.getHeader(Names.SEC_WEBSOCKET_ACCEPT);
        if (accept == null || !accept.equals(expectedChallengeResponseString)) {
            throw new WebSocketHandshakeException(String.format("Invalid challenge. Actual: %s. Expected: %s", accept, expectedChallengeResponseString));
        }
        channel.getPipeline().replace(HttpResponseDecoder.class, "ws-decoder", new WebSocket08FrameDecoder(false, allowExtensions));
        setHandshakeComplete();
    }
}