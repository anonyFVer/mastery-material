package io.netty.handler.codec.http.websocketx;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import io.netty.buffer.ChannelBuffers;
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

public class WebSocketClientHandshaker00 extends WebSocketClientHandshaker {

    private byte[] expectedChallengeResponseBytes;

    public WebSocketClientHandshaker00(URI webSocketURL, WebSocketVersion version, String subprotocol, Map<String, String> customHeaders) {
        super(webSocketURL, version, subprotocol, customHeaders);
    }

    @Override
    public ChannelFuture handshake(Channel channel) {
        int spaces1 = WebSocketUtil.randomNumber(1, 12);
        int spaces2 = WebSocketUtil.randomNumber(1, 12);
        int max1 = Integer.MAX_VALUE / spaces1;
        int max2 = Integer.MAX_VALUE / spaces2;
        int number1 = WebSocketUtil.randomNumber(0, max1);
        int number2 = WebSocketUtil.randomNumber(0, max2);
        int product1 = number1 * spaces1;
        int product2 = number2 * spaces2;
        String key1 = Integer.toString(product1);
        String key2 = Integer.toString(product2);
        key1 = insertRandomCharacters(key1);
        key2 = insertRandomCharacters(key2);
        key1 = insertSpaces(key1, spaces1);
        key2 = insertSpaces(key2, spaces2);
        byte[] key3 = WebSocketUtil.randomBytes(8);
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(number1);
        byte[] number1Array = buffer.array();
        buffer = ByteBuffer.allocate(4);
        buffer.putInt(number2);
        byte[] number2Array = buffer.array();
        byte[] challenge = new byte[16];
        System.arraycopy(number1Array, 0, challenge, 0, 4);
        System.arraycopy(number2Array, 0, challenge, 4, 4);
        System.arraycopy(key3, 0, challenge, 8, 8);
        expectedChallengeResponseBytes = WebSocketUtil.md5(challenge);
        URI wsURL = getWebSocketUrl();
        String path = wsURL.getPath();
        if (wsURL.getQuery() != null && wsURL.getQuery().length() > 0) {
            path = wsURL.getPath() + "?" + wsURL.getQuery();
        }
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);
        request.addHeader(Names.UPGRADE, Values.WEBSOCKET);
        request.addHeader(Names.CONNECTION, Values.UPGRADE);
        request.addHeader(Names.HOST, wsURL.getHost());
        request.addHeader(Names.ORIGIN, "http://" + wsURL.getHost());
        request.addHeader(Names.SEC_WEBSOCKET_KEY1, key1);
        request.addHeader(Names.SEC_WEBSOCKET_KEY2, key2);
        if (getExpectedSubprotocol() != null && !getExpectedSubprotocol().equals("")) {
            request.addHeader(Names.SEC_WEBSOCKET_PROTOCOL, getExpectedSubprotocol());
        }
        if (customHeaders != null) {
            for (String header : customHeaders.keySet()) {
                request.addHeader(header, customHeaders.get(header));
            }
        }
        request.setContent(ChannelBuffers.copiedBuffer(key3));
        ChannelFuture future = channel.write(request);
        channel.getPipeline().replace(HttpRequestEncoder.class, "ws-encoder", new WebSocket00FrameEncoder());
        return future;
    }

    @Override
    public void finishHandshake(Channel channel, HttpResponse response) throws WebSocketHandshakeException {
        final HttpResponseStatus status = new HttpResponseStatus(101, "WebSocket Protocol Handshake");
        if (!response.getStatus().equals(status)) {
            throw new WebSocketHandshakeException("Invalid handshake response status: " + response.getStatus());
        }
        String upgrade = response.getHeader(Names.UPGRADE);
        if (upgrade == null || !upgrade.equals(Values.WEBSOCKET)) {
            throw new WebSocketHandshakeException("Invalid handshake response upgrade: " + response.getHeader(Names.UPGRADE));
        }
        String connection = response.getHeader(Names.CONNECTION);
        if (connection == null || !connection.equals(Values.UPGRADE)) {
            throw new WebSocketHandshakeException("Invalid handshake response connection: " + response.getHeader(Names.CONNECTION));
        }
        byte[] challenge = response.getContent().array();
        if (!Arrays.equals(challenge, expectedChallengeResponseBytes)) {
            throw new WebSocketHandshakeException("Invalid challenge");
        }
        String protocol = response.getHeader(Names.SEC_WEBSOCKET_PROTOCOL);
        setActualSubprotocol(protocol);
        channel.getPipeline().replace(HttpResponseDecoder.class, "ws-decoder", new WebSocket00FrameDecoder());
        setHandshakeComplete();
    }

    private String insertRandomCharacters(String key) {
        int count = WebSocketUtil.randomNumber(1, 12);
        char[] randomChars = new char[count];
        int randCount = 0;
        while (randCount < count) {
            int rand = (int) (Math.random() * 0x7e + 0x21);
            if (0x21 < rand && rand < 0x2f || 0x3a < rand && rand < 0x7e) {
                randomChars[randCount] = (char) rand;
                randCount += 1;
            }
        }
        for (int i = 0; i < count; i++) {
            int split = WebSocketUtil.randomNumber(0, key.length());
            String part1 = key.substring(0, split);
            String part2 = key.substring(split);
            key = part1 + randomChars[i] + part2;
        }
        return key;
    }

    private String insertSpaces(String key, int spaces) {
        for (int i = 0; i < spaces; i++) {
            int split = WebSocketUtil.randomNumber(1, key.length() - 1);
            String part1 = key.substring(0, split);
            String part2 = key.substring(split);
            key = part1 + " " + part2;
        }
        return key;
    }
}