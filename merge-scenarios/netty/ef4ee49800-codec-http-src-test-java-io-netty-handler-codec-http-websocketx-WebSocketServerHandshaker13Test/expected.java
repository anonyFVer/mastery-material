package io.netty.handler.codec.http.websocketx;

import static io.netty.handler.codec.http.HttpHeaders.Values.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import static org.easymock.EasyMock.*;
import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelFuture;
import io.netty.channel.DefaultChannelPipeline;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpChunkAggregator;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class WebSocketServerHandshaker13Test {

    private static DefaultChannelPipeline createPipeline(Channel ch) {
        DefaultChannelPipeline pipeline = new DefaultChannelPipeline(ch);
        pipeline.addLast("chunkAggregator", new HttpChunkAggregator(42));
        pipeline.addLast("requestDecoder", new HttpRequestDecoder());
        pipeline.addLast("responseEncoder", new HttpResponseEncoder());
        return pipeline;
    }

    @Test
    public void testPerformOpeningHandshake() {
        Channel channelMock = EasyMock.createMock(Channel.class);
        DefaultChannelPipeline pipeline = createPipeline(channelMock);
        EasyMock.expect(channelMock.pipeline()).andReturn(pipeline);
        Capture<HttpResponse> res = new Capture<HttpResponse>();
        EasyMock.expect(channelMock.write(capture(res))).andReturn(new DefaultChannelFuture(channelMock, true));
        replay(channelMock);
        HttpRequest req = new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, "/chat");
        req.setHeader(Names.HOST, "server.example.com");
        req.setHeader(Names.UPGRADE, WEBSOCKET.toLowerCase());
        req.setHeader(Names.CONNECTION, "Upgrade");
        req.setHeader(Names.SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==");
        req.setHeader(Names.SEC_WEBSOCKET_ORIGIN, "http://example.com");
        req.setHeader(Names.SEC_WEBSOCKET_PROTOCOL, "chat, superchat");
        req.setHeader(Names.SEC_WEBSOCKET_VERSION, "13");
        WebSocketServerHandshaker13 handsaker = new WebSocketServerHandshaker13("ws://example.com/chat", "chat", false);
        handsaker.handshake(channelMock, req);
        Assert.assertEquals("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", res.getValue().getHeader(Names.SEC_WEBSOCKET_ACCEPT));
        Assert.assertEquals("chat", res.getValue().getHeader(Names.SEC_WEBSOCKET_PROTOCOL));
    }
}