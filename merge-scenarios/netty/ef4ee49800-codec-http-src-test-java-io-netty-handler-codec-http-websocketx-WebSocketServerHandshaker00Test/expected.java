package io.netty.handler.codec.http.websocketx;

import static io.netty.handler.codec.http.HttpHeaders.Values.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import static org.easymock.EasyMock.*;
import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBuffers;
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
import java.nio.charset.Charset;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class WebSocketServerHandshaker00Test {

    private static DefaultChannelPipeline createPipeline(Channel ch) {
        DefaultChannelPipeline pipeline = new DefaultChannelPipeline(ch);
        pipeline.addLast("chunkAggregator", new HttpChunkAggregator(42));
        pipeline.addLast("wsdecoder", new HttpRequestDecoder());
        pipeline.addLast("wsencoder", new HttpResponseEncoder());
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
        req.setHeader(Names.ORIGIN, "http://example.com");
        req.setHeader(Names.SEC_WEBSOCKET_KEY1, "4 @1  46546xW%0l 1 5");
        req.setHeader(Names.SEC_WEBSOCKET_KEY2, "12998 5 Y3 1  .P00");
        req.setHeader(Names.SEC_WEBSOCKET_PROTOCOL, "chat, superchat");
        ChannelBuffer buffer = ChannelBuffers.copiedBuffer("^n:ds[4U", Charset.defaultCharset());
        req.setContent(buffer);
        WebSocketServerHandshaker00 handsaker = new WebSocketServerHandshaker00("ws://example.com/chat", "chat");
        handsaker.handshake(channelMock, req);
        Assert.assertEquals("ws://example.com/chat", res.getValue().getHeader(Names.SEC_WEBSOCKET_LOCATION));
        Assert.assertEquals("chat", res.getValue().getHeader(Names.SEC_WEBSOCKET_PROTOCOL));
        Assert.assertEquals("8jKS'y:G*Co,Wxa-", res.getValue().getContent().toString(Charset.defaultCharset()));
    }
}