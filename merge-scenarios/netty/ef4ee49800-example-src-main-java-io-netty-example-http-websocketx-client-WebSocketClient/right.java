package io.netty.example.http.websocketx.client;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.Executors;
import io.netty.bootstrap.ClientBootstrap;
import io.netty.buffer.ChannelBuffers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPipelineFactory;
import io.netty.channel.Channels;
import io.netty.channel.socket.nio.NioClientSocketChannelFactory;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

public class WebSocketClient {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(WebSocketClient.class);

    private final URI uri;

    public WebSocketClient(URI uri) {
        this.uri = uri;
    }

    public void run() throws Exception {
        ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool()));
        Channel ch = null;
        try {
            String protocol = uri.getScheme();
            if (!protocol.equals("ws")) {
                throw new IllegalArgumentException("Unsupported protocol: " + protocol);
            }
            HashMap<String, String> customHeaders = new HashMap<String, String>();
            customHeaders.put("MyHeader", "MyValue");
            final WebSocketClientHandshaker handshaker = new WebSocketClientHandshakerFactory().newHandshaker(uri, WebSocketVersion.V13, null, false, customHeaders);
            bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

                public ChannelPipeline getPipeline() throws Exception {
                    ChannelPipeline pipeline = Channels.pipeline();
                    pipeline.addLast("decoder", new HttpResponseDecoder());
                    pipeline.addLast("encoder", new HttpRequestEncoder());
                    pipeline.addLast("ws-handler", new WebSocketClientHandler(handshaker));
                    return pipeline;
                }
            });
            logger.info("WebSocket Client connecting");
            ChannelFuture future = bootstrap.connect(new InetSocketAddress(uri.getHost(), uri.getPort()));
            future.awaitUninterruptibly().rethrowIfFailed();
            ch = future.getChannel();
            handshaker.handshake(ch).awaitUninterruptibly().rethrowIfFailed();
            logger.info("WebSocket Client sending message");
            for (int i = 0; i < 1000; i++) {
                ch.write(new TextWebSocketFrame("Message #" + i));
            }
            logger.info("WebSocket Client sending ping");
            ch.write(new PingWebSocketFrame(ChannelBuffers.copiedBuffer(new byte[] { 1, 2, 3, 4, 5, 6 })));
            logger.info("WebSocket Client sending close");
            ch.write(new CloseWebSocketFrame());
            ch.getCloseFuture().awaitUninterruptibly();
        } finally {
            if (ch != null) {
                ch.close();
            }
            bootstrap.releaseExternalResources();
        }
    }

    public static void main(String[] args) throws Exception {
        URI uri;
        if (args.length > 0) {
            uri = new URI(args[0]);
        } else {
            uri = new URI("ws://localhost:8080/websocket");
        }
        new WebSocketClient(uri).run();
    }
}