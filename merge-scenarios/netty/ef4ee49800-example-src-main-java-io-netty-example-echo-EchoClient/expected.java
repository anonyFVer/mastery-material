package io.netty.example.echo;

import io.netty.channel.ChannelBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioEventLoop;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import java.net.InetSocketAddress;

public class EchoClient {

    private final String host;

    private final int port;

    private final int firstMessageSize;

    public EchoClient(String host, int port, int firstMessageSize) {
        this.host = host;
        this.port = port;
        this.firstMessageSize = firstMessageSize;
    }

    public void run() throws Exception {
        ChannelBootstrap b = new ChannelBootstrap();
        try {
            b.eventLoop(new NioEventLoop()).channel(new NioSocketChannel()).option(ChannelOption.TCP_NODELAY, true).remoteAddress(new InetSocketAddress(host, port)).initializer(new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new LoggingHandler(LogLevel.INFO), new EchoClientHandler(firstMessageSize));
                }
            });
            ChannelFuture f = b.connect().sync();
            f.channel().closeFuture().sync();
        } finally {
            b.shutdown();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: " + EchoClient.class.getSimpleName() + " <host> <port> [<first message size>]");
            return;
        }
        final String host = args[0];
        final int port = Integer.parseInt(args[1]);
        final int firstMessageSize;
        if (args.length == 3) {
            firstMessageSize = Integer.parseInt(args[2]);
        } else {
            firstMessageSize = 256;
        }
        new EchoClient(host, port, firstMessageSize).run();
    }
}