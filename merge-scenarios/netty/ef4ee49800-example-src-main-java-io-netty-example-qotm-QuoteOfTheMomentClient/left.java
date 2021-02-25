package io.netty.example.qotm;

import io.netty.buffer.ChannelBuffers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioEventLoop;
import io.netty.util.CharsetUtil;
import java.net.InetSocketAddress;

public class QuoteOfTheMomentClient {

    private final int port;

    public QuoteOfTheMomentClient(int port) {
        this.port = port;
    }

    public void run() throws Exception {
        ChannelBootstrap b = new ChannelBootstrap();
        try {
            b.eventLoop(new NioEventLoop()).channel(new NioDatagramChannel()).localAddress(new InetSocketAddress(0)).option(ChannelOption.SO_BROADCAST, true).initializer(new ChannelInitializer<DatagramChannel>() {

                @Override
                public void initChannel(DatagramChannel ch) throws Exception {
                    ch.pipeline().addLast(new QuoteOfTheMomentClientHandler());
                }
            });
            Channel ch = b.bind().sync().channel();
            ch.write(new DatagramPacket(ChannelBuffers.copiedBuffer("QOTM?", CharsetUtil.UTF_8), new InetSocketAddress("255.255.255.255", port)));
            if (!ch.closeFuture().await(5000)) {
                System.err.println("QOTM request timed out.");
            }
        } finally {
            b.shutdown();
        }
    }

    public static void main(String[] args) throws Exception {
        int port;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        } else {
            port = 8080;
        }
        new QuoteOfTheMomentClient(port).run();
    }
}