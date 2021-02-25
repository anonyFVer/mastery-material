package io.netty.example.echo;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import io.netty.bootstrap.ClientBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPipelineFactory;
import io.netty.channel.Channels;
import io.netty.channel.socket.nio.NioClientSocketChannelFactory;

public class EchoClient {

    private final String host;

    private final int port;

    private final int firstMessageSize;

    public EchoClient(String host, int port, int firstMessageSize) {
        this.host = host;
        this.port = port;
        this.firstMessageSize = firstMessageSize;
    }

    public void run() {
        ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new EchoClientHandler(firstMessageSize));
            }
        });
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));
        future.getChannel().getCloseFuture().awaitUninterruptibly();
        bootstrap.releaseExternalResources();
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