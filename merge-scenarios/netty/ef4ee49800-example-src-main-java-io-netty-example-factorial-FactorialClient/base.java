package io.netty.example.factorial;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import io.netty.bootstrap.ClientBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.nio.NioClientSocketChannelFactory;

public class FactorialClient {

    private final String host;

    private final int port;

    private final int count;

    public FactorialClient(String host, int port, int count) {
        this.host = host;
        this.port = port;
        this.count = count;
    }

    public void run() {
        ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new FactorialClientPipelineFactory(count));
        ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(host, port));
        Channel channel = connectFuture.awaitUninterruptibly().getChannel();
        FactorialClientHandler handler = (FactorialClientHandler) channel.getPipeline().getLast();
        System.err.format("Factorial of %,d is: %,d", count, handler.getFactorial());
        bootstrap.releaseExternalResources();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: " + FactorialClient.class.getSimpleName() + " <host> <port> <count>");
            return;
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int count = Integer.parseInt(args[2]);
        if (count <= 0) {
            throw new IllegalArgumentException("count must be a positive integer.");
        }
        new FactorialClient(host, port, count).run();
    }
}