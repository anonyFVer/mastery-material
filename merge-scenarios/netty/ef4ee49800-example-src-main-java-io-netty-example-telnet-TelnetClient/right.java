package io.netty.example.telnet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import io.netty.bootstrap.ClientBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.nio.NioClientSocketChannelFactory;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

public class TelnetClient {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(TelnetClient.class);

    private final String host;

    private final int port;

    public TelnetClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void run() throws IOException {
        ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new TelnetClientPipelineFactory());
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));
        Channel channel = future.awaitUninterruptibly().getChannel();
        if (!future.isSuccess()) {
            future.getCause().printStackTrace();
            bootstrap.releaseExternalResources();
            return;
        }
        ChannelFuture lastWriteFuture = null;
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        for (; ; ) {
            String line = in.readLine();
            if (line == null) {
                break;
            }
            lastWriteFuture = channel.write(line + "\r\n");
            if (line.toLowerCase().equals("bye")) {
                channel.getCloseFuture().awaitUninterruptibly();
                break;
            }
        }
        if (lastWriteFuture != null) {
            lastWriteFuture.awaitUninterruptibly();
        }
        channel.close().awaitUninterruptibly();
        bootstrap.releaseExternalResources();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            logger.error("Usage: " + TelnetClient.class.getSimpleName() + " <host> <port>");
            return;
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        new TelnetClient(host, port).run();
    }
}