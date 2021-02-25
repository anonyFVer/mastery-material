package io.netty.example.local;

import java.util.concurrent.TimeUnit;
import io.netty.bootstrap.ClientBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPipelineFactory;
import io.netty.channel.Channels;
import io.netty.channel.local.DefaultLocalClientChannelFactory;
import io.netty.channel.local.DefaultLocalServerChannelFactory;
import io.netty.channel.local.LocalAddress;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import io.netty.handler.logging.LoggingHandler;
import io.netty.logging.InternalLogLevel;

public class LocalExampleMultithreaded {

    private final String port;

    public LocalExampleMultithreaded(String port) {
        this.port = port;
    }

    public void run() {
        LocalAddress socketAddress = new LocalAddress(port);
        OrderedMemoryAwareThreadPoolExecutor eventExecutor = new OrderedMemoryAwareThreadPoolExecutor(5, 1000000, 10000000, 100, TimeUnit.MILLISECONDS);
        ServerBootstrap sb = new ServerBootstrap(new DefaultLocalServerChannelFactory());
        sb.setPipelineFactory(new LocalServerPipelineFactory(eventExecutor));
        sb.bind(socketAddress);
        ClientBootstrap cb = new ClientBootstrap(new DefaultLocalClientChannelFactory());
        cb.setPipelineFactory(new ChannelPipelineFactory() {

            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new StringDecoder(), new StringEncoder(), new LoggingHandler(InternalLogLevel.INFO));
            }
        });
        String[] commands = { "First", "Second", "Third", "quit" };
        for (int j = 0; j < 5; j++) {
            System.err.println("Start " + j);
            ChannelFuture channelFuture = cb.connect(socketAddress);
            channelFuture.awaitUninterruptibly();
            if (!channelFuture.isSuccess()) {
                System.err.println("CANNOT CONNECT");
                channelFuture.cause().printStackTrace();
                break;
            }
            ChannelFuture lastWriteFuture = null;
            for (String line : commands) {
                lastWriteFuture = channelFuture.channel().write(line);
            }
            if (lastWriteFuture != null) {
                lastWriteFuture.awaitUninterruptibly();
            }
            channelFuture.channel().close();
            channelFuture.channel().getCloseFuture().awaitUninterruptibly();
            System.err.println("End " + j);
        }
        cb.releaseExternalResources();
        sb.releaseExternalResources();
        eventExecutor.shutdownNow();
    }

    public static void main(String[] args) throws Exception {
        new LocalExampleMultithreaded("1").run();
    }
}