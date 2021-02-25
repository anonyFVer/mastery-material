package io.netty.example.qotm;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import io.netty.bootstrap.ConnectionlessBootstrap;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPipelineFactory;
import io.netty.channel.Channels;
import io.netty.channel.FixedReceiveBufferSizePredictorFactory;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramChannelFactory;
import io.netty.channel.socket.nio.NioDatagramChannelFactory;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import io.netty.util.CharsetUtil;

public class QuoteOfTheMomentClient {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(QuoteOfTheMomentClient.class);

    private final int port;

    public QuoteOfTheMomentClient(int port) {
        this.port = port;
    }

    public void run() {
        DatagramChannelFactory f = new NioDatagramChannelFactory(Executors.newCachedThreadPool());
        ConnectionlessBootstrap b = new ConnectionlessBootstrap(f);
        b.setPipelineFactory(new ChannelPipelineFactory() {

            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new StringEncoder(CharsetUtil.ISO_8859_1), new StringDecoder(CharsetUtil.ISO_8859_1), new QuoteOfTheMomentClientHandler());
            }
        });
        b.setOption("broadcast", "true");
        b.setOption("receiveBufferSizePredictorFactory", new FixedReceiveBufferSizePredictorFactory(1024));
        DatagramChannel c = (DatagramChannel) b.bind(new InetSocketAddress(0));
        c.write("QOTM?", new InetSocketAddress("255.255.255.255", port));
        if (!c.getCloseFuture().awaitUninterruptibly(5000)) {
            logger.error("QOTM request timed out.");
            c.close().awaitUninterruptibly();
        }
        f.releaseExternalResources();
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