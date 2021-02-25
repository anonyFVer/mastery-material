package io.netty.channel.sctp;

import static io.netty.channel.Channels.*;
import java.io.IOException;
import com.sun.nio.sctp.SctpChannel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelSink;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

final class SctpClientChannel extends SctpChannelImpl {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(SctpClientChannel.class);

    private static SctpChannel newChannael() {
        SctpChannel underlayingChannel;
        try {
            underlayingChannel = SctpChannel.open();
        } catch (IOException e) {
            throw new ChannelException("Failed to open a sctp channel.", e);
        }
        boolean success = false;
        try {
            underlayingChannel.configureBlocking(false);
            success = true;
        } catch (IOException e) {
            throw new ChannelException("Failed to enter non-blocking mode.", e);
        } finally {
            if (!success) {
                try {
                    underlayingChannel.close();
                } catch (IOException e) {
                    logger.warn("Failed to close a partially initialized sctp channel.", e);
                }
            }
        }
        return underlayingChannel;
    }

    volatile ChannelFuture connectFuture;

    volatile boolean boundManually;

    long connectDeadlineNanos;

    SctpClientChannel(ChannelFactory factory, ChannelPipeline pipeline, ChannelSink sink, SctpWorker worker) {
        super(null, factory, pipeline, sink, newChannael(), worker);
        fireChannelOpen(this);
    }
}