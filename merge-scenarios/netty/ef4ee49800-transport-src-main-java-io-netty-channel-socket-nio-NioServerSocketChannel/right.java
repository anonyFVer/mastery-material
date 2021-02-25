package io.netty.channel.socket.nio;

import static io.netty.channel.Channels.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import io.netty.channel.AbstractServerChannel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelSink;
import io.netty.channel.socket.DefaultServerSocketChannelConfig;
import io.netty.channel.socket.ServerSocketChannelConfig;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

final class NioServerSocketChannel extends AbstractServerChannel implements io.netty.channel.socket.ServerSocketChannel, NioChannel {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(NioServerSocketChannel.class);

    final ServerSocketChannel socket;

    final Lock shutdownLock = new ReentrantLock();

    final NioWorker worker;

    final WorkerPool<NioWorker> workers;

    private final ServerSocketChannelConfig config;

    static NioServerSocketChannel create(ChannelFactory factory, ChannelPipeline pipeline, ChannelSink sink, NioWorker worker, WorkerPool<NioWorker> workers) {
        NioServerSocketChannel instance = new NioServerSocketChannel(factory, pipeline, sink, worker, workers);
        fireChannelOpen(instance);
        return instance;
    }

    private NioServerSocketChannel(ChannelFactory factory, ChannelPipeline pipeline, ChannelSink sink, NioWorker worker, WorkerPool<NioWorker> workers) {
        super(factory, pipeline, sink);
        this.worker = worker;
        this.workers = workers;
        try {
            socket = ServerSocketChannel.open();
        } catch (IOException e) {
            throw new ChannelException("Failed to open a server socket.", e);
        }
        try {
            socket.configureBlocking(false);
        } catch (IOException e) {
            try {
                socket.close();
            } catch (IOException e2) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to close a partially initialized socket.", e2);
                }
            }
            throw new ChannelException("Failed to enter non-blocking mode.", e);
        }
        config = new DefaultServerSocketChannelConfig(socket.socket());
    }

    @Override
    public ServerSocketChannelConfig getConfig() {
        return config;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) socket.socket().getLocalSocketAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return null;
    }

    @Override
    public boolean isBound() {
        return isOpen() && socket.socket().isBound();
    }

    @Override
    protected boolean setClosed() {
        return super.setClosed();
    }

    @Override
    public NioWorker getWorker() {
        return worker;
    }
}