package io.netty.channel.sctp;

import static io.netty.channel.Channels.*;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.Selector;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import io.netty.channel.AbstractServerChannel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelSink;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

class SctpServerChannelImpl extends AbstractServerChannel implements SctpServerChannel {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(SctpServerChannelImpl.class);

    final com.sun.nio.sctp.SctpServerChannel serverChannel;

    final Lock shutdownLock = new ReentrantLock();

    volatile Selector selector;

    private final SctpServerChannelConfig config;

    private volatile boolean bound;

    SctpServerChannelImpl(ChannelFactory factory, ChannelPipeline pipeline, ChannelSink sink) {
        super(factory, pipeline, sink);
        try {
            serverChannel = com.sun.nio.sctp.SctpServerChannel.open();
        } catch (IOException e) {
            throw new ChannelException("Failed to open a server sctp channel.", e);
        }
        try {
            serverChannel.configureBlocking(false);
        } catch (IOException e) {
            try {
                serverChannel.close();
            } catch (IOException e2) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to close a partially initialized socket.", e2);
                }
            }
            throw new ChannelException("Failed to enter non-blocking mode.", e);
        }
        config = new DefaultSctpServerChannelConfig(serverChannel);
        fireChannelOpen(this);
    }

    @Override
    public ChannelFuture bindAddress(InetAddress localAddress) {
        ChannelFuture future = future(this);
        getPipeline().sendDownstream(new SctpBindAddressEvent(this, future, localAddress));
        return future;
    }

    @Override
    public ChannelFuture unbindAddress(InetAddress localAddress) {
        ChannelFuture future = future(this);
        getPipeline().sendDownstream(new SctpUnbindAddressEvent(this, future, localAddress));
        return future;
    }

    @Override
    public SctpServerChannelConfig getConfig() {
        return config;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        try {
            final Iterator<SocketAddress> iterator = serverChannel.getAllLocalAddresses().iterator();
            return iterator.hasNext() ? (InetSocketAddress) iterator.next() : null;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public Set<InetSocketAddress> getAllLocalAddresses() {
        try {
            final Set<SocketAddress> allLocalAddresses = serverChannel.getAllLocalAddresses();
            final Set<InetSocketAddress> addresses = new HashSet<InetSocketAddress>(allLocalAddresses.size());
            for (SocketAddress socketAddress : allLocalAddresses) {
                addresses.add((InetSocketAddress) socketAddress);
            }
            return addresses;
        } catch (Throwable t) {
            return Collections.emptySet();
        }
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return null;
    }

    @Override
    public Set<InetSocketAddress> getAllRemoteAddresses() {
        return null;
    }

    @Override
    public boolean isBound() {
        return isOpen() && bound;
    }

    public void setBound() {
        bound = true;
    }

    @Override
    protected boolean setClosed() {
        return super.setClosed();
    }
}