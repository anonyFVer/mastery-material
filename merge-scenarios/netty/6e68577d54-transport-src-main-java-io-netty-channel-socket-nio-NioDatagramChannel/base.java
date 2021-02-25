package io.netty.channel.socket.nio;

import static io.netty.channel.Channels.fireChannelOpen;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelSink;
import io.netty.channel.socket.DatagramChannelConfig;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.channels.DatagramChannel;

final class NioDatagramChannel extends AbstractNioChannel<DatagramChannel> implements io.netty.channel.socket.DatagramChannel {

    private final NioDatagramChannelConfig config;

    static NioDatagramChannel create(ChannelFactory factory, ChannelPipeline pipeline, ChannelSink sink, NioDatagramWorker worker) {
        NioDatagramChannel instance = new NioDatagramChannel(factory, pipeline, sink, worker);
        fireChannelOpen(instance);
        return instance;
    }

    private NioDatagramChannel(final ChannelFactory factory, final ChannelPipeline pipeline, final ChannelSink sink, final NioDatagramWorker worker) {
        super(null, factory, pipeline, sink, worker, openNonBlockingChannel());
        config = new DefaultNioDatagramChannelConfig(channel.socket());
    }

    private static DatagramChannel openNonBlockingChannel() {
        try {
            final DatagramChannel channel = DatagramChannel.open();
            channel.configureBlocking(false);
            return channel;
        } catch (final IOException e) {
            throw new ChannelException("Failed to open a DatagramChannel.", e);
        }
    }

    @Override
    public boolean isBound() {
        return isOpen() && channel.socket().isBound();
    }

    @Override
    public boolean isConnected() {
        return channel.isConnected();
    }

    @Override
    protected boolean setClosed() {
        return super.setClosed();
    }

    @Override
    public NioDatagramChannelConfig getConfig() {
        return config;
    }

    DatagramChannel getDatagramChannel() {
        return channel;
    }

    @Override
    public void joinGroup(InetAddress multicastAddress) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void joinGroup(InetSocketAddress multicastAddress, NetworkInterface networkInterface) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void leaveGroup(InetAddress multicastAddress) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void leaveGroup(InetSocketAddress multicastAddress, NetworkInterface networkInterface) {
        throw new UnsupportedOperationException();
    }

    @Override
    InetSocketAddress getLocalSocketAddress() throws Exception {
        return (InetSocketAddress) channel.socket().getLocalSocketAddress();
    }

    @Override
    InetSocketAddress getRemoteSocketAddress() throws Exception {
        return (InetSocketAddress) channel.socket().getRemoteSocketAddress();
    }
}