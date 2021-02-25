package io.netty.channel.socket;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;

public interface DatagramChannel extends Channel {

    @Override
    DatagramChannelConfig config();

    @Override
    InetSocketAddress localAddress();

    @Override
    InetSocketAddress remoteAddress();

    ChannelFuture joinGroup(InetAddress multicastAddress);

    ChannelFuture joinGroup(InetAddress multicastAddress, ChannelFuture future);

    ChannelFuture joinGroup(InetSocketAddress multicastAddress, NetworkInterface networkInterface);

    ChannelFuture joinGroup(InetSocketAddress multicastAddress, NetworkInterface networkInterface, ChannelFuture future);

    ChannelFuture joinGroup(InetAddress multicastAddress, NetworkInterface networkInterface, InetAddress source);

    ChannelFuture joinGroup(InetAddress multicastAddress, NetworkInterface networkInterface, InetAddress source, ChannelFuture future);

    ChannelFuture leaveGroup(InetAddress multicastAddress);

    ChannelFuture leaveGroup(InetAddress multicastAddress, ChannelFuture future);

    ChannelFuture leaveGroup(InetSocketAddress multicastAddress, NetworkInterface networkInterface);

    ChannelFuture leaveGroup(InetSocketAddress multicastAddress, NetworkInterface networkInterface, ChannelFuture future);

    ChannelFuture leaveGroup(InetAddress multicastAddress, NetworkInterface networkInterface, InetAddress source);

    ChannelFuture leaveGroup(InetAddress multicastAddress, NetworkInterface networkInterface, InetAddress source, ChannelFuture future);

    ChannelFuture block(InetAddress multicastAddress, NetworkInterface networkInterface, InetAddress sourceToBlock);

    ChannelFuture block(InetAddress multicastAddress, NetworkInterface networkInterface, InetAddress sourceToBlock, ChannelFuture future);

    ChannelFuture block(InetAddress multicastAddress, InetAddress sourceToBlock);

    ChannelFuture block(InetAddress multicastAddress, InetAddress sourceToBlock, ChannelFuture future);
}