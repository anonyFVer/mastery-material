package io.netty.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.MessageBuf;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.AttributeMap;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;

public interface Channel extends AttributeMap, ChannelOutboundInvoker, ChannelFutureFactory, Comparable<Channel> {

    Integer id();

    EventLoop eventLoop();

    Channel parent();

    ChannelConfig config();

    ChannelPipeline pipeline();

    boolean isOpen();

    boolean isRegistered();

    boolean isActive();

    ChannelMetadata metadata();

    ByteBuf outboundByteBuffer();

    <T> MessageBuf<T> outboundMessageBuffer();

    SocketAddress localAddress();

    SocketAddress remoteAddress();

    ChannelFuture closeFuture();

    Unsafe unsafe();

    interface Unsafe {

        ChannelHandlerContext directOutboundContext();

        ChannelFuture voidFuture();

        SocketAddress localAddress();

        SocketAddress remoteAddress();

        void register(EventLoop eventLoop, ChannelFuture future);

        void bind(SocketAddress localAddress, ChannelFuture future);

        void connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelFuture future);

        void disconnect(ChannelFuture future);

        void close(ChannelFuture future);

        void deregister(ChannelFuture future);

        void flush(ChannelFuture future);

        void flushNow();
    }
}