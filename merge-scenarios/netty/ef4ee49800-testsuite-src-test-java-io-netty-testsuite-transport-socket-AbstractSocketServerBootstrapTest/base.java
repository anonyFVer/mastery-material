package io.netty.testsuite.transport.socket;

import static org.junit.Assert.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import io.netty.bootstrap.ClientBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipelineException;
import io.netty.channel.ChannelPipelineFactory;
import io.netty.channel.ChildChannelStateEvent;
import io.netty.channel.ServerChannelFactory;
import io.netty.channel.SimpleChannelUpstreamHandler;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.testsuite.util.DummyHandler;
import io.netty.util.SocketAddresses;
import io.netty.util.internal.ExecutorUtil;
import org.easymock.EasyMock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class AbstractSocketServerBootstrapTest {

    private static final boolean BUFSIZE_MODIFIABLE;

    static {
        boolean bufSizeModifiable = true;
        Socket s = new Socket();
        try {
            s.setReceiveBufferSize(1234);
            try {
                if (s.getReceiveBufferSize() != 1234) {
                    throw new IllegalStateException();
                }
            } catch (Exception e) {
                bufSizeModifiable = false;
                System.err.println("Socket.getReceiveBufferSize() does not work: " + e);
            }
        } catch (Exception e) {
            bufSizeModifiable = false;
            System.err.println("Socket.setReceiveBufferSize() does not work: " + e);
        } finally {
            BUFSIZE_MODIFIABLE = bufSizeModifiable;
            try {
                s.close();
            } catch (IOException e) {
            }
        }
    }

    private static ExecutorService executor;

    @BeforeClass
    public static void init() {
        executor = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void destroy() {
        ExecutorUtil.terminate(executor);
    }

    protected abstract ChannelFactory newServerSocketChannelFactory(Executor executor);

    @Test(timeout = 30000, expected = ChannelException.class)
    public void testFailedBindAttempt() throws Exception {
        final ServerSocket ss = new ServerSocket(0);
        final int boundPort = ss.getLocalPort();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.setFactory(newServerSocketChannelFactory(executor));
            bootstrap.setOption("localAddress", new InetSocketAddress(boundPort));
            bootstrap.bind().close().awaitUninterruptibly();
        } finally {
            ss.close();
        }
    }

    @Test(timeout = 30000)
    public void testSuccessfulBindAttempt() throws Exception {
        ServerBootstrap bootstrap = new ServerBootstrap(newServerSocketChannelFactory(executor));
        bootstrap.setParentHandler(new ParentChannelHandler());
        bootstrap.setOption("localAddress", new InetSocketAddress(0));
        bootstrap.setOption("child.receiveBufferSize", 9753);
        bootstrap.setOption("child.sendBufferSize", 8642);
        bootstrap.getPipeline().addLast("dummy", new DummyHandler());
        Channel channel = bootstrap.bind();
        ParentChannelHandler pch = channel.getPipeline().get(ParentChannelHandler.class);
        Socket socket = null;
        try {
            socket = new Socket(SocketAddresses.LOCALHOST, ((InetSocketAddress) channel.getLocalAddress()).getPort());
            while (pch.child == null) {
                Thread.yield();
            }
            SocketChannelConfig cfg = (SocketChannelConfig) pch.child.getConfig();
            if (BUFSIZE_MODIFIABLE) {
                assertEquals(9753, cfg.getReceiveBufferSize());
                assertEquals(8642, cfg.getSendBufferSize());
            }
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                }
            }
            channel.close().awaitUninterruptibly();
        }
        while (pch.child.isOpen()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
        }
        while (pch.result.length() < 2) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
        }
        assertEquals("12", pch.result.toString());
    }

    @Test(expected = ChannelPipelineException.class)
    public void testFailedPipelineInitialization() throws Exception {
        ClientBootstrap bootstrap = new ClientBootstrap(EasyMock.createMock(ChannelFactory.class));
        ChannelPipelineFactory pipelineFactory = EasyMock.createMock(ChannelPipelineFactory.class);
        bootstrap.setPipelineFactory(pipelineFactory);
        EasyMock.expect(pipelineFactory.getPipeline()).andThrow(new ChannelPipelineException());
        EasyMock.replay(pipelineFactory);
        bootstrap.connect(new InetSocketAddress(SocketAddresses.LOCALHOST, 1));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldHaveLocalAddressOption() {
        new ServerBootstrap(EasyMock.createMock(ServerChannelFactory.class)).bind();
    }

    @Test(expected = NullPointerException.class)
    public void shouldDisallowNullLocalAddressParameter() {
        new ServerBootstrap(EasyMock.createMock(ServerChannelFactory.class)).bind(null);
    }

    private static class ParentChannelHandler extends SimpleChannelUpstreamHandler {

        volatile Channel child;

        final StringBuffer result = new StringBuffer();

        ParentChannelHandler() {
        }

        @Override
        public void childChannelClosed(ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception {
            result.append('2');
        }

        @Override
        public void childChannelOpen(ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception {
            child = e.getChildChannel();
            result.append('1');
        }
    }
}