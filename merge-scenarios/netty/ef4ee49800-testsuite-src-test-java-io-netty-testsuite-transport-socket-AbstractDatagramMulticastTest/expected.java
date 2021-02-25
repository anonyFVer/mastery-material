package io.netty.testsuite.transport.socket;

import static org.junit.Assert.assertTrue;
import io.netty.bootstrap.ConnectionlessBootstrap;
import io.netty.buffer.ChannelBuffer;
import io.netty.buffer.ChannelBuffers;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.MessageEvent;
import io.netty.channel.SimpleChannelUpstreamHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramChannelFactory;
import io.netty.testsuite.util.TestUtils;
import io.netty.util.internal.ExecutorUtil;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class AbstractDatagramMulticastTest {

    private static ExecutorService executor;

    @BeforeClass
    public static void init() {
        executor = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void destroy() {
        ExecutorUtil.terminate(executor);
    }

    protected abstract DatagramChannelFactory newServerSocketChannelFactory(Executor executor);

    protected abstract DatagramChannelFactory newClientSocketChannelFactory(Executor executor);

    @Test
    public void testMulticast() throws Throwable {
        ConnectionlessBootstrap sb = new ConnectionlessBootstrap(newServerSocketChannelFactory(executor));
        ConnectionlessBootstrap cb = new ConnectionlessBootstrap(newClientSocketChannelFactory(executor));
        MulticastTestHandler mhandler = new MulticastTestHandler();
        cb.pipeline().addFirst("handler", mhandler);
        sb.pipeline().addFirst("handler", new SimpleChannelUpstreamHandler());
        int port = TestUtils.getFreePort();
        NetworkInterface iface = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());
        if (iface == null) {
            iface = NetworkInterface.getNetworkInterfaces().nextElement();
        }
        sb.setOption("networkInterface", iface);
        sb.setOption("reuseAddress", true);
        Channel sc = sb.bind(new InetSocketAddress(port));
        String group = "230.0.0.1";
        InetSocketAddress groupAddress = new InetSocketAddress(group, port);
        cb.setOption("networkInterface", iface);
        cb.setOption("reuseAddress", true);
        DatagramChannel cc = (DatagramChannel) cb.bind(new InetSocketAddress(port));
        assertTrue(cc.joinGroup(groupAddress, iface).awaitUninterruptibly().isSuccess());
        assertTrue(sc.write(ChannelBuffers.wrapInt(1), groupAddress).awaitUninterruptibly().isSuccess());
        assertTrue(mhandler.await());
        assertTrue(sc.write(ChannelBuffers.wrapInt(1), groupAddress).awaitUninterruptibly().isSuccess());
        assertTrue(cc.leaveGroup(groupAddress, iface).awaitUninterruptibly().isSuccess());
        Thread.sleep(1000);
        assertTrue(sc.write(ChannelBuffers.wrapInt(1), groupAddress).awaitUninterruptibly().isSuccess());
        sc.close().awaitUninterruptibly();
        cc.close().awaitUninterruptibly();
    }

    private final class MulticastTestHandler extends SimpleChannelUpstreamHandler {

        private final CountDownLatch latch = new CountDownLatch(1);

        private boolean done = false;

        private volatile boolean fail = false;

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            super.messageReceived(ctx, e);
            if (done) {
                fail = true;
            }
            Assert.assertEquals(1, ((ChannelBuffer) e.getMessage()).readInt());
            latch.countDown();
            done = true;
        }

        public boolean await() throws Exception {
            boolean success = latch.await(10, TimeUnit.SECONDS);
            if (fail) {
                Assert.fail();
            }
            return success;
        }
    }
}