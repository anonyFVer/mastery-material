package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.protocol.dubbo.support.ProtocolUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import java.lang.reflect.Field;
import static org.junit.Assert.fail;

public class DubboInvokerAvilableTest {

    private static DubboProtocol protocol = DubboProtocol.getDubboProtocol();

    private static ProxyFactory proxy = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() {
        ProtocolUtils.closeAll();
    }

    @Test
    public void test_Normal_available() {
        URL url = URL.valueOf("dubbo://127.0.0.1:20883/org.apache.dubbo.rpc.protocol.dubbo.IDemoService");
        ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);
        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);
        Assert.assertEquals(true, invoker.isAvailable());
        invoker.destroy();
        Assert.assertEquals(false, invoker.isAvailable());
    }

    @Test
    public void test_Normal_ChannelReadOnly() throws Exception {
        URL url = URL.valueOf("dubbo://127.0.0.1:20883/org.apache.dubbo.rpc.protocol.dubbo.IDemoService");
        ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);
        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);
        Assert.assertEquals(true, invoker.isAvailable());
        getClients(invoker)[0].setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
        Assert.assertEquals(false, invoker.isAvailable());
        getClients(invoker)[0].removeAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY);
    }

    @Ignore
    public void test_normal_channel_close_wait_gracefully() throws Exception {
        int testPort = NetUtils.getAvailablePort();
        URL url = URL.valueOf("dubbo://127.0.0.1:" + testPort + "/org.apache.dubbo.rpc.protocol.dubbo.IDemoService?scope=true&lazy=false");
        Exporter<IDemoService> exporter = ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);
        Exporter<IDemoService> exporter0 = ProtocolUtils.export(new DemoServiceImpl0(), IDemoService.class, url);
        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);
        long start = System.currentTimeMillis();
        try {
            System.setProperty(Constants.SHUTDOWN_WAIT_KEY, "2000");
            System.out.println("------------ConfigUtils.getServerShutdownTimeout(): " + ConfigUtils.getServerShutdownTimeout());
            protocol.destroy();
        } finally {
            System.getProperties().remove(Constants.SHUTDOWN_WAIT_KEY);
        }
        long waitTime = System.currentTimeMillis() - start;
        Assert.assertTrue(waitTime >= 2000);
        Assert.assertEquals(false, invoker.isAvailable());
    }

    @Test
    public void test_NoInvokers() throws Exception {
        URL url = URL.valueOf("dubbo://127.0.0.1:20883/org.apache.dubbo.rpc.protocol.dubbo.IDemoService?connections=1");
        ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);
        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);
        ExchangeClient[] clients = getClients(invoker);
        clients[0].close();
        Assert.assertEquals(false, invoker.isAvailable());
    }

    @Test
    public void test_Lazy_ChannelReadOnly() throws Exception {
        URL url = URL.valueOf("dubbo://127.0.0.1:20883/org.apache.dubbo.rpc.protocol.dubbo.IDemoService?lazy=true&connections=1&timeout=10000");
        ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);
        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);
        Assert.assertEquals(true, invoker.isAvailable());
        try {
            getClients(invoker)[0].setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
            fail();
        } catch (IllegalStateException e) {
        }
        IDemoService service = (IDemoService) proxy.getProxy(invoker);
        Assert.assertEquals("ok", service.get());
        Assert.assertEquals(true, invoker.isAvailable());
        getClients(invoker)[0].setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
        Assert.assertEquals(false, invoker.isAvailable());
    }

    private ExchangeClient[] getClients(DubboInvoker<?> invoker) throws Exception {
        Field field = DubboInvoker.class.getDeclaredField("clients");
        field.setAccessible(true);
        ExchangeClient[] clients = (ExchangeClient[]) field.get(invoker);
        Assert.assertEquals(1, clients.length);
        return clients;
    }

    public class DemoServiceImpl implements IDemoService {

        public String get() {
            return "ok";
        }
    }

    public class DemoServiceImpl0 implements IDemoService {

        public String get() {
            return "ok";
        }
    }
}