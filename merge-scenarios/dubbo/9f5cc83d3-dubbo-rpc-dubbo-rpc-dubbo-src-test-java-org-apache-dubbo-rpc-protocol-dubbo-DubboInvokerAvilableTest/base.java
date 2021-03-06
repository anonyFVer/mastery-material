package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.protocol.dubbo.support.ProtocolUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Field;
import static org.junit.jupiter.api.Assertions.fail;

public class DubboInvokerAvilableTest {

    private static DubboProtocol protocol = DubboProtocol.getDubboProtocol();

    private static ProxyFactory proxy = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
    }

    @BeforeEach
    public void setUp() throws Exception {
    }

    @AfterAll
    public static void tearDownAfterClass() {
        ProtocolUtils.closeAll();
    }

    @Test
    public void test_Normal_available() {
        URL url = URL.valueOf("dubbo://127.0.0.1:20883/org.apache.dubbo.rpc.protocol.dubbo.IDemoService");
        ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);
        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);
        Assertions.assertEquals(true, invoker.isAvailable());
        invoker.destroy();
        Assertions.assertEquals(false, invoker.isAvailable());
    }

    @Test
    public void test_Normal_ChannelReadOnly() throws Exception {
        URL url = URL.valueOf("dubbo://127.0.0.1:20883/org.apache.dubbo.rpc.protocol.dubbo.IDemoService");
        ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);
        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);
        Assertions.assertEquals(true, invoker.isAvailable());
        getClients(invoker)[0].setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
        Assertions.assertEquals(false, invoker.isAvailable());
        getClients(invoker)[0].removeAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY);
    }

    @Disabled
    public void test_normal_channel_close_wait_gracefully() throws Exception {
        int testPort = NetUtils.getAvailablePort();
        URL url = URL.valueOf("dubbo://127.0.0.1:" + testPort + "/org.apache.dubbo.rpc.protocol.dubbo.IDemoService?scope=true&lazy=false");
        Exporter<IDemoService> exporter = ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);
        Exporter<IDemoService> exporter0 = ProtocolUtils.export(new DemoServiceImpl0(), IDemoService.class, url);
        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);
        long start = System.currentTimeMillis();
        try {
            System.setProperty(Constants.SHUTDOWN_WAIT_KEY, "2000");
            protocol.destroy();
        } finally {
            System.getProperties().remove(Constants.SHUTDOWN_WAIT_KEY);
        }
        long waitTime = System.currentTimeMillis() - start;
        Assertions.assertTrue(waitTime >= 2000);
        Assertions.assertEquals(false, invoker.isAvailable());
    }

    @Test
    public void test_NoInvokers() throws Exception {
        URL url = URL.valueOf("dubbo://127.0.0.1:20883/org.apache.dubbo.rpc.protocol.dubbo.IDemoService?connections=1");
        ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);
        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);
        ExchangeClient[] clients = getClients(invoker);
        clients[0].close();
        Assertions.assertEquals(false, invoker.isAvailable());
    }

    @Test
    public void test_Lazy_ChannelReadOnly() throws Exception {
        URL url = URL.valueOf("dubbo://127.0.0.1:20883/org.apache.dubbo.rpc.protocol.dubbo.IDemoService?lazy=true&connections=1&timeout=10000");
        ProtocolUtils.export(new DemoServiceImpl(), IDemoService.class, url);
        DubboInvoker<?> invoker = (DubboInvoker<?>) protocol.refer(IDemoService.class, url);
        Assertions.assertEquals(true, invoker.isAvailable());
        try {
            getClients(invoker)[0].setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
            fail();
        } catch (IllegalStateException e) {
        }
        IDemoService service = (IDemoService) proxy.getProxy(invoker);
        Assertions.assertEquals("ok", service.get());
        Assertions.assertEquals(true, invoker.isAvailable());
        getClients(invoker)[0].setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
        Assertions.assertEquals(false, invoker.isAvailable());
    }

    private ExchangeClient[] getClients(DubboInvoker<?> invoker) throws Exception {
        Field field = DubboInvoker.class.getDeclaredField("clients");
        field.setAccessible(true);
        ExchangeClient[] clients = (ExchangeClient[]) field.get(invoker);
        Assertions.assertEquals(1, clients.length);
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