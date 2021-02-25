package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.DubboAppender;
import org.apache.dubbo.common.utils.LogUtil;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.protocol.AsyncToSyncInvoker;
import org.apache.dubbo.rpc.protocol.dubbo.support.ProtocolUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class ReferenceCountExchangeClientTest {

    public static ProxyFactory proxy = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    private static DubboProtocol protocol = DubboProtocol.getDubboProtocol();

    Exporter<?> demoExporter;

    Exporter<?> helloExporter;

    Invoker<IDemoService> demoServiceInvoker;

    Invoker<IHelloService> helloServiceInvoker;

    IDemoService demoService;

    IHelloService helloService;

    ExchangeClient demoClient;

    ExchangeClient helloClient;

    String errorMsg = "safe guard client , should not be called ,must have a bug";

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterAll
    public static void tearDownAfterClass() {
        ProtocolUtils.closeAll();
    }

    public static Invoker<?> referInvoker(Class<?> type, URL url) {
        return (Invoker<?>) protocol.refer(type, url);
    }

    public static <T> Exporter<T> export(T instance, Class<T> type, String url) {
        return export(instance, type, URL.valueOf(url));
    }

    public static <T> Exporter<T> export(T instance, Class<T> type, URL url) {
        return protocol.export(proxy.getInvoker(instance, type, url));
    }

    @BeforeEach
    public void setUp() throws Exception {
    }

    @Test
    public void test_share_connect() {
        init(0, 1);
        Assertions.assertEquals(demoClient.getLocalAddress(), helloClient.getLocalAddress());
        Assertions.assertEquals(demoClient, helloClient);
        destoy();
    }

    @Test
    public void test_not_share_connect() {
        init(1, 1);
        Assertions.assertNotSame(demoClient.getLocalAddress(), helloClient.getLocalAddress());
        Assertions.assertNotSame(demoClient, helloClient);
        destoy();
    }

    @Test
    public void test_mult_share_connect() {
        final int shareConnectionNum = 3;
        init(0, shareConnectionNum);
        List<ReferenceCountExchangeClient> helloReferenceClientList = getReferenceClientList(helloServiceInvoker);
        Assertions.assertEquals(shareConnectionNum, helloReferenceClientList.size());
        List<ReferenceCountExchangeClient> demoReferenceClientList = getReferenceClientList(demoServiceInvoker);
        Assertions.assertEquals(shareConnectionNum, demoReferenceClientList.size());
        Assertions.assertTrue(Objects.equals(helloReferenceClientList, demoReferenceClientList));
        Assertions.assertEquals(demoClient.getLocalAddress(), helloClient.getLocalAddress());
        Assertions.assertEquals(demoClient, helloClient);
        destoy();
    }

    @Test
    public void test_multi_destory() {
        init(0, 1);
        DubboAppender.doStart();
        DubboAppender.clear();
        demoServiceInvoker.destroy();
        demoServiceInvoker.destroy();
        Assertions.assertEquals("hello", helloService.hello());
        Assertions.assertEquals(0, LogUtil.findMessage(errorMsg), "should not  warning message");
        LogUtil.checkNoError();
        DubboAppender.doStop();
        destoy();
    }

    @Test
    public void test_counter_error() {
        init(0, 1);
        DubboAppender.doStart();
        DubboAppender.clear();
        ReferenceCountExchangeClient client = getReferenceClient(helloServiceInvoker);
        client.close();
        Assertions.assertEquals("hello", helloService.hello());
        Assertions.assertEquals(0, LogUtil.findMessage(errorMsg), "should not warning message");
        client.close();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Assertions.fail();
        }
        Assertions.assertEquals("hello", helloService.hello());
        Assertions.assertEquals(1, LogUtil.findMessage(errorMsg), "should warning message");
        Assertions.assertEquals("hello", helloService.hello());
        Assertions.assertEquals(1, LogUtil.findMessage(errorMsg), "should warning message");
        DubboAppender.doStop();
        Assertions.assertEquals(true, helloServiceInvoker.isAvailable(), "client status available");
        client.close();
        Assertions.assertEquals(false, client.isClosed(), "client status close");
        Assertions.assertEquals(false, helloServiceInvoker.isAvailable(), "client status close");
        destoy();
    }

    @SuppressWarnings("unchecked")
    private void init(int connections, int shareConnections) {
        Assertions.assertTrue(connections >= 0);
        Assertions.assertTrue(shareConnections >= 1);
        int port = NetUtils.getAvailablePort();
        URL demoUrl = URL.valueOf("dubbo://127.0.0.1:" + port + "/demo?" + Constants.CONNECTIONS_KEY + "=" + connections + "&" + Constants.SHARE_CONNECTIONS_KEY + "=" + shareConnections);
        URL helloUrl = URL.valueOf("dubbo://127.0.0.1:" + port + "/hello?" + Constants.CONNECTIONS_KEY + "=" + connections + "&" + Constants.SHARE_CONNECTIONS_KEY + "=" + shareConnections);
        demoExporter = export(new DemoServiceImpl(), IDemoService.class, demoUrl);
        helloExporter = export(new HelloServiceImpl(), IHelloService.class, helloUrl);
        demoServiceInvoker = (Invoker<IDemoService>) referInvoker(IDemoService.class, demoUrl);
        demoService = proxy.getProxy(demoServiceInvoker);
        Assertions.assertEquals("demo", demoService.demo());
        helloServiceInvoker = (Invoker<IHelloService>) referInvoker(IHelloService.class, helloUrl);
        helloService = proxy.getProxy(helloServiceInvoker);
        Assertions.assertEquals("hello", helloService.hello());
        demoClient = getClient(demoServiceInvoker);
        helloClient = getClient(helloServiceInvoker);
    }

    private void destoy() {
        demoServiceInvoker.destroy();
        helloServiceInvoker.destroy();
        demoExporter.getInvoker().destroy();
        helloExporter.getInvoker().destroy();
    }

    private ExchangeClient getClient(Invoker<?> invoker) {
        if (invoker.getUrl().getParameter(Constants.CONNECTIONS_KEY, 1) == 1) {
            return getInvokerClient(invoker);
        } else {
            ReferenceCountExchangeClient client = getReferenceClient(invoker);
            try {
                Field clientField = ReferenceCountExchangeClient.class.getDeclaredField("client");
                clientField.setAccessible(true);
                return (ExchangeClient) clientField.get(client);
            } catch (Exception e) {
                e.printStackTrace();
                Assertions.fail(e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    private ReferenceCountExchangeClient getReferenceClient(Invoker<?> invoker) {
        return getReferenceClientList(invoker).get(0);
    }

    private List<ReferenceCountExchangeClient> getReferenceClientList(Invoker<?> invoker) {
        List<ExchangeClient> invokerClientList = getInvokerClientList(invoker);
        List<ReferenceCountExchangeClient> referenceCountExchangeClientList = new ArrayList<>(invokerClientList.size());
        for (ExchangeClient exchangeClient : invokerClientList) {
            Assertions.assertTrue(exchangeClient instanceof ReferenceCountExchangeClient);
            referenceCountExchangeClientList.add((ReferenceCountExchangeClient) exchangeClient);
        }
        return referenceCountExchangeClientList;
    }

    private ExchangeClient getInvokerClient(Invoker<?> invoker) {
        return getInvokerClientList(invoker).get(0);
    }

    private List<ExchangeClient> getInvokerClientList(Invoker<?> invoker) {
        @SuppressWarnings("rawtypes")
        DubboInvoker dInvoker = (DubboInvoker) ((AsyncToSyncInvoker) invoker).getInvoker();
        try {
            Field clientField = DubboInvoker.class.getDeclaredField("clients");
            clientField.setAccessible(true);
            ExchangeClient[] clients = (ExchangeClient[]) clientField.get(dInvoker);
            List<ExchangeClient> clientList = new ArrayList<ExchangeClient>(clients.length);
            for (ExchangeClient client : clients) {
                clientList.add(client);
            }
            Collections.sort(clientList, Comparator.comparing(c -> Integer.valueOf(Objects.hashCode(c))));
            return clientList;
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public interface IDemoService {

        public String demo();
    }

    public interface IHelloService {

        public String hello();
    }

    public class DemoServiceImpl implements IDemoService {

        public String demo() {
            return "demo";
        }
    }

    public class HelloServiceImpl implements IHelloService {

        public String hello() {
            return "hello";
        }
    }
}