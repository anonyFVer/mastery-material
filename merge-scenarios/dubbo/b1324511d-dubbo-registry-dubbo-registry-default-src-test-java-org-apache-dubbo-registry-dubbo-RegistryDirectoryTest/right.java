package org.apache.dubbo.registry.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.LogUtil;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.RegistryFactory;
import org.apache.dubbo.registry.integration.RegistryDirectory;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.loadbalance.LeastActiveLoadBalance;
import org.apache.dubbo.rpc.cluster.loadbalance.RoundRobinLoadBalance;
import org.apache.dubbo.rpc.cluster.router.script.ScriptRouterFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import javax.script.ScriptEngineManager;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import static org.junit.Assert.fail;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class RegistryDirectoryTest {

    private static boolean isScriptUnsupported = new ScriptEngineManager().getEngineByName("javascript") == null;

    RegistryFactory registryFactory = ExtensionLoader.getExtensionLoader(RegistryFactory.class).getAdaptiveExtension();

    Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();

    String service = DemoService.class.getName();

    RpcInvocation invocation = new RpcInvocation();

    URL noMeaningUrl = URL.valueOf("notsupport:/" + service + "?refer=" + URL.encode("interface=" + service));

    URL SERVICEURL = URL.valueOf("dubbo://127.0.0.1:9091/" + service + "?lazy=true&side=consumer");

    URL SERVICEURL2 = URL.valueOf("dubbo://127.0.0.1:9092/" + service + "?lazy=true&side=consumer");

    URL SERVICEURL3 = URL.valueOf("dubbo://127.0.0.1:9093/" + service + "?lazy=true&side=consumer");

    URL SERVICEURL_DUBBO_NOPATH = URL.valueOf("dubbo://127.0.0.1:9092" + "?lazy=true&side=consumer");

    @Before
    public void setUp() {
    }

    private RegistryDirectory getRegistryDirectory(URL url) {
        RegistryDirectory registryDirectory = new RegistryDirectory(URL.class, url);
        registryDirectory.setProtocol(protocol);
        List invokers = registryDirectory.list(invocation);
        Assert.assertEquals(0, invokers.size());
        Assert.assertEquals(false, registryDirectory.isAvailable());
        return registryDirectory;
    }

    private RegistryDirectory getRegistryDirectory() {
        return getRegistryDirectory(noMeaningUrl);
    }

    @Test
    public void test_Constructor_WithErrorParam() {
        try {
            new RegistryDirectory(null, null);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            new RegistryDirectory(null, noMeaningUrl);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            new RegistryDirectory(RegistryDirectoryTest.class, URL.valueOf("dubbo://10.20.30.40:9090"));
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void test_Constructor_CheckStatus() throws Exception {
        URL url = URL.valueOf("notsupported://10.20.30.40/" + service + "?a=b").addParameterAndEncoded(Constants.REFER_KEY, "foo=bar");
        RegistryDirectory reg = getRegistryDirectory(url);
        Field field = reg.getClass().getDeclaredField("queryMap");
        field.setAccessible(true);
        Map<String, String> queryMap = (Map<String, String>) field.get(reg);
        Assert.assertEquals("bar", queryMap.get("foo"));
        Assert.assertEquals(url.clearParameters().addParameter("foo", "bar"), reg.getUrl());
    }

    @Test
    public void testNotified_Normal() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        test_Notified2invokers(registryDirectory);
        test_Notified1invokers(registryDirectory);
        test_Notified3invokers(registryDirectory);
        testforbid(registryDirectory);
    }

    @Test
    public void testNotified_Normal_withRouters() {
        LogUtil.start();
        RegistryDirectory registryDirectory = getRegistryDirectory();
        test_Notified1invokers(registryDirectory);
        test_Notified_only_routers(registryDirectory);
        Assert.assertEquals(true, registryDirectory.isAvailable());
        Assert.assertTrue("notify no invoker urls ,should not error", LogUtil.checkNoError());
        LogUtil.stop();
        test_Notified2invokers(registryDirectory);
    }

    @Test
    public void testNotified_WithError() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        List<URL> serviceUrls = new ArrayList<URL>();
        URL badurl = URL.valueOf("notsupported://127.0.0.1/" + service);
        serviceUrls.add(badurl);
        serviceUrls.add(SERVICEURL);
        registryDirectory.notify(serviceUrls);
        Assert.assertEquals(true, registryDirectory.isAvailable());
        List invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
    }

    @Test
    public void testNotified_WithDuplicateUrls() {
        List<URL> serviceUrls = new ArrayList<URL>();
        serviceUrls.add(SERVICEURL);
        serviceUrls.add(SERVICEURL);
        RegistryDirectory registryDirectory = getRegistryDirectory();
        registryDirectory.notify(serviceUrls);
        List invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
    }

    private void testforbid(RegistryDirectory registryDirectory) {
        invocation = new RpcInvocation();
        List<URL> serviceUrls = new ArrayList<URL>();
        serviceUrls.add(new URL(Constants.EMPTY_PROTOCOL, Constants.ANYHOST_VALUE, 0, service, Constants.CATEGORY_KEY, Constants.PROVIDERS_CATEGORY));
        registryDirectory.notify(serviceUrls);
        Assert.assertEquals("invokers size=0 ,then the registry directory is not available", false, registryDirectory.isAvailable());
        try {
            registryDirectory.list(invocation);
            fail("forbid must throw RpcException");
        } catch (RpcException e) {
            Assert.assertEquals(RpcException.FORBIDDEN_EXCEPTION, e.getCode());
        }
    }

    @Test
    public void test_NotifiedDubbo1() {
        URL errorPathUrl = URL.valueOf("notsupport:/" + "xxx" + "?refer=" + URL.encode("interface=" + service));
        RegistryDirectory registryDirectory = getRegistryDirectory(errorPathUrl);
        List<URL> serviceUrls = new ArrayList<URL>();
        URL Dubbo1URL = URL.valueOf("dubbo://127.0.0.1:9098?lazy=true");
        serviceUrls.add(Dubbo1URL.addParameter("methods", "getXXX"));
        registryDirectory.notify(serviceUrls);
        Assert.assertEquals(true, registryDirectory.isAvailable());
        invocation = new RpcInvocation();
        List<Invoker<DemoService>> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
        invocation.setMethodName("getXXX");
        invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
        Assert.assertEquals(DemoService.class.getName(), invokers.get(0).getUrl().getPath());
    }

    private void test_Notified_only_routers(RegistryDirectory registryDirectory) {
        List<URL> serviceUrls = new ArrayList<URL>();
        serviceUrls.add(URL.valueOf("empty://127.0.0.1/?category=routers"));
        registryDirectory.notify(serviceUrls);
    }

    private void test_Notified1invokers(RegistryDirectory registryDirectory) {
        List<URL> serviceUrls = new ArrayList<URL>();
        serviceUrls.add(SERVICEURL.addParameter("methods", "getXXX1"));
        registryDirectory.notify(serviceUrls);
        Assert.assertEquals(true, registryDirectory.isAvailable());
        invocation = new RpcInvocation();
        List invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
        invocation.setMethodName("getXXX");
        invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
        invocation.setMethodName("getXXX1");
        invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
        invocation.setMethodName("getXXX2");
        invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
    }

    private void test_Notified2invokers(RegistryDirectory registryDirectory) {
        List<URL> serviceUrls = new ArrayList<URL>();
        serviceUrls.add(SERVICEURL.addParameter("methods", "getXXX1"));
        serviceUrls.add(SERVICEURL2.addParameter("methods", "getXXX1,getXXX2"));
        serviceUrls.add(SERVICEURL2.addParameter("methods", "getXXX1,getXXX2"));
        registryDirectory.notify(serviceUrls);
        Assert.assertEquals(true, registryDirectory.isAvailable());
        invocation = new RpcInvocation();
        List invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
        invocation.setMethodName("getXXX");
        invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
        invocation.setMethodName("getXXX1");
        invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
        invocation.setMethodName("getXXX2");
        invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
    }

    private void test_Notified3invokers(RegistryDirectory registryDirectory) {
        List<URL> serviceUrls = new ArrayList<URL>();
        serviceUrls.add(SERVICEURL.addParameter("methods", "getXXX1"));
        serviceUrls.add(SERVICEURL2.addParameter("methods", "getXXX1,getXXX2"));
        serviceUrls.add(SERVICEURL3.addParameter("methods", "getXXX1,getXXX2,getXXX3"));
        registryDirectory.notify(serviceUrls);
        Assert.assertEquals(true, registryDirectory.isAvailable());
        invocation = new RpcInvocation();
        List invokers = registryDirectory.list(invocation);
        Assert.assertEquals(3, invokers.size());
        invocation.setMethodName("getXXX");
        invokers = registryDirectory.list(invocation);
        Assert.assertEquals(3, invokers.size());
        invocation.setMethodName("getXXX1");
        invokers = registryDirectory.list(invocation);
        Assert.assertEquals(3, invokers.size());
        invocation.setMethodName("getXXX2");
        invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
        invocation.setMethodName("getXXX3");
        invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
    }

    @Test
    public void testParametersMerge() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        URL regurl = noMeaningUrl.addParameter("test", "reg").addParameterAndEncoded(Constants.REFER_KEY, "key=query&" + Constants.LOADBALANCE_KEY + "=" + LeastActiveLoadBalance.NAME);
        RegistryDirectory<RegistryDirectoryTest> registryDirectory2 = new RegistryDirectory(RegistryDirectoryTest.class, regurl);
        registryDirectory2.setProtocol(protocol);
        List<URL> serviceUrls = new ArrayList<URL>();
        {
            serviceUrls.clear();
            serviceUrls.add(SERVICEURL.addParameter("methods", "getXXX1"));
            registryDirectory.notify(serviceUrls);
            invocation = new RpcInvocation();
            List invokers = registryDirectory.list(invocation);
            Invoker invoker = (Invoker) invokers.get(0);
            URL url = invoker.getUrl();
            Assert.assertEquals(null, url.getParameter("key"));
        }
        {
            serviceUrls.clear();
            serviceUrls.add(SERVICEURL.addParameter("methods", "getXXX2").addParameter("key", "provider"));
            registryDirectory.notify(serviceUrls);
            invocation = new RpcInvocation();
            List invokers = registryDirectory.list(invocation);
            Invoker invoker = (Invoker) invokers.get(0);
            URL url = invoker.getUrl();
            Assert.assertEquals("provider", url.getParameter("key"));
        }
        {
            serviceUrls.clear();
            serviceUrls.add(SERVICEURL.addParameter("methods", "getXXX3").addParameter("key", "provider"));
            registryDirectory2.notify(serviceUrls);
            invocation = new RpcInvocation();
            List invokers = registryDirectory2.list(invocation);
            Invoker invoker = (Invoker) invokers.get(0);
            URL url = invoker.getUrl();
            Assert.assertEquals("query", url.getParameter("key"));
        }
        {
            serviceUrls.clear();
            serviceUrls.add(SERVICEURL.addParameter("methods", "getXXX1"));
            registryDirectory.notify(serviceUrls);
            invocation = new RpcInvocation();
            List invokers = registryDirectory.list(invocation);
            Invoker invoker = (Invoker) invokers.get(0);
            URL url = invoker.getUrl();
            Assert.assertEquals(false, url.getParameter(Constants.CHECK_KEY, false));
        }
        {
            serviceUrls.clear();
            serviceUrls.add(SERVICEURL.addParameter(Constants.LOADBALANCE_KEY, RoundRobinLoadBalance.NAME));
            registryDirectory2.notify(serviceUrls);
            invocation = new RpcInvocation();
            invocation.setMethodName("get");
            List invokers = registryDirectory2.list(invocation);
            Invoker invoker = (Invoker) invokers.get(0);
            URL url = invoker.getUrl();
            Assert.assertEquals(LeastActiveLoadBalance.NAME, url.getMethodParameter("get", Constants.LOADBALANCE_KEY));
        }
        {
            Assert.assertEquals(null, registryDirectory2.getUrl().getParameter("mock"));
            serviceUrls.clear();
            serviceUrls.add(SERVICEURL.addParameter(Constants.MOCK_KEY, "true"));
            registryDirectory2.notify(serviceUrls);
            Assert.assertEquals("true", registryDirectory2.getUrl().getParameter("mock"));
        }
    }

    @Test
    public void testDestroy() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        List<URL> serviceUrls = new ArrayList<URL>();
        serviceUrls.add(SERVICEURL.addParameter("methods", "getXXX1"));
        serviceUrls.add(SERVICEURL2.addParameter("methods", "getXXX1,getXXX2"));
        serviceUrls.add(SERVICEURL3.addParameter("methods", "getXXX1,getXXX2,getXXX3"));
        registryDirectory.notify(serviceUrls);
        List<Invoker> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(true, registryDirectory.isAvailable());
        Assert.assertEquals(true, invokers.get(0).isAvailable());
        registryDirectory.destroy();
        Assert.assertEquals(false, registryDirectory.isAvailable());
        Assert.assertEquals(false, invokers.get(0).isAvailable());
        registryDirectory.destroy();
        List<Invoker<RegistryDirectoryTest>> cachedInvokers = registryDirectory.getInvokers();
        Map<String, Invoker<RegistryDirectoryTest>> urlInvokerMap = registryDirectory.getUrlInvokerMap();
        Assert.assertTrue(cachedInvokers == null);
        Assert.assertEquals(0, urlInvokerMap.size());
        RpcInvocation inv = new RpcInvocation();
        try {
            registryDirectory.list(inv);
            fail();
        } catch (RpcException e) {
            Assert.assertTrue(e.getMessage().contains("already destroyed"));
        }
    }

    @Test
    public void testDestroy_WithDestroyRegistry() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        CountDownLatch latch = new CountDownLatch(1);
        registryDirectory.setRegistry(new MockRegistry(latch));
        registryDirectory.subscribe(URL.valueOf("consumer://" + NetUtils.getLocalHost() + "/DemoService?category=providers"));
        registryDirectory.destroy();
        Assert.assertEquals(0, latch.getCount());
    }

    @Test
    public void testDestroy_WithDestroyRegistry_WithError() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        registryDirectory.setRegistry(new MockRegistry(true));
        registryDirectory.destroy();
    }

    @Test
    public void testDubbo1UrlWithGenericInvocation() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        List<URL> serviceUrls = new ArrayList<URL>();
        URL serviceURL = SERVICEURL_DUBBO_NOPATH.addParameter("methods", "getXXX1,getXXX2,getXXX3");
        serviceUrls.add(serviceURL);
        registryDirectory.notify(serviceUrls);
        invocation = new RpcInvocation(Constants.$INVOKE, new Class[] { String.class, String[].class, Object[].class }, new Object[] { "getXXX1", "", new Object[] {} });
        List<Invoker> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
        Assert.assertEquals(serviceURL.setPath(service).addParameters("check", "false", "interface", DemoService.class.getName()), invokers.get(0).getUrl());
    }

    @Test
    public void testParmeterRoute() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        List<URL> serviceUrls = new ArrayList<URL>();
        serviceUrls.add(SERVICEURL.addParameter("methods", "getXXX1.napoli"));
        serviceUrls.add(SERVICEURL2.addParameter("methods", "getXXX1.MORGAN,getXXX2"));
        serviceUrls.add(SERVICEURL3.addParameter("methods", "getXXX1.morgan,getXXX2,getXXX3"));
        registryDirectory.notify(serviceUrls);
        invocation = new RpcInvocation(Constants.$INVOKE, new Class[] { String.class, String[].class, Object[].class }, new Object[] { "getXXX1", new String[] { "Enum" }, new Object[] { Param.MORGAN } });
        List invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
    }

    @Test
    public void testEmptyNotifyCauseForbidden() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        List invokers = null;
        List<URL> serviceUrls = new ArrayList<URL>();
        registryDirectory.notify(serviceUrls);
        RpcInvocation inv = new RpcInvocation();
        try {
            invokers = registryDirectory.list(inv);
        } catch (RpcException e) {
            Assert.assertEquals(RpcException.FORBIDDEN_EXCEPTION, e.getCode());
            Assert.assertEquals(false, registryDirectory.isAvailable());
        }
        serviceUrls.add(SERVICEURL.addParameter("methods", "getXXX1"));
        serviceUrls.add(SERVICEURL2.addParameter("methods", "getXXX1,getXXX2"));
        serviceUrls.add(SERVICEURL3.addParameter("methods", "getXXX1,getXXX2,getXXX3"));
        registryDirectory.notify(serviceUrls);
        inv.setMethodName("getXXX2");
        invokers = registryDirectory.list(inv);
        Assert.assertEquals(true, registryDirectory.isAvailable());
        Assert.assertEquals(2, invokers.size());
    }

    @Test
    public void testNotifyRouterUrls() {
        if (isScriptUnsupported)
            return;
        RegistryDirectory registryDirectory = getRegistryDirectory();
        URL routerurl = URL.valueOf(Constants.ROUTE_PROTOCOL + "://127.0.0.1:9096/");
        URL routerurl2 = URL.valueOf(Constants.ROUTE_PROTOCOL + "://127.0.0.1:9097/");
        List<URL> serviceUrls = new ArrayList<URL>();
        serviceUrls.add(routerurl.addParameter(Constants.CATEGORY_KEY, Constants.ROUTERS_CATEGORY).addParameter(Constants.TYPE_KEY, "javascript").addParameter(Constants.ROUTER_KEY, "notsupported").addParameter(Constants.RULE_KEY, "function test1(){}"));
        serviceUrls.add(routerurl2.addParameter(Constants.CATEGORY_KEY, Constants.ROUTERS_CATEGORY).addParameter(Constants.TYPE_KEY, "javascript").addParameter(Constants.ROUTER_KEY, ScriptRouterFactory.NAME).addParameter(Constants.RULE_KEY, "function test1(){}"));
    }

    @Test
    public void testNotifyoverrideUrls_beforeInvoker() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        List<URL> overrideUrls = new ArrayList<URL>();
        overrideUrls.add(URL.valueOf("override://0.0.0.0?timeout=1&connections=5"));
        registryDirectory.notify(overrideUrls);
        Assert.assertEquals(false, registryDirectory.isAvailable());
        List<URL> serviceUrls = new ArrayList<URL>();
        serviceUrls.add(SERVICEURL.addParameter("timeout", "1000"));
        serviceUrls.add(SERVICEURL2.addParameter("timeout", "1000").addParameter("connections", "10"));
        registryDirectory.notify(serviceUrls);
        Assert.assertEquals(true, registryDirectory.isAvailable());
        invocation = new RpcInvocation();
        List<Invoker<?>> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
        Assert.assertEquals("override rute must be first priority", "1", invokers.get(0).getUrl().getParameter("timeout"));
        Assert.assertEquals("override rute must be first priority", "5", invokers.get(0).getUrl().getParameter("connections"));
    }

    @Test
    public void testNotifyoverrideUrls_afterInvoker() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        List<URL> serviceUrls = new ArrayList<URL>();
        serviceUrls.add(SERVICEURL.addParameter("timeout", "1000"));
        serviceUrls.add(SERVICEURL2.addParameter("timeout", "1000").addParameter("connections", "10"));
        registryDirectory.notify(serviceUrls);
        Assert.assertEquals(true, registryDirectory.isAvailable());
        List<URL> overrideUrls = new ArrayList<URL>();
        overrideUrls.add(URL.valueOf("override://0.0.0.0?timeout=1&connections=5"));
        registryDirectory.notify(overrideUrls);
        invocation = new RpcInvocation();
        List<Invoker<?>> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
        Assert.assertEquals("override rute must be first priority", "1", invokers.get(0).getUrl().getParameter("timeout"));
        Assert.assertEquals("override rute must be first priority", "5", invokers.get(0).getUrl().getParameter("connections"));
    }

    @Test
    public void testNotifyoverrideUrls_withInvoker() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        List<URL> durls = new ArrayList<URL>();
        durls.add(SERVICEURL.addParameter("timeout", "1000"));
        durls.add(SERVICEURL2.addParameter("timeout", "1000").addParameter("connections", "10"));
        durls.add(URL.valueOf("override://0.0.0.0?timeout=1&connections=5"));
        registryDirectory.notify(durls);
        Assert.assertEquals(true, registryDirectory.isAvailable());
        invocation = new RpcInvocation();
        List<Invoker<?>> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
        Assert.assertEquals("override rute must be first priority", "1", invokers.get(0).getUrl().getParameter("timeout"));
        Assert.assertEquals("override rute must be first priority", "5", invokers.get(0).getUrl().getParameter("connections"));
    }

    @Test
    public void testNotifyoverrideUrls_Nouse() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        invocation = new RpcInvocation();
        List<URL> durls = new ArrayList<URL>();
        durls.add(SERVICEURL.addParameter("timeout", "1"));
        durls.add(SERVICEURL2.addParameter("timeout", "1").addParameter("connections", "5"));
        registryDirectory.notify(durls);
        List<Invoker<?>> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
        Invoker<?> a1Invoker = invokers.get(0);
        Invoker<?> b1Invoker = invokers.get(1);
        durls = new ArrayList<URL>();
        durls.add(URL.valueOf("override://0.0.0.0?timeout=1&connections=5"));
        registryDirectory.notify(durls);
        Assert.assertEquals(true, registryDirectory.isAvailable());
        invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
        Invoker<?> a2Invoker = invokers.get(0);
        Invoker<?> b2Invoker = invokers.get(1);
        Assert.assertFalse("object not same", a1Invoker == a2Invoker);
        Assert.assertTrue("object same", b1Invoker == b2Invoker);
    }

    @Test
    public void testNofityOverrideUrls_Provider() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        invocation = new RpcInvocation();
        List<URL> durls = new ArrayList<URL>();
        durls.add(SERVICEURL.setHost("10.20.30.140").addParameter("timeout", "1").addParameter(Constants.SIDE_KEY, Constants.CONSUMER_SIDE));
        durls.add(SERVICEURL2.setHost("10.20.30.141").addParameter("timeout", "2").addParameter(Constants.SIDE_KEY, Constants.CONSUMER_SIDE));
        registryDirectory.notify(durls);
        durls = new ArrayList<URL>();
        durls.add(URL.valueOf("override://0.0.0.0?timeout=3"));
        durls.add(URL.valueOf("override://10.20.30.141:9092?timeout=4"));
        registryDirectory.notify(durls);
        List<Invoker<?>> invokers = registryDirectory.list(invocation);
        Invoker<?> aInvoker = invokers.get(0);
        Invoker<?> bInvoker = invokers.get(1);
        Assert.assertEquals("3", aInvoker.getUrl().getParameter("timeout"));
        Assert.assertEquals("4", bInvoker.getUrl().getParameter("timeout"));
    }

    @Test
    public void testNofityOverrideUrls_Clean1() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        invocation = new RpcInvocation();
        List<URL> durls = new ArrayList<URL>();
        durls.add(SERVICEURL.setHost("10.20.30.140").addParameter("timeout", "1"));
        registryDirectory.notify(durls);
        durls = new ArrayList<URL>();
        durls.add(URL.valueOf("override://0.0.0.0?timeout=1000"));
        registryDirectory.notify(durls);
        durls = new ArrayList<URL>();
        durls.add(URL.valueOf("override://0.0.0.0?timeout=3"));
        durls.add(URL.valueOf("override://0.0.0.0"));
        registryDirectory.notify(durls);
        List<Invoker<?>> invokers = registryDirectory.list(invocation);
        Invoker<?> aInvoker = invokers.get(0);
        Assert.assertEquals("1", aInvoker.getUrl().getParameter("timeout"));
    }

    @Test
    public void testNofityOverrideUrls_CleanOnly() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        invocation = new RpcInvocation();
        List<URL> durls = new ArrayList<URL>();
        durls.add(SERVICEURL.setHost("10.20.30.140").addParameter("timeout", "1"));
        registryDirectory.notify(durls);
        Assert.assertEquals(null, registryDirectory.getUrl().getParameter("mock"));
        durls = new ArrayList<URL>();
        durls.add(URL.valueOf("override://0.0.0.0?timeout=1000&mock=fail"));
        registryDirectory.notify(durls);
        List<Invoker<?>> invokers = registryDirectory.list(invocation);
        Invoker<?> aInvoker = invokers.get(0);
        Assert.assertEquals("1000", aInvoker.getUrl().getParameter("timeout"));
        Assert.assertEquals("fail", registryDirectory.getUrl().getParameter("mock"));
        durls = new ArrayList<URL>();
        durls.add(URL.valueOf("override://0.0.0.0/dubbo.test.api.HelloService"));
        registryDirectory.notify(durls);
        invokers = registryDirectory.list(invocation);
        aInvoker = invokers.get(0);
        Assert.assertEquals("1", aInvoker.getUrl().getParameter("timeout"));
        Assert.assertEquals(null, registryDirectory.getUrl().getParameter("mock"));
    }

    @Test
    public void testNofityOverrideUrls_CleanNOverride() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        invocation = new RpcInvocation();
        List<URL> durls = new ArrayList<URL>();
        durls.add(SERVICEURL.setHost("10.20.30.140").addParameter("timeout", "1"));
        registryDirectory.notify(durls);
        durls = new ArrayList<URL>();
        durls.add(URL.valueOf("override://0.0.0.0?timeout=3"));
        durls.add(URL.valueOf("override://0.0.0.0"));
        durls.add(URL.valueOf("override://10.20.30.140:9091?timeout=4"));
        registryDirectory.notify(durls);
        List<Invoker<?>> invokers = registryDirectory.list(invocation);
        Invoker<?> aInvoker = invokers.get(0);
        Assert.assertEquals("4", aInvoker.getUrl().getParameter("timeout"));
    }

    @Test
    public void testNofityOverrideUrls_disabled_allProvider() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        invocation = new RpcInvocation();
        List<URL> durls = new ArrayList<URL>();
        durls.add(SERVICEURL.setHost("10.20.30.140"));
        durls.add(SERVICEURL.setHost("10.20.30.141"));
        registryDirectory.notify(durls);
        durls = new ArrayList<URL>();
        durls.add(URL.valueOf("override://0.0.0.0?" + Constants.ENABLED_KEY + "=false"));
        registryDirectory.notify(durls);
        List<Invoker<?>> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
    }

    @Test
    public void testNofityOverrideUrls_disabled_specifiedProvider() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        invocation = new RpcInvocation();
        List<URL> durls = new ArrayList<URL>();
        durls.add(SERVICEURL.setHost("10.20.30.140"));
        durls.add(SERVICEURL.setHost("10.20.30.141"));
        registryDirectory.notify(durls);
        durls = new ArrayList<URL>();
        durls.add(URL.valueOf("override://10.20.30.140:9091?" + Constants.DISABLED_KEY + "=true"));
        registryDirectory.notify(durls);
        List<Invoker<?>> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
        Assert.assertEquals("10.20.30.141", invokers.get(0).getUrl().getHost());
        durls = new ArrayList<URL>();
        durls.add(URL.valueOf("empty://0.0.0.0?" + Constants.DISABLED_KEY + "=true&" + Constants.CATEGORY_KEY + "=" + Constants.CONFIGURATORS_CATEGORY));
        registryDirectory.notify(durls);
        List<Invoker<?>> invokers2 = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers2.size());
    }

    @Test
    public void testNofity_To_Decrease_provider() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        invocation = new RpcInvocation();
        List<URL> durls = new ArrayList<URL>();
        durls.add(SERVICEURL.setHost("10.20.30.140"));
        durls.add(SERVICEURL.setHost("10.20.30.141"));
        registryDirectory.notify(durls);
        List<Invoker<?>> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
        durls = new ArrayList<URL>();
        durls.add(SERVICEURL.setHost("10.20.30.140"));
        registryDirectory.notify(durls);
        List<Invoker<?>> invokers2 = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers2.size());
        Assert.assertEquals("10.20.30.140", invokers.get(0).getUrl().getHost());
        durls = new ArrayList<URL>();
        durls.add(URL.valueOf("empty://0.0.0.0?" + Constants.DISABLED_KEY + "=true&" + Constants.CATEGORY_KEY + "=" + Constants.CONFIGURATORS_CATEGORY));
        registryDirectory.notify(durls);
        List<Invoker<?>> invokers3 = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers3.size());
    }

    @Test
    public void testNofity_disabled_specifiedProvider() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        invocation = new RpcInvocation();
        List<URL> durls = new ArrayList<URL>();
        durls.add(SERVICEURL.setHost("10.20.30.140").addParameter(Constants.ENABLED_KEY, "false"));
        durls.add(SERVICEURL.setHost("10.20.30.141"));
        registryDirectory.notify(durls);
        List<Invoker<?>> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
        Assert.assertEquals("10.20.30.141", invokers.get(0).getUrl().getHost());
        durls = new ArrayList<URL>();
        durls.add(URL.valueOf("override://10.20.30.140:9091?" + Constants.DISABLED_KEY + "=false"));
        registryDirectory.notify(durls);
        List<Invoker<?>> invokers2 = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers2.size());
    }

    @Test
    public void testNotifyRouterUrls_Clean() {
        if (isScriptUnsupported)
            return;
        RegistryDirectory registryDirectory = getRegistryDirectory();
        URL routerurl = URL.valueOf(Constants.ROUTE_PROTOCOL + "://127.0.0.1:9096/").addParameter(Constants.ROUTER_KEY, "javascript").addParameter(Constants.RULE_KEY, "function test1(){}").addParameter(Constants.ROUTER_KEY, "script");
        List<URL> serviceUrls = new ArrayList<URL>();
        serviceUrls.add(routerurl);
        registryDirectory.notify(serviceUrls);
    }

    @Test
    public void testNotify_MockProviderOnly() {
        RegistryDirectory registryDirectory = getRegistryDirectory();
        List<URL> serviceUrls = new ArrayList<URL>();
        serviceUrls.add(SERVICEURL.addParameter("methods", "getXXX1"));
        serviceUrls.add(SERVICEURL2.addParameter("methods", "getXXX1,getXXX2"));
        serviceUrls.add(SERVICEURL.setProtocol(Constants.MOCK_PROTOCOL));
        registryDirectory.notify(serviceUrls);
        Assert.assertEquals(true, registryDirectory.isAvailable());
        invocation = new RpcInvocation();
        List invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
        RpcInvocation mockinvocation = new RpcInvocation();
        mockinvocation.setAttachment(Constants.INVOCATION_NEED_MOCK, "true");
        invokers = registryDirectory.list(mockinvocation);
        Assert.assertEquals(1, invokers.size());
    }

    @Test
    public void test_Notified_acceptProtocol0() {
        URL errorPathUrl = URL.valueOf("notsupport:/xxx?refer=" + URL.encode("interface=" + service));
        RegistryDirectory registryDirectory = getRegistryDirectory(errorPathUrl);
        List<URL> serviceUrls = new ArrayList<URL>();
        URL dubbo1URL = URL.valueOf("dubbo://127.0.0.1:9098?lazy=true&methods=getXXX");
        URL dubbo2URL = URL.valueOf("injvm://127.0.0.1:9099?lazy=true&methods=getXXX");
        serviceUrls.add(dubbo1URL);
        serviceUrls.add(dubbo2URL);
        registryDirectory.notify(serviceUrls);
        invocation = new RpcInvocation();
        List<Invoker<DemoService>> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
    }

    @Test
    public void test_Notified_acceptProtocol1() {
        URL errorPathUrl = URL.valueOf("notsupport:/xxx");
        errorPathUrl = errorPathUrl.addParameterAndEncoded(Constants.REFER_KEY, "interface=" + service + "&protocol=dubbo");
        RegistryDirectory registryDirectory = getRegistryDirectory(errorPathUrl);
        List<URL> serviceUrls = new ArrayList<URL>();
        URL dubbo1URL = URL.valueOf("dubbo://127.0.0.1:9098?lazy=true&methods=getXXX");
        URL dubbo2URL = URL.valueOf("injvm://127.0.0.1:9098?lazy=true&methods=getXXX");
        serviceUrls.add(dubbo1URL);
        serviceUrls.add(dubbo2URL);
        registryDirectory.notify(serviceUrls);
        invocation = new RpcInvocation();
        List<Invoker<DemoService>> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(1, invokers.size());
    }

    @Test
    public void test_Notified_acceptProtocol2() {
        URL errorPathUrl = URL.valueOf("notsupport:/xxx");
        errorPathUrl = errorPathUrl.addParameterAndEncoded(Constants.REFER_KEY, "interface=" + service + "&protocol=dubbo,injvm");
        RegistryDirectory registryDirectory = getRegistryDirectory(errorPathUrl);
        List<URL> serviceUrls = new ArrayList<URL>();
        URL dubbo1URL = URL.valueOf("dubbo://127.0.0.1:9098?lazy=true&methods=getXXX");
        URL dubbo2URL = URL.valueOf("injvm://127.0.0.1:9099?lazy=true&methods=getXXX");
        serviceUrls.add(dubbo1URL);
        serviceUrls.add(dubbo2URL);
        registryDirectory.notify(serviceUrls);
        invocation = new RpcInvocation();
        List<Invoker<DemoService>> invokers = registryDirectory.list(invocation);
        Assert.assertEquals(2, invokers.size());
    }

    enum Param {

        MORGAN
    }

    private static interface DemoService {
    }

    private static class MockRegistry implements Registry {

        CountDownLatch latch;

        boolean destroyWithError;

        public MockRegistry(CountDownLatch latch) {
            this.latch = latch;
        }

        public MockRegistry(boolean destroyWithError) {
            this.destroyWithError = destroyWithError;
        }

        @Override
        public void register(URL url) {
        }

        @Override
        public void unregister(URL url) {
        }

        @Override
        public void subscribe(URL url, NotifyListener listener) {
        }

        @Override
        public void unsubscribe(URL url, NotifyListener listener) {
            if (latch != null)
                latch.countDown();
        }

        @Override
        public List<URL> lookup(URL url) {
            return null;
        }

        public URL getUrl() {
            return null;
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public void destroy() {
            if (destroyWithError) {
                throw new RpcException("test exception ignore.");
            }
        }
    }
}