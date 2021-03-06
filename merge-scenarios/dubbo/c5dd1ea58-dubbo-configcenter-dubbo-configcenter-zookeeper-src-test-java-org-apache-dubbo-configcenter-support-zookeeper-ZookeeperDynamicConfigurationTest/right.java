package org.apache.dubbo.configcenter.support.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.configcenter.ConfigChangeEvent;
import org.apache.dubbo.configcenter.ConfigurationListener;
import org.apache.dubbo.configcenter.DynamicConfiguration;
import org.apache.dubbo.configcenter.DynamicConfigurationFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ZookeeperDynamicConfigurationTest {

    private static CuratorFramework client;

    private static URL configUrl;

    private static int zkServerPort = NetUtils.getAvailablePort();

    private static TestingServer zkServer;

    private static DynamicConfiguration configuration;

    @BeforeAll
    public static void setUp() throws Exception {
        zkServer = new TestingServer(zkServerPort, true);
        client = CuratorFrameworkFactory.newClient("localhost:" + zkServerPort, 60 * 1000, 60 * 1000, new ExponentialBackoffRetry(1000, 3));
        client.start();
        try {
            setData("/dubbo/config/dubbo/dubbo.properties", "The content from dubbo.properties");
            setData("/dubbo/config/dubbo/service:version:group.configurators", "The content from configurators");
            setData("/dubbo/config/appname", "The content from higer level node");
            setData("/dubbo/config/dubbo/appname.tag-router", "The content from appname tagrouters");
            setData("/dubbo/config/dubbo/never.change.DemoService.configurators", "Never change value from configurators");
        } catch (Exception e) {
            e.printStackTrace();
        }
        configUrl = URL.valueOf("zookeeper://localhost:" + zkServerPort);
        configuration = ExtensionLoader.getExtensionLoader(DynamicConfigurationFactory.class).getExtension(configUrl.getProtocol()).getDynamicConfiguration(configUrl);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        zkServer.stop();
    }

    private static void setData(String path, String data) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path);
        }
        client.setData().forPath(path, data.getBytes());
    }

    @Test
    public void testGetConfig() throws Exception {
        Assertions.assertEquals("The content from dubbo.properties", configuration.getProperties("dubbo.properties", "dubbo"));
    }

    @Test
    public void testAddListener() throws Exception {
        CountDownLatch latch = new CountDownLatch(4);
        TestListener listener1 = new TestListener(latch);
        TestListener listener2 = new TestListener(latch);
        TestListener listener3 = new TestListener(latch);
        TestListener listener4 = new TestListener(latch);
        configuration.addListener("service:version:group.configurators", listener1);
        configuration.addListener("service:version:group.configurators", listener2);
        configuration.addListener("appname.tag-router", listener3);
        configuration.addListener("appname.tag-router", listener4);
        setData("/dubbo/config/dubbo/service:version:group.configurators", "new value1");
        Thread.sleep(100);
        setData("/dubbo/config/dubbo/appname.tag-router", "new value2");
        Thread.sleep(100);
        setData("/dubbo/config/appname", "new value3");
        Thread.sleep(5000);
        latch.await();
        Assertions.assertEquals(1, listener1.getCount("service:version:group.configurators"));
        Assertions.assertEquals(1, listener2.getCount("service:version:group.configurators"));
        Assertions.assertEquals(1, listener3.getCount("appname.tag-router"));
        Assertions.assertEquals(1, listener4.getCount("appname.tag-router"));
        Assertions.assertEquals("new value1", listener1.getValue());
        Assertions.assertEquals("new value1", listener2.getValue());
        Assertions.assertEquals("new value2", listener3.getValue());
        Assertions.assertEquals("new value2", listener4.getValue());
    }

    private class TestListener implements ConfigurationListener {

        private CountDownLatch latch;

        private String value;

        private Map<String, Integer> countMap = new HashMap<>();

        public TestListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void process(ConfigChangeEvent event) {
            System.out.println(this + ": " + event);
            Integer count = countMap.computeIfAbsent(event.getKey(), k -> new Integer(0));
            countMap.put(event.getKey(), ++count);
            value = event.getValue();
            latch.countDown();
        }

        public int getCount(String key) {
            return countMap.get(key);
        }

        public String getValue() {
            return value;
        }
    }
}