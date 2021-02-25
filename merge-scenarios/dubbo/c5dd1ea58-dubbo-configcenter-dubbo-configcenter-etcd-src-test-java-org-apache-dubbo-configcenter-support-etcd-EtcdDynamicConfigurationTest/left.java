package org.apache.dubbo.configcenter.support.etcd;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigChangeEvent;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.launcher.EtcdClusterFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.dubbo.remoting.etcd.Constants.SESSION_TIMEOUT_KEY;

public class EtcdDynamicConfigurationTest {

    private static EtcdDynamicConfiguration config;

    public EtcdCluster etcdCluster = EtcdClusterFactory.buildCluster(getClass().getSimpleName(), 3, false, false);

    private static Client client;

    @Test
    public void testGetConfig() {
        put("/dubbo/config/org.apache.dubbo.etcd.testService/configurators", "hello");
        put("/dubbo/config/test/dubbo.properties", "aaa=bbb");
        Assert.assertEquals("hello", config.getConfig("org.apache.dubbo.etcd.testService.configurators"));
        Assert.assertEquals("aaa=bbb", config.getConfig("dubbo.properties", "test"));
    }

    @Test
    public void testAddListener() throws Exception {
        CountDownLatch latch = new CountDownLatch(4);
        TestListener listener1 = new TestListener(latch);
        TestListener listener2 = new TestListener(latch);
        TestListener listener3 = new TestListener(latch);
        TestListener listener4 = new TestListener(latch);
        config.addListener("AService.configurators", listener1);
        config.addListener("AService.configurators", listener2);
        config.addListener("testapp.tagrouters", listener3);
        config.addListener("testapp.tagrouters", listener4);
        put("/dubbo/config/AService/configurators", "new value1");
        Thread.sleep(200);
        put("/dubbo/config/testapp/tagrouters", "new value2");
        Thread.sleep(200);
        put("/dubbo/config/testapp", "new value3");
        Thread.sleep(1000);
        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(1, listener1.getCount("/dubbo/config/AService/configurators"));
        Assert.assertEquals(1, listener2.getCount("/dubbo/config/AService/configurators"));
        Assert.assertEquals(1, listener3.getCount("/dubbo/config/testapp/tagrouters"));
        Assert.assertEquals(1, listener4.getCount("/dubbo/config/testapp/tagrouters"));
        Assert.assertEquals("new value1", listener1.getValue());
        Assert.assertEquals("new value1", listener2.getValue());
        Assert.assertEquals("new value2", listener3.getValue());
        Assert.assertEquals("new value2", listener4.getValue());
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
            Integer count = countMap.computeIfAbsent(event.getKey(), k -> 0);
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

    private void put(String key, String value) {
        try {
            client.getKVClient().put(ByteSequence.from(key, UTF_8), ByteSequence.from(value, UTF_8)).get();
        } catch (Exception e) {
            System.out.println("Error put value to etcd.");
        }
    }

    @Before
    public void setUp() {
        etcdCluster.start();
        client = Client.builder().endpoints(etcdCluster.getClientEndpoints()).build();
        List<URI> clientEndPoints = etcdCluster.getClientEndpoints();
        String ipAddress = clientEndPoints.get(0).getHost() + ":" + clientEndPoints.get(0).getPort();
        String urlForDubbo = "etcd3://" + ipAddress + "/org.apache.dubbo.etcd.testService";
        URL url = URL.valueOf(urlForDubbo).addParameter(SESSION_TIMEOUT_KEY, 15000);
        config = new EtcdDynamicConfiguration(url);
    }

    @After
    public void tearDown() {
        etcdCluster.close();
    }
}