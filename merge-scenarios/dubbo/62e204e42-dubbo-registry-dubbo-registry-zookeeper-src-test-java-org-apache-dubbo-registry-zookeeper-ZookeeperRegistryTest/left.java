package org.apache.dubbo.registry.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.status.Status;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.status.RegistryStatusChecker;
import org.apache.dubbo.remoting.zookeeper.curator.CuratorZookeeperTransporter;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

public class ZookeeperRegistryTest {

    private TestingServer zkServer;

    private ZookeeperRegistry zookeeperRegistry;

    private String service = "org.apache.dubbo.test.injvmServie";

    private URL serviceUrl = URL.valueOf("zookeeper://zookeeper/" + service + "?notify=false&methods=test1,test2");

    private URL anyUrl = URL.valueOf("zookeeper://zookeeper/*");

    private URL registryUrl;

    private ZookeeperRegistryFactory zookeeperRegistryFactory;

    @BeforeEach
    public void setUp() throws Exception {
        int zkServerPort = NetUtils.getAvailablePort();
        this.zkServer = new TestingServer(zkServerPort, true);
        this.zkServer.start();
        this.registryUrl = URL.valueOf("zookeeper://localhost:" + zkServerPort);
        zookeeperRegistryFactory = new ZookeeperRegistryFactory();
        zookeeperRegistryFactory.setZookeeperTransporter(new CuratorZookeeperTransporter());
        this.zookeeperRegistry = (ZookeeperRegistry) zookeeperRegistryFactory.createRegistry(registryUrl);
    }

    @AfterEach
    public void tearDown() throws Exception {
        zkServer.stop();
    }

    @Test
    public void testAnyHost() {
        Assertions.assertThrows(IllegalStateException.class, () -> {
            URL errorUrl = URL.valueOf("multicast://0.0.0.0/");
            new ZookeeperRegistryFactory().createRegistry(errorUrl);
        });
    }

    @Test
    public void testRegister() {
        Set<URL> registered;
        for (int i = 0; i < 2; i++) {
            zookeeperRegistry.register(serviceUrl);
            registered = zookeeperRegistry.getRegistered();
            assertThat(registered.contains(serviceUrl), is(true));
        }
        registered = zookeeperRegistry.getRegistered();
        assertThat(registered.size(), is(1));
    }

    @Test
    public void testSubscribe() {
        NotifyListener listener = mock(NotifyListener.class);
        zookeeperRegistry.subscribe(serviceUrl, listener);
        Map<URL, Set<NotifyListener>> subscribed = zookeeperRegistry.getSubscribed();
        assertThat(subscribed.size(), is(1));
        assertThat(subscribed.get(serviceUrl).size(), is(1));
        zookeeperRegistry.unsubscribe(serviceUrl, listener);
        subscribed = zookeeperRegistry.getSubscribed();
        assertThat(subscribed.size(), is(1));
        assertThat(subscribed.get(serviceUrl).size(), is(0));
    }

    @Test
    public void testAvailable() {
        zookeeperRegistry.register(serviceUrl);
        assertThat(zookeeperRegistry.isAvailable(), is(true));
        zookeeperRegistry.destroy();
        assertThat(zookeeperRegistry.isAvailable(), is(false));
    }

    @Test
    public void testLookup() {
        List<URL> lookup = zookeeperRegistry.lookup(serviceUrl);
        assertThat(lookup.size(), is(0));
        zookeeperRegistry.register(serviceUrl);
        lookup = zookeeperRegistry.lookup(serviceUrl);
        assertThat(lookup.size(), is(1));
    }

    @Disabled
    @Test
    public void testStatusChecker() {
        RegistryStatusChecker registryStatusChecker = new RegistryStatusChecker();
        Status status = registryStatusChecker.check();
        assertThat(status.getLevel(), is(Status.Level.UNKNOWN));
        Registry registry = zookeeperRegistryFactory.getRegistry(registryUrl);
        assertThat(registry, not(nullValue()));
        status = registryStatusChecker.check();
        assertThat(status.getLevel(), is(Status.Level.ERROR));
        registry.register(serviceUrl);
        status = registryStatusChecker.check();
        assertThat(status.getLevel(), is(Status.Level.OK));
    }

    @Test
    public void testSubscribeAnyValue() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        zookeeperRegistry.register(serviceUrl);
        zookeeperRegistry.subscribe(anyUrl, urls -> latch.countDown());
        zookeeperRegistry.register(serviceUrl);
        latch.await();
    }
}