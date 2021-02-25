package org.apache.dubbo.remoting.zookeeper.curator;

import org.apache.curator.test.TestingServer;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;

public class CuratorZookeeperTransporterTest {

    private TestingServer zkServer;

    private ZookeeperClient zookeeperClient;

    private CuratorZookeeperTransporter curatorZookeeperTransporter;

    private int zkServerPort;

    @Before
    public void setUp() throws Exception {
        zkServerPort = NetUtils.getAvailablePort();
        zkServer = new TestingServer(zkServerPort, true);
        zookeeperClient = new CuratorZookeeperTransporter().connect(URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort + "/service"));
        curatorZookeeperTransporter = new CuratorZookeeperTransporter();
    }

    @After
    public void tearDown() throws Exception {
        zkServer.stop();
    }

    @Test
    public void testZookeeperClient() {
        assertThat(zookeeperClient, not(nullValue()));
        zookeeperClient.close();
    }
}