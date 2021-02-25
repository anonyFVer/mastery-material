package org.apache.dubbo.remoting.zookeeper.curator;

import org.apache.curator.test.TestingServer;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;

public class CuratorZookeeperTransporterTest {

    private TestingServer zkServer;

    private ZookeeperClient zookeeperClient;

    private CuratorZookeeperTransporter curatorZookeeperTransporter;

    private int zkServerPort;

    @BeforeEach
    public void setUp() throws Exception {
        zkServerPort = NetUtils.getAvailablePort();
        zkServer = new TestingServer(zkServerPort, true);
        zookeeperClient = new CuratorZookeeperTransporter().connect(URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort + "/service"));
        curatorZookeeperTransporter = new CuratorZookeeperTransporter();
    }

    @Test
    public void testZookeeperClient() {
        assertThat(zookeeperClient, not(nullValue()));
        zookeeperClient.close();
    }

    @AfterEach
    public void tearDown() throws Exception {
        zkServer.stop();
    }
}