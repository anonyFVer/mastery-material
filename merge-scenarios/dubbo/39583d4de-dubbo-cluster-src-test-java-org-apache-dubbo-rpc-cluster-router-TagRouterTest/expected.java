package org.apache.dubbo.rpc.cluster.router;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("FIXME This is not a formal UT")
public class TagRouterTest {

    private static CuratorFramework client;

    @Before
    public void init() {
        client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 60 * 1000, 60 * 1000, new ExponentialBackoffRetry(1000, 3));
        client.start();
    }

    @Test
    public void normalTagRuleTest() {
        String serviceStr = "---\n" + "force: false\n" + "runtime: true\n" + "enabled: false\n" + "priority: 1\n" + "key: demo-provider\n" + "tags:\n" + "  - name: tag1\n" + "    addresses: [\"30.5.120.37:20881\"]\n" + "  - name: tag2\n" + "    addresses: [\"30.5.120.37:20880\"]\n" + "...";
        try {
            String servicePath = "/dubbo/config/demo-provider/tag-router";
            if (client.checkExists().forPath(servicePath) == null) {
                client.create().creatingParentsIfNeeded().forPath(servicePath);
            }
            setData(servicePath, serviceStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setData(String path, String data) throws Exception {
        client.setData().forPath(path, data.getBytes());
    }
}