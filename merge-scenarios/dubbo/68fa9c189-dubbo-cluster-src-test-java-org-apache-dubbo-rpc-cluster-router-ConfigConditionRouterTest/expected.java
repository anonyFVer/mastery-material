package org.apache.dubbo.rpc.cluster.router;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.Ignore;

@Ignore("FIXME This is not a formal UT")
public class ConfigConditionRouterTest {

    private static CuratorFramework client;

    @BeforeEach
    public void init() {
        client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", 60 * 1000, 60 * 1000, new ExponentialBackoffRetry(1000, 3));
        client.start();
    }

    @Test
    public void normalConditionRuleApplicationLevelTest() {
        String serviceStr = "---\n" + "scope: application\n" + "force: true\n" + "runtime: true\n" + "enabled: true\n" + "priority: 2\n" + "key: demo-consumer\n" + "conditions:\n" + "  - method=notExitMethod => \n" + "...";
        try {
            String servicePath = "/dubbo/config/demo-consumer/condition-router";
            if (client.checkExists().forPath(servicePath) == null) {
                client.create().creatingParentsIfNeeded().forPath(servicePath);
            }
            setData(servicePath, serviceStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void normalConditionRuleApplicationServiceLevelTest() {
        String serviceStr = "---\n" + "scope: application\n" + "force: true\n" + "runtime: false\n" + "enabled: true\n" + "priority: 2\n" + "key: demo-consumer\n" + "conditions:\n" + "  - interface=org.apache.dubbo.demo.DemoService&method=sayHello => host=30.5.120.37\n" + "  - method=routeMethod1 => host=30.5.120.37\n" + "...";
        try {
            String servicePath = "/dubbo/config/demo-consumer/condition-router";
            if (client.checkExists().forPath(servicePath) == null) {
                client.create().creatingParentsIfNeeded().forPath(servicePath);
            }
            setData(servicePath, serviceStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void normalConditionRuleServiceLevelTest() {
        String serviceStr = "---\n" + "scope: service\n" + "force: true\n" + "runtime: true\n" + "enabled: true\n" + "priority: 1\n" + "key: org.apache.dubbo.demo.DemoService\n" + "conditions:\n" + "  - method!=sayHello =>\n" + "  - method=routeMethod1 => address=30.5.120.37:20880\n" + "...";
        try {
            String servicePath = "/dubbo/config/org.apache.dubbo.demo.DemoService/condition-router";
            if (client.checkExists().forPath(servicePath) == null) {
                client.create().creatingParentsIfNeeded().forPath(servicePath);
            }
            setData(servicePath, serviceStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void abnormalNoruleConditionRuleTest() {
        String serviceStr = "---\n" + "scope: service\n" + "force: true\n" + "runtime: false\n" + "enabled: true\n" + "priority: 1\n" + "key: org.apache.dubbo.demo.DemoService\n" + "...";
        try {
            String servicePath = "/dubbo/config/org.apache.dubbo.demo.DemoService/condition-router";
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