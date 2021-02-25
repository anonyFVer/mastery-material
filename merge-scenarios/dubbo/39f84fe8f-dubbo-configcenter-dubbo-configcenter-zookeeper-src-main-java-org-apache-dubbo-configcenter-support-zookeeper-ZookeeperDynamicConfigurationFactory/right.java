package org.apache.dubbo.configcenter.support.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.AbstractDynamicConfigurationFactory;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;

public class ZookeeperDynamicConfigurationFactory extends AbstractDynamicConfigurationFactory {

    private ZookeeperTransporter zookeeperTransporter;

    public void setZookeeperTransporter(ZookeeperTransporter zookeeperTransporter) {
        this.zookeeperTransporter = zookeeperTransporter;
    }

    @Override
    protected DynamicConfiguration createDynamicConfiguration(URL url) {
        return new ZookeeperDynamicConfiguration(url, zookeeperTransporter);
    }
}