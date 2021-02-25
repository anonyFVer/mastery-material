package org.apache.dubbo.metadata.store.test;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.apache.dubbo.metadata.store.MetadataReport;
import org.apache.dubbo.metadata.support.AbstractMetadataReportFactory;

public class JTestMetadataReportFactory4Test extends AbstractMetadataReportFactory {

    private ZookeeperTransporter zookeeperTransporter;

    public void setZookeeperTransporter(ZookeeperTransporter zookeeperTransporter) {
        this.zookeeperTransporter = zookeeperTransporter;
    }

    @Override
    public MetadataReport createMetadataReport(URL url) {
        return new JTestMetadataReport4Test(url, zookeeperTransporter);
    }
}