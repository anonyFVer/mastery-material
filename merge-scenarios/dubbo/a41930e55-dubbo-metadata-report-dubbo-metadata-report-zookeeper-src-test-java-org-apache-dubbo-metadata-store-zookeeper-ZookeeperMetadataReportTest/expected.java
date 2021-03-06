package org.apache.dubbo.metadata.store.zookeeper;

import com.google.gson.Gson;
import org.apache.curator.test.TestingServer;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.metadata.definition.ServiceDefinitionBuilder;
import org.apache.dubbo.metadata.definition.model.FullServiceDefinition;
import org.apache.dubbo.metadata.identifier.ConsumerMetadataIdentifier;
import org.apache.dubbo.metadata.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.identifier.ProviderMetadataIdentifier;
import org.apache.dubbo.remoting.zookeeper.curator.CuratorZookeeperTransporter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.List;

public class ZookeeperMetadataReportTest {

    private TestingServer zkServer;

    private ZookeeperMetadataReport zookeeperMetadataReport;

    private URL registryUrl;

    private ZookeeperMetadataReportFactory zookeeperMetadataReportFactory;

    @Before
    public void setUp() throws Exception {
        int zkServerPort = NetUtils.getAvailablePort();
        this.zkServer = new TestingServer(zkServerPort, true);
        this.registryUrl = URL.valueOf("zookeeper://localhost:" + zkServerPort);
        zookeeperMetadataReportFactory = new ZookeeperMetadataReportFactory();
        zookeeperMetadataReportFactory.setZookeeperTransporter(new CuratorZookeeperTransporter());
        this.zookeeperMetadataReport = (ZookeeperMetadataReport) zookeeperMetadataReportFactory.createMetadataReport(registryUrl);
    }

    @After
    public void tearDown() throws Exception {
        zkServer.stop();
    }

    private void deletePath(MetadataIdentifier metadataIdentifier, ZookeeperMetadataReport zookeeperMetadataReport) {
        String category = zookeeperMetadataReport.toRootDir() + metadataIdentifier.getFilePathKey();
        zookeeperMetadataReport.deletePath(category);
    }

    @Test
    public void testStoreProvider() throws ClassNotFoundException {
        String interfaceName = "org.apache.dubbo.metadata.store.zookeeper.ZookeeperMetadataReport4TstService";
        String version = "1.0.0.zk.md";
        String group = null;
        String application = "vic.zk.md";
        ProviderMetadataIdentifier providerMetadataIdentifier = storePrivider(zookeeperMetadataReport, interfaceName, version, group, application);
        List<String> files = zookeeperMetadataReport.zkClient.getChildren(zookeeperMetadataReport.getCategory(providerMetadataIdentifier));
        Assert.assertTrue(!files.isEmpty());
        deletePath(providerMetadataIdentifier, zookeeperMetadataReport);
        files = zookeeperMetadataReport.zkClient.getChildren(zookeeperMetadataReport.getCategory(providerMetadataIdentifier));
        Assert.assertTrue(files.isEmpty());
        providerMetadataIdentifier = storePrivider(zookeeperMetadataReport, interfaceName, version, group, application);
        files = zookeeperMetadataReport.zkClient.getChildren(zookeeperMetadataReport.getCategory(providerMetadataIdentifier));
        Assert.assertTrue(files.size() == 1);
        String result = URL.decode(files.get(0));
        Gson gson = new Gson();
        FullServiceDefinition fullServiceDefinition = gson.fromJson(result, FullServiceDefinition.class);
        Assert.assertEquals(fullServiceDefinition.getParameters().get("paramTest"), "zkTest");
    }

    @Test
    public void testConsumer() throws ClassNotFoundException {
        String interfaceName = "org.apache.dubbo.metadata.store.zookeeper.ZookeeperMetadataReport4TstService";
        String version = "1.0.0.zk.md";
        String group = null;
        String application = "vic.zk.md";
        ConsumerMetadataIdentifier consumerMetadataIdentifier = storeConsumer(zookeeperMetadataReport, interfaceName, version, group, application);
        List<String> files = zookeeperMetadataReport.zkClient.getChildren(zookeeperMetadataReport.getCategory(consumerMetadataIdentifier));
        Assert.assertTrue(!files.isEmpty());
        deletePath(consumerMetadataIdentifier, zookeeperMetadataReport);
        files = zookeeperMetadataReport.zkClient.getChildren(zookeeperMetadataReport.getCategory(consumerMetadataIdentifier));
        Assert.assertTrue(files.isEmpty());
        consumerMetadataIdentifier = storeConsumer(zookeeperMetadataReport, interfaceName, version, group, application);
        files = zookeeperMetadataReport.zkClient.getChildren(zookeeperMetadataReport.getCategory(consumerMetadataIdentifier));
        Assert.assertTrue(files.size() == 1);
        String result = URL.decode(files.get(0));
        Assert.assertEquals(result, "paramConsumerTest=zkCm");
    }

    private ProviderMetadataIdentifier storePrivider(ZookeeperMetadataReport zookeeperMetadataReport, String interfaceName, String version, String group, String application) throws ClassNotFoundException {
        URL url = URL.valueOf("xxx://" + NetUtils.getLocalAddress().getHostName() + ":4444/" + interfaceName + "?paramTest=zkTest&version=" + version + "&application=" + application + (group == null ? "" : "&group=" + group));
        ProviderMetadataIdentifier providerMetadataIdentifier = new ProviderMetadataIdentifier(interfaceName, version, group);
        Class interfaceClass = Class.forName(interfaceName);
        FullServiceDefinition fullServiceDefinition = ServiceDefinitionBuilder.buildFullDefinition(interfaceClass, url.getParameters());
        zookeeperMetadataReport.storeProviderMetadata(providerMetadataIdentifier, fullServiceDefinition);
        return providerMetadataIdentifier;
    }

    private ConsumerMetadataIdentifier storeConsumer(ZookeeperMetadataReport zookeeperMetadataReport, String interfaceName, String version, String group, String application) throws ClassNotFoundException {
        URL url = URL.valueOf("xxx://" + NetUtils.getLocalAddress().getHostName() + ":4444/" + interfaceName + "?version=" + version + "&application=" + application + (group == null ? "" : "&group=" + group));
        ConsumerMetadataIdentifier consumerMetadataIdentifier = new ConsumerMetadataIdentifier(interfaceName, version, group, application);
        Class interfaceClass = Class.forName(interfaceName);
        zookeeperMetadataReport.storeConsumerMetadata(consumerMetadataIdentifier, "paramConsumerTest=zkCm");
        return consumerMetadataIdentifier;
    }
}