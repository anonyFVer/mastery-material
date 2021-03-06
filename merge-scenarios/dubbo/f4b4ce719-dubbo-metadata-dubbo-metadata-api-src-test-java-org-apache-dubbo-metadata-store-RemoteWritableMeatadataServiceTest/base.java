package org.apache.dubbo.metadata.store;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.metadata.WritableMetadataService;
import org.apache.dubbo.metadata.definition.model.FullServiceDefinition;
import org.apache.dubbo.metadata.report.MetadataReportInstance;
import org.apache.dubbo.metadata.test.JTestMetadataReport4Test;
import com.google.gson.Gson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.apache.dubbo.common.constants.CommonConstants.METADATA_REMOTE;

public class RemoteWritableMeatadataServiceTest {

    URL url = URL.valueOf("JTest://" + NetUtils.getLocalAddress().getHostName() + ":4444/org.apache.dubbo.TestService?version=1.0.0&application=vic");

    RemoteWritableMetadataService metadataReportService1;

    @BeforeEach
    public void before() {
        metadataReportService1 = (RemoteWritableMetadataService) WritableMetadataService.getExtension(METADATA_REMOTE);
        MetadataReportInstance.init(url);
    }

    @Test
    public void testInstance() {
        RemoteWritableMetadataService metadataReportService2 = (RemoteWritableMetadataService) WritableMetadataService.getExtension(METADATA_REMOTE);
        Assertions.assertSame(metadataReportService1, metadataReportService2);
    }

    @Test
    public void testPublishProviderNoInterfaceName() {
        URL publishUrl = URL.valueOf("dubbo://" + NetUtils.getLocalAddress().getHostName() + ":4444/org.apache.dubbo.TestService?version=1.0.0&application=vicpubprovder&side=provider");
        metadataReportService1.publishProvider(publishUrl);
        Assertions.assertTrue(metadataReportService1.getMetadataReport() instanceof JTestMetadataReport4Test);
        JTestMetadataReport4Test jTestMetadataReport4Test = (JTestMetadataReport4Test) metadataReportService1.getMetadataReport();
        Assertions.assertTrue(!jTestMetadataReport4Test.store.containsKey(JTestMetadataReport4Test.getProviderKey(publishUrl)));
    }

    @Test
    public void testPublishProviderWrongInterface() {
        URL publishUrl = URL.valueOf("dubbo://" + NetUtils.getLocalAddress().getHostName() + ":4444/org.apache.dubbo.TestService?version=1.0.0&application=vicpu&interface=ccc&side=provider");
        metadataReportService1.publishProvider(publishUrl);
        Assertions.assertTrue(metadataReportService1.getMetadataReport() instanceof JTestMetadataReport4Test);
        JTestMetadataReport4Test jTestMetadataReport4Test = (JTestMetadataReport4Test) metadataReportService1.getMetadataReport();
        Assertions.assertTrue(!jTestMetadataReport4Test.store.containsKey(JTestMetadataReport4Test.getProviderKey(publishUrl)));
    }

    @Test
    public void testPublishProviderContainInterface() throws InterruptedException {
        URL publishUrl = URL.valueOf("dubbo://" + NetUtils.getLocalAddress().getHostName() + ":4444/org.apache.dubbo.TestService?version=1.0.3&application=vicpubp&interface=org.apache.dubbo.metadata.store.InterfaceNameTestService&side=provider");
        metadataReportService1.publishProvider(publishUrl);
        Thread.sleep(300);
        Assertions.assertTrue(metadataReportService1.getMetadataReport() instanceof JTestMetadataReport4Test);
        JTestMetadataReport4Test jTestMetadataReport4Test = (JTestMetadataReport4Test) metadataReportService1.getMetadataReport();
        Assertions.assertTrue(jTestMetadataReport4Test.store.containsKey(JTestMetadataReport4Test.getProviderKey(publishUrl)));
        String value = jTestMetadataReport4Test.store.get(JTestMetadataReport4Test.getProviderKey(publishUrl));
        FullServiceDefinition fullServiceDefinition = toServiceDefinition(value);
        Map<String, String> map = fullServiceDefinition.getParameters();
        Assertions.assertEquals(map.get("application"), "vicpubp");
        Assertions.assertEquals(map.get("version"), "1.0.3");
        Assertions.assertEquals(map.get("interface"), "org.apache.dubbo.metadata.store.InterfaceNameTestService");
    }

    @Test
    public void testPublishConsumer() throws InterruptedException {
        URL publishUrl = URL.valueOf("dubbo://" + NetUtils.getLocalAddress().getHostName() + ":4444/org.apache.dubbo.TestService?version=1.0.x&application=vicpubconsumer&side=consumer");
        metadataReportService1.publishConsumer(publishUrl);
        Thread.sleep(300);
        Assertions.assertTrue(metadataReportService1.getMetadataReport() instanceof JTestMetadataReport4Test);
        JTestMetadataReport4Test jTestMetadataReport4Test = (JTestMetadataReport4Test) metadataReportService1.getMetadataReport();
        Assertions.assertTrue(jTestMetadataReport4Test.store.containsKey(JTestMetadataReport4Test.getConsumerKey(publishUrl)));
        String value = jTestMetadataReport4Test.store.get(JTestMetadataReport4Test.getConsumerKey(publishUrl));
        Gson gson = new Gson();
        Map<String, String> map = gson.fromJson(value, Map.class);
        Assertions.assertEquals(map.get("application"), "vicpubconsumer");
        Assertions.assertEquals(map.get("version"), "1.0.x");
    }

    private FullServiceDefinition toServiceDefinition(String urlQuery) {
        Gson gson = new Gson();
        return gson.fromJson(urlQuery, FullServiceDefinition.class);
    }
}