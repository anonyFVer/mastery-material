package org.apache.dubbo.metadata.support;

import com.google.gson.Gson;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.metadata.definition.ServiceDefinitionBuilder;
import org.apache.dubbo.metadata.definition.model.FullServiceDefinition;
import org.apache.dubbo.metadata.identifier.ConsumerMetadataIdentifier;
import org.apache.dubbo.metadata.identifier.ProviderMetadataIdentifier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AbstractMetadataReportTest {

    private NewMetadataReport abstractMetadataReport;

    @Before
    public void before() {
        URL url = URL.valueOf("zookeeper://" + NetUtils.getLocalAddress().getHostName() + ":4444/org.apache.dubbo.TestService?version=1.0.0&application=vic");
        abstractMetadataReport = new NewMetadataReport(url);
    }

    @Test
    public void testGetProtocol() {
        URL url = URL.valueOf("dubbo://" + NetUtils.getLocalAddress().getHostName() + ":4444/org.apache.dubbo.TestService?version=1.0.0&application=vic&side=provider");
        String protocol = abstractMetadataReport.getProtocol(url);
        Assert.assertEquals(protocol, "provider");
        URL url2 = URL.valueOf("consumer://" + NetUtils.getLocalAddress().getHostName() + ":4444/org.apache.dubbo.TestService?version=1.0.0&application=vic");
        String protocol2 = abstractMetadataReport.getProtocol(url2);
        Assert.assertEquals(protocol2, "consumer");
    }

    @Test
    public void testStoreProviderUsual() throws ClassNotFoundException {
        String interfaceName = "org.apache.dubbo.metadata.integration.InterfaceNameTestService";
        String version = "1.0.0";
        String group = null;
        String application = "vic";
        ProviderMetadataIdentifier providerMetadataIdentifier = storePrivider(abstractMetadataReport, interfaceName, version, group, application);
        Assert.assertNotNull(abstractMetadataReport.store.get(providerMetadataIdentifier.getIdentifierKey()));
    }

    @Test
    public void testFileExistAfterPut() throws InterruptedException, ClassNotFoundException {
        URL singleUrl = URL.valueOf("redis://" + NetUtils.getLocalAddress().getHostName() + ":4444/org.apache.dubbo.metadata.integration.InterfaceNameTestService?version=1.0.0&application=singleTest");
        NewMetadataReport singleMetadataReport = new NewMetadataReport(singleUrl);
        Assert.assertFalse(singleMetadataReport.file.exists());
        String interfaceName = "org.apache.dubbo.metadata.integration.InterfaceNameTestService";
        String version = "1.0.0";
        String group = null;
        String application = "vic";
        ProviderMetadataIdentifier providerMetadataIdentifier = storePrivider(singleMetadataReport, interfaceName, version, group, application);
        Thread.sleep(2000);
        Assert.assertTrue(singleMetadataReport.file.exists());
        Assert.assertTrue(singleMetadataReport.properties.containsKey(providerMetadataIdentifier.getIdentifierKey()));
    }

    @Test
    public void testRetry() throws InterruptedException, ClassNotFoundException {
        String interfaceName = "org.apache.dubbo.metadata.integration.RetryTestService";
        String version = "1.0.0.retry";
        String group = null;
        String application = "vic.retry";
        URL storeUrl = URL.valueOf("retryReport://" + NetUtils.getLocalAddress().getHostName() + ":4444/org.apache.dubbo.TestServiceForRetry?version=1.0.0.retry&application=vic.retry");
        RetryMetadataReport retryReport = new RetryMetadataReport(storeUrl, 2);
        retryReport.metadataReportRetry.retryPeriod = 200L;
        URL url = URL.valueOf("dubbo://" + NetUtils.getLocalAddress().getHostName() + ":4444/org.apache.dubbo.TestService?version=1.0.0&application=vic");
        Assert.assertNull(retryReport.metadataReportRetry.retryScheduledFuture);
        Assert.assertTrue(retryReport.metadataReportRetry.retryCounter.get() == 0);
        Assert.assertTrue(retryReport.store.isEmpty());
        Assert.assertTrue(retryReport.failedReports.isEmpty());
        storePrivider(retryReport, interfaceName, version, group, application);
        Assert.assertTrue(retryReport.store.isEmpty());
        Assert.assertFalse(retryReport.failedReports.isEmpty());
        Assert.assertNotNull(retryReport.metadataReportRetry.retryScheduledFuture);
        Thread.sleep(1200L);
        Assert.assertTrue(retryReport.metadataReportRetry.retryCounter.get() != 0);
        Assert.assertTrue(retryReport.metadataReportRetry.retryCounter.get() >= 3);
        Assert.assertFalse(retryReport.store.isEmpty());
        Assert.assertTrue(retryReport.failedReports.isEmpty());
    }

    @Test
    public void testRetryCancel() throws InterruptedException, ClassNotFoundException {
        String interfaceName = "org.apache.dubbo.metadata.integration.RetryTestService";
        String version = "1.0.0.retrycancel";
        String group = null;
        String application = "vic.retry";
        URL storeUrl = URL.valueOf("retryReport://" + NetUtils.getLocalAddress().getHostName() + ":4444/org.apache.dubbo.TestServiceForRetryCancel?version=1.0.0.retrycancel&application=vic.retry");
        RetryMetadataReport retryReport = new RetryMetadataReport(storeUrl, 2);
        retryReport.metadataReportRetry.retryPeriod = 150L;
        retryReport.metadataReportRetry.retryTimesIfNonFail = 2;
        storePrivider(retryReport, interfaceName, version, group, application);
        Assert.assertFalse(retryReport.metadataReportRetry.retryScheduledFuture.isCancelled());
        Assert.assertFalse(retryReport.metadataReportRetry.retryExecutor.isShutdown());
        Thread.sleep(1000L);
        Assert.assertTrue(retryReport.metadataReportRetry.retryScheduledFuture.isCancelled());
        Assert.assertTrue(retryReport.metadataReportRetry.retryExecutor.isShutdown());
    }

    private ProviderMetadataIdentifier storePrivider(AbstractMetadataReport abstractMetadataReport, String interfaceName, String version, String group, String application) throws ClassNotFoundException {
        URL url = URL.valueOf("xxx://" + NetUtils.getLocalAddress().getHostName() + ":4444/" + interfaceName + "?version=" + version + "&application=" + application + (group == null ? "" : "&group=" + group) + "&testPKey=8989");
        ProviderMetadataIdentifier providerMetadataIdentifier = new ProviderMetadataIdentifier(interfaceName, version, group);
        Class interfaceClass = Class.forName(interfaceName);
        FullServiceDefinition fullServiceDefinition = ServiceDefinitionBuilder.buildFullDefinition(interfaceClass, url.getParameters());
        abstractMetadataReport.storeProviderMetadata(providerMetadataIdentifier, fullServiceDefinition);
        return providerMetadataIdentifier;
    }

    private ConsumerMetadataIdentifier storeConsumer(AbstractMetadataReport abstractMetadataReport, String interfaceName, String version, String group, String application, String storeValue) throws ClassNotFoundException {
        URL url = URL.valueOf("xxx://" + NetUtils.getLocalAddress().getHostName() + ":4444/" + interfaceName + "?version=" + version + "&application=" + application + (group == null ? "" : "&group=" + group) + "&testPKey=9090");
        ConsumerMetadataIdentifier consumerMetadataIdentifier = new ConsumerMetadataIdentifier(interfaceName, version, group, application);
        abstractMetadataReport.storeConsumerMetadata(consumerMetadataIdentifier, storeValue + "&" + url.toParameterString());
        return consumerMetadataIdentifier;
    }

    @Test
    public void testPublishAll() throws ClassNotFoundException {
        Assert.assertTrue(abstractMetadataReport.store.isEmpty());
        Assert.assertTrue(abstractMetadataReport.allMetadataReports.isEmpty());
        String interfaceName = "org.apache.dubbo.metadata.integration.InterfaceNameTestService";
        String version = "1.0.0";
        String group = null;
        String application = "vic";
        ProviderMetadataIdentifier providerMetadataIdentifier1 = storePrivider(abstractMetadataReport, interfaceName, version, group, application);
        Assert.assertEquals(abstractMetadataReport.allMetadataReports.size(), 1);
        Assert.assertTrue(((FullServiceDefinition) abstractMetadataReport.allMetadataReports.get(providerMetadataIdentifier1)).getParameters().containsKey("testPKey"));
        ProviderMetadataIdentifier providerMetadataIdentifier2 = storePrivider(abstractMetadataReport, interfaceName, version + "_2", group + "_2", application);
        Assert.assertEquals(abstractMetadataReport.allMetadataReports.size(), 2);
        Assert.assertTrue(((FullServiceDefinition) abstractMetadataReport.allMetadataReports.get(providerMetadataIdentifier2)).getParameters().containsKey("testPKey"));
        Assert.assertEquals(((FullServiceDefinition) abstractMetadataReport.allMetadataReports.get(providerMetadataIdentifier2)).getParameters().get("version"), version + "_2");
        ConsumerMetadataIdentifier consumerMetadataIdentifier = storeConsumer(abstractMetadataReport, interfaceName, version + "_3", group + "_3", application, "testKey=value");
        Assert.assertEquals(abstractMetadataReport.allMetadataReports.size(), 3);
        Assert.assertTrue(((String) abstractMetadataReport.allMetadataReports.get(consumerMetadataIdentifier)).contains("testPKey=9090"));
        Assert.assertTrue(((String) abstractMetadataReport.allMetadataReports.get(consumerMetadataIdentifier)).contains("testKey=value"));
        Assert.assertTrue(abstractMetadataReport.store.size() == 3);
        abstractMetadataReport.store.clear();
        Assert.assertTrue(abstractMetadataReport.store.size() == 0);
        abstractMetadataReport.publishAll();
        Assert.assertTrue(abstractMetadataReport.store.size() == 3);
        String v = abstractMetadataReport.store.get(providerMetadataIdentifier1.getIdentifierKey());
        Gson gson = new Gson();
        FullServiceDefinition data = gson.fromJson(v, FullServiceDefinition.class);
        checkParam(data.getParameters(), application, version);
        String v2 = abstractMetadataReport.store.get(providerMetadataIdentifier2.getIdentifierKey());
        gson = new Gson();
        data = gson.fromJson(v2, FullServiceDefinition.class);
        checkParam(data.getParameters(), application, version + "_2");
        String v3 = abstractMetadataReport.store.get(consumerMetadataIdentifier.getIdentifierKey());
        checkParam(queryUrlToMap(v3), application, version + "_3");
    }

    @Test
    public void testCalculateStartTime() {
        for (int i = 0; i < 300; i++) {
            long t = abstractMetadataReport.calculateStartTime() + System.currentTimeMillis();
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(t);
            Assert.assertTrue(c.get(Calendar.HOUR_OF_DAY) >= 2);
            Assert.assertTrue(c.get(Calendar.HOUR_OF_DAY) <= 6);
        }
    }

    private FullServiceDefinition toServiceDefinition(String v) {
        Gson gson = new Gson();
        FullServiceDefinition data = gson.fromJson(v, FullServiceDefinition.class);
        return data;
    }

    private void checkParam(Map<String, String> map, String application, String version) {
        Assert.assertEquals(map.get("application"), application);
        Assert.assertEquals(map.get("version"), version);
    }

    private Map<String, String> queryUrlToMap(String urlQuery) {
        if (urlQuery == null) {
            return Collections.emptyMap();
        }
        String[] pairs = urlQuery.split("&");
        Map<String, String> map = new HashMap<>();
        for (String pairStr : pairs) {
            String[] pair = pairStr.split("=");
            map.put(pair[0], pair[1]);
        }
        return map;
    }

    private static class NewMetadataReport extends AbstractMetadataReport {

        Map<String, String> store = new ConcurrentHashMap<>();

        public NewMetadataReport(URL metadataReportURL) {
            super(metadataReportURL);
        }

        @Override
        protected void doStoreProviderMetadata(ProviderMetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
            store.put(providerMetadataIdentifier.getIdentifierKey(), serviceDefinitions);
        }

        @Override
        protected void doStoreConsumerMetadata(ConsumerMetadataIdentifier consumerMetadataIdentifier, String serviceParameterString) {
            store.put(consumerMetadataIdentifier.getIdentifierKey(), serviceParameterString);
        }
    }

    private static class RetryMetadataReport extends AbstractMetadataReport {

        Map<String, String> store = new ConcurrentHashMap<>();

        int needRetryTimes;

        int executeTimes = 0;

        public RetryMetadataReport(URL metadataReportURL, int needRetryTimes) {
            super(metadataReportURL);
            this.needRetryTimes = needRetryTimes;
        }

        @Override
        protected void doStoreProviderMetadata(ProviderMetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
            ++executeTimes;
            System.out.println("***" + executeTimes + ";" + System.currentTimeMillis());
            if (executeTimes <= needRetryTimes) {
                throw new RuntimeException("must retry:" + executeTimes);
            }
            store.put(providerMetadataIdentifier.getIdentifierKey(), serviceDefinitions);
        }

        @Override
        protected void doStoreConsumerMetadata(ConsumerMetadataIdentifier consumerMetadataIdentifier, String serviceParameterString) {
            ++executeTimes;
            if (executeTimes <= needRetryTimes) {
                throw new RuntimeException("must retry:" + executeTimes);
            }
            store.put(consumerMetadataIdentifier.getIdentifierKey(), serviceParameterString);
        }
    }
}