package org.apache.dubbo.rpc.cluster.configurator.parser;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.cluster.configurator.parser.model.ConfigItem;
import org.apache.dubbo.rpc.cluster.configurator.parser.model.ConfiguratorConfig;
import org.junit.Assert;
import org.junit.Test;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ConfigParserTest {

    private String streamToString(InputStream stream) {
        try {
            byte[] bytes = new byte[stream.available()];
            stream.read(bytes);
            return new String(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Test
    public void snakeYamlBasicTest() throws IOException {
        try (InputStream yamlStream = this.getClass().getResourceAsStream("/ServiceNoApp.yml")) {
            Constructor constructor = new Constructor(ConfiguratorConfig.class);
            TypeDescription carDescription = new TypeDescription(ConfiguratorConfig.class);
            carDescription.addPropertyParameters("items", ConfigItem.class);
            constructor.addTypeDescription(carDescription);
            Yaml yaml = new Yaml(constructor);
            ConfiguratorConfig config = yaml.load(yamlStream);
            System.out.println(config);
        }
    }

    @Test
    public void parseConfiguratorsServiceNoAppTest() throws Exception {
        try (InputStream yamlStream = this.getClass().getResourceAsStream("/ServiceNoApp.yml")) {
            List<URL> urls = ConfigParser.parseConfigurators(streamToString(yamlStream), "serviceKey");
            Assert.assertNotNull(urls);
            Assert.assertEquals(2, urls.size());
            URL url = urls.get(0);
            Assert.assertEquals(url.getAddress(), "127.0.0.1:20880");
            Assert.assertEquals(url.getParameter(Constants.WEIGHT_KEY, 0), 222);
        }
    }

    @Test
    public void parseConfiguratorsServiceGroupVersionTest() throws Exception {
        try (InputStream yamlStream = this.getClass().getResourceAsStream("/ServiceGroupVersion.yml")) {
            List<URL> urls = ConfigParser.parseConfigurators(streamToString(yamlStream), "testgroup/servicekey:1.0.0");
            Assert.assertNotNull(urls);
            Assert.assertEquals(1, urls.size());
            URL url = urls.get(0);
            Assert.assertEquals("testgroup", url.getParameter(Constants.GROUP_KEY));
            Assert.assertEquals("1.0.0", url.getParameter(Constants.VERSION_KEY));
        }
    }

    @Test
    public void parseConfiguratorsServiceMultiAppsTest() throws IOException {
        try (InputStream yamlStream = this.getClass().getResourceAsStream("/ServiceMultiApps.yml")) {
            List<URL> urls = ConfigParser.parseConfigurators(streamToString(yamlStream), "serviceKey");
            Assert.assertNotNull(urls);
            Assert.assertEquals(4, urls.size());
            URL url = urls.get(0);
            Assert.assertEquals("127.0.0.1", url.getAddress());
            Assert.assertEquals(6666, url.getParameter(Constants.TIMEOUT_KEY, 0));
            Assert.assertNotNull(url.getParameter(Constants.APPLICATION_KEY));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void parseConfiguratorsServiceNoRuleTest() throws IOException {
        try (InputStream yamlStream = this.getClass().getResourceAsStream("/ServiceNoRule.yml")) {
            ConfigParser.parseConfigurators(streamToString(yamlStream), "serviceKey");
            Assert.fail();
        }
    }

    @Test
    public void parseConfiguratorsAppMultiServicesTest() throws IOException {
        try (InputStream yamlStream = this.getClass().getResourceAsStream("/AppMultiServices.yml")) {
            String yamlFile = streamToString(yamlStream);
            List<URL> urls = ConfigParser.parseConfigurators(yamlFile, "service1");
            Assert.assertNotNull(urls);
            Assert.assertEquals(2, urls.size());
            URL url = urls.get(0);
            Assert.assertEquals("127.0.0.1", url.getAddress());
            Assert.assertEquals("service1", url.getServiceInterface());
            Assert.assertEquals(6666, url.getParameter(Constants.TIMEOUT_KEY, 0));
            Assert.assertEquals("random", url.getParameter(Constants.LOADBALANCE_KEY));
            Assert.assertEquals(url.getParameter(Constants.APPLICATION_KEY), "demo-consumer");
            List<URL> urls2 = ConfigParser.parseConfigurators(yamlFile, "service-not-exist");
            Assert.assertNotNull(urls2);
            Assert.assertEquals(0, urls2.size());
        }
    }

    @Test
    public void parseConfiguratorsAppAnyServicesTest() throws IOException {
        try (InputStream yamlStream = this.getClass().getResourceAsStream("/AppAnyServices.yml")) {
            List<URL> urls = ConfigParser.parseConfigurators(streamToString(yamlStream), "service1");
            Assert.assertNotNull(urls);
            Assert.assertEquals(2, urls.size());
            URL url = urls.get(0);
            Assert.assertEquals("127.0.0.1", url.getAddress());
            Assert.assertEquals("*", url.getServiceInterface());
            Assert.assertEquals(6666, url.getParameter(Constants.TIMEOUT_KEY, 0));
            Assert.assertEquals("random", url.getParameter(Constants.LOADBALANCE_KEY));
            Assert.assertEquals(url.getParameter(Constants.APPLICATION_KEY), "demo-consumer");
        }
    }

    @Test
    public void parseConfiguratorsAppNoServiceTest() throws IOException {
        try (InputStream yamlStream = this.getClass().getResourceAsStream("/AppNoService.yml")) {
            List<URL> urls = ConfigParser.parseConfigurators(streamToString(yamlStream), "service1");
            Assert.assertNotNull(urls);
            Assert.assertEquals(1, urls.size());
            URL url = urls.get(0);
            Assert.assertEquals("127.0.0.1", url.getAddress());
            Assert.assertEquals("*", url.getServiceInterface());
            Assert.assertEquals(6666, url.getParameter(Constants.TIMEOUT_KEY, 0));
            Assert.assertEquals("random", url.getParameter(Constants.LOADBALANCE_KEY));
            Assert.assertEquals(url.getParameter(Constants.APPLICATION_KEY), "demo-consumer");
        }
    }

    @Test
    public void parseConsumerSpecificProvidersTest() throws IOException {
        try (InputStream yamlStream = this.getClass().getResourceAsStream("/ConsumerSpecificProviders.yml")) {
            List<URL> urls = ConfigParser.parseConfigurators(streamToString(yamlStream), "service1");
            Assert.assertNotNull(urls);
            Assert.assertEquals(1, urls.size());
            URL url = urls.get(0);
            Assert.assertEquals("127.0.0.1", url.getAddress());
            Assert.assertEquals("*", url.getServiceInterface());
            Assert.assertEquals(6666, url.getParameter(Constants.TIMEOUT_KEY, 0));
            Assert.assertEquals("random", url.getParameter(Constants.LOADBALANCE_KEY));
            Assert.assertEquals("127.0.0.1:20880", url.getParameter(Constants.OVERRIDE_PROVIDERS_KEY));
            Assert.assertEquals(url.getParameter(Constants.APPLICATION_KEY), "demo-consumer");
        }
    }
}