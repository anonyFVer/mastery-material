package org.apache.dubbo.config.spring.context.properties;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@TestPropertySource(locations = "classpath:/dubbo.properties")
@ContextConfiguration(classes = DefaultDubboConfigBinder.class)
public class DefaultDubboConfigBinderTest {

    @Autowired
    private DubboConfigBinder dubboConfigBinder;

    @Test
    public void testBinder() {
        ApplicationConfig applicationConfig = new ApplicationConfig();
        dubboConfigBinder.bind("dubbo.application", applicationConfig);
        Assert.assertEquals("hello", applicationConfig.getName());
        Assert.assertEquals("world", applicationConfig.getOwner());
        RegistryConfig registryConfig = new RegistryConfig();
        dubboConfigBinder.bind("dubbo.registry", registryConfig);
        Assert.assertEquals("10.20.153.17", registryConfig.getAddress());
        ProtocolConfig protocolConfig = new ProtocolConfig();
        dubboConfigBinder.bind("dubbo.protocol", protocolConfig);
        Assert.assertEquals(Integer.valueOf(20881), protocolConfig.getPort());
    }
}