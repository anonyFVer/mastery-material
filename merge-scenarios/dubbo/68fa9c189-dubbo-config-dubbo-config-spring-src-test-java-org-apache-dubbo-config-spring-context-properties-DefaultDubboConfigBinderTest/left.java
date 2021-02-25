package org.apache.dubbo.config.spring.context.properties;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@TestPropertySource(locations = "classpath:/dubbo.properties")
@ContextConfiguration(classes = DefaultDubboConfigBinder.class)
public class DefaultDubboConfigBinderTest {

    @Autowired
    private DubboConfigBinder dubboConfigBinder;

    @Disabled
    public void testBinder() {
        ApplicationConfig applicationConfig = new ApplicationConfig();
        dubboConfigBinder.bind("dubbo.application", applicationConfig);
        Assertions.assertEquals("hello", applicationConfig.getName());
        Assertions.assertEquals("world", applicationConfig.getOwner());
        RegistryConfig registryConfig = new RegistryConfig();
        dubboConfigBinder.bind("dubbo.registry", registryConfig);
        Assertions.assertEquals("10.20.153.17", registryConfig.getAddress());
        ProtocolConfig protocolConfig = new ProtocolConfig();
        dubboConfigBinder.bind("dubbo.protocol", protocolConfig);
        Assertions.assertEquals(Integer.valueOf(20881), protocolConfig.getPort());
    }
}