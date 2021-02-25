package org.apache.dubbo.config;

import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.service.DemoService;
import org.apache.dubbo.service.DemoServiceImpl;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.ServiceConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConfigTest {

    private com.alibaba.dubbo.config.ApplicationConfig applicationConfig = new com.alibaba.dubbo.config.ApplicationConfig("first-dubbo-test");

    private com.alibaba.dubbo.config.RegistryConfig registryConfig = new com.alibaba.dubbo.config.RegistryConfig("multicast://224.5.6.7:1234");

    @After
    public void tearDown() {
        ConfigManager.getInstance().clear();
    }

    @BeforeEach
    public void setup() {
        System.setProperty("java.net.preferIPv4Stack", "true");
        ConfigManager.getInstance().clear();
    }

    @Test
    public void testConfig() {
        com.alibaba.dubbo.config.ServiceConfig<DemoService> service = new ServiceConfig<>();
        service.setApplication(applicationConfig);
        service.setRegistry(registryConfig);
        service.setInterface(DemoService.class);
        service.setRef(new DemoServiceImpl());
        service.export();
        com.alibaba.dubbo.config.ReferenceConfig<DemoService> reference = new ReferenceConfig<>();
        reference.setApplication(applicationConfig);
        reference.setRegistry(registryConfig);
        reference.setInterface(DemoService.class);
        DemoService demoService = reference.get();
        String message = demoService.sayHello("dubbo");
        Assertions.assertEquals("hello dubbo", message);
    }
}