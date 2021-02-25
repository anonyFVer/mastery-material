package org.apache.dubbo.config;

import org.apache.dubbo.service.DemoService;
import org.apache.dubbo.service.DemoServiceImpl;
import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.ServiceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConfigTest {

    @BeforeEach
    public void setup() {
        System.setProperty("java.net.preferIPv4Stack", "true");
    }

    @Test
    public void testConfig() {
        com.alibaba.dubbo.config.ServiceConfig<DemoService> service = new ServiceConfig<>();
        service.setApplication(new com.alibaba.dubbo.config.ApplicationConfig("first-dubbo-provider"));
        service.setRegistry(new com.alibaba.dubbo.config.RegistryConfig("multicast://224.5.6.7:1234"));
        service.setInterface(DemoService.class);
        service.setRef(new DemoServiceImpl());
        service.export();
        com.alibaba.dubbo.config.ReferenceConfig<DemoService> reference = new ReferenceConfig<>();
        reference.setApplication(new ApplicationConfig("first-dubbo-client"));
        reference.setRegistry(new RegistryConfig("multicast://224.5.6.7:1234"));
        reference.setInterface(DemoService.class);
        DemoService demoService = reference.get();
        String message = demoService.sayHello("dubbo");
        Assertions.assertEquals("hello dubbo", message);
    }
}