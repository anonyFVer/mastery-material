package org.apache.dubbo.config.spring.context.annotation;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ConsumerConfig;
import org.apache.dubbo.config.ModuleConfig;
import org.apache.dubbo.config.MonitorConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.PropertySource;

public class EnableDubboConfigTest {

    @Test
    public void testSingle() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(TestConfig.class);
        context.refresh();
        ApplicationConfig applicationConfig = context.getBean("applicationBean", ApplicationConfig.class);
        Assertions.assertEquals("dubbo-demo-application", applicationConfig.getName());
        ModuleConfig moduleConfig = context.getBean("moduleBean", ModuleConfig.class);
        Assertions.assertEquals("dubbo-demo-module", moduleConfig.getName());
        RegistryConfig registryConfig = context.getBean(RegistryConfig.class);
        Assertions.assertEquals("zookeeper://192.168.99.100:32770", registryConfig.getAddress());
        ProtocolConfig protocolConfig = context.getBean(ProtocolConfig.class);
        Assertions.assertEquals("dubbo", protocolConfig.getName());
        Assertions.assertEquals(Integer.valueOf(20880), protocolConfig.getPort());
        MonitorConfig monitorConfig = context.getBean(MonitorConfig.class);
        Assertions.assertEquals("zookeeper://127.0.0.1:32770", monitorConfig.getAddress());
        ProviderConfig providerConfig = context.getBean(ProviderConfig.class);
        Assertions.assertEquals("127.0.0.1", providerConfig.getHost());
        ConsumerConfig consumerConfig = context.getBean(ConsumerConfig.class);
        Assertions.assertEquals("netty", consumerConfig.getClient());
    }

    @Test
    public void testMultiple() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(TestMultipleConfig.class);
        context.refresh();
        ApplicationConfig applicationConfig = context.getBean("applicationBean", ApplicationConfig.class);
        Assertions.assertEquals("dubbo-demo-application", applicationConfig.getName());
        ApplicationConfig applicationBean2 = context.getBean("applicationBean2", ApplicationConfig.class);
        Assertions.assertEquals("dubbo-demo-application2", applicationBean2.getName());
        ApplicationConfig applicationBean3 = context.getBean("applicationBean3", ApplicationConfig.class);
        Assertions.assertEquals("dubbo-demo-application3", applicationBean3.getName());
    }

    @EnableDubboConfig(multiple = true)
    @PropertySource("META-INF/config.properties")
    private static class TestMultipleConfig {
    }

    @EnableDubboConfig
    @PropertySource("META-INF/config.properties")
    private static class TestConfig {
    }
}