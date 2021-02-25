package org.apache.dubbo.config.spring.context.annotation;

import org.apache.dubbo.config.ApplicationConfig;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.PropertySource;

public class DubboConfigBindingsRegistrarTest {

    @Test
    public void test() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(TestConfig.class);
        context.refresh();
        ApplicationConfig applicationConfig = context.getBean("applicationBean", ApplicationConfig.class);
        Assert.assertEquals("dubbo-demo-application", applicationConfig.getName());
        Assert.assertEquals(2, context.getBeansOfType(ApplicationConfig.class).size());
    }

    @EnableDubboConfigBindings({ @EnableDubboConfigBinding(prefix = "${application.prefix}", type = ApplicationConfig.class), @EnableDubboConfigBinding(prefix = "dubbo.applications.applicationBean", type = ApplicationConfig.class) })
    @PropertySource("META-INF/config.properties")
    private static class TestConfig {
    }
}