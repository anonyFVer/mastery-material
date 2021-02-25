package org.apache.dubbo.config.spring.context.annotation;

import org.apache.dubbo.config.ApplicationConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.PropertySource;

public class DubboConfigBindingsRegistrarTest {

    @Test
    public void test() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(TestConfig.class);
        context.refresh();
        ApplicationConfig applicationConfig = context.getBean("applicationBean", ApplicationConfig.class);
        Assertions.assertEquals("dubbo-demo-application", applicationConfig.getName());
        Assertions.assertEquals(2, context.getBeansOfType(ApplicationConfig.class).size());
    }

    @EnableDubboConfigBindings({ @EnableDubboConfigBinding(prefix = "${application.prefix}", type = ApplicationConfig.class), @EnableDubboConfigBinding(prefix = "dubbo.applications.applicationBean", type = ApplicationConfig.class) })
    @PropertySource("META-INF/config.properties")
    private static class TestConfig {
    }
}