package org.apache.dubbo.config.spring.context.annotation;

import org.apache.dubbo.config.ApplicationConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.PropertySource;

public class DubboConfigBindingRegistrarTest {

    @Test
    public void testRegisterBeanDefinitionsForSingle() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(TestApplicationConfig.class);
        context.refresh();
        ApplicationConfig applicationConfig = context.getBean("applicationBean", ApplicationConfig.class);
        Assertions.assertEquals("dubbo-demo-application", applicationConfig.getName());
    }

    @Test
    public void testRegisterBeanDefinitionsForMultiple() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(TestMultipleApplicationConfig.class);
        context.refresh();
        ApplicationConfig applicationConfig = context.getBean("applicationBean", ApplicationConfig.class);
        Assertions.assertEquals("dubbo-demo-application", applicationConfig.getName());
        applicationConfig = context.getBean("applicationBean2", ApplicationConfig.class);
        Assertions.assertEquals("dubbo-demo-application2", applicationConfig.getName());
        applicationConfig = context.getBean("applicationBean3", ApplicationConfig.class);
        Assertions.assertEquals("dubbo-demo-application3", applicationConfig.getName());
    }

    @EnableDubboConfigBinding(prefix = "${application.prefixes}", type = ApplicationConfig.class, multiple = true)
    @PropertySource("META-INF/config.properties")
    private static class TestMultipleApplicationConfig {
    }

    @EnableDubboConfigBinding(prefix = "${application.prefix}", type = ApplicationConfig.class)
    @PropertySource("META-INF/config.properties")
    private static class TestApplicationConfig {
    }
}