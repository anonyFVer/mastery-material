package org.apache.dubbo.config.spring.beans.factory.annotation;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.spring.context.properties.DefaultDubboConfigBinder;
import org.apache.dubbo.config.spring.context.properties.DubboConfigBinder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;

@PropertySource({ "classpath:/META-INF/config.properties" })
@Configuration
public class DubboConfigBindingBeanPostProcessorTest {

    @Bean("applicationBean")
    public ApplicationConfig applicationConfig() {
        return new ApplicationConfig();
    }

    @Bean
    public DubboConfigBinder dubboConfigBinder() {
        return new DefaultDubboConfigBinder();
    }

    @Test
    public void test() {
        final AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();
        applicationContext.register(getClass());
        Class<?> processorClass = DubboConfigBindingBeanPostProcessor.class;
        applicationContext.registerBeanDefinition("DubboConfigBindingBeanPostProcessor", rootBeanDefinition(processorClass).addConstructorArgValue("dubbo.application").addConstructorArgValue("applicationBean").getBeanDefinition());
        applicationContext.refresh();
        ApplicationConfig applicationConfig = applicationContext.getBean(ApplicationConfig.class);
        Assertions.assertEquals("dubbo-demo-application", applicationConfig.getName());
    }
}