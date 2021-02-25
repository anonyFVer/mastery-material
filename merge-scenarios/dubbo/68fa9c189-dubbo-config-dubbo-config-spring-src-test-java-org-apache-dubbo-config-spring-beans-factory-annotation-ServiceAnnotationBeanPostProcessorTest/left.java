package org.apache.dubbo.config.spring.beans.factory.annotation;

import org.apache.dubbo.config.spring.ServiceBean;
import org.apache.dubbo.config.spring.api.HelloService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import java.util.Map;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { ServiceAnnotationBeanPostProcessorTest.TestConfiguration.class })
@TestPropertySource(properties = { "package1 = org.apache.dubbo.config.spring.context.annotation", "packagesToScan = ${package1}", "provider.version = 1.2" })
public class ServiceAnnotationBeanPostProcessorTest {

    @Autowired
    private ConfigurableListableBeanFactory beanFactory;

    @Disabled
    public void test() {
        Map<String, HelloService> helloServicesMap = beanFactory.getBeansOfType(HelloService.class);
        Assertions.assertEquals(2, helloServicesMap.size());
        Map<String, ServiceBean> serviceBeansMap = beanFactory.getBeansOfType(ServiceBean.class);
        Assertions.assertEquals(3, serviceBeansMap.size());
        Map<String, ServiceAnnotationBeanPostProcessor> beanPostProcessorsMap = beanFactory.getBeansOfType(ServiceAnnotationBeanPostProcessor.class);
        Assertions.assertEquals(4, beanPostProcessorsMap.size());
        Assertions.assertTrue(beanPostProcessorsMap.containsKey("doubleServiceAnnotationBeanPostProcessor"));
        Assertions.assertTrue(beanPostProcessorsMap.containsKey("emptyServiceAnnotationBeanPostProcessor"));
        Assertions.assertTrue(beanPostProcessorsMap.containsKey("serviceAnnotationBeanPostProcessor"));
        Assertions.assertTrue(beanPostProcessorsMap.containsKey("serviceAnnotationBeanPostProcessor2"));
    }

    @ImportResource("META-INF/spring/dubbo-annotation-provider.xml")
    @PropertySource("META-INF/default.properties")
    @ComponentScan("org.apache.dubbo.config.spring.context.annotation.provider")
    public static class TestConfiguration {

        @Bean
        public ServiceAnnotationBeanPostProcessor serviceAnnotationBeanPostProcessor(@Value("${packagesToScan}") String... packagesToScan) {
            return new ServiceAnnotationBeanPostProcessor(packagesToScan);
        }

        @Bean
        public ServiceAnnotationBeanPostProcessor serviceAnnotationBeanPostProcessor2(@Value("${packagesToScan}") String... packagesToScan) {
            return new ServiceAnnotationBeanPostProcessor(packagesToScan);
        }
    }
}