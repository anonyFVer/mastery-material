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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import java.util.Map;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { ServiceAnnotationTestConfiguration.class, ServiceAnnotationBeanPostProcessorTest.class })
@TestPropertySource(properties = { "provider.package = org.apache.dubbo.config.spring.context.annotation.provider", "packagesToScan = ${provider.package}" })
public class ServiceAnnotationBeanPostProcessorTest {

    @Autowired
    private ConfigurableListableBeanFactory beanFactory;

    @Disabled
    @Bean
    public ServiceAnnotationBeanPostProcessor serviceAnnotationBeanPostProcessor2(@Value("${packagesToScan}") String... packagesToScan) {
        return new ServiceAnnotationBeanPostProcessor(packagesToScan);
    }

    @Test
    public void test() {
        Map<String, HelloService> helloServicesMap = beanFactory.getBeansOfType(HelloService.class);
        Assertions.assertEquals(2, helloServicesMap.size());
        Map<String, ServiceBean> serviceBeansMap = beanFactory.getBeansOfType(ServiceBean.class);
        Assert.assertEquals(2, serviceBeansMap.size());
        Map<String, ServiceAnnotationBeanPostProcessor> beanPostProcessorsMap = beanFactory.getBeansOfType(ServiceAnnotationBeanPostProcessor.class);
        Assert.assertEquals(2, beanPostProcessorsMap.size());
        Assert.assertTrue(beanPostProcessorsMap.containsKey("serviceAnnotationBeanPostProcessor"));
        Assert.assertTrue(beanPostProcessorsMap.containsKey("serviceAnnotationBeanPostProcessor2"));
    }
}