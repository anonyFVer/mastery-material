package org.apache.dubbo.config.spring.context.annotation;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.spring.api.DemoService;
import org.apache.dubbo.config.spring.context.annotation.consumer.test.TestConsumerConfiguration;
import org.apache.dubbo.config.spring.context.annotation.provider.DemoServiceImpl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import static org.springframework.core.annotation.AnnotationUtils.findAnnotation;

public class EnableDubboTest {

    @Test
    public void test() {
        AnnotationConfigApplicationContext providerContext = new AnnotationConfigApplicationContext();
        providerContext.register(TestProviderConfiguration.class);
        providerContext.refresh();
        DemoService demoService = providerContext.getBean(DemoService.class);
        String value = demoService.sayName("Mercy");
        Assert.assertEquals("Hello,Mercy", value);
        Class<?> beanClass = AopUtils.getTargetClass(demoService);
        Assert.assertEquals(DemoServiceImpl.class, beanClass);
        Assert.assertNotNull(findAnnotation(beanClass, Transactional.class));
        AnnotationConfigApplicationContext consumerContext = new AnnotationConfigApplicationContext();
        consumerContext.register(TestConsumerConfiguration.class);
        consumerContext.refresh();
        TestConsumerConfiguration consumerConfiguration = consumerContext.getBean(TestConsumerConfiguration.class);
        demoService = consumerConfiguration.getDemoService();
        value = demoService.sayName("Mercy");
        Assert.assertEquals("Hello,Mercy", value);
        TestConsumerConfiguration.Child child = consumerContext.getBean(TestConsumerConfiguration.Child.class);
        demoService = child.getDemoServiceFromChild();
        Assert.assertNotNull(demoService);
        value = demoService.sayName("Mercy");
        Assert.assertEquals("Hello,Mercy", value);
        demoService = child.getDemoServiceFromParent();
        Assert.assertNotNull(demoService);
        value = demoService.sayName("Mercy");
        Assert.assertEquals("Hello,Mercy", value);
        demoService = child.getDemoServiceFromAncestor();
        Assert.assertNotNull(demoService);
        value = demoService.sayName("Mercy");
        Assert.assertEquals("Hello,Mercy", value);
        ApplicationConfig applicationConfig = consumerContext.getBean("dubbo-annotation-consumer2", ApplicationConfig.class);
        Assert.assertEquals("dubbo-consumer2", applicationConfig.getName());
        RegistryConfig registryConfig = consumerContext.getBean("my-registry2", RegistryConfig.class);
        Assert.assertEquals("N/A", registryConfig.getAddress());
        providerContext.close();
        consumerContext.close();
    }

    @EnableDubbo(scanBasePackages = "org.apache.dubbo.config.spring.context.annotation.provider")
    @ComponentScan(basePackages = "org.apache.dubbo.config.spring.context.annotation.provider")
    @PropertySource("META-INF/dubbb-provider.properties")
    @EnableTransactionManagement
    public static class TestProviderConfiguration {

        @Primary
        @Bean
        public PlatformTransactionManager platformTransactionManager() {
            return new PlatformTransactionManager() {

                @Override
                public TransactionStatus getTransaction(TransactionDefinition definition) throws TransactionException {
                    return null;
                }

                @Override
                public void commit(TransactionStatus status) throws TransactionException {
                }

                @Override
                public void rollback(TransactionStatus status) throws TransactionException {
                }
            };
        }
    }
}