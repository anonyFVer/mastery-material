package org.apache.dubbo.config.spring.context.annotation;

import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.config.spring.api.DemoService;
import org.apache.dubbo.config.spring.context.annotation.consumer.test.TestConsumerConfiguration;
import org.apache.dubbo.config.spring.context.annotation.provider.DemoServiceImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.After;
import org.junit.Before;
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

    private AnnotationConfigApplicationContext context;

    @Before
    public void setUp() {
        ConfigManager.getInstance().clear();
        context = new AnnotationConfigApplicationContext();
    }

    @After
    public void tearDown() {
        ConfigManager.getInstance().clear();
        context.close();
    }

    @Test
    public void testProvider() {
        context.register(TestProviderConfiguration.class);
        context.refresh();
        DemoService demoService = context.getBean(DemoService.class);
        String value = demoService.sayName("Mercy");
        Assertions.assertEquals("Hello,Mercy", value);
        Class<?> beanClass = AopUtils.getTargetClass(demoService);
        Assertions.assertEquals(DemoServiceImpl.class, beanClass);
        Assertions.assertNotNull(findAnnotation(beanClass, Transactional.class));
    }

    @Test
    public void testConsumer() {
        context.register(TestProviderConfiguration.class, TestConsumerConfiguration.class);
        context.refresh();
        TestConsumerConfiguration consumerConfiguration = context.getBean(TestConsumerConfiguration.class);
        DemoService demoService = consumerConfiguration.getDemoService();
        String value = demoService.sayName("Mercy");
        Assertions.assertEquals("Hello,Mercy", value);
        TestConsumerConfiguration.Child child = context.getBean(TestConsumerConfiguration.Child.class);
        demoService = child.getDemoServiceFromChild();
        Assertions.assertNotNull(demoService);
        value = demoService.sayName("Mercy");
        Assertions.assertEquals("Hello,Mercy", value);
        demoService = child.getDemoServiceFromParent();
        Assertions.assertNotNull(demoService);
        value = demoService.sayName("Mercy");
        Assertions.assertEquals("Hello,Mercy", value);
        demoService = child.getDemoServiceFromAncestor();
        Assertions.assertNotNull(demoService);
        value = demoService.sayName("Mercy");
        Assertions.assertEquals("Hello,Mercy", value);
        RegistryConfig registryConfig = context.getBean("my-registry2", RegistryConfig.class);
        Assertions.assertEquals("N/A", registryConfig.getAddress());
    }

    @EnableDubbo(scanBasePackages = "org.apache.dubbo.config.spring.context.annotation.provider")
    @ComponentScan(basePackages = "org.apache.dubbo.config.spring.context.annotation.provider")
    @PropertySource("classpath:/META-INF/dubbo-provider.properties")
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