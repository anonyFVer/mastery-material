package org.apache.dubbo.config.spring.extension;

import org.apache.dubbo.config.spring.api.DemoService;
import org.apache.dubbo.config.spring.api.HelloService;
import org.apache.dubbo.config.spring.impl.DemoServiceImpl;
import org.apache.dubbo.config.spring.impl.HelloServiceImpl;
import org.apache.dubbo.rpc.Protocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringExtensionFactoryTest {

    private SpringExtensionFactory springExtensionFactory = new SpringExtensionFactory();

    private AnnotationConfigApplicationContext context1;

    private AnnotationConfigApplicationContext context2;

    @BeforeEach
    public void init() {
        context1 = new AnnotationConfigApplicationContext();
        context1.register(getClass());
        context1.refresh();
        context2 = new AnnotationConfigApplicationContext();
        context2.register(BeanForContext2.class);
        context2.refresh();
        SpringExtensionFactory.addApplicationContext(context1);
        SpringExtensionFactory.addApplicationContext(context2);
    }

    @Test
    public void testGetExtensionBySPI() {
        Protocol protocol = springExtensionFactory.getExtension(Protocol.class, "protocol");
        Assertions.assertNull(protocol);
    }

    @Test
    public void testGetExtensionByName() {
        DemoService bean = springExtensionFactory.getExtension(DemoService.class, "bean1");
        Assertions.assertNotNull(bean);
    }

    @Test
    public void testGetExtensionByTypeMultiple() {
        try {
            springExtensionFactory.getExtension(DemoService.class, "beanname-not-exist");
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.assertTrue(e instanceof NoUniqueBeanDefinitionException);
        }
    }

    @Test
    public void testGetExtensionByType() {
        HelloService bean = springExtensionFactory.getExtension(HelloService.class, "beanname-not-exist");
        Assertions.assertNotNull(bean);
    }

    @AfterEach
    public void destroy() {
        SpringExtensionFactory.clearContexts();
        context1.close();
        context2.close();
    }

    @Bean("bean1")
    public DemoService bean1() {
        return new DemoServiceImpl();
    }

    @Bean("bean2")
    public DemoService bean2() {
        return new DemoServiceImpl();
    }

    @Bean("hello")
    public HelloService helloService() {
        return new HelloServiceImpl();
    }
}