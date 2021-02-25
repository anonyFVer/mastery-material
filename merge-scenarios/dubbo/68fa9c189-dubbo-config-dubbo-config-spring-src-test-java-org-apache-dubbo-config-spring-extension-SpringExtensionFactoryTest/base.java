package org.apache.dubbo.config.spring.extension;

import org.apache.dubbo.config.spring.api.DemoService;
import org.apache.dubbo.config.spring.api.HelloService;
import org.apache.dubbo.config.spring.impl.DemoServiceImpl;
import org.apache.dubbo.config.spring.impl.HelloServiceImpl;
import org.apache.dubbo.rpc.Protocol;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringExtensionFactoryTest {

    private SpringExtensionFactory springExtensionFactory = new SpringExtensionFactory();

    private AnnotationConfigApplicationContext context1;

    private AnnotationConfigApplicationContext context2;

    @Before
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
        Assert.assertNull(protocol);
    }

    @Test
    public void testGetExtensionByName() {
        DemoService bean = springExtensionFactory.getExtension(DemoService.class, "bean1");
        Assert.assertNotNull(bean);
    }

    @Test
    public void testGetExtensionByTypeMultiple() {
        try {
            springExtensionFactory.getExtension(DemoService.class, "beanname-not-exist");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e instanceof NoUniqueBeanDefinitionException);
        }
    }

    @Test
    public void testGetExtensionByType() {
        HelloService bean = springExtensionFactory.getExtension(HelloService.class, "beanname-not-exist");
        Assert.assertNotNull(bean);
    }

    @After
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