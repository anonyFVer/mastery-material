package org.apache.dubbo.config.spring.context.annotation;

import org.apache.dubbo.config.spring.api.DemoService;
import org.apache.dubbo.config.spring.context.annotation.consumer.ConsumerConfiguration;
import org.apache.dubbo.config.spring.context.annotation.provider.DemoServiceImpl;
import org.apache.dubbo.config.spring.context.annotation.provider.ProviderConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.transaction.annotation.Transactional;
import static org.springframework.core.annotation.AnnotationUtils.findAnnotation;

public class DubboComponentScanRegistrarTest {

    @Test
    public void test() {
        AnnotationConfigApplicationContext providerContext = new AnnotationConfigApplicationContext();
        providerContext.register(ProviderConfiguration.class);
        providerContext.refresh();
        DemoService demoService = providerContext.getBean(DemoService.class);
        String value = demoService.sayName("Mercy");
        Assert.assertEquals("Hello,Mercy", value);
        Class<?> beanClass = AopUtils.getTargetClass(demoService);
        Assert.assertEquals(DemoServiceImpl.class, beanClass);
        Assert.assertNotNull(findAnnotation(beanClass, Transactional.class));
        AnnotationConfigApplicationContext consumerContext = new AnnotationConfigApplicationContext();
        consumerContext.register(ConsumerConfiguration.class);
        consumerContext.refresh();
        ConsumerConfiguration consumerConfiguration = consumerContext.getBean(ConsumerConfiguration.class);
        demoService = consumerConfiguration.getDemoService();
        value = demoService.sayName("Mercy");
        Assert.assertEquals("Hello,Mercy", value);
        ConsumerConfiguration.Child child = consumerContext.getBean(ConsumerConfiguration.Child.class);
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
        providerContext.close();
        consumerContext.close();
    }
}