package org.apache.dubbo.config.spring.beans.factory.annotation;

import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.spring.ReferenceBean;
import org.apache.dubbo.config.spring.api.DemoService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.InjectionMetadata;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import java.util.Collection;
import java.util.Map;
import static org.apache.dubbo.config.spring.beans.factory.annotation.ReferenceAnnotationBeanPostProcessor.BEAN_NAME;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = { ServiceAnnotationTestConfiguration.class, ReferenceAnnotationBeanPostProcessorTest.class })
@TestPropertySource(properties = { "packagesToScan = org.apache.dubbo.config.spring.context.annotation.provider", "consumer.version = ${demo.service.version}", "consumer.url = dubbo://127.0.0.1:12345" })
public class ReferenceAnnotationBeanPostProcessorTest {

    @Bean
    public TestBean testBean() {
        return new TestBean();
    }

    @Bean(BEAN_NAME)
    public ReferenceAnnotationBeanPostProcessor referenceAnnotationBeanPostProcessor() {
        return new ReferenceAnnotationBeanPostProcessor();
    }

    @Autowired
    private ConfigurableApplicationContext context;

    @Test
    public void test() throws Exception {
        TestBean testBean = context.getBean(TestBean.class);
        DemoService demoService = testBean.getDemoService();
        Assert.assertEquals("Hello,Mercy", demoService.sayName("Mercy"));
        Assert.assertNotNull(testBean.getDemoServiceFromAncestor());
        Assert.assertNotNull(testBean.getDemoServiceFromParent());
        Assert.assertNotNull(testBean.getDemoService());
        Assert.assertEquals("Hello,Mercy", testBean.getDemoServiceFromAncestor().sayName("Mercy"));
        Assert.assertEquals("Hello,Mercy", testBean.getDemoServiceFromParent().sayName("Mercy"));
        Assert.assertEquals("Hello,Mercy", testBean.getDemoService().sayName("Mercy"));
    }

    @Test
    public void testGetReferenceBeans() {
        ReferenceAnnotationBeanPostProcessor beanPostProcessor = context.getBean(BEAN_NAME, ReferenceAnnotationBeanPostProcessor.class);
        Collection<ReferenceBean<?>> referenceBeans = beanPostProcessor.getReferenceBeans();
        Assert.assertEquals(1, referenceBeans.size());
        ReferenceBean<?> referenceBean = referenceBeans.iterator().next();
        TestBean testBean = context.getBean(TestBean.class);
        Assert.assertNotNull(referenceBean.get());
    }

    @Test
    public void testGetInjectedFieldReferenceBeanMap() {
        ReferenceAnnotationBeanPostProcessor beanPostProcessor = context.getBean(BEAN_NAME, ReferenceAnnotationBeanPostProcessor.class);
        Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> referenceBeanMap = beanPostProcessor.getInjectedFieldReferenceBeanMap();
        Assert.assertEquals(1, referenceBeanMap.size());
        for (Map.Entry<InjectionMetadata.InjectedElement, ReferenceBean<?>> entry : referenceBeanMap.entrySet()) {
            InjectionMetadata.InjectedElement injectedElement = entry.getKey();
            Assert.assertEquals("org.apache.dubbo.config.spring.beans.factory.annotation.AnnotationInjectedBeanPostProcessor$AnnotatedFieldElement", injectedElement.getClass().getName());
            ReferenceBean<?> referenceBean = entry.getValue();
            Assert.assertEquals("2.5.7", referenceBean.getVersion());
            Assert.assertEquals("dubbo://127.0.0.1:12345", referenceBean.getUrl());
        }
    }

    @Test
    public void testGetInjectedMethodReferenceBeanMap() {
        ReferenceAnnotationBeanPostProcessor beanPostProcessor = context.getBean(BEAN_NAME, ReferenceAnnotationBeanPostProcessor.class);
        Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> referenceBeanMap = beanPostProcessor.getInjectedMethodReferenceBeanMap();
        Assert.assertEquals(2, referenceBeanMap.size());
        for (Map.Entry<InjectionMetadata.InjectedElement, ReferenceBean<?>> entry : referenceBeanMap.entrySet()) {
            InjectionMetadata.InjectedElement injectedElement = entry.getKey();
            Assert.assertEquals("org.apache.dubbo.config.spring.beans.factory.annotation.AnnotationInjectedBeanPostProcessor$AnnotatedMethodElement", injectedElement.getClass().getName());
            ReferenceBean<?> referenceBean = entry.getValue();
            Assert.assertEquals("2.5.7", referenceBean.getVersion());
            Assert.assertEquals("dubbo://127.0.0.1:12345", referenceBean.getUrl());
        }
    }

    private static class AncestorBean {

        private DemoService demoServiceFromAncestor;

        @Autowired
        private ApplicationContext applicationContext;

        public DemoService getDemoServiceFromAncestor() {
            return demoServiceFromAncestor;
        }

        @Reference(version = "2.5.7", url = "dubbo://127.0.0.1:12345")
        public void setDemoServiceFromAncestor(DemoService demoServiceFromAncestor) {
            this.demoServiceFromAncestor = demoServiceFromAncestor;
        }

        public ApplicationContext getApplicationContext() {
            return applicationContext;
        }
    }

    private static class ParentBean extends AncestorBean {

        @Reference(version = "${consumer.version}", url = "${consumer.url}")
        private DemoService demoServiceFromParent;

        public DemoService getDemoServiceFromParent() {
            return demoServiceFromParent;
        }
    }

    static class TestBean extends ParentBean {

        private DemoService demoService;

        @Autowired
        private ApplicationContext applicationContext;

        public DemoService getDemoService() {
            return demoService;
        }

        @Reference(version = "2.5.7", url = "dubbo://127.0.0.1:12345")
        public void setDemoService(DemoService demoService) {
            this.demoService = demoService;
        }
    }
}