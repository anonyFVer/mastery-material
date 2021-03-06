package org.apache.dubbo.config.spring.beans.factory.annotation;

import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.spring.ReferenceBean;
import org.apache.dubbo.config.spring.api.DemoService;
import org.apache.dubbo.config.spring.context.annotation.DubboComponentScan;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.InjectionMetadata;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ImportResource;
import java.util.Collection;
import java.util.Map;
import static org.apache.dubbo.config.spring.beans.factory.annotation.ReferenceAnnotationBeanPostProcessor.BEAN_NAME;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ReferenceAnnotationBeanPostProcessorTest {

    private ConfigurableApplicationContext providerApplicationContext;

    @BeforeAll
    public static void prepare() {
        System.setProperty("provider.version", "1.2");
        System.setProperty("package1", "org.apache.dubbo.config.spring.annotation.provider");
        System.setProperty("packagesToScan", "${package1}");
        System.setProperty("consumer.version", "1.2");
        System.setProperty("consumer.url", "dubbo://127.0.0.1:12345");
    }

    @BeforeEach
    public void init() {
        providerApplicationContext = new AnnotationConfigApplicationContext(ServiceAnnotationBeanPostProcessorTest.TestConfiguration.class);
    }

    @AfterEach
    public void destroy() {
        providerApplicationContext.close();
    }

    @Test
    public void test() throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestBean.class);
        TestBean testBean = context.getBean(TestBean.class);
        Assertions.assertNotNull(testBean.getDemoServiceFromAncestor());
        Assertions.assertNotNull(testBean.getDemoServiceFromParent());
        Assertions.assertNotNull(testBean.getDemoService());
        Assertions.assertEquals(testBean.getDemoServiceFromAncestor(), testBean.getDemoServiceFromParent());
        Assertions.assertEquals(testBean.getDemoService(), testBean.getDemoServiceFromParent());
        DemoService demoService = testBean.getDemoService();
        Assertions.assertEquals("annotation:Mercy", demoService.sayName("Mercy"));
        context.close();
    }

    @Test
    public void testGetReferenceBeans() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestBean.class);
        ReferenceAnnotationBeanPostProcessor beanPostProcessor = context.getBean(BEAN_NAME, ReferenceAnnotationBeanPostProcessor.class);
        Collection<ReferenceBean<?>> referenceBeans = beanPostProcessor.getReferenceBeans();
        Assertions.assertEquals(3, referenceBeans.size());
        ReferenceBean<?> referenceBean = referenceBeans.iterator().next();
        TestBean testBean = context.getBean(TestBean.class);
        Assertions.assertEquals(referenceBean.get(), testBean.getDemoServiceFromAncestor());
        Assertions.assertEquals(referenceBean.get(), testBean.getDemoServiceFromParent());
        Assertions.assertEquals(referenceBean.get(), testBean.getDemoService());
    }

    @Test
    public void testGetInjectedFieldReferenceBeanMap() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestBean.class);
        ReferenceAnnotationBeanPostProcessor beanPostProcessor = context.getBean(BEAN_NAME, ReferenceAnnotationBeanPostProcessor.class);
        Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> referenceBeanMap = beanPostProcessor.getInjectedFieldReferenceBeanMap();
        Assertions.assertEquals(5, referenceBeanMap.size());
        for (Map.Entry<InjectionMetadata.InjectedElement, ReferenceBean<?>> entry : referenceBeanMap.entrySet()) {
            InjectionMetadata.InjectedElement injectedElement = entry.getKey();
            Assertions.assertEquals("org.apache.dubbo.config.spring.beans.factory.annotation.ReferenceAnnotationBeanPostProcessor$ReferenceFieldElement", injectedElement.getClass().getName());
            ReferenceBean<?> referenceBean = entry.getValue();
            Assertions.assertEquals("1.2", referenceBean.getVersion());
            Assertions.assertEquals("dubbo://127.0.0.1:12345", referenceBean.getUrl());
        }
    }

    @Test
    public void testGetInjectedMethodReferenceBeanMap() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestBean.class);
        ReferenceAnnotationBeanPostProcessor beanPostProcessor = context.getBean(BEAN_NAME, ReferenceAnnotationBeanPostProcessor.class);
        Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> referenceBeanMap = beanPostProcessor.getInjectedMethodReferenceBeanMap();
        Assertions.assertEquals(2, referenceBeanMap.size());
        for (Map.Entry<InjectionMetadata.InjectedElement, ReferenceBean<?>> entry : referenceBeanMap.entrySet()) {
            InjectionMetadata.InjectedElement injectedElement = entry.getKey();
            Assertions.assertEquals("org.apache.dubbo.config.spring.beans.factory.annotation.ReferenceAnnotationBeanPostProcessor$ReferenceMethodElement", injectedElement.getClass().getName());
            ReferenceBean<?> referenceBean = entry.getValue();
            Assertions.assertEquals("1.2", referenceBean.getVersion());
            Assertions.assertEquals("dubbo://127.0.0.1:12345", referenceBean.getUrl());
        }
    }

    @Test
    public void testModuleInfo() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestBean.class);
        ReferenceAnnotationBeanPostProcessor beanPostProcessor = context.getBean(BEAN_NAME, ReferenceAnnotationBeanPostProcessor.class);
        Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> referenceBeanMap = beanPostProcessor.getInjectedMethodReferenceBeanMap();
        for (Map.Entry<InjectionMetadata.InjectedElement, ReferenceBean<?>> entry : referenceBeanMap.entrySet()) {
            ReferenceBean<?> referenceBean = entry.getValue();
            assertThat(referenceBean.getModule().getName(), is("defaultModule"));
            assertThat(referenceBean.getMonitor(), not(nullValue()));
        }
    }

    @Test
    public void testReferenceCache() throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestBean.class);
        TestBean testBean = context.getBean(TestBean.class);
        Assertions.assertNotNull(testBean.getDemoServiceFromAncestor());
        Assertions.assertNotNull(testBean.getDemoServiceFromParent());
        Assertions.assertNotNull(testBean.getDemoService());
        Assertions.assertEquals(testBean.getDemoServiceFromAncestor(), testBean.getDemoServiceFromParent());
        Assertions.assertEquals(testBean.getDemoService(), testBean.getDemoServiceFromParent());
        DemoService demoService = testBean.getDemoService();
        Assertions.assertEquals(demoService, testBean.getDemoServiceShouldBeSame());
        Assertions.assertNotEquals(demoService, testBean.getDemoServiceShouldNotBeSame());
        context.close();
    }

    @Test
    public void testReferenceCacheWithArray() throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(TestBean.class);
        TestBean testBean = context.getBean(TestBean.class);
        Assertions.assertNotNull(testBean.getDemoServiceFromAncestor());
        Assertions.assertNotNull(testBean.getDemoServiceFromParent());
        Assertions.assertNotNull(testBean.getDemoService());
        Assertions.assertEquals(testBean.getDemoServiceFromAncestor(), testBean.getDemoServiceFromParent());
        Assertions.assertEquals(testBean.getDemoService(), testBean.getDemoServiceFromParent());
        Assertions.assertEquals(testBean.getDemoServiceWithArray(), testBean.getDemoServiceWithArrayShouldBeSame());
        context.close();
    }

    private static class AncestorBean {

        private DemoService demoServiceFromAncestor;

        @Autowired
        private ApplicationContext applicationContext;

        public DemoService getDemoServiceFromAncestor() {
            return demoServiceFromAncestor;
        }

        @Reference(version = "1.2", url = "dubbo://127.0.0.1:12345")
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

    @ImportResource("META-INF/spring/dubbo-annotation-consumer.xml")
    @DubboComponentScan(basePackageClasses = ReferenceAnnotationBeanPostProcessorTest.class)
    static class TestBean extends ParentBean {

        private DemoService demoService;

        @Reference(version = "1.2", url = "dubbo://127.0.0.1:12345")
        private DemoService demoServiceShouldBeSame;

        @Reference(version = "1.2", url = "dubbo://127.0.0.1:12345", async = true)
        private DemoService demoServiceShouldNotBeSame;

        @Reference(version = "1.2", url = "dubbo://127.0.0.1:12345", parameters = { "key1", "value1" })
        private DemoService demoServiceWithArray;

        @Reference(version = "1.2", url = "dubbo://127.0.0.1:12345", parameters = { "key1", "value1" })
        private DemoService demoServiceWithArrayShouldBeSame;

        @Autowired
        private ApplicationContext applicationContext;

        public DemoService getDemoService() {
            return demoService;
        }

        @Reference(version = "1.2", url = "dubbo://127.0.0.1:12345")
        public void setDemoService(DemoService demoService) {
            this.demoService = demoService;
        }

        public DemoService getDemoServiceShouldNotBeSame() {
            return demoServiceShouldNotBeSame;
        }

        public DemoService getDemoServiceShouldBeSame() {
            return demoServiceShouldBeSame;
        }

        public DemoService getDemoServiceWithArray() {
            return demoServiceWithArray;
        }

        public DemoService getDemoServiceWithArrayShouldBeSame() {
            return demoServiceWithArrayShouldBeSame;
        }
    }
}