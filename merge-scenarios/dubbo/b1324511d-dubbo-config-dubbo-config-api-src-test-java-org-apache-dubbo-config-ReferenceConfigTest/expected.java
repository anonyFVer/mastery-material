package org.apache.dubbo.config;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.config.api.DemoService;
import org.apache.dubbo.config.provider.impl.DemoServiceImpl;
import org.junit.Assert;
import org.junit.Test;

public class ReferenceConfigTest {

    @Test
    public void testInjvm() throws Exception {
        ApplicationConfig application = new ApplicationConfig();
        application.setName("test-protocol-random-port");
        RegistryConfig registry = new RegistryConfig();
        registry.setAddress("multicast://224.5.6.7:1234");
        ProtocolConfig protocol = new ProtocolConfig();
        protocol.setName("mockprotocol");
        ServiceConfig<DemoService> demoService;
        demoService = new ServiceConfig<DemoService>();
        demoService.setInterface(DemoService.class);
        demoService.setRef(new DemoServiceImpl());
        demoService.setApplication(application);
        demoService.setRegistry(registry);
        demoService.setProtocol(protocol);
        ReferenceConfig<DemoService> rc = new ReferenceConfig<DemoService>();
        rc.setApplication(application);
        rc.setRegistry(registry);
        rc.setInterface(DemoService.class.getName());
        rc.setInjvm(false);
        try {
            System.setProperty("java.net.preferIPv4Stack", "true");
            demoService.export();
            rc.get();
            Assert.assertTrue(!Constants.LOCAL_PROTOCOL.equalsIgnoreCase(rc.getInvoker().getUrl().getProtocol()));
        } finally {
            System.clearProperty("java.net.preferIPv4Stack");
            demoService.unexport();
        }
    }

    @Test
    public void testReferenceRetry() {
        ApplicationConfig application = new ApplicationConfig();
        application.setName("test-reference-retry");
        RegistryConfig registry = new RegistryConfig();
        registry.setAddress("multicast://224.5.6.7:1234");
        ProtocolConfig protocol = new ProtocolConfig();
        protocol.setName("mockprotocol");
        ReferenceConfig<DemoService> rc = new ReferenceConfig<DemoService>();
        rc.setApplication(application);
        rc.setRegistry(registry);
        rc.setInterface(DemoService.class.getName());
        boolean success = false;
        DemoService demoService = null;
        try {
            demoService = rc.get();
            success = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        Assert.assertFalse(success);
        Assert.assertNull(demoService);
        ServiceConfig<DemoService> sc = new ServiceConfig<DemoService>();
        sc.setInterface(DemoService.class);
        sc.setRef(new DemoServiceImpl());
        sc.setApplication(application);
        sc.setRegistry(registry);
        sc.setProtocol(protocol);
        try {
            System.setProperty("java.net.preferIPv4Stack", "true");
            sc.export();
            demoService = rc.get();
            success = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.clearProperty("java.net.preferIPv4Stack");
        }
        Assert.assertTrue(success);
        Assert.assertNotNull(demoService);
    }
}