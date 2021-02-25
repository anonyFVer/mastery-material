package org.apache.dubbo.config;

import org.apache.dubbo.service.DemoService;
import org.apache.dubbo.service.DemoServiceImpl;
import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ProtocolConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.ServiceConfig;
import org.junit.jupiter.api.Test;

public class ReferenceConfigTest {

    @Test
    public void testInjvm() throws Exception {
        ApplicationConfig application = new ApplicationConfig();
        application.setName("test-protocol-random-port");
        RegistryConfig registry = new RegistryConfig();
        registry.setAddress("multicast://224.5.6.7:1234");
        ProtocolConfig protocol = new ProtocolConfig();
        protocol.setName("dubbo");
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
            demoService.export();
            rc.get();
        } finally {
            demoService.unexport();
        }
    }
}