package org.apache.dubbo.bootstrap;

import org.apache.dubbo.bootstrap.rest.UserService;
import org.apache.dubbo.bootstrap.rest.UserServiceImpl;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;
import java.util.Arrays;

public class DubboServiceProviderBootstrap {

    public static void main(String[] args) {
        multipleRegistries();
    }

    private static void multipleRegistries() {
        ProtocolConfig restProtocol = new ProtocolConfig();
        restProtocol.setName("rest");
        restProtocol.setId("rest");
        restProtocol.setPort(-1);
        RegistryConfig interfaceRegistry = new RegistryConfig();
        interfaceRegistry.setId("interfaceRegistry");
        interfaceRegistry.setAddress("zookeeper://127.0.0.1:2181");
        RegistryConfig serviceRegistry = new RegistryConfig();
        serviceRegistry.setId("serviceRegistry");
        serviceRegistry.setAddress("zookeeper://127.0.0.1:2181?registry.type=service");
        ServiceConfig<EchoService> echoService = new ServiceConfig<>();
        echoService.setInterface(EchoService.class.getName());
        echoService.setRef(new EchoServiceImpl());
        ServiceConfig<UserService> userService = new ServiceConfig<>();
        userService.setInterface(UserService.class.getName());
        userService.setRef(new UserServiceImpl());
        userService.setProtocol(restProtocol);
        new DubboBootstrap().application("dubbo-provider-demo").registries(Arrays.asList(interfaceRegistry, serviceRegistry)).protocol(builder -> builder.port(-1).name("dubbo")).service(echoService).service(userService).start().await();
    }

    private static void testSCCallDubbo() {
    }

    private static void testDubboCallSC() {
    }

    private static void testDubboTansormation() {
    }
}