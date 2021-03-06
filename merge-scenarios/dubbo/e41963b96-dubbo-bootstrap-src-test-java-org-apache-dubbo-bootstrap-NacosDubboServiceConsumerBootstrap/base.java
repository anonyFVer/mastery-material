package org.apache.dubbo.bootstrap;

import org.apache.dubbo.bootstrap.rest.UserService;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.context.ConfigManager;

public class NacosDubboServiceConsumerBootstrap {

    public static void main(String[] args) throws Exception {
        ApplicationConfig applicationConfig = new ApplicationConfig("dubbo-nacos-consumer-demo");
        applicationConfig.setMetadataType("remote");
        new DubboBootstrap().application(applicationConfig).registry("nacos", builder -> builder.address("nacos://127.0.0.1:8848?registry.type=service&subscribed.services=dubbo-nacos-provider-demo")).metadataReport(new MetadataReportConfig("nacos://127.0.0.1:8848")).reference("echo", builder -> builder.interfaceClass(EchoService.class).protocol("dubbo")).reference("user", builder -> builder.interfaceClass(UserService.class).protocol("rest")).start().await();
        ConfigManager configManager = ConfigManager.getInstance();
        ReferenceConfig<EchoService> referenceConfig = configManager.getReference("echo");
        EchoService echoService = referenceConfig.get();
        for (int i = 0; i < 500; i++) {
            Thread.sleep(2000L);
            System.out.println(echoService.echo("Hello,World"));
        }
    }
}