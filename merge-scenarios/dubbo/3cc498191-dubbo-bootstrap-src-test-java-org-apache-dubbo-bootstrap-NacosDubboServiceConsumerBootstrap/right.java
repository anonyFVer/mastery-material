package org.apache.dubbo.bootstrap;

import org.apache.dubbo.bootstrap.rest.UserService;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.context.ConfigManager;

public class NacosDubboServiceConsumerBootstrap {

    public static void main(String[] args) throws Exception {
        ApplicationConfig applicationConfig = new ApplicationConfig("dubbo-nacos-consumer-demo");
        new DubboBootstrap().application(applicationConfig).registry("nacos", builder -> builder.address("nacos://127.0.0.1:8848?registry-type=service&subscribed-services=service-provider")).metadataReport(new MetadataReportConfig("nacos://127.0.0.1:8848")).reference("user", builder -> builder.interfaceClass(UserService.class).protocol("rest")).start().await();
        ConfigManager configManager = ConfigManager.getInstance();
        ReferenceConfig<UserService> referenceConfig = configManager.getReference("user");
        UserService userService = referenceConfig.get();
        for (int i = 0; i < 500; i++) {
            Thread.sleep(2000L);
            System.out.println(userService.getUser(i * 1L));
        }
    }
}