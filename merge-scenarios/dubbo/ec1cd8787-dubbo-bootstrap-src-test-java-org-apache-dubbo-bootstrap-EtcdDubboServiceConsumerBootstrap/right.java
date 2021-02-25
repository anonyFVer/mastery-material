package org.apache.dubbo.bootstrap;

import org.apache.dubbo.bootstrap.rest.UserService;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.context.ConfigManager;

public class EtcdDubboServiceConsumerBootstrap {

    public static void main(String[] args) throws Exception {
        new DubboBootstrap().application("dubbo-consumer-demo").protocol(builder -> builder.port(20887).name("dubbo")).registry("zookeeper", builder -> builder.address("etcd3://127.0.0.1:2379?registry.type=service&subscribed.services=dubbo-provider-demo")).metadataReport(new MetadataReportConfig("etcd://127.0.0.1:2379")).reference("echo", builder -> builder.interfaceClass(EchoService.class).protocol("dubbo")).reference("user", builder -> builder.interfaceClass(UserService.class).protocol("rest")).start().await();
        ConfigManager configManager = ConfigManager.getInstance();
        ReferenceConfig<EchoService> referenceConfig = configManager.getReference("echo");
        EchoService echoService = referenceConfig.get();
        for (int i = 0; i < 500; i++) {
            Thread.sleep(2000L);
            System.out.println(echoService.echo("Hello,World"));
        }
    }
}