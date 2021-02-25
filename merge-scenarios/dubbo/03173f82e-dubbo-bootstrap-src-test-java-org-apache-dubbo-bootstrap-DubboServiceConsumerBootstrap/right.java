package org.apache.dubbo.bootstrap;

import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.context.ConfigManager;

public class DubboServiceConsumerBootstrap {

    public static void main(String[] args) throws Exception {
        new DubboBootstrap().application("dubbo-consumer-demo").registry("zookeeper", builder -> builder.address("zookeeper://127.0.0.1:2181?registry-type=service&subscribed-services=dubbo-provider-demo")).registry("nacos", builder -> builder.address("nacos://127.0.0.1:8848?registry-type=service&subscribed-services=dubbo-provider-demo")).reference("ref", builder -> builder.interfaceClass(EchoService.class)).onlyRegisterProvider(true).start().await();
        ConfigManager configManager = ConfigManager.getInstance();
        ReferenceConfig<EchoService> referenceConfig = configManager.getReferenceConfig("ref");
        EchoService echoService = referenceConfig.get();
        for (int i = 0; i < 500; i++) {
            Thread.sleep(2000L);
            System.out.println(echoService.echo("Hello,World"));
        }
    }
}