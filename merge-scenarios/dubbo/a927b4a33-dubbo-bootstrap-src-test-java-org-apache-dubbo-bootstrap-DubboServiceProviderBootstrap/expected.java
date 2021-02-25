package org.apache.dubbo.bootstrap;

import org.apache.dubbo.config.MetadataReportConfig;

public class DubboServiceProviderBootstrap {

    public static void main(String[] args) {
        new DubboBootstrap().application("dubbo-provider-demo").registry("zookeeper", builder -> builder.address("zookeeper://127.0.0.1:2181?registry-type=service")).protocol(builder -> builder.port(-1).name("dubbo")).metadataReport(new MetadataReportConfig("zookeeper://127.0.0.1:2181")).service(builder -> builder.id("test").interfaceClass(EchoService.class).ref(new EchoServiceImpl())).start().await();
    }
}