package org.apache.dubbo.bootstrap;

public class DubboServiceProviderBootstrap {

    public static void main(String[] args) {
        new DubboBootstrap().application("dubbo-provider-demo").registry("zookeeper", builder -> builder.address("zookeeper://127.0.0.1:2181?registry-type=service")).registry("nacos", builder -> builder.address("nacos://127.0.0.1:8848?registry-type=service")).protocol(builder -> builder.port(-1).name("dubbo")).service(builder -> builder.id("test").interfaceClass(EchoService.class).ref(new EchoServiceImpl())).start().await();
    }
}