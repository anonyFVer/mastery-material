package org.apache.dubbo.bootstrap;

import org.apache.dubbo.config.builders.ApplicationBuilder;
import org.apache.dubbo.config.builders.ReferenceBuilder;
import org.apache.dubbo.config.builders.RegistryBuilder;

public class DubboServiceConsumerBootstrap {

    public static void main(String[] args) throws Exception {
        DubboBootstrap bootstrap = new DubboBootstrap().application(ApplicationBuilder.newBuilder().name("dubbo-consumer-demo").build()).registry(RegistryBuilder.newBuilder().address("zookeeper://127.0.0.1:2181?registry-type=service&subscribed-services=dubbo-provider-demo&metadata=remote").build()).reference(ReferenceBuilder.newBuilder().id("ref").interfaceClass(EchoService.class).build()).onlyRegisterProvider(true).start().await();
    }
}