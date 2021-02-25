package org.apache.dubbo.bootstrap;

import org.apache.dubbo.config.builders.ApplicationBuilder;
import org.apache.dubbo.config.builders.MetadataReportBuilder;
import org.apache.dubbo.config.builders.ProtocolBuilder;
import org.apache.dubbo.config.builders.RegistryBuilder;
import org.apache.dubbo.config.builders.ServiceBuilder;
import java.io.IOException;

public class DubboServiceProviderBootstrap {

    public static void main(String[] args) throws IOException {
        new DubboBootstrap().application(ApplicationBuilder.newBuilder().name("dubbo-provider-demo").metadata("remote").build()).metadataReport(MetadataReportBuilder.newBuilder().address("zookeeper://127.0.0.1:2181").build()).registry(RegistryBuilder.newBuilder().address("zookeeper://127.0.0.1:2181?registry-type=service").build()).protocol(ProtocolBuilder.newBuilder().port(-1).name("dubbo").build()).service(ServiceBuilder.newBuilder().id("test").interfaceClass(EchoService.class).ref(new EchoServiceImpl()).build()).start().await();
    }
}