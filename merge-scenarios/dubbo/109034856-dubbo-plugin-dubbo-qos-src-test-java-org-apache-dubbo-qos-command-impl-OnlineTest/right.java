package org.apache.dubbo.qos.command.impl;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.qos.command.CommandContext;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.support.ProviderConsumerRegTable;
import org.apache.dubbo.registry.support.ProviderInvokerWrapper;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ProviderModel;
import org.apache.dubbo.rpc.model.ServiceMetadata;
import org.junit.jupiter.api.Test;
import static org.apache.dubbo.registry.support.ProviderConsumerRegTable.getProviderInvoker;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OnlineTest {

    @Test
    public void testExecute() throws Exception {
        ProviderModel providerModel = mock(ProviderModel.class);
        when(providerModel.getServiceMetadata()).thenReturn(new ServiceMetadata("org.apache.dubbo.qos.command.impl.TestInterface", "ddd", "1.2.3.online", TestInterface.class));
        when(providerModel.getServiceName()).thenReturn("org.apache.dubbo.qos.command.impl.TestInterface");
        ApplicationModel.initProviderModel("org.apache.dubbo.qos.command.impl.TestInterface", providerModel);
        Invoker providerInvoker = mock(Invoker.class);
        URL registryUrl = mock(URL.class);
        when(registryUrl.toFullString()).thenReturn("test://localhost:8080");
        URL providerUrl = mock(URL.class);
        String serviceKey = "ddd/org.apache.dubbo.qos.command.impl.TestInterface:1.2.3.online";
        when(providerUrl.getServiceKey()).thenReturn(serviceKey);
        when(providerUrl.toFullString()).thenReturn("dubbo://localhost:8888/org.apache.dubbo.qos.command.impl.TestInterface?version=1.2.3.online&group=ddd");
        when(providerInvoker.getUrl()).thenReturn(providerUrl);
        ProviderConsumerRegTable.registerProvider(providerInvoker, registryUrl, providerUrl);
        Registry registry = mock(Registry.class);
        TestRegistryFactory.registry = registry;
        for (ProviderInvokerWrapper wrapper : getProviderInvoker(serviceKey)) {
            assertTrue(!wrapper.isReg());
        }
        Online online = new Online();
        String output = online.execute(mock(CommandContext.class), new String[] { "org.apache.dubbo.qos.command.impl.TestInterface" });
        assertThat(output, equalTo("OK"));
        for (ProviderInvokerWrapper wrapper : getProviderInvoker(serviceKey)) {
            assertTrue(wrapper.isReg());
        }
    }
}