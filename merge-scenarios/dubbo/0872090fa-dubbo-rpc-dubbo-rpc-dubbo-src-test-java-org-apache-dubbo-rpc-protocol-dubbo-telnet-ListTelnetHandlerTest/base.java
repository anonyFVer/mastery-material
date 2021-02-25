package org.apache.dubbo.rpc.protocol.dubbo.telnet;

import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.telnet.TelnetHandler;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ProviderModel;
import org.apache.dubbo.rpc.protocol.dubbo.support.DemoService;
import org.apache.dubbo.rpc.protocol.dubbo.support.DemoServiceImpl;
import org.apache.dubbo.rpc.protocol.dubbo.support.ProtocolUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Method;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ListTelnetHandlerTest {

    private static TelnetHandler list = new ListTelnetHandler();

    private Channel mockChannel;

    @BeforeAll
    public static void setUp() {
        ProtocolUtils.closeAll();
    }

    @BeforeEach
    public void init() {
        ApplicationModel.reset();
    }

    @AfterEach
    public void after() {
        ProtocolUtils.closeAll();
    }

    @Test
    public void testListDetailService() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService");
        ProviderModel providerModel = new ProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", providerModel);
        String result = list.telnet(mockChannel, "-l DemoService");
        for (Method method : DemoService.class.getMethods()) {
            assertTrue(result.contains(ReflectUtils.getName(method)));
        }
    }

    @Test
    public void testListService() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService");
        ProviderModel providerModel = new ProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", providerModel);
        String result = list.telnet(mockChannel, "DemoService");
        for (Method method : DemoService.class.getMethods()) {
            assertTrue(result.contains(method.getName()));
        }
    }

    @Test
    public void testList() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn(null);
        ProviderModel providerModel = new ProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", providerModel);
        String result = list.telnet(mockChannel, "");
        assertEquals("PROVIDER:\r\norg.apache.dubbo.rpc.protocol.dubbo.support.DemoService\r\n", result);
    }

    @Test
    public void testListDetail() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn(null);
        ProviderModel providerModel = new ProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", providerModel);
        String result = list.telnet(mockChannel, "-l");
        assertEquals("PROVIDER:\r\norg.apache.dubbo.rpc.protocol.dubbo.support.DemoService ->  published: N\r\n", result);
    }

    @Test
    public void testListDefault() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService");
        ProviderModel providerModel = new ProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", providerModel);
        String result = list.telnet(mockChannel, "");
        assertTrue(result.startsWith("Use default service org.apache.dubbo.rpc.protocol.dubbo.support.DemoService.\r\n" + "org.apache.dubbo.rpc.protocol.dubbo.support.DemoService (as provider):\r\n"));
        for (Method method : DemoService.class.getMethods()) {
            assertTrue(result.contains(method.getName()));
        }
    }

    @Test
    public void testInvalidMessage() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn(null);
        String result = list.telnet(mockChannel, "xx");
        assertEquals("No such service: xx", result);
    }
}