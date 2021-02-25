package org.apache.dubbo.rpc.protocol.dubbo.telnet;

import org.apache.dubbo.common.utils.NetUtils;
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
import org.junit.jupiter.api.Test;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class SelectTelnetHandlerTest {

    private static TelnetHandler select = new SelectTelnetHandler();

    private Channel mockChannel;

    List<Method> methods;

    @BeforeEach
    public void setup() {
        String methodName = "getPerson";
        methods = new ArrayList<>();
        for (Method method : DemoService.class.getMethods()) {
            if (method.getName().equals(methodName)) {
                methods.add(method);
            }
        }
        ApplicationModel.reset();
    }

    @AfterEach
    public void after() {
        ProtocolUtils.closeAll();
    }

    @Test
    public void testInvokeWithoutMethodList() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn(DemoService.class.getName());
        given(mockChannel.getLocalAddress()).willReturn(NetUtils.toAddress("127.0.0.1:5555"));
        given(mockChannel.getRemoteAddress()).willReturn(NetUtils.toAddress("127.0.0.1:20886"));
        ProviderModel providerModel = new ProviderModel(DemoService.class.getName(), new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel(DemoService.class.getName(), providerModel);
        String result = select.telnet(mockChannel, "1");
        assertTrue(result.contains("Please use the invoke command first."));
    }

    @Test
    public void testInvokeWithIllegalMessage() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn(DemoService.class.getName());
        given(mockChannel.getAttribute(InvokeTelnetHandler.INVOKE_METHOD_LIST_KEY)).willReturn(methods);
        given(mockChannel.getLocalAddress()).willReturn(NetUtils.toAddress("127.0.0.1:5555"));
        given(mockChannel.getRemoteAddress()).willReturn(NetUtils.toAddress("127.0.0.1:20886"));
        ProviderModel providerModel = new ProviderModel(DemoService.class.getName(), new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel(DemoService.class.getName(), providerModel);
        String result = select.telnet(mockChannel, "index");
        assertTrue(result.contains("Illegal index ,please input select 1"));
        result = select.telnet(mockChannel, "0");
        assertTrue(result.contains("Illegal index ,please input select 1"));
        result = select.telnet(mockChannel, "1000");
        assertTrue(result.contains("Illegal index ,please input select 1"));
    }

    @Test
    public void testInvokeWithNull() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn(DemoService.class.getName());
        given(mockChannel.getAttribute(InvokeTelnetHandler.INVOKE_METHOD_LIST_KEY)).willReturn(methods);
        given(mockChannel.getLocalAddress()).willReturn(NetUtils.toAddress("127.0.0.1:5555"));
        given(mockChannel.getRemoteAddress()).willReturn(NetUtils.toAddress("127.0.0.1:20886"));
        ProviderModel providerModel = new ProviderModel(DemoService.class.getName(), new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel(DemoService.class.getName(), providerModel);
        String result = select.telnet(mockChannel, null);
        assertTrue(result.contains("Please input the index of the method you want to invoke"));
    }
}