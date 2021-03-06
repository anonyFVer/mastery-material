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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class InvokerTelnetHandlerTest {

    private static TelnetHandler invoke = new InvokeTelnetHandler();

    private Channel mockChannel;

    @Before
    public void setup() {
        ApplicationModel.reset();
    }

    @After
    public void after() {
        ProtocolUtils.closeAll();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInvokeDefaultSService() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService");
        given(mockChannel.getLocalAddress()).willReturn(NetUtils.toAddress("127.0.0.1:5555"));
        given(mockChannel.getRemoteAddress()).willReturn(NetUtils.toAddress("127.0.0.1:20886"));
        ProviderModel providerModel = new ProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", providerModel);
        String result = invoke.telnet(mockChannel, "DemoService.echo(\"ok\")");
        assertTrue(result.contains("Use default service org.apache.dubbo.rpc.protocol.dubbo.support.DemoService.\r\n\"ok\"\r\n"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInvokeByPassingNullValue() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService");
        given(mockChannel.getLocalAddress()).willReturn(NetUtils.toAddress("127.0.0.1:5555"));
        given(mockChannel.getRemoteAddress()).willReturn(NetUtils.toAddress("127.0.0.1:20886"));
        ProviderModel providerModel = new ProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", providerModel);
        try {
            invoke.telnet(mockChannel, "DemoService.add(null, 2)");
            fail("It should cause a NullPointerException by the above code.");
        } catch (NullPointerException ex) {
            String message = ex.getMessage();
            assertEquals("The type of No.1 parameter is primitive(int), but the value passed is null.", message);
        }
        try {
            invoke.telnet(mockChannel, "DemoService.add(1, null)");
            fail("It should cause a NullPointerException by the above code.");
        } catch (NullPointerException ex) {
            String message = ex.getMessage();
            assertEquals("The type of No.2 parameter is primitive(long), but the value passed is null.", message);
        }
        try {
            invoke.telnet(mockChannel, "DemoService.sayHello(null)");
        } catch (NullPointerException ex) {
            fail("It shouldn't cause a NullPointerException by the above code.");
        }
    }

    @Test
    public void testInvokeByPassingEnumValue() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn(null);
        given(mockChannel.getLocalAddress()).willReturn(NetUtils.toAddress("127.0.0.1:5555"));
        given(mockChannel.getRemoteAddress()).willReturn(NetUtils.toAddress("127.0.0.1:20886"));
        ProviderModel providerModel = new ProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", providerModel);
        String result = invoke.telnet(mockChannel, "getType(\"High\")");
        assertTrue(result.contains("High"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testComplexParamWithoutSpecifyParamType() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService");
        given(mockChannel.getLocalAddress()).willReturn(NetUtils.toAddress("127.0.0.1:5555"));
        given(mockChannel.getRemoteAddress()).willReturn(NetUtils.toAddress("127.0.0.1:20886"));
        ProviderModel providerModel = new ProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", providerModel);
        String result = invoke.telnet(mockChannel, "DemoService.getPerson({\"name\":\"zhangsan\",\"age\":12})");
        assertTrue(result.contains("No such method getPerson in service DemoService"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testComplexParamSpecifyParamType() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService");
        given(mockChannel.getLocalAddress()).willReturn(NetUtils.toAddress("127.0.0.1:5555"));
        given(mockChannel.getRemoteAddress()).willReturn(NetUtils.toAddress("127.0.0.1:20886"));
        ProviderModel providerModel = new ProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", providerModel);
        String result = invoke.telnet(mockChannel, "DemoService.getPerson({\"name\":\"zhangsan\",\"age\":12}) -p org.apache.dubbo.rpc.protocol.dubbo.support.Person");
        assertTrue(result.contains("Use default service org.apache.dubbo.rpc.protocol.dubbo.support.DemoService.\r\n1\r\n"));
        result = invoke.telnet(mockChannel, "DemoService.getPerson({\"name\":\"zhangsan\",\"age\":12},{\"name\":\"lisi\",\"age\":12}) " + "-p org.apache.dubbo.rpc.protocol.dubbo.support.Person " + "org.apache.dubbo.rpc.protocol.dubbo.support.Person");
        assertTrue(result.contains("Use default service org.apache.dubbo.rpc.protocol.dubbo.support.DemoService.\r\n2\r\n"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testComplexParamSpecifyWrongParamType() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService");
        given(mockChannel.getLocalAddress()).willReturn(NetUtils.toAddress("127.0.0.1:5555"));
        given(mockChannel.getRemoteAddress()).willReturn(NetUtils.toAddress("127.0.0.1:20886"));
        ProviderModel providerModel = new ProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", providerModel);
        String result = invoke.telnet(mockChannel, "DemoService.getPerson({\"name\":\"zhangsan\",\"age\":12}) -p wrongType");
        assertEquals("Unknown parameter class for name wrongType", result);
        result = invoke.telnet(mockChannel, "DemoService.getPerson({\"name\":\"zhangsan\",\"age\":12},{\"name\":\"lisi\",\"age\":12}) " + "-p org.apache.dubbo.rpc.protocol.dubbo.support.Person");
        assertEquals("Parameter's number does not match the number of parameter class", result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testInvokeAutoFindMethod() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn(null);
        given(mockChannel.getLocalAddress()).willReturn(NetUtils.toAddress("127.0.0.1:5555"));
        given(mockChannel.getRemoteAddress()).willReturn(NetUtils.toAddress("127.0.0.1:20886"));
        ProviderModel providerModel = new ProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", new DemoServiceImpl(), DemoService.class);
        ApplicationModel.initProviderModel("org.apache.dubbo.rpc.protocol.dubbo.support.DemoService", providerModel);
        String result = invoke.telnet(mockChannel, "echo(\"ok\")");
        assertTrue(result.contains("ok"));
    }

    @Test
    public void testMessageNull() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn(null);
        String result = invoke.telnet(mockChannel, null);
        assertEquals("Please input method name, eg: \r\ninvoke xxxMethod(1234, \"abcd\", {\"prop\" : \"value\"})\r\ninvoke XxxService.xxxMethod(1234, \"abcd\", {\"prop\" : \"value\"})\r\ninvoke com.xxx.XxxService.xxxMethod(1234, \"abcd\", {\"prop\" : \"value\"})", result);
    }

    @Test
    public void testInvalidMessage() throws RemotingException {
        mockChannel = mock(Channel.class);
        given(mockChannel.getAttribute("telnet.service")).willReturn(null);
        String result = invoke.telnet(mockChannel, "(");
        assertEquals("Invalid parameters, format: service.method(args)", result);
    }
}