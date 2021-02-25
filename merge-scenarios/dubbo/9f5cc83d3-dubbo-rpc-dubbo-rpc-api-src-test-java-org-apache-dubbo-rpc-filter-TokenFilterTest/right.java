package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.util.HashMap;
import java.util.Map;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class TokenFilterTest {

    private TokenFilter tokenFilter = new TokenFilter();

    @Test
    public void testInvokeWithToken() throws Exception {
        String token = "token";
        Invoker invoker = Mockito.mock(Invoker.class);
        URL url = URL.valueOf("test://test:11/test?accesslog=true&group=dubbo&version=1.1&token=" + token);
        when(invoker.getUrl()).thenReturn(url);
        when(invoker.invoke(any(Invocation.class))).thenReturn(new AppResponse("result"));
        Map<String, Object> attachments = new HashMap<String, Object>();
        attachments.put(Constants.TOKEN_KEY, token);
        Invocation invocation = Mockito.mock(Invocation.class);
        when(invocation.getAttachments()).thenReturn(attachments);
        Result result = tokenFilter.invoke(invoker, invocation);
        Assertions.assertEquals("result", result.getValue());
    }

    @Test
    public void testInvokeWithWrongToken() throws Exception {
        Assertions.assertThrows(RpcException.class, () -> {
            String token = "token";
            Invoker invoker = Mockito.mock(Invoker.class);
            URL url = URL.valueOf("test://test:11/test?accesslog=true&group=dubbo&version=1.1&token=" + token);
            when(invoker.getUrl()).thenReturn(url);
            when(invoker.invoke(any(Invocation.class))).thenReturn(new AppResponse("result"));
            Map<String, Object> attachments = new HashMap<String, Object>();
            attachments.put(Constants.TOKEN_KEY, "wrongToken");
            Invocation invocation = Mockito.mock(Invocation.class);
            when(invocation.getAttachments()).thenReturn(attachments);
            tokenFilter.invoke(invoker, invocation);
        });
    }

    @Test
    public void testInvokeWithoutToken() throws Exception {
        Assertions.assertThrows(RpcException.class, () -> {
            String token = "token";
            Invoker invoker = Mockito.mock(Invoker.class);
            URL url = URL.valueOf("test://test:11/test?accesslog=true&group=dubbo&version=1.1&token=" + token);
            when(invoker.getUrl()).thenReturn(url);
            when(invoker.invoke(any(Invocation.class))).thenReturn(new AppResponse("result"));
            Invocation invocation = Mockito.mock(Invocation.class);
            tokenFilter.invoke(invoker, invocation);
        });
    }
}