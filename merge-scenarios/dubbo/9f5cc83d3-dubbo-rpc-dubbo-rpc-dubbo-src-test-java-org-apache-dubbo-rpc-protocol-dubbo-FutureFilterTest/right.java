package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.dubbo.filter.FutureFilter;
import org.apache.dubbo.rpc.protocol.dubbo.support.DemoService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class FutureFilterTest {

    private static RpcInvocation invocation;

    private Filter eventFilter = new FutureFilter();

    @BeforeAll
    public static void setUp() {
        invocation = new RpcInvocation();
        invocation.setMethodName("echo");
        invocation.setParameterTypes(new Class<?>[] { Enum.class });
        invocation.setArguments(new Object[] { "hello" });
    }

    @Test
    public void testSyncCallback() {
        @SuppressWarnings("unchecked")
        Invoker<DemoService> invoker = mock(Invoker.class);
        given(invoker.isAvailable()).willReturn(true);
        given(invoker.getInterface()).willReturn(DemoService.class);
        AppResponse result = new AppResponse();
        result.setValue("High");
        given(invoker.invoke(invocation)).willReturn(result);
        URL url = URL.valueOf("test://test:11/test?group=dubbo&version=1.1");
        given(invoker.getUrl()).willReturn(url);
        Result filterResult = eventFilter.invoke(invoker, invocation);
        assertEquals("High", filterResult.getValue());
    }

    @Test
    public void testSyncCallbackHasException() throws RpcException, Throwable {
        Assertions.assertThrows(RuntimeException.class, () -> {
            @SuppressWarnings("unchecked")
            Invoker<DemoService> invoker = mock(Invoker.class);
            given(invoker.isAvailable()).willReturn(true);
            given(invoker.getInterface()).willReturn(DemoService.class);
            AppResponse result = new AppResponse();
            result.setException(new RuntimeException());
            given(invoker.invoke(invocation)).willReturn(result);
            URL url = URL.valueOf("test://test:11/test?group=dubbo&version=1.1&" + Constants.ON_THROW_METHOD_KEY + "=echo");
            given(invoker.getUrl()).willReturn(url);
            eventFilter.invoke(invoker, invocation).recreate();
        });
    }
}