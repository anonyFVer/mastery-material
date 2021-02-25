package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.support.DemoService;
import org.apache.dubbo.rpc.support.MockInvocation;
import org.apache.dubbo.rpc.support.MyInvoker;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerContextFilterTest {

    Filter consumerContextFilter = new ConsumerContextFilter();

    @Test
    public void testSetContext() {
        URL url = URL.valueOf("test://test:11/test?group=dubbo&version=1.1");
        Invoker<DemoService> invoker = new MyInvoker<DemoService>(url);
        Invocation invocation = new MockInvocation();
        Result asyncResult = consumerContextFilter.invoke(invoker, invocation);
        asyncResult.whenCompleteWithContext((result, t) -> {
            assertEquals(invoker, RpcContext.getContext().getInvoker());
            assertEquals(invocation, RpcContext.getContext().getInvocation());
            assertEquals(NetUtils.getLocalHost() + ":0", RpcContext.getContext().getLocalAddressString());
            assertEquals("test:11", RpcContext.getContext().getRemoteAddressString());
        });
    }
}