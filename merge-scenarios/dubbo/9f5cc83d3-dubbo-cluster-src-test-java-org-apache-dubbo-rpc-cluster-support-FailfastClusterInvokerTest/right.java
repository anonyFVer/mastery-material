package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.Directory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@SuppressWarnings("unchecked")
public class FailfastClusterInvokerTest {

    List<Invoker<FailfastClusterInvokerTest>> invokers = new ArrayList<Invoker<FailfastClusterInvokerTest>>();

    URL url = URL.valueOf("test://test:11/test");

    Invoker<FailfastClusterInvokerTest> invoker1 = mock(Invoker.class);

    RpcInvocation invocation = new RpcInvocation();

    Directory<FailfastClusterInvokerTest> dic;

    Result result = new AppResponse();

    @BeforeEach
    public void setUp() throws Exception {
        dic = mock(Directory.class);
        given(dic.getUrl()).willReturn(url);
        given(dic.list(invocation)).willReturn(invokers);
        given(dic.getInterface()).willReturn(FailfastClusterInvokerTest.class);
        invocation.setMethodName("method1");
        invokers.add(invoker1);
    }

    private void resetInvoker1ToException() {
        given(invoker1.invoke(invocation)).willThrow(new RuntimeException());
        given(invoker1.getUrl()).willReturn(url);
        given(invoker1.getInterface()).willReturn(FailfastClusterInvokerTest.class);
    }

    private void resetInvoker1ToNoException() {
        given(invoker1.invoke(invocation)).willReturn(result);
        given(invoker1.getUrl()).willReturn(url);
        given(invoker1.getInterface()).willReturn(FailfastClusterInvokerTest.class);
    }

    @Test
    public void testInvokeExceptoin() {
        Assertions.assertThrows(RpcException.class, () -> {
            resetInvoker1ToException();
            FailfastClusterInvoker<FailfastClusterInvokerTest> invoker = new FailfastClusterInvoker<FailfastClusterInvokerTest>(dic);
            invoker.invoke(invocation);
            Assertions.assertSame(invoker1, RpcContext.getContext().getInvoker());
        });
    }

    @Test()
    public void testInvokeNoExceptoin() {
        resetInvoker1ToNoException();
        FailfastClusterInvoker<FailfastClusterInvokerTest> invoker = new FailfastClusterInvoker<FailfastClusterInvokerTest>(dic);
        Result ret = invoker.invoke(invocation);
        Assertions.assertSame(result, ret);
    }

    @Test()
    public void testNoInvoke() {
        dic = mock(Directory.class);
        given(dic.getUrl()).willReturn(url);
        given(dic.list(invocation)).willReturn(null);
        given(dic.getInterface()).willReturn(FailfastClusterInvokerTest.class);
        invocation.setMethodName("method1");
        invokers.add(invoker1);
        resetInvoker1ToNoException();
        FailfastClusterInvoker<FailfastClusterInvokerTest> invoker = new FailfastClusterInvoker<FailfastClusterInvokerTest>(dic);
        try {
            invoker.invoke(invocation);
            fail();
        } catch (RpcException expected) {
            assertFalse(expected.getCause() instanceof RpcException);
        }
    }
}