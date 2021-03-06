package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.RpcResult;
import org.apache.dubbo.rpc.service.GenericException;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.DemoService;
import org.apache.dubbo.rpc.support.Person;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

public class GenericImplFilterTest {

    private GenericImplFilter genericImplFilter = new GenericImplFilter();

    @Test
    public void testInvoke() throws Exception {
        RpcInvocation invocation = new RpcInvocation("getPerson", new Class[] { Person.class }, new Object[] { new Person("dubbo", 10) });
        URL url = URL.valueOf("test://test:11/org.apache.dubbo.rpc.support.DemoService?" + "accesslog=true&group=dubbo&version=1.1&generic=true");
        Invoker invoker = Mockito.mock(Invoker.class);
        Map<String, Object> person = new HashMap<String, Object>();
        person.put("name", "dubbo");
        person.put("age", 10);
        when(invoker.invoke(any(Invocation.class))).thenReturn(new RpcResult(person));
        when(invoker.getUrl()).thenReturn(url);
        when(invoker.getInterface()).thenReturn(DemoService.class);
        Result result = genericImplFilter.invoke(invoker, invocation);
        Assertions.assertEquals(Person.class, result.getValue().getClass());
        Assertions.assertEquals(10, ((Person) result.getValue()).getAge());
    }

    @Test
    public void testInvokeWithException() throws Exception {
        RpcInvocation invocation = new RpcInvocation("getPerson", new Class[] { Person.class }, new Object[] { new Person("dubbo", 10) });
        URL url = URL.valueOf("test://test:11/org.apache.dubbo.rpc.support.DemoService?" + "accesslog=true&group=dubbo&version=1.1&generic=true");
        Invoker invoker = Mockito.mock(Invoker.class);
        when(invoker.invoke(any(Invocation.class))).thenReturn(new RpcResult(new GenericException(new RuntimeException("failed"))));
        when(invoker.getUrl()).thenReturn(url);
        when(invoker.getInterface()).thenReturn(DemoService.class);
        Result result = genericImplFilter.invoke(invoker, invocation);
        Assertions.assertEquals(RuntimeException.class, result.getException().getClass());
    }

    @Test
    public void testInvokeWith$Invoke() throws Exception {
        Method genericInvoke = GenericService.class.getMethods()[0];
        Map<String, Object> person = new HashMap<String, Object>();
        person.put("name", "dubbo");
        person.put("age", 10);
        RpcInvocation invocation = new RpcInvocation(Constants.$INVOKE, genericInvoke.getParameterTypes(), new Object[] { "getPerson", new String[] { Person.class.getCanonicalName() }, new Object[] { person } });
        URL url = URL.valueOf("test://test:11/org.apache.dubbo.rpc.support.DemoService?" + "accesslog=true&group=dubbo&version=1.1&generic=true");
        Invoker invoker = Mockito.mock(Invoker.class);
        when(invoker.invoke(any(Invocation.class))).thenReturn(new RpcResult(new Person("person", 10)));
        when(invoker.getUrl()).thenReturn(url);
        genericImplFilter.invoke(invoker, invocation);
        Assertions.assertEquals("true", invocation.getAttachment(Constants.GENERIC_KEY));
    }
}