package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import java.util.List;
import java.util.Map;

public interface Router extends Comparable<Router> {

    URL getUrl();

    <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException;

    default <T> Map<String, List<Invoker<T>>> preRoute(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        return null;
    }

    void addRouterChain(RouterChain routerChain);

    boolean isRuntime();

    boolean isForce();

    int getPriority();
}