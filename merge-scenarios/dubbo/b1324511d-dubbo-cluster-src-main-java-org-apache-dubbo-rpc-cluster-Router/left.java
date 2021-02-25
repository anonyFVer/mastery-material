package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import java.util.List;

public interface Router extends Comparable<Router> {

    URL getUrl();

    <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException;

    int getPriority();

    @Override
    default int compareTo(Router o) {
        if (o == null) {
            throw new IllegalArgumentException();
        }
        if (this.getPriority() == o.getPriority()) {
            if (o.getUrl() == null) {
                return -1;
            }
            return getUrl().toFullString().compareTo(o.getUrl().toFullString());
        } else {
            return getPriority() > o.getPriority() ? 1 : -1;
        }
    }
}