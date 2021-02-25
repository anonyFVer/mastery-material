package org.apache.dubbo.rpc.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcResult;

public class MyInvoker<T> implements Invoker<T> {

    URL url;

    Class<T> type;

    boolean hasException = false;

    public MyInvoker(URL url) {
        this.url = url;
        type = (Class<T>) DemoService.class;
    }

    public MyInvoker(URL url, boolean hasException) {
        this.url = url;
        type = (Class<T>) DemoService.class;
        this.hasException = hasException;
    }

    @Override
    public Class<T> getInterface() {
        return type;
    }

    public URL getUrl() {
        return url;
    }

    @Override
    public boolean isAvailable() {
        return false;
    }

    public Result invoke(Invocation invocation) throws RpcException {
        RpcResult result = new RpcResult();
        if (hasException == false) {
            result.setValue("alibaba");
            return result;
        } else {
            result.setException(new RuntimeException("mocked exception"));
            return result;
        }
    }

    @Override
    public void destroy() {
    }

    @Override
    public String toString() {
        return "MyInvoker.toString()";
    }
}