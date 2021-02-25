package org.apache.dubbo.rpc.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;

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

    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        AppResponse result = new AppResponse();
        if (!hasException) {
            result.setValue("alibaba");
        } else {
            result.setException(new RuntimeException("mocked exception"));
        }
        return AsyncRpcResult.newDefaultAsyncResult(result, invocation);
    }

    @Override
    public void destroy() {
    }

    @Override
    public String toString() {
        return "MyInvoker.toString()";
    }
}