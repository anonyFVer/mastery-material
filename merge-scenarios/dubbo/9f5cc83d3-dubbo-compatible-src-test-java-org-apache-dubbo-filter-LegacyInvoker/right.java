package org.apache.dubbo.filter;

import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.service.DemoService;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;

public class LegacyInvoker<T> implements Invoker<T> {

    URL url;

    Class<T> type;

    boolean hasException = false;

    public LegacyInvoker(URL url) {
        this.url = url;
        type = (Class<T>) DemoService.class;
    }

    public LegacyInvoker(URL url, boolean hasException) {
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
        AppResponse result = new AppResponse();
        if (hasException == false) {
            result.setValue("alibaba");
        } else {
            result.setException(new RuntimeException("mocked exception"));
        }
        return new Result.CompatibleResult(result);
    }

    @Override
    public void destroy() {
    }
}