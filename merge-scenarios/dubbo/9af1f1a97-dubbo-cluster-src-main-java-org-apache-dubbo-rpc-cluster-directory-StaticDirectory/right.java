package org.apache.dubbo.rpc.cluster.directory;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Router;
import java.util.List;

public class StaticDirectory<T> extends AbstractDirectory<T> {

    private final List<Invoker<T>> invokers;

    public StaticDirectory(List<Invoker<T>> invokers) {
        this(null, invokers, null);
    }

    public StaticDirectory(List<Invoker<T>> invokers, List<Router> routers) {
        this(null, invokers, routers);
    }

    public StaticDirectory(URL url, List<Invoker<T>> invokers) {
        this(url, invokers, null);
    }

    public StaticDirectory(URL url, List<Invoker<T>> invokers, List<Router> routers) {
        super(url == null && invokers != null && !invokers.isEmpty() ? invokers.get(0).getUrl() : url, routers);
        if (invokers == null || invokers.isEmpty()) {
            throw new IllegalArgumentException("invokers == null");
        }
        this.invokers = invokers;
    }

    @Override
    public Class<T> getInterface() {
        return invokers.get(0).getInterface();
    }

    @Override
    public boolean isAvailable() {
        if (isDestroyed()) {
            return false;
        }
        for (Invoker<T> invoker : invokers) {
            if (invoker.isAvailable()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void destroy() {
        if (isDestroyed()) {
            return;
        }
        super.destroy();
        for (Invoker<T> invoker : invokers) {
            invoker.destroy();
        }
        invokers.clear();
    }

    @Override
    protected List<Invoker<T>> doList(Invocation invocation) throws RpcException {
        return invokers;
    }
}