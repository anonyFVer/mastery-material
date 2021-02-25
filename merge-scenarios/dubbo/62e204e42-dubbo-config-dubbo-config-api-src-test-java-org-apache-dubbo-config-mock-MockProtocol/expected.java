package org.apache.dubbo.config.mock;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProtocolServer;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.mockito.Mockito;
import java.util.List;

public class MockProtocol implements Protocol {

    @Override
    public int getDefaultPort() {
        return 0;
    }

    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        return Mockito.mock(Exporter.class);
    }

    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        final URL u = url;
        return new Invoker<T>() {

            @Override
            public Class<T> getInterface() {
                return null;
            }

            public URL getUrl() {
                return u;
            }

            @Override
            public boolean isAvailable() {
                return true;
            }

            @Override
            public Result invoke(Invocation invocation) throws RpcException {
                return null;
            }

            @Override
            public void destroy() {
            }
        };
    }

    @Override
    public void destroy() {
    }

    @Override
    public List<ProtocolServer> getServers() {
        return null;
    }
}