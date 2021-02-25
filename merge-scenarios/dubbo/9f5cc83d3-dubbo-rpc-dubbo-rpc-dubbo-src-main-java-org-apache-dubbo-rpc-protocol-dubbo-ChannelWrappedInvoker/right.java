package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.support.header.HeaderExchangeClient;
import org.apache.dubbo.remoting.transport.ClientDelegate;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.AbstractInvoker;
import org.apache.dubbo.rpc.support.RpcUtils;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

class ChannelWrappedInvoker<T> extends AbstractInvoker<T> {

    private final Channel channel;

    private final String serviceKey;

    private final ExchangeClient currentClient;

    ChannelWrappedInvoker(Class<T> serviceType, Channel channel, URL url, String serviceKey) {
        super(serviceType, url, new String[] { Constants.GROUP_KEY, Constants.TOKEN_KEY, Constants.TIMEOUT_KEY });
        this.channel = channel;
        this.serviceKey = serviceKey;
        this.currentClient = new HeaderExchangeClient(new ChannelWrapper(this.channel), false);
    }

    @Override
    protected Result doInvoke(Invocation invocation) throws Throwable {
        RpcInvocation inv = (RpcInvocation) invocation;
        inv.setAttachment(Constants.PATH_KEY, getInterface().getName());
        inv.setAttachment(Constants.CALLBACK_SERVICE_KEY, serviceKey);
        try {
            if (RpcUtils.isOneway(getUrl(), inv)) {
                currentClient.send(inv, getUrl().getMethodParameter(invocation.getMethodName(), Constants.SENT_KEY, false));
                return AsyncRpcResult.newDefaultAsyncResult(invocation);
            } else {
                CompletableFuture<Object> responseFuture = currentClient.request(inv);
                AsyncRpcResult asyncRpcResult = new AsyncRpcResult(inv);
                responseFuture.whenComplete((appResponse, t) -> {
                    if (t != null) {
                        asyncRpcResult.completeExceptionally(t);
                    } else {
                        asyncRpcResult.complete((AppResponse) appResponse);
                    }
                });
                return asyncRpcResult;
            }
        } catch (RpcException e) {
            throw e;
        } catch (TimeoutException e) {
            throw new RpcException(RpcException.TIMEOUT_EXCEPTION, e.getMessage(), e);
        } catch (RemotingException e) {
            throw new RpcException(RpcException.NETWORK_EXCEPTION, e.getMessage(), e);
        } catch (Throwable e) {
            throw new RpcException(e.getMessage(), e);
        }
    }

    @Override
    public void destroy() {
    }

    public static class ChannelWrapper extends ClientDelegate {

        private final Channel channel;

        private final URL url;

        ChannelWrapper(Channel channel) {
            this.channel = channel;
            this.url = channel.getUrl().addParameter("codec", DubboCodec.NAME);
        }

        @Override
        public URL getUrl() {
            return url;
        }

        @Override
        public ChannelHandler getChannelHandler() {
            return channel.getChannelHandler();
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return channel.getLocalAddress();
        }

        @Override
        public void close() {
            channel.close();
        }

        @Override
        public boolean isClosed() {
            return channel == null || channel.isClosed();
        }

        @Override
        public void reset(URL url) {
            throw new RpcException("ChannelInvoker can not reset.");
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return channel.getLocalAddress();
        }

        @Override
        public boolean isConnected() {
            return channel != null && channel.isConnected();
        }

        @Override
        public boolean hasAttribute(String key) {
            return channel.hasAttribute(key);
        }

        @Override
        public Object getAttribute(String key) {
            return channel.getAttribute(key);
        }

        @Override
        public void setAttribute(String key, Object value) {
            channel.setAttribute(key, value);
        }

        @Override
        public void removeAttribute(String key) {
            channel.removeAttribute(key);
        }

        @Override
        public void reconnect() throws RemotingException {
        }

        @Override
        public void send(Object message) throws RemotingException {
            channel.send(message);
        }

        @Override
        public void send(Object message, boolean sent) throws RemotingException {
            channel.send(message, sent);
        }
    }
}