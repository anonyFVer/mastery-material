package org.apache.dubbo.remoting.transport.dispatcher;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.transport.ChannelHandlerDelegate;
import java.util.concurrent.ExecutorService;

public class WrappedChannelHandler implements ChannelHandlerDelegate {

    protected static final Logger logger = LoggerFactory.getLogger(WrappedChannelHandler.class);

    protected final ChannelHandler handler;

    protected final URL url;

    public WrappedChannelHandler(ChannelHandler handler, URL url) {
        this.handler = handler;
        this.url = url;
    }

    public void close() {
    }

    @Override
    public void connected(Channel channel) throws RemotingException {
        handler.connected(channel);
    }

    @Override
    public void disconnected(Channel channel) throws RemotingException {
        handler.disconnected(channel);
    }

    @Override
    public void sent(Channel channel, Object message) throws RemotingException {
        handler.sent(channel, message);
    }

    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        handler.received(channel, message);
    }

    @Override
    public void caught(Channel channel, Throwable exception) throws RemotingException {
        handler.caught(channel, exception);
    }

    protected void sendFeedback(Channel channel, Request request, Throwable t) throws RemotingException {
        if (request.isTwoWay()) {
            String msg = "Server side(" + url.getIp() + "," + url.getPort() + ") thread pool is exhausted, detail msg:" + t.getMessage();
            Response response = new Response(request.getId(), request.getVersion());
            response.setStatus(Response.SERVER_THREADPOOL_EXHAUSTED_ERROR);
            response.setErrorMessage(msg);
            channel.send(response);
            return;
        }
    }

    @Override
    public ChannelHandler getHandler() {
        if (handler instanceof ChannelHandlerDelegate) {
            return ((ChannelHandlerDelegate) handler).getHandler();
        } else {
            return handler;
        }
    }

    public URL getUrl() {
        return url;
    }

    public ExecutorService getPreferredExecutorService(Object msg) {
        if (msg instanceof Response) {
            Response response = (Response) msg;
            DefaultFuture responseFuture = DefaultFuture.getFuture(response.getId());
            if (responseFuture == null) {
                return getSharedExecutorService();
            } else {
                ExecutorService executor = responseFuture.getExecutor();
                if (executor == null || executor.isShutdown()) {
                    executor = getSharedExecutorService();
                }
                return executor;
            }
        } else {
            return getSharedExecutorService();
        }
    }

    public ExecutorService getSharedExecutorService() {
        return ExtensionLoader.getExtensionLoader(ExecutorRepository.class).getDefaultExtension().createExecutorIfAbsent(url);
    }

    @Deprecated
    public ExecutorService getExecutorService() {
        return getSharedExecutorService();
    }
}