package org.apache.dubbo.registry.dubbo;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class MockChannel implements ExchangeChannel {

    public static boolean closed = false;

    public static boolean closing = true;

    final InetSocketAddress localAddress;

    final InetSocketAddress remoteAddress;

    public MockChannel(String localHostname, int localPort, String remoteHostName, int remotePort) {
        localAddress = new InetSocketAddress(localHostname, localPort);
        remoteAddress = new InetSocketAddress(remoteHostName, remotePort);
        closed = false;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void send(Object message) throws RemotingException {
    }

    @Override
    public void close(int timeout) {
    }

    @Override
    public void startClose() {
        closing = true;
    }

    @Override
    public URL getUrl() {
        return null;
    }

    public CompletableFuture<Object> send(Object request, int timeout) throws RemotingException {
        return null;
    }

    @Override
    public ChannelHandler getChannelHandler() {
        return null;
    }

    public CompletableFuture<Object> request(Object request) throws RemotingException {
        return null;
    }

    public CompletableFuture<Object> request(Object request, int timeout) throws RemotingException {
        return null;
    }

    @Override
    public CompletableFuture<Object> request(Object request, ExecutorService executor) throws RemotingException {
        return null;
    }

    @Override
    public CompletableFuture<Object> request(Object request, int timeout, ExecutorService executor) throws RemotingException {
        return null;
    }

    public ExchangeHandler getExchangeHandler() {
        return null;
    }

    @Override
    public Object getAttribute(String key) {
        return null;
    }

    @Override
    public void setAttribute(String key, Object value) {
    }

    @Override
    public boolean hasAttribute(String key) {
        return false;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void removeAttribute(String key) {
    }

    @Override
    public void send(Object message, boolean sent) throws RemotingException {
    }
}