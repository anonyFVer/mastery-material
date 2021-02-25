package org.apache.dubbo.registry.dubbo;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Codec;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.support.Replier;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class MockedClient implements ExchangeClient {

    private boolean connected;

    private Object received;

    private Object sent;

    private Object invoked;

    private Replier<?> handler;

    private InetSocketAddress address;

    private boolean closed = false;

    public MockedClient(String host, int port, boolean connected) {
        this(host, port, connected, null);
    }

    public MockedClient(String host, int port, boolean connected, Object received) {
        this.address = new InetSocketAddress(host, port);
        this.connected = connected;
        this.received = received;
    }

    public void open() {
    }

    @Override
    public void close() {
        this.closed = true;
    }

    @Override
    public void send(Object msg) throws RemotingException {
        this.sent = msg;
    }

    public CompletableFuture<Object> request(Object msg) throws RemotingException {
        return request(msg, 0);
    }

    public CompletableFuture<Object> request(Object msg, int timeout) throws RemotingException {
        this.invoked = msg;
        return new CompletableFuture<Object>() {

            public Object get() throws InterruptedException, ExecutionException {
                return received;
            }

            public Object get(int timeoutInMillis) throws InterruptedException, ExecutionException, TimeoutException {
                return received;
            }

            public boolean isDone() {
                return true;
            }
        };
    }

    public void registerHandler(Replier<?> handler) {
        this.handler = handler;
    }

    public void unregisterHandler(Replier<?> handler) {
    }

    public void addChannelListener(ChannelHandler listener) {
    }

    public void removeChannelListener(ChannelHandler listener) {
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    public Object getSent() {
        return sent;
    }

    public Replier<?> getHandler() {
        return handler;
    }

    public Object getInvoked() {
        return invoked;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return address;
    }

    public String getName() {
        return "mocked";
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return null;
    }

    public int getTimeout() {
        return 0;
    }

    public void setTimeout(int timeout) {
    }

    @Override
    public void close(int timeout) {
        close();
    }

    @Override
    public void startClose() {
    }

    public boolean isOpen() {
        return closed;
    }

    public Codec getCodec() {
        return null;
    }

    public void setCodec(Codec codec) {
    }

    public String getHost() {
        return null;
    }

    public void setHost(String host) {
    }

    public int getPort() {
        return 0;
    }

    public void setPort(int port) {
    }

    public int getThreadCount() {
        return 0;
    }

    public void setThreadCount(int threadCount) {
    }

    @Override
    public URL getUrl() {
        return null;
    }

    public Replier<?> getReceiver() {
        return null;
    }

    @Override
    public ChannelHandler getChannelHandler() {
        return null;
    }

    public void reset(Map<String, String> parameters) {
    }

    public Channel getChannel() {
        return this;
    }

    public ExchangeHandler getExchangeHandler() {
        return null;
    }

    @Override
    public void reconnect() throws RemotingException {
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
        return closed;
    }

    @Override
    public void removeAttribute(String key) {
    }

    public Object getReceived() {
        return received;
    }

    public void setReceived(Object received) {
        this.received = received;
    }

    @Override
    public void send(Object message, boolean sent) throws RemotingException {
    }

    @Override
    public void reset(URL url) {
    }

    @Deprecated
    public void reset(org.apache.dubbo.common.Parameters parameters) {
    }
}