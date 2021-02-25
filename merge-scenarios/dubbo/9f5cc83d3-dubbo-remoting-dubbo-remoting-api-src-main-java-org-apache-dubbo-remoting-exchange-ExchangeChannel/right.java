package org.apache.dubbo.remoting.exchange;

import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public interface ExchangeChannel extends Channel {

    @Deprecated
    CompletableFuture<Object> request(Object request) throws RemotingException;

    @Deprecated
    CompletableFuture<Object> request(Object request, int timeout) throws RemotingException;

    CompletableFuture<Object> request(Object request, ExecutorService executor) throws RemotingException;

    CompletableFuture<Object> request(Object request, int timeout, ExecutorService executor) throws RemotingException;

    ExchangeHandler getExchangeHandler();

    @Override
    void close(int timeout);
}