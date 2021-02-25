package org.apache.dubbo.remoting.exchange;

import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import java.util.concurrent.CompletableFuture;

public interface ExchangeChannel extends Channel {

    CompletableFuture<Object> request(Object request) throws RemotingException;

    CompletableFuture<Object> request(Object request, int timeout) throws RemotingException;

    ExchangeHandler getExchangeHandler();

    @Override
    void close(int timeout);
}