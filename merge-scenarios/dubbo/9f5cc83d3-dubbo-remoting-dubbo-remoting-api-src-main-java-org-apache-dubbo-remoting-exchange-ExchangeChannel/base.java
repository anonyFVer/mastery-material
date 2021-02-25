package org.apache.dubbo.remoting.exchange;

import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;

public interface ExchangeChannel extends Channel {

    ResponseFuture request(Object request) throws RemotingException;

    ResponseFuture request(Object request, int timeout) throws RemotingException;

    ExchangeHandler getExchangeHandler();

    @Override
    void close(int timeout);
}