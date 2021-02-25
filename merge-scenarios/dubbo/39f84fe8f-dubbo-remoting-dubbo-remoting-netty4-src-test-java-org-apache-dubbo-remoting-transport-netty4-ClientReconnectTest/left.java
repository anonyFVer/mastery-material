package org.apache.dubbo.remoting.transport.netty4;

import org.apache.dubbo.common.utils.DubboAppender;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Client;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.Server;
import org.apache.dubbo.remoting.exchange.Exchangers;
import org.apache.dubbo.remoting.exchange.support.ExchangeHandlerAdapter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClientReconnectTest {

    public static void main(String[] args) {
        System.out.println(3 % 1);
    }

    @BeforeEach
    public void clear() {
        DubboAppender.clear();
    }

    @Test
    public void testReconnect() throws RemotingException, InterruptedException {
        {
            int port = NetUtils.getAvailablePort();
            Client client = startClient(port, 200);
            Assertions.assertFalse(client.isConnected());
            Server server = startServer(port);
            for (int i = 0; i < 100 && !client.isConnected(); i++) {
                Thread.sleep(20);
            }
            Assertions.assertTrue(client.isConnected());
            client.close(2000);
            server.close(2000);
        }
        {
            int port = NetUtils.getAvailablePort();
            Client client = startClient(port, 20000);
            Assertions.assertFalse(client.isConnected());
            Server server = startServer(port);
            for (int i = 0; i < 5; i++) {
                Thread.sleep(200);
            }
            Assertions.assertFalse(client.isConnected());
            client.close(2000);
            server.close(2000);
        }
    }

    public Client startClient(int port, int heartbeat) throws RemotingException {
        final String url = "exchange://127.0.0.1:" + port + "/client.reconnect.test?client=netty4&check=false&" + Constants.HEARTBEAT_KEY + "=" + heartbeat;
        return Exchangers.connect(url);
    }

    public Server startServer(int port) throws RemotingException {
        final String url = "exchange://127.0.0.1:" + port + "/client.reconnect.test?server=netty4";
        return Exchangers.bind(url, new HandlerAdapter());
    }

    static class HandlerAdapter extends ExchangeHandlerAdapter {

        @Override
        public void connected(Channel channel) throws RemotingException {
        }

        @Override
        public void disconnected(Channel channel) throws RemotingException {
        }

        @Override
        public void caught(Channel channel, Throwable exception) throws RemotingException {
        }
    }
}