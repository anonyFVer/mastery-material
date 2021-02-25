package org.apache.dubbo.remoting.exchange.support;

import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.handler.MockedChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultFutureTest {

    private static final AtomicInteger index = new AtomicInteger();

    @Test
    public void newFuture() {
        DefaultFuture future = defaultFuture(3000);
        Assertions.assertNotNull(future, "new future return null");
    }

    @Test
    public void isDone() {
        DefaultFuture future = defaultFuture(3000);
        Assertions.assertTrue(!future.isDone(), "init future is finished!");
        future.cancel();
        Assertions.assertTrue(future.isDone(), "cancel a future failed!");
    }

    @Test
    public void timeoutNotSend() throws Exception {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        System.out.println("before a future is create , time is : " + LocalDateTime.now().format(formatter));
        DefaultFuture f = defaultFuture(5000);
        while (!f.isDone()) {
            Thread.sleep(100);
        }
        System.out.println("after a future is timeout , time is : " + LocalDateTime.now().format(formatter));
        try {
            f.get();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof TimeoutException, "catch exception is not timeout exception!");
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void timeoutSend() throws Exception {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        System.out.println("before a future is create , time is : " + LocalDateTime.now().format(formatter));
        Channel channel = new MockedChannel();
        Request request = new Request(10);
        DefaultFuture f = DefaultFuture.newFuture(channel, request, 5000);
        DefaultFuture.sent(channel, request);
        while (!f.isDone()) {
            Thread.sleep(100);
        }
        System.out.println("after a future is timeout , time is : " + LocalDateTime.now().format(formatter));
        try {
            f.get();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof TimeoutException, "catch exception is not timeout exception!");
            System.out.println(e.getMessage());
        }
    }

    private DefaultFuture defaultFuture(int timeout) {
        Channel channel = new MockedChannel();
        Request request = new Request(index.getAndIncrement());
        return DefaultFuture.newFuture(channel, request, timeout);
    }
}