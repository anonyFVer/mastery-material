package org.apache.dubbo.rpc;

import java.util.concurrent.CompletableFuture;

public interface AsyncContext {

    CompletableFuture getInternalFuture();

    void write(Object value);

    boolean isAsyncStarted();

    boolean stop();

    void start();

    void signalContextSwitch();
}