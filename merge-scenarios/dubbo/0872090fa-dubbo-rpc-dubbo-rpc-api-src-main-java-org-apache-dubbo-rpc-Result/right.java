package org.apache.dubbo.rpc;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public interface Result extends Serializable {

    Object getValue();

    void setValue(Object value);

    Throwable getException();

    void setException(Throwable t);

    boolean hasException();

    Object recreate() throws Throwable;

    Map<String, String> getAttachments();

    void addAttachments(Map<String, String> map);

    void setAttachments(Map<String, String> map);

    String getAttachment(String key);

    String getAttachment(String key, String defaultValue);

    void setAttachment(String key, String value);

    Result thenApplyWithContext(Function<AppResponse, AppResponse> fn);

    <U> CompletableFuture<U> thenApply(Function<Result, ? extends U> fn);

    Result get() throws InterruptedException, ExecutionException;

    Result get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;
}