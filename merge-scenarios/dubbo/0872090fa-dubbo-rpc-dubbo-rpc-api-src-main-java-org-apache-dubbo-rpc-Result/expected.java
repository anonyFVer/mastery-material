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

    Map<String, Object> getAttachments();

    void addAttachments(Map<String, Object> map);

    void setAttachments(Map<String, Object> map);

    Object getAttachment(String key);

    Object getAttachment(String key, Object defaultValue);

    void setAttachment(String key, Object value);

    Result thenApplyWithContext(Function<AppResponse, AppResponse> fn);

    <U> CompletableFuture<U> thenApply(Function<Result, ? extends U> fn);

    Result get() throws InterruptedException, ExecutionException;

    Result get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;
}