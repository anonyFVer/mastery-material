package org.apache.dubbo.rpc;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.function.Function;

public interface Result extends CompletionStage<Result>, Future<Result>, Serializable {

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

    Result getNow(Result valueIfAbsent);

    Result thenApplyWithContext(Function<Result, Result> fn);

    default CompletableFuture<Result> completionFuture() {
        return toCompletableFuture();
    }
}