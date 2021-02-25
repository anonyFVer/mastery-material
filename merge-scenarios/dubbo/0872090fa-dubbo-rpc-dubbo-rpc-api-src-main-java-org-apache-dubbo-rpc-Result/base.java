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

    Map<String, String> getAttachments();

    void addAttachments(Map<String, String> map);

    void setAttachments(Map<String, String> map);

    String getAttachment(String key);

    String getAttachment(String key, String defaultValue);

    void setAttachment(String key, String value);

    Result getNow(Result valueIfAbsent);

    Result thenApplyWithContext(Function<Result, Result> fn);

    default CompletableFuture<Result> completionFuture() {
        return toCompletableFuture();
    }
}