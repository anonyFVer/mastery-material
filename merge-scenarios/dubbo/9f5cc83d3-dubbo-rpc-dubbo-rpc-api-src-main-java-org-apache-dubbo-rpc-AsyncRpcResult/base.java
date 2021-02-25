package org.apache.dubbo.rpc;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

public class AsyncRpcResult extends AbstractResult {

    private static final Logger logger = LoggerFactory.getLogger(AsyncRpcResult.class);

    private RpcContext storedContext;

    private RpcContext storedServerContext;

    protected CompletableFuture<Object> valueFuture;

    protected CompletableFuture<Result> resultFuture;

    public AsyncRpcResult(CompletableFuture<Object> future) {
        this(future, true);
    }

    public AsyncRpcResult(CompletableFuture<Object> future, boolean registerCallback) {
        this(future, new CompletableFuture<>(), registerCallback);
    }

    public AsyncRpcResult(CompletableFuture<Object> future, final CompletableFuture<Result> rFuture, boolean registerCallback) {
        if (rFuture == null) {
            throw new IllegalArgumentException();
        }
        resultFuture = rFuture;
        if (registerCallback) {
            future.whenComplete((v, t) -> {
                RpcResult rpcResult;
                if (t != null) {
                    if (t instanceof CompletionException) {
                        rpcResult = new RpcResult(t.getCause());
                    } else {
                        rpcResult = new RpcResult(t);
                    }
                } else {
                    rpcResult = new RpcResult(v);
                }
                rFuture.complete(rpcResult);
            });
        }
        this.valueFuture = future;
        this.storedContext = RpcContext.getContext().copyOf();
        this.storedServerContext = RpcContext.getServerContext().copyOf();
    }

    @Override
    public Object getValue() {
        return getRpcResult().getValue();
    }

    @Override
    public Throwable getException() {
        return getRpcResult().getException();
    }

    @Override
    public boolean hasException() {
        return getRpcResult().hasException();
    }

    @Override
    public Object getResult() {
        return getRpcResult().getResult();
    }

    public CompletableFuture getValueFuture() {
        return valueFuture;
    }

    public CompletableFuture<Result> getResultFuture() {
        return resultFuture;
    }

    public void setResultFuture(CompletableFuture<Result> resultFuture) {
        this.resultFuture = resultFuture;
    }

    public Result getRpcResult() {
        try {
            if (resultFuture.isDone()) {
                return resultFuture.get();
            }
        } catch (Exception e) {
            logger.error("Got exception when trying to fetch the underlying result from AsyncRpcResult.", e);
        }
        return new RpcResult();
    }

    @Override
    public Object recreate() throws Throwable {
        return valueFuture;
    }

    public void thenApplyWithContext(Function<Result, Result> fn) {
        this.resultFuture = resultFuture.thenApply(fn.compose(beforeContext).andThen(afterContext));
    }

    @Override
    public Map<String, String> getAttachments() {
        return getRpcResult().getAttachments();
    }

    @Override
    public void setAttachments(Map<String, String> map) {
        getRpcResult().setAttachments(map);
    }

    @Override
    public void addAttachments(Map<String, String> map) {
        getRpcResult().addAttachments(map);
    }

    @Override
    public String getAttachment(String key) {
        return getRpcResult().getAttachment(key);
    }

    @Override
    public String getAttachment(String key, String defaultValue) {
        return getRpcResult().getAttachment(key, defaultValue);
    }

    @Override
    public void setAttachment(String key, String value) {
        getRpcResult().setAttachment(key, value);
    }

    private RpcContext tmpContext;

    private RpcContext tmpServerContext;

    private Function<Result, Result> beforeContext = (result) -> {
        tmpContext = RpcContext.getContext();
        tmpServerContext = RpcContext.getServerContext();
        RpcContext.restoreContext(storedContext);
        RpcContext.restoreServerContext(storedServerContext);
        return result;
    };

    private Function<Result, Result> afterContext = (result) -> {
        RpcContext.restoreContext(tmpContext);
        RpcContext.restoreServerContext(tmpServerContext);
        return result;
    };
}