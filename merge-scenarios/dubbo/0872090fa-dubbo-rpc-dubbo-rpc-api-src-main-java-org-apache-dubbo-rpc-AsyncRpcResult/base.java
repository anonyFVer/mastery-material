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

    private Invocation invocation;

    public AsyncRpcResult(Invocation invocation) {
        this.invocation = invocation;
        this.storedContext = RpcContext.getContext();
        this.storedServerContext = RpcContext.getServerContext();
    }

    public AsyncRpcResult(AsyncRpcResult asyncRpcResult) {
        this.invocation = asyncRpcResult.getInvocation();
        this.storedContext = asyncRpcResult.getStoredContext();
        this.storedServerContext = asyncRpcResult.getStoredServerContext();
    }

    @Override
    public Object getValue() {
        return getAppResponse().getValue();
    }

    @Override
    public void setValue(Object value) {
        AppResponse appResponse = new AppResponse();
        appResponse.setValue(value);
        this.complete(appResponse);
    }

    @Override
    public Throwable getException() {
        return getAppResponse().getException();
    }

    @Override
    public void setException(Throwable t) {
        AppResponse appResponse = new AppResponse();
        appResponse.setException(t);
        this.complete(appResponse);
    }

    @Override
    public boolean hasException() {
        return getAppResponse().hasException();
    }

    public Result getAppResponse() {
        try {
            if (this.isDone()) {
                return this.get();
            }
        } catch (Exception e) {
            logger.error("Got exception when trying to fetch the underlying result from AsyncRpcResult.", e);
        }
        return new AppResponse();
    }

    @Override
    public Object recreate() throws Throwable {
        RpcInvocation rpcInvocation = (RpcInvocation) invocation;
        if (InvokeMode.FUTURE == rpcInvocation.getInvokeMode()) {
            AppResponse appResponse = new AppResponse();
            CompletableFuture<Object> future = new CompletableFuture<>();
            appResponse.setValue(future);
            this.whenComplete((result, t) -> {
                if (t != null) {
                    if (t instanceof CompletionException) {
                        t = t.getCause();
                    }
                    future.completeExceptionally(t);
                } else {
                    if (result.hasException()) {
                        future.completeExceptionally(result.getException());
                    } else {
                        future.complete(result.getValue());
                    }
                }
            });
            return appResponse.recreate();
        } else if (this.isDone()) {
            return this.get().recreate();
        }
        return (new AppResponse()).recreate();
    }

    @Override
    public Result thenApplyWithContext(Function<Result, Result> fn) {
        this.thenApply(fn.compose(beforeContext).andThen(afterContext));
        return this;
    }

    public void subscribeTo(CompletableFuture<?> future) {
        future.whenComplete((obj, t) -> {
            if (t != null) {
                this.completeExceptionally(t);
            } else {
                this.complete((Result) obj);
            }
        });
    }

    @Override
    public Map<String, String> getAttachments() {
        return getAppResponse().getAttachments();
    }

    @Override
    public void setAttachments(Map<String, String> map) {
        getAppResponse().setAttachments(map);
    }

    @Override
    public void addAttachments(Map<String, String> map) {
        getAppResponse().addAttachments(map);
    }

    @Override
    public String getAttachment(String key) {
        return getAppResponse().getAttachment(key);
    }

    @Override
    public String getAttachment(String key, String defaultValue) {
        return getAppResponse().getAttachment(key, defaultValue);
    }

    @Override
    public void setAttachment(String key, String value) {
        getAppResponse().setAttachment(key, value);
    }

    public RpcContext getStoredContext() {
        return storedContext;
    }

    public RpcContext getStoredServerContext() {
        return storedServerContext;
    }

    public Invocation getInvocation() {
        return invocation;
    }

    private RpcContext tmpContext;

    private RpcContext tmpServerContext;

    private Function<Result, Result> beforeContext = (appResponse) -> {
        tmpContext = RpcContext.getContext();
        tmpServerContext = RpcContext.getServerContext();
        RpcContext.restoreContext(storedContext);
        RpcContext.restoreServerContext(storedServerContext);
        return appResponse;
    };

    private Function<Result, Result> afterContext = (appResponse) -> {
        RpcContext.restoreContext(tmpContext);
        RpcContext.restoreServerContext(tmpServerContext);
        return appResponse;
    };

    public static AsyncRpcResult newDefaultAsyncResult(AppResponse appResponse, Invocation invocation) {
        AsyncRpcResult asyncRpcResult = new AsyncRpcResult(invocation);
        asyncRpcResult.complete(appResponse);
        return asyncRpcResult;
    }

    public static AsyncRpcResult newDefaultAsyncResult(Invocation invocation) {
        return newDefaultAsyncResult(null, null, invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Object value, Invocation invocation) {
        return newDefaultAsyncResult(value, null, invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Throwable t, Invocation invocation) {
        return newDefaultAsyncResult(null, t, invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Object value, Throwable t, Invocation invocation) {
        AsyncRpcResult asyncRpcResult = new AsyncRpcResult(invocation);
        AppResponse appResponse = new AppResponse();
        if (t != null) {
            appResponse.setException(t);
        } else {
            appResponse.setValue(value);
        }
        asyncRpcResult.complete(appResponse);
        return asyncRpcResult;
    }
}