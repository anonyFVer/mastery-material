package org.apache.dubbo.rpc;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.ThreadlessExecutor;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class AsyncRpcResult extends AbstractResult {

    private static final Logger logger = LoggerFactory.getLogger(AsyncRpcResult.class);

    private RpcContext storedContext;

    private RpcContext storedServerContext;

    private Executor executor;

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
    public Result get() throws InterruptedException, ExecutionException {
        if (executor != null) {
            ThreadlessExecutor threadlessExecutor = (ThreadlessExecutor) executor;
            threadlessExecutor.waitAndDrain();
        }
        return super.get();
    }

    @Override
    public Result get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return this.get();
    }

    @Override
    public Object recreate() throws Throwable {
        RpcInvocation rpcInvocation = (RpcInvocation) invocation;
        if (InvokeMode.FUTURE == rpcInvocation.getInvokeMode()) {
            return RpcContext.getContext().getFuture();
        } else if (this.isDone()) {
            return this.get().recreate();
        }
        return (new AppResponse()).recreate();
    }

    public Result thenApplyWithContext(Function<Result, Result> fn) {
        CompletableFuture<Result> future = this.thenApply(fn.compose(beforeContext).andThen(afterContext));
        AsyncRpcResult nextAsyncRpcResult = new AsyncRpcResult(this);
        nextAsyncRpcResult.subscribeTo(future);
        return nextAsyncRpcResult;
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
    public Map<String, Object> getAttachments() {
        return getAppResponse().getAttachments();
    }

    @Override
    public void setAttachments(Map<String, Object> map) {
        getAppResponse().setAttachments(map);
    }

    @Override
    public void addAttachments(Map<String, Object> map) {
        getAppResponse().addAttachments(map);
    }

    @Override
    public Object getAttachment(String key) {
        return getAppResponse().getAttachment(key);
    }

    @Override
    public Object getAttachment(String key, Object defaultValue) {
        return getAppResponse().getAttachment(key, defaultValue);
    }

    @Override
    public void setAttachment(String key, Object value) {
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

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
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