package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ListenableFilter;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcStatus;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.rpc.Constants.ACTIVES_KEY;

@Activate(group = CONSUMER, value = ACTIVES_KEY)
public class ActiveLimitFilter extends ListenableFilter {

    private static final String ACTIVELIMIT_FILTER_START_TIME = "activelimit_filter_start_time";

    public ActiveLimitFilter() {
        super.listener = new ActiveLimitListener();
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        URL url = invoker.getUrl();
        String methodName = invocation.getMethodName();
        int max = invoker.getUrl().getMethodParameter(methodName, ACTIVES_KEY, 0);
        RpcStatus rpcStatus = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName());
        if (!RpcStatus.beginCount(url, methodName, max)) {
            long timeout = invoker.getUrl().getMethodParameter(invocation.getMethodName(), TIMEOUT_KEY, 0);
            long start = System.currentTimeMillis();
            long remain = timeout;
            synchronized (rpcStatus) {
                while (!RpcStatus.beginCount(url, methodName, max)) {
                    try {
                        rpcStatus.wait(remain);
                    } catch (InterruptedException e) {
                    }
                    long elapsed = System.currentTimeMillis() - start;
                    remain = timeout - elapsed;
                    if (remain <= 0) {
                        throw new RpcException("Waiting concurrent invoke timeout in client-side for service:  " + invoker.getInterface().getName() + ", method: " + invocation.getMethodName() + ", elapsed: " + elapsed + ", timeout: " + timeout + ". concurrent invokes: " + rpcStatus.getActive() + ". max concurrent invoke limit: " + max);
                    }
                }
            }
        }
        invocation.setAttachment(ACTIVELIMIT_FILTER_START_TIME, String.valueOf(System.currentTimeMillis()));
        return invoker.invoke(invocation);
    }

    static class ActiveLimitListener implements Listener {

        @Override
        public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
            String methodName = invocation.getMethodName();
            URL url = invoker.getUrl();
            int max = invoker.getUrl().getMethodParameter(methodName, ACTIVES_KEY, 0);
            RpcStatus.endCount(url, methodName, getElapsed(invocation), true);
            notifyFinish(RpcStatus.getStatus(url, methodName), max);
        }

        @Override
        public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
            String methodName = invocation.getMethodName();
            URL url = invoker.getUrl();
            int max = invoker.getUrl().getMethodParameter(methodName, ACTIVES_KEY, 0);
            RpcStatus.endCount(url, methodName, getElapsed(invocation), false);
            notifyFinish(RpcStatus.getStatus(url, methodName), max);
        }

        private long getElapsed(Invocation invocation) {
            String beginTime = invocation.getAttachment(ACTIVELIMIT_FILTER_START_TIME);
            return StringUtils.isNotEmpty(beginTime) ? System.currentTimeMillis() - Long.parseLong(beginTime) : 0;
        }

        private void notifyFinish(RpcStatus rpcStatus, int max) {
            if (max > 0) {
                synchronized (rpcStatus) {
                    rpcStatus.notifyAll();
                }
            }
        }
    }
}