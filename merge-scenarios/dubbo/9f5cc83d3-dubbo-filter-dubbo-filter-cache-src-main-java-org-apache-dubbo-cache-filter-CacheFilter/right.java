package org.apache.dubbo.cache.filter;

import org.apache.dubbo.cache.Cache;
import org.apache.dubbo.cache.CacheFactory;
import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import java.io.Serializable;

@Activate(group = { Constants.CONSUMER, Constants.PROVIDER }, value = Constants.CACHE_KEY)
public class CacheFilter implements Filter {

    private CacheFactory cacheFactory;

    public void setCacheFactory(CacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        if (cacheFactory != null && ConfigUtils.isNotEmpty(invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.CACHE_KEY))) {
            Cache cache = cacheFactory.getCache(invoker.getUrl(), invocation);
            if (cache != null) {
                String key = StringUtils.toArgumentString(invocation.getArguments());
                Object value = cache.get(key);
                if (value != null) {
                    if (value instanceof ValueWrapper) {
                        return AsyncRpcResult.newDefaultAsyncResult(((ValueWrapper) value).get(), invocation);
                    } else {
                        return AsyncRpcResult.newDefaultAsyncResult(value, invocation);
                    }
                }
                Result result = invoker.invoke(invocation);
                if (!result.hasException()) {
                    cache.put(key, new ValueWrapper(result.getValue()));
                }
                return result;
            }
        }
        return invoker.invoke(invocation);
    }

    static class ValueWrapper implements Serializable {

        private static final long serialVersionUID = -1777337318019193256L;

        private final Object value;

        public ValueWrapper(Object value) {
            this.value = value;
        }

        public Object get() {
            return this.value;
        }
    }
}