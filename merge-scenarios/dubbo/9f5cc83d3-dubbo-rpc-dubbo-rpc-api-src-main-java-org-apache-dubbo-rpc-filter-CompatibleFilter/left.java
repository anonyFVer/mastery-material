package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CompatibleTypeUtils;
import org.apache.dubbo.common.utils.PojoUtils;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ListenableFilter;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import static org.apache.dubbo.remoting.Constants.SERIALIZATION_KEY;

public class CompatibleFilter extends ListenableFilter {

    private static Logger logger = LoggerFactory.getLogger(CompatibleFilter.class);

    public CompatibleFilter() {
        super.listener = new CompatibleListener();
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        return invoker.invoke(invocation);
    }

    static class CompatibleListener implements Listener {

        @Override
        public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
            if (!invocation.getMethodName().startsWith("$") && !appResponse.hasException()) {
                Object value = appResponse.getValue();
                if (value != null) {
                    try {
                        Method method = invoker.getInterface().getMethod(invocation.getMethodName(), invocation.getParameterTypes());
                        Class<?> type = method.getReturnType();
                        Object newValue;
                        String serialization = invoker.getUrl().getParameter(SERIALIZATION_KEY);
                        if ("json".equals(serialization) || "fastjson".equals(serialization)) {
                            Type gtype = method.getGenericReturnType();
                            newValue = PojoUtils.realize(value, type, gtype);
                        } else if (!type.isInstance(value)) {
                            newValue = PojoUtils.isPojo(type) ? PojoUtils.realize(value, type) : CompatibleTypeUtils.compatibleTypeConvert(value, type);
                        } else {
                            newValue = value;
                        }
                        if (newValue != value) {
                            appResponse.setValue(newValue);
                        }
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }
            }
        }

        @Override
        public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        }
    }
}