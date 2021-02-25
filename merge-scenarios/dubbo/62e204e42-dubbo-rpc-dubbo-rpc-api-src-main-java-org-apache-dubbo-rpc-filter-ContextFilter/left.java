package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import java.util.HashMap;
import java.util.Map;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.remoting.Constants.DUBBO_VERSION_KEY;
import static org.apache.dubbo.rpc.Constants.ASYNC_KEY;
import static org.apache.dubbo.rpc.Constants.FORCE_USE_TAG;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;

@Activate(group = PROVIDER, order = -10000)
public class ContextFilter implements Filter, Filter.Listener {

    private static final String TAG_KEY = "dubbo.tag";

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        Map<String, Object> attachments = invocation.getAttachments();
        if (attachments != null) {
            attachments = new HashMap<>(attachments);
            attachments.remove(PATH_KEY);
            attachments.remove(INTERFACE_KEY);
            attachments.remove(GROUP_KEY);
            attachments.remove(VERSION_KEY);
            attachments.remove(DUBBO_VERSION_KEY);
            attachments.remove(TOKEN_KEY);
            attachments.remove(TIMEOUT_KEY);
            attachments.remove(ASYNC_KEY);
            attachments.remove(TAG_KEY);
            attachments.remove(FORCE_USE_TAG);
        }
        RpcContext.getContext().setInvoker(invoker).setInvocation(invocation).setLocalAddress(invoker.getUrl().getHost(), invoker.getUrl().getPort()).setRemoteApplicationName(invoker.getUrl().getParameter(REMOTE_APPLICATION_KEY));
        if (attachments != null) {
            if (RpcContext.getContext().getAttachments() != null) {
                RpcContext.getContext().getAttachments().putAll(attachments);
            } else {
                RpcContext.getContext().setAttachments(attachments);
            }
        }
        if (invocation instanceof RpcInvocation) {
            ((RpcInvocation) invocation).setInvoker(invoker);
        }
        try {
            return invoker.invoke(invocation);
        } finally {
            RpcContext.removeContext();
            RpcContext.removeServerContext();
        }
    }

    @Override
    public void onMessage(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        appResponse.addAttachments(RpcContext.getServerContext().getAttachments());
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
    }
}