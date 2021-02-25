package org.apache.dubbo.rpc.protocol.rmi;

import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.RpcContext;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.remoting.support.RemoteInvocation;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;

public class RmiRemoteInvocation extends RemoteInvocation {

    private static final long serialVersionUID = 1L;

    private static final String DUBBO_ATTACHMENTS_ATTR_NAME = "dubbo.attachments";

    public RmiRemoteInvocation(MethodInvocation methodInvocation) {
        super(methodInvocation);
        addAttribute(DUBBO_ATTACHMENTS_ATTR_NAME, new HashMap<>(RpcContext.getContext().getAttachments()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object invoke(Object targetObject) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        RpcContext context = RpcContext.getContext();
        context.setAttachments((Map<String, Object>) getAttribute(DUBBO_ATTACHMENTS_ATTR_NAME));
        String generic = (String) getAttribute(GENERIC_KEY);
        if (StringUtils.isNotEmpty(generic)) {
            context.setAttachment(GENERIC_KEY, generic);
        }
        try {
            return super.invoke(targetObject);
        } finally {
            context.setAttachments(null);
        }
    }
}