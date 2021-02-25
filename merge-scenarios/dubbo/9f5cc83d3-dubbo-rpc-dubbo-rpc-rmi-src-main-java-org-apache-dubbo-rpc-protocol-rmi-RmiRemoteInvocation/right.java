package org.apache.dubbo.rpc.protocol.rmi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.RpcContext;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.remoting.support.RemoteInvocation;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class RmiRemoteInvocation extends RemoteInvocation {

    private static final long serialVersionUID = 1L;

    private static final String dubboAttachmentsAttrName = "dubbo.attachments";

    public RmiRemoteInvocation(MethodInvocation methodInvocation) {
        super(methodInvocation);
        addAttribute(dubboAttachmentsAttrName, new HashMap<>(RpcContext.getContext().getAttachments()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object invoke(Object targetObject) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        RpcContext context = RpcContext.getContext();
        context.setAttachments((Map<String, Object>) getAttribute(dubboAttachmentsAttrName));
        String generic = (String) getAttribute(Constants.GENERIC_KEY);
        if (StringUtils.isNotEmpty(generic)) {
            context.setAttachment(Constants.GENERIC_KEY, generic);
        }
        try {
            return super.invoke(targetObject);
        } finally {
            context.setAttachments(null);
        }
    }
}