package org.apache.dubbo.filter;

import org.apache.dubbo.common.Constants;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import java.util.HashMap;
import java.util.Map;

public class LegacyInvocation implements Invocation {

    private String arg0;

    public LegacyInvocation(String arg0) {
        this.arg0 = arg0;
    }

    public String getMethodName() {
        return "echo";
    }

    public Class<?>[] getParameterTypes() {
        return new Class[] { String.class };
    }

    public Object[] getArguments() {
        return new Object[] { arg0 };
    }

    public Map<String, String> getAttachments() {
        Map<String, String> attachments = new HashMap<String, String>();
        attachments.put(Constants.PATH_KEY, "dubbo");
        attachments.put(Constants.GROUP_KEY, "dubbo");
        attachments.put(Constants.VERSION_KEY, "1.0.0");
        attachments.put(Constants.DUBBO_VERSION_KEY, "1.0.0");
        attachments.put(Constants.TOKEN_KEY, "sfag");
        attachments.put(Constants.TIMEOUT_KEY, "1000");
        return attachments;
    }

    public Invoker<?> getInvoker() {
        return null;
    }

    public String getAttachment(String key) {
        return getAttachments().get(key);
    }

    public String getAttachment(String key, String defaultValue) {
        return getAttachments().get(key);
    }
}