package org.apache.dubbo.filter;

import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import java.util.HashMap;
import java.util.Map;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.remoting.Constants.DUBBO_VERSION_KEY;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;

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
        attachments.put(PATH_KEY, "dubbo");
        attachments.put(GROUP_KEY, "dubbo");
        attachments.put(VERSION_KEY, "1.0.0");
        attachments.put(DUBBO_VERSION_KEY, "1.0.0");
        attachments.put(TOKEN_KEY, "sfag");
        attachments.put(TIMEOUT_KEY, "1000");
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