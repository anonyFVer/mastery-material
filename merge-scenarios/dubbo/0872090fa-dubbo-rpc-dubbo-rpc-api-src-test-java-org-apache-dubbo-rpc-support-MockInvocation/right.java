package org.apache.dubbo.rpc.support;

import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import java.util.HashMap;
import java.util.Map;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.remoting.Constants.DUBBO_VERSION_KEY;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;

public class MockInvocation implements Invocation {

    public String getMethodName() {
        return "echo";
    }

    @Override
    public String getServiceName() {
        return "DemoService";
    }

    public Class<?>[] getParameterTypes() {
        return new Class[] { String.class };
    }

    public Object[] getArguments() {
        return new Object[] { "aa" };
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

    @Override
    public void setAttachment(String key, String value) {
    }

    @Override
    public void setAttachmentIfAbsent(String key, String value) {
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