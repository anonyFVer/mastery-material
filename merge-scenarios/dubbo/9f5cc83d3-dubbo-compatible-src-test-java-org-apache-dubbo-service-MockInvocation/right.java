package org.apache.dubbo.service;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import java.util.HashMap;
import java.util.Map;

public class MockInvocation implements Invocation {

    private String arg0;

    public MockInvocation(String arg0) {
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

    public Map<String, Object> getAttachments() {
        Map<String, Object> attachments = new HashMap<String, Object>();
        attachments.put(Constants.PATH_KEY, "dubbo");
        attachments.put(Constants.GROUP_KEY, "dubbo");
        attachments.put(Constants.VERSION_KEY, "1.0.0");
        attachments.put(Constants.DUBBO_VERSION_KEY, "1.0.0");
        attachments.put(Constants.TOKEN_KEY, "sfag");
        attachments.put(Constants.TIMEOUT_KEY, "1000");
        return attachments;
    }

    @Override
    public void setAttachment(String key, Object value) {
    }

    @Override
    public void setAttachmentIfAbsent(String key, Object value) {
    }

    public Invoker<?> getInvoker() {
        return null;
    }

    @Override
    public Object put(Object key, Object value) {
        return null;
    }

    @Override
    public Object get(Object key) {
        return null;
    }

    @Override
    public Map<Object, Object> getAttributes() {
        return null;
    }

    public Object getAttachment(String key) {
        return getAttachments().get(key);
    }

    public Object getAttachment(String key, Object defaultValue) {
        return getAttachments().get(key);
    }
}