package org.apache.dubbo.rpc.cluster.directory;

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

public class MockDirInvocation implements Invocation {

    @Override
    public String getTargetServiceUniqueName() {
        return null;
    }

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

    public Map<String, Object> getAttachments() {
        Map<String, Object> attachments = new HashMap<String, Object>();
        attachments.put(PATH_KEY, "dubbo");
        attachments.put(GROUP_KEY, "dubbo");
        attachments.put(VERSION_KEY, "1.0.0");
        attachments.put(DUBBO_VERSION_KEY, "1.0.0");
        attachments.put(TOKEN_KEY, "sfag");
        attachments.put(TIMEOUT_KEY, "1000");
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