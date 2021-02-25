package org.apache.dubbo.rpc;

import java.util.Map;

public interface Invocation {

    String getMethodName();

    String getServiceName();

    Class<?>[] getParameterTypes();

    Object[] getArguments();

    Map<String, String> getAttachments();

    void setAttachment(String key, String value);

    void setAttachmentIfAbsent(String key, String value);

    String getAttachment(String key);

    String getAttachment(String key, String defaultValue);

    Invoker<?> getInvoker();
}