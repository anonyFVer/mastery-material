package org.apache.dubbo.rpc;

import java.util.Map;

public interface Invocation {

    String getTargetServiceUniqueName();

    String getMethodName();

    String getServiceName();

    Class<?>[] getParameterTypes();

    Object[] getArguments();

    Map<String, Object> getAttachments();

    void setAttachment(String key, Object value);

    void setAttachmentIfAbsent(String key, Object value);

    Object getAttachment(String key);

    Object getAttachment(String key, Object defaultValue);

    Invoker<?> getInvoker();

    Object put(Object key, Object value);

    Object get(Object key);

    Map<Object, Object> getAttributes();
}