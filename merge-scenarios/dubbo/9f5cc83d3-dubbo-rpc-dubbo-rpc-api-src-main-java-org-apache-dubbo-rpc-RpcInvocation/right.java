package org.apache.dubbo.rpc;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RpcInvocation implements Invocation, Serializable {

    private static final long serialVersionUID = -4355285085441097045L;

    private String methodName;

    private Class<?>[] parameterTypes;

    private Object[] arguments;

    private Map<String, Object> attachments;

    private Map<Object, Object> attributes = new HashMap<Object, Object>();

    private transient Invoker<?> invoker;

    private transient Class<?> returnType;

    private transient InvokeMode invokeMode;

    public RpcInvocation() {
    }

    public RpcInvocation(Invocation invocation, Invoker<?> invoker) {
        this(invocation.getMethodName(), invocation.getParameterTypes(), invocation.getArguments(), new HashMap<String, Object>(invocation.getAttachments()), invocation.getInvoker());
        if (invoker != null) {
            URL url = invoker.getUrl();
            setAttachment(Constants.PATH_KEY, url.getPath());
            if (url.hasParameter(Constants.INTERFACE_KEY)) {
                setAttachment(Constants.INTERFACE_KEY, url.getParameter(Constants.INTERFACE_KEY));
            }
            if (url.hasParameter(Constants.GROUP_KEY)) {
                setAttachment(Constants.GROUP_KEY, url.getParameter(Constants.GROUP_KEY));
            }
            if (url.hasParameter(Constants.VERSION_KEY)) {
                setAttachment(Constants.VERSION_KEY, url.getParameter(Constants.VERSION_KEY, "0.0.0"));
            }
            if (url.hasParameter(Constants.TIMEOUT_KEY)) {
                setAttachment(Constants.TIMEOUT_KEY, url.getParameter(Constants.TIMEOUT_KEY));
            }
            if (url.hasParameter(Constants.TOKEN_KEY)) {
                setAttachment(Constants.TOKEN_KEY, url.getParameter(Constants.TOKEN_KEY));
            }
            if (url.hasParameter(Constants.APPLICATION_KEY)) {
                setAttachment(Constants.APPLICATION_KEY, url.getParameter(Constants.APPLICATION_KEY));
            }
        }
    }

    public RpcInvocation(Invocation invocation) {
        this(invocation.getMethodName(), invocation.getParameterTypes(), invocation.getArguments(), invocation.getAttachments(), invocation.getInvoker());
    }

    public RpcInvocation(Method method, Object[] arguments) {
        this(method.getName(), method.getParameterTypes(), arguments, null, null);
    }

    public RpcInvocation(Method method, Object[] arguments, Map<String, Object> attachment, Map<Object, Object> attributes) {
        this(method.getName(), method.getParameterTypes(), arguments, attachment, null);
        this.returnType = method.getReturnType();
        this.attributes = attributes == null ? new HashMap<>() : attributes;
    }

    public RpcInvocation(String methodName, Class<?>[] parameterTypes, Object[] arguments) {
        this(methodName, parameterTypes, arguments, null, null);
    }

    public RpcInvocation(String methodName, Class<?>[] parameterTypes, Object[] arguments, Map<String, Object> attachments) {
        this(methodName, parameterTypes, arguments, attachments, null);
    }

    public RpcInvocation(String methodName, Class<?>[] parameterTypes, Object[] arguments, Map<String, Object> attachments, Invoker<?> invoker) {
        this.methodName = methodName;
        this.parameterTypes = parameterTypes == null ? new Class<?>[0] : parameterTypes;
        this.arguments = arguments == null ? new Object[0] : arguments;
        this.attachments = attachments == null ? new HashMap<String, Object>() : attachments;
        this.invoker = invoker;
    }

    @Override
    public Invoker<?> getInvoker() {
        return invoker;
    }

    public void setInvoker(Invoker<?> invoker) {
        this.invoker = invoker;
    }

    public Object put(Object key, Object value) {
        return attributes.put(key, value);
    }

    public Object get(Object key) {
        return attributes.get(key);
    }

    @Override
    public Map<Object, Object> getAttributes() {
        return attributes;
    }

    @Override
    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    @Override
    public Class<?>[] getParameterTypes() {
        return parameterTypes;
    }

    public void setParameterTypes(Class<?>[] parameterTypes) {
        this.parameterTypes = parameterTypes == null ? new Class<?>[0] : parameterTypes;
    }

    @Override
    public Object[] getArguments() {
        return arguments;
    }

    public void setArguments(Object[] arguments) {
        this.arguments = arguments == null ? new Object[0] : arguments;
    }

    @Override
    public Map<String, Object> getAttachments() {
        return attachments;
    }

    public void setAttachments(Map<String, Object> attachments) {
        this.attachments = attachments == null ? new HashMap<String, Object>() : attachments;
    }

    public void setAttachment(String key, Object value) {
        if (attachments == null) {
            attachments = new HashMap<String, Object>();
        }
        attachments.put(key, value);
    }

    public void setAttachmentIfAbsent(String key, Object value) {
        if (attachments == null) {
            attachments = new HashMap<String, Object>();
        }
        if (!attachments.containsKey(key)) {
            attachments.put(key, value);
        }
    }

    public void addAttachments(Map<String, Object> attachments) {
        if (attachments == null) {
            return;
        }
        if (this.attachments == null) {
            this.attachments = new HashMap<String, Object>();
        }
        this.attachments.putAll(attachments);
    }

    public void addAttachmentsIfAbsent(Map<String, Object> attachments) {
        if (attachments == null) {
            return;
        }
        for (Map.Entry<String, Object> entry : attachments.entrySet()) {
            setAttachmentIfAbsent(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Object getAttachment(String key) {
        if (attachments == null) {
            return null;
        }
        return attachments.get(key);
    }

    @Override
    public Object getAttachment(String key, Object defaultValue) {
        if (attachments == null) {
            return defaultValue;
        }
        Object value = attachments.get(key);
        if (null == value) {
            return defaultValue;
        }
        return value;
    }

    public Class<?> getReturnType() {
        return returnType;
    }

    public void setReturnType(Class<?> returnType) {
        this.returnType = returnType;
    }

    public InvokeMode getInvokeMode() {
        return invokeMode;
    }

    public void setInvokeMode(InvokeMode invokeMode) {
        this.invokeMode = invokeMode;
    }

    @Override
    public String toString() {
        return "RpcInvocation [methodName=" + methodName + ", parameterTypes=" + Arrays.toString(parameterTypes) + ", arguments=" + Arrays.toString(arguments) + ", attachments=" + attachments + "]";
    }
}