package com.alibaba.dubbo.rpc;

import java.util.Map;

@Deprecated
public interface Invocation extends org.apache.dubbo.rpc.Invocation {

    @Override
    Invoker<?> getInvoker();

    default org.apache.dubbo.rpc.Invocation getOriginal() {
        return null;
    }

    @Override
    default void setAttachmentIfAbsent(String key, Object value) {
    }

    @Override
    default void setAttachment(String key, Object value) {
    }

    @Override
    default String getServiceName() {
        return null;
    }

    class CompatibleInvocation implements Invocation {

        private org.apache.dubbo.rpc.Invocation delegate;

        public CompatibleInvocation(org.apache.dubbo.rpc.Invocation invocation) {
            this.delegate = invocation;
        }

        @Override
        public String getMethodName() {
            return delegate.getMethodName();
        }

        @Override
        public Class<?>[] getParameterTypes() {
            return delegate.getParameterTypes();
        }

        @Override
        public Object[] getArguments() {
            return delegate.getArguments();
        }

        @Override
        public Map<String, Object> getAttachments() {
            return delegate.getAttachments();
        }

        @Override
        public Object getAttachment(String key) {
            return delegate.getAttachment(key);
        }

        @Override
        public Object getAttachment(String key, Object defaultValue) {
            return delegate.getAttachment(key, defaultValue);
        }

        @Override
        public Invoker<?> getInvoker() {
            return new Invoker.CompatibleInvoker(delegate.getInvoker());
        }

        @Override
        public Object put(Object key, Object value) {
            return delegate.put(key, value);
        }

        @Override
        public Object get(Object key) {
            return delegate.get(key);
        }

        @Override
        public Map<Object, Object> getAttributes() {
            return delegate.getAttributes();
        }

        @Override
        public org.apache.dubbo.rpc.Invocation getOriginal() {
            return delegate;
        }
    }
}