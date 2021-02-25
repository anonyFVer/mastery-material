package com.alibaba.dubbo.rpc;

import java.util.Map;

@Deprecated
public interface Invocation extends org.apache.dubbo.rpc.Invocation {

    @Override
    Invoker<?> getInvoker();

    default org.apache.dubbo.rpc.Invocation getOriginal() {
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
        public Map<String, String> getAttachments() {
            return delegate.getAttachments();
        }

        @Override
        public String getAttachment(String key) {
            return delegate.getAttachment(key);
        }

        @Override
        public String getAttachment(String key, String defaultValue) {
            return delegate.getAttachment(key, defaultValue);
        }

        @Override
        public Invoker<?> getInvoker() {
            return new Invoker.CompatibleInvoker(delegate.getInvoker());
        }

        @Override
        public org.apache.dubbo.rpc.Invocation getOriginal() {
            return delegate;
        }
    }
}