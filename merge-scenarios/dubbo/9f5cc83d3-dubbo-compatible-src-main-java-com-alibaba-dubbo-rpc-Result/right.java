package com.alibaba.dubbo.rpc;

import java.util.Map;
import java.util.function.Function;

@Deprecated
public interface Result extends org.apache.dubbo.rpc.Result {

    @Override
    default void setValue(Object value) {
    }

    @Override
    default void setException(Throwable t) {
    }

    abstract class AbstractResult extends org.apache.dubbo.rpc.AbstractResult implements Result {

        @Override
        public org.apache.dubbo.rpc.Result thenApplyWithContext(Function<org.apache.dubbo.rpc.Result, org.apache.dubbo.rpc.Result> fn) {
            return null;
        }
    }

    class CompatibleResult extends AbstractResult {

        private org.apache.dubbo.rpc.Result delegate;

        public CompatibleResult(org.apache.dubbo.rpc.Result result) {
            this.delegate = result;
        }

        public org.apache.dubbo.rpc.Result getDelegate() {
            return delegate;
        }

        @Override
        public Object getValue() {
            return delegate.getValue();
        }

        @Override
        public void setValue(Object value) {
            delegate.setValue(value);
        }

        @Override
        public Throwable getException() {
            return delegate.getException();
        }

        @Override
        public void setException(Throwable t) {
            delegate.setException(t);
        }

        @Override
        public boolean hasException() {
            return delegate.hasException();
        }

        @Override
        public Object recreate() throws Throwable {
            return delegate.recreate();
        }

        @Override
        public Map<String, Object> getAttachments() {
            return delegate.getAttachments();
        }

        @Override
        public void addAttachments(Map<String, Object> map) {
            delegate.addAttachments(map);
        }

        @Override
        public void setAttachments(Map<String, Object> map) {
            delegate.setAttachments(map);
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
        public void setAttachment(String key, Object value) {
            delegate.setAttachment(key, value);
        }
    }
}