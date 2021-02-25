package com.alibaba.dubbo.rpc;

import java.util.Map;

@Deprecated
public interface Result extends org.apache.dubbo.rpc.Result {

    class CompatibleResult implements Result {

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
        public Throwable getException() {
            return delegate.getException();
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
        public Object getResult() {
            return delegate.getResult();
        }

        @Override
        public Map<String, String> getAttachments() {
            return delegate.getAttachments();
        }

        @Override
        public void addAttachments(Map<String, String> map) {
            delegate.addAttachments(map);
        }

        @Override
        public void setAttachments(Map<String, String> map) {
            delegate.setAttachments(map);
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
        public void setAttachment(String key, String value) {
            delegate.setAttachment(key, value);
        }
    }
}