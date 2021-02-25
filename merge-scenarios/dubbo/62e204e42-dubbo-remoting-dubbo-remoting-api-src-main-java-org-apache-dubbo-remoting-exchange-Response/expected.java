package org.apache.dubbo.remoting.exchange;

import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Codec;
import org.apache.dubbo.remoting.Decodeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class Response {

    public static final byte OK = 20;

    public static final byte CLIENT_TIMEOUT = 30;

    public static final byte SERVER_TIMEOUT = 31;

    public static final byte CHANNEL_INACTIVE = 35;

    public static final byte BAD_REQUEST = 40;

    public static final byte BAD_RESPONSE = 50;

    public static final byte SERVICE_NOT_FOUND = 60;

    public static final byte SERVICE_ERROR = 70;

    public static final byte SERVER_ERROR = 80;

    public static final byte CLIENT_ERROR = 90;

    public static final byte SERVER_THREADPOOL_EXHAUSTED_ERROR = 100;

    private long mId = 0;

    private String mVersion;

    private byte mStatus = OK;

    private boolean mEvent = false;

    private String mErrorMsg;

    private Object mResult;

    public Response() {
    }

    public Response(long id) {
        mId = id;
    }

    public Response(long id, String version) {
        mId = id;
        mVersion = version;
    }

    public long getId() {
        return mId;
    }

    public void setId(long id) {
        mId = id;
    }

    public String getVersion() {
        return mVersion;
    }

    public void setVersion(String version) {
        mVersion = version;
    }

    public byte getStatus() {
        return mStatus;
    }

    public void setStatus(byte status) {
        mStatus = status;
    }

    public boolean isEvent() {
        return mEvent;
    }

    public void setEvent(String event) {
        mEvent = true;
        mResult = event;
    }

    public void setEvent(boolean mEvent) {
        this.mEvent = mEvent;
    }

    public boolean isHeartbeat() {
        return mEvent && HEARTBEAT_EVENT == mResult;
    }

    @Deprecated
    public void setHeartbeat(boolean isHeartbeat) {
        if (isHeartbeat) {
            setEvent(HEARTBEAT_EVENT);
        }
    }

    public Object getResult() {
        return mResult;
    }

    public void setResult(Object msg) {
        mResult = msg;
    }

    public String getErrorMessage() {
        return mErrorMsg;
    }

    public void setErrorMessage(String msg) {
        mErrorMsg = msg;
    }

    @Override
    public String toString() {
        return "Response [id=" + mId + ", version=" + mVersion + ", status=" + mStatus + ", event=" + mEvent + ", error=" + mErrorMsg + ", result=" + (mResult == this ? "this" : mResult) + "]";
    }

    public static class AppResult {

        protected Map<String, String> attachments = new HashMap<String, String>();

        protected Object result;

        protected Throwable exception;

        public Map<String, String> getAttachments() {
            return attachments;
        }

        public void setAttachments(Map<String, String> attachments) {
            this.attachments = attachments;
        }

        public Object getResult() {
            return result;
        }

        public void setResult(Object result) {
            this.result = result;
        }

        public Throwable getException() {
            return exception;
        }

        public void setException(Throwable exception) {
            this.exception = exception;
        }
    }

    public static class DecodeableAppResult implements Codec, Decodeable {

        @Override
        public void decode() throws Exception {
        }

        @Override
        public void encode(Channel channel, OutputStream output, Object message) throws IOException {
        }

        @Override
        public Object decode(Channel channel, InputStream input) throws IOException {
            return null;
        }
    }
}