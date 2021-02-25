package org.elasticsearch.client;

import org.elasticsearch.common.unit.TimeValue;

public class TimedRequest implements Validatable {

    private TimeValue timeout;

    private TimeValue masterTimeout;

    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    public void setMasterTimeout(TimeValue masterTimeout) {
        this.masterTimeout = masterTimeout;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public TimeValue masterNodeTimeout() {
        return masterTimeout;
    }
}