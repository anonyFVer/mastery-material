package org.elasticsearch.client;

import org.elasticsearch.common.unit.TimeValue;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

public class TimedRequest implements Validatable {

    public static final TimeValue DEFAULT_ACK_TIMEOUT = timeValueSeconds(30);

    public static final TimeValue DEFAULT_MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    private TimeValue timeout = DEFAULT_ACK_TIMEOUT;

    private TimeValue masterTimeout = DEFAULT_MASTER_NODE_TIMEOUT;

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