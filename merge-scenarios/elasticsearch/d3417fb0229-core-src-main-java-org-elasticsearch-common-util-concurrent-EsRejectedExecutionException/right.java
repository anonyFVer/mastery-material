package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import java.io.IOException;

public class EsRejectedExecutionException extends ElasticsearchException {

    private final boolean isExecutorShutdown;

    public EsRejectedExecutionException(String message, boolean isExecutorShutdown) {
        super(message);
        this.isExecutorShutdown = isExecutorShutdown;
    }

    public EsRejectedExecutionException(String message) {
        this(message, false);
    }

    public EsRejectedExecutionException() {
        super((String) null);
        this.isExecutorShutdown = false;
    }

    public EsRejectedExecutionException(Throwable e) {
        super(null, e);
        this.isExecutorShutdown = false;
    }

    @Override
    public RestStatus status() {
        return RestStatus.TOO_MANY_REQUESTS;
    }

    public EsRejectedExecutionException(StreamInput in) throws IOException {
        super(in);
        isExecutorShutdown = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(isExecutorShutdown);
    }

    public boolean isExecutorShutdown() {
        return isExecutorShutdown;
    }
}