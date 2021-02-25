package org.elasticsearch.action;

import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import java.util.Objects;
import java.util.function.Supplier;

public class ActionListenerResponseHandler<Response extends TransportResponse> extends BaseTransportResponseHandler<Response> {

    private final ActionListener<Response> listener;

    private final Supplier<Response> responseSupplier;

    public ActionListenerResponseHandler(ActionListener<Response> listener, Supplier<Response> responseSupplier) {
        this.listener = Objects.requireNonNull(listener);
        this.responseSupplier = Objects.requireNonNull(responseSupplier);
    }

    @Override
    public void handleResponse(Response response) {
        listener.onResponse(response);
    }

    @Override
    public void handleException(TransportException e) {
        listener.onFailure(e);
    }

    @Override
    public Response newInstance() {
        return responseSupplier.get();
    }

    @Override
    public String executor() {
        return ThreadPool.Names.SAME;
    }
}