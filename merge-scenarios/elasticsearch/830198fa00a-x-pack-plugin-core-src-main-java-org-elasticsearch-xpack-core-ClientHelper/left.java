package org.elasticsearch.xpack.core;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class ClientHelper {

    public static final Set<String> SECURITY_HEADER_FILTERS = Sets.newHashSet(AuthenticationServiceField.RUN_AS_USER_HEADER, AuthenticationField.AUTHENTICATION_KEY);

    public static final String ACTION_ORIGIN_TRANSIENT_NAME = "action.origin";

    public static final String SECURITY_ORIGIN = "security";

    public static final String WATCHER_ORIGIN = "watcher";

    public static final String ML_ORIGIN = "ml";

    public static final String INDEX_LIFECYCLE_ORIGIN = "index_lifecycle";

    public static final String MONITORING_ORIGIN = "monitoring";

    public static final String DEPRECATION_ORIGIN = "deprecation";

    public static final String PERSISTENT_TASK_ORIGIN = "persistent_tasks";

    public static final String ROLLUP_ORIGIN = "rollup";

    private ClientHelper() {
    }

    public static ThreadContext.StoredContext stashWithOrigin(ThreadContext threadContext, String origin) {
        final ThreadContext.StoredContext storedContext = threadContext.stashContext();
        threadContext.putTransient(ACTION_ORIGIN_TRANSIENT_NAME, origin);
        return storedContext;
    }

    public static Client clientWithOrigin(Client client, String origin) {
        return new ClientWithOrigin(client, origin);
    }

    public static <Request extends ActionRequest, Response extends ActionResponse> void executeAsyncWithOrigin(ThreadContext threadContext, String origin, Request request, ActionListener<Response> listener, BiConsumer<Request, ActionListener<Response>> consumer) {
        final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = stashWithOrigin(threadContext, origin)) {
            consumer.accept(request, new ContextPreservingActionListener<>(supplier, listener));
        }
    }

    public static <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void executeAsyncWithOrigin(Client client, String origin, Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = stashWithOrigin(threadContext, origin)) {
            client.execute(action, request, new ContextPreservingActionListener<>(supplier, listener));
        }
    }

    public static <T extends ActionResponse> T executeWithHeaders(Map<String, String> headers, String origin, Client client, Supplier<T> supplier) {
        Map<String, String> filteredHeaders = headers.entrySet().stream().filter(e -> SECURITY_HEADER_FILTERS.contains(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (filteredHeaders.isEmpty()) {
            try (ThreadContext.StoredContext ignore = stashWithOrigin(client.threadPool().getThreadContext(), origin)) {
                return supplier.get();
            }
        } else {
            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashContext()) {
                client.threadPool().getThreadContext().copyHeaders(filteredHeaders.entrySet());
                return supplier.get();
            }
        }
    }

    public static <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void executeWithHeadersAsync(Map<String, String> headers, String origin, Client client, Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        Map<String, String> filteredHeaders = headers.entrySet().stream().filter(e -> SECURITY_HEADER_FILTERS.contains(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        if (filteredHeaders.isEmpty()) {
            ClientHelper.executeAsyncWithOrigin(client, origin, action, request, listener);
        } else {
            final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
            try (ThreadContext.StoredContext ignore = stashWithHeaders(threadContext, filteredHeaders)) {
                client.execute(action, request, new ContextPreservingActionListener<>(supplier, listener));
            }
        }
    }

    private static ThreadContext.StoredContext stashWithHeaders(ThreadContext threadContext, Map<String, String> headers) {
        final ThreadContext.StoredContext storedContext = threadContext.stashContext();
        threadContext.copyHeaders(headers.entrySet());
        return storedContext;
    }

    private static final class ClientWithOrigin extends FilterClient {

        private final String origin;

        private ClientWithOrigin(Client in, String origin) {
            super(in);
            this.origin = origin;
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
            final Supplier<ThreadContext.StoredContext> supplier = in().threadPool().getThreadContext().newRestorableContext(false);
            try (ThreadContext.StoredContext ignore = in().threadPool().getThreadContext().stashContext()) {
                in().threadPool().getThreadContext().putTransient(ACTION_ORIGIN_TRANSIENT_NAME, origin);
                super.doExecute(action, request, new ContextPreservingActionListener<>(supplier, listener));
            }
        }
    }
}