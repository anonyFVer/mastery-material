package org.elasticsearch.transport;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import static java.util.Collections.emptyList;
import static org.elasticsearch.common.settings.Setting.listSetting;

public class TransportService extends AbstractLifecycleComponent {

    public static final String DIRECT_RESPONSE_PROFILE = ".direct";

    private static final String HANDSHAKE_ACTION_NAME = "internal:transport/handshake";

    private final CountDownLatch blockIncomingRequestsLatch = new CountDownLatch(1);

    protected final Transport transport;

    protected final ThreadPool threadPool;

    protected final ClusterName clusterName;

    protected final TaskManager taskManager;

    private final TransportInterceptor.AsyncSender asyncSender;

    private final Function<BoundTransportAddress, DiscoveryNode> localNodeFactory;

    volatile Map<String, RequestHandlerRegistry> requestHandlers = Collections.emptyMap();

    final Object requestHandlerMutex = new Object();

    final ConcurrentMapLong<RequestHolder> clientHandlers = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    final CopyOnWriteArrayList<TransportConnectionListener> connectionListeners = new CopyOnWriteArrayList<>();

    private final TransportInterceptor interceptor;

    final Map<Long, TimeoutInfoHolder> timeoutInfoHandlers = Collections.synchronizedMap(new LinkedHashMap<Long, TimeoutInfoHolder>(100, .75F, true) {

        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > 100;
        }
    });

    private final TransportService.Adapter adapter;

    public static final TransportInterceptor NOOP_TRANSPORT_INTERCEPTOR = new TransportInterceptor() {
    };

    public static final Setting<List<String>> TRACE_LOG_INCLUDE_SETTING = listSetting("transport.tracer.include", emptyList(), Function.identity(), Property.Dynamic, Property.NodeScope);

    public static final Setting<List<String>> TRACE_LOG_EXCLUDE_SETTING = listSetting("transport.tracer.exclude", Arrays.asList("internal:discovery/zen/fd*", TransportLivenessAction.NAME), Function.identity(), Property.Dynamic, Property.NodeScope);

    private final Logger tracerLog;

    volatile String[] tracerLogInclude;

    volatile String[] tracerLogExclude;

    volatile DiscoveryNode localNode = null;

    private final Transport.Connection localNodeConnection = new Transport.Connection() {

        @Override
        public DiscoveryNode getNode() {
            return localNode;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
            sendLocalRequest(requestId, action, request);
        }

        @Override
        public void close() throws IOException {
        }
    };

    public TransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor transportInterceptor, Function<BoundTransportAddress, DiscoveryNode> localNodeFactory, @Nullable ClusterSettings clusterSettings) {
        super(settings);
        this.transport = transport;
        this.threadPool = threadPool;
        this.localNodeFactory = localNodeFactory;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        setTracerLogInclude(TRACE_LOG_INCLUDE_SETTING.get(settings));
        setTracerLogExclude(TRACE_LOG_EXCLUDE_SETTING.get(settings));
        tracerLog = Loggers.getLogger(logger, ".tracer");
        adapter = createAdapter();
        taskManager = createTaskManager();
        this.interceptor = transportInterceptor;
        this.asyncSender = interceptor.interceptSender(this::sendRequestInternal);
        if (clusterSettings != null) {
            clusterSettings.addSettingsUpdateConsumer(TRACE_LOG_INCLUDE_SETTING, this::setTracerLogInclude);
            clusterSettings.addSettingsUpdateConsumer(TRACE_LOG_EXCLUDE_SETTING, this::setTracerLogExclude);
        }
    }

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    protected Adapter createAdapter() {
        return new Adapter();
    }

    protected TaskManager createTaskManager() {
        return new TaskManager(settings);
    }

    void setTracerLogInclude(List<String> tracerLogInclude) {
        this.tracerLogInclude = tracerLogInclude.toArray(Strings.EMPTY_ARRAY);
    }

    void setTracerLogExclude(List<String> tracerLogExclude) {
        this.tracerLogExclude = tracerLogExclude.toArray(Strings.EMPTY_ARRAY);
    }

    @Override
    protected void doStart() {
        adapter.rxMetric.clear();
        adapter.txMetric.clear();
        transport.transportServiceAdapter(adapter);
        transport.start();
        if (transport.boundAddress() != null && logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
            for (Map.Entry<String, BoundTransportAddress> entry : transport.profileBoundAddresses().entrySet()) {
                logger.info("profile [{}]: {}", entry.getKey(), entry.getValue());
            }
        }
        localNode = localNodeFactory.apply(transport.boundAddress());
        registerRequestHandler(HANDSHAKE_ACTION_NAME, () -> HandshakeRequest.INSTANCE, ThreadPool.Names.SAME, (request, channel) -> channel.sendResponse(new HandshakeResponse(localNode, clusterName, localNode.getVersion())));
    }

    @Override
    protected void doStop() {
        try {
            transport.stop();
        } finally {
            for (Map.Entry<Long, RequestHolder> entry : clientHandlers.entrySet()) {
                final RequestHolder holderToNotify = clientHandlers.remove(entry.getKey());
                if (holderToNotify != null) {
                    threadPool.generic().execute(new AbstractRunnable() {

                        @Override
                        public void onRejection(Exception e) {
                            logger.debug((Supplier<?>) () -> new ParameterizedMessage("failed to notify response handler on rejection, action: {}", holderToNotify.action()), e);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to notify response handler on exception, action: {}", holderToNotify.action()), e);
                        }

                        @Override
                        public void doRun() {
                            TransportException ex = new TransportException("transport stopped, action: " + holderToNotify.action());
                            holderToNotify.handler().handleException(ex);
                        }
                    });
                }
            }
        }
    }

    @Override
    protected void doClose() {
        transport.close();
    }

    public final void acceptIncomingRequests() {
        blockIncomingRequestsLatch.countDown();
    }

    public TransportInfo info() {
        BoundTransportAddress boundTransportAddress = boundAddress();
        if (boundTransportAddress == null) {
            return null;
        }
        return new TransportInfo(boundTransportAddress, transport.profileBoundAddresses());
    }

    public TransportStats stats() {
        return new TransportStats(transport.serverOpen(), adapter.rxMetric.count(), adapter.rxMetric.sum(), adapter.txMetric.count(), adapter.txMetric.sum());
    }

    public BoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    public List<String> getLocalAddresses() {
        return transport.getLocalAddresses();
    }

    public boolean nodeConnected(DiscoveryNode node) {
        return isLocalNode(node) || transport.nodeConnected(node);
    }

    public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
        connectToNode(node, null);
    }

    public void connectToNode(final DiscoveryNode node, ConnectionProfile connectionProfile) {
        if (isLocalNode(node)) {
            return;
        }
        transport.connectToNode(node, connectionProfile);
    }

    public Transport.Connection openConnection(final DiscoveryNode node, ConnectionProfile profile) throws IOException {
        if (isLocalNode(node)) {
            return localNodeConnection;
        } else {
            return transport.openConnection(node, profile);
        }
    }

    public DiscoveryNode handshake(final Transport.Connection connection, final long handshakeTimeout) throws ConnectTransportException {
        final HandshakeResponse response;
        final DiscoveryNode node = connection.getNode();
        try {
            PlainTransportFuture<HandshakeResponse> futureHandler = new PlainTransportFuture<>(new FutureTransportResponseHandler<HandshakeResponse>() {

                @Override
                public HandshakeResponse newInstance() {
                    return new HandshakeResponse();
                }
            });
            sendRequest(connection, HANDSHAKE_ACTION_NAME, HandshakeRequest.INSTANCE, TransportRequestOptions.builder().withTimeout(handshakeTimeout).build(), futureHandler);
            response = futureHandler.txGet();
        } catch (Exception e) {
            throw new IllegalStateException("handshake failed with " + node, e);
        }
        if (!Objects.equals(clusterName, response.clusterName)) {
            throw new IllegalStateException("handshake failed, mismatched cluster name [" + response.clusterName + "] - " + node);
        } else if (response.version.isCompatible(localNode.getVersion()) == false) {
            throw new IllegalStateException("handshake failed, incompatible version [" + response.version + "] - " + node);
        }
        return response.discoveryNode;
    }

    static class HandshakeRequest extends TransportRequest {

        public static final HandshakeRequest INSTANCE = new HandshakeRequest();

        private HandshakeRequest() {
        }
    }

    static class HandshakeResponse extends TransportResponse {

        private DiscoveryNode discoveryNode;

        private ClusterName clusterName;

        private Version version;

        public HandshakeResponse() {
        }

        public HandshakeResponse(DiscoveryNode discoveryNode, ClusterName clusterName, Version version) {
            this.discoveryNode = discoveryNode;
            this.version = version;
            this.clusterName = clusterName;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            discoveryNode = in.readOptionalWriteable(DiscoveryNode::new);
            clusterName = new ClusterName(in);
            version = Version.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalWriteable(discoveryNode);
            clusterName.writeTo(out);
            Version.writeVersion(version, out);
        }
    }

    public void disconnectFromNode(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return;
        }
        transport.disconnectFromNode(node);
    }

    public void addConnectionListener(TransportConnectionListener listener) {
        connectionListeners.add(listener);
    }

    public void removeConnectionListener(TransportConnectionListener listener) {
        connectionListeners.remove(listener);
    }

    public <T extends TransportResponse> TransportFuture<T> submitRequest(DiscoveryNode node, String action, TransportRequest request, TransportResponseHandler<T> handler) throws TransportException {
        return submitRequest(node, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> TransportFuture<T> submitRequest(DiscoveryNode node, String action, TransportRequest request, TransportRequestOptions options, TransportResponseHandler<T> handler) throws TransportException {
        PlainTransportFuture<T> futureHandler = new PlainTransportFuture<>(handler);
        try {
            Transport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, options, futureHandler);
        } catch (NodeNotConnectedException ex) {
            futureHandler.handleException(ex);
        }
        return futureHandler;
    }

    public <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action, final TransportRequest request, final TransportResponseHandler<T> handler) {
        try {
            Transport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, TransportRequestOptions.EMPTY, handler);
        } catch (NodeNotConnectedException ex) {
            handler.handleException(ex);
        }
    }

    public final <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action, final TransportRequest request, final TransportRequestOptions options, TransportResponseHandler<T> handler) {
        try {
            Transport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, options, handler);
        } catch (NodeNotConnectedException ex) {
            handler.handleException(ex);
        }
    }

    public final <T extends TransportResponse> void sendRequest(final Transport.Connection connection, final String action, final TransportRequest request, final TransportRequestOptions options, TransportResponseHandler<T> handler) {
        asyncSender.sendRequest(connection, action, request, options, handler);
    }

    public Transport.Connection getConnection(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return localNodeConnection;
        } else {
            return transport.getConnection(node);
        }
    }

    public <T extends TransportResponse> void sendChildRequest(final DiscoveryNode node, final String action, final TransportRequest request, final Task parentTask, final TransportResponseHandler<T> handler) {
        sendChildRequest(node, action, request, parentTask, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> void sendChildRequest(final DiscoveryNode node, final String action, final TransportRequest request, final Task parentTask, final TransportRequestOptions options, final TransportResponseHandler<T> handler) {
        request.setParentTask(localNode.getId(), parentTask.getId());
        try {
            taskManager.registerChildTask(parentTask, node.getId());
            final Transport.Connection connection = getConnection(node);
            sendRequest(connection, action, request, options, handler);
        } catch (TaskCancelledException ex) {
            handler.handleException(new TransportException(ex));
        } catch (NodeNotConnectedException ex) {
            handler.handleException(ex);
        }
    }

    private <T extends TransportResponse> void sendRequestInternal(final Transport.Connection connection, final String action, final TransportRequest request, final TransportRequestOptions options, TransportResponseHandler<T> handler) {
        if (connection == null) {
            throw new IllegalStateException("can't send request to a null connection");
        }
        DiscoveryNode node = connection.getNode();
        final long requestId = transport.newRequestId();
        final TimeoutHandler timeoutHandler;
        try {
            if (options.timeout() == null) {
                timeoutHandler = null;
            } else {
                timeoutHandler = new TimeoutHandler(requestId);
            }
            TransportResponseHandler<T> responseHandler = new ContextRestoreResponseHandler<>(threadPool.getThreadContext().newStoredContext(), handler);
            clientHandlers.put(requestId, new RequestHolder<>(responseHandler, connection.getNode(), action, timeoutHandler));
            if (lifecycle.stoppedOrClosed()) {
                throw new TransportException("TransportService is closed stopped can't send request");
            }
            if (timeoutHandler != null) {
                assert options.timeout() != null;
                timeoutHandler.future = threadPool.schedule(options.timeout(), ThreadPool.Names.GENERIC, timeoutHandler);
            }
            connection.sendRequest(requestId, action, request, options);
        } catch (final Exception e) {
            final RequestHolder holderToNotify = clientHandlers.remove(requestId);
            if (holderToNotify != null) {
                holderToNotify.cancelTimeout();
                final SendRequestTransportException sendRequestException = new SendRequestTransportException(node, action, e);
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {

                    @Override
                    public void onRejection(Exception e) {
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage("failed to notify response handler on rejection, action: {}", holderToNotify.action()), e);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to notify response handler on exception, action: {}", holderToNotify.action()), e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        holderToNotify.handler().handleException(sendRequestException);
                    }
                });
            } else {
                logger.debug("Exception while sending request, handler likely already notified due to timeout", e);
            }
        }
    }

    private void sendLocalRequest(long requestId, final String action, final TransportRequest request) {
        final DirectResponseChannel channel = new DirectResponseChannel(logger, localNode, action, requestId, adapter, threadPool);
        try {
            final RequestHandlerRegistry reg = adapter.getRequestHandler(action);
            if (reg == null) {
                throw new ActionNotFoundTransportException("Action [" + action + "] not found");
            }
            final String executor = reg.getExecutor();
            if (ThreadPool.Names.SAME.equals(executor)) {
                reg.processMessageReceived(request, channel);
            } else {
                threadPool.executor(executor).execute(new AbstractRunnable() {

                    @Override
                    protected void doRun() throws Exception {
                        reg.processMessageReceived(request, channel);
                    }

                    @Override
                    public boolean isForceExecution() {
                        return reg.isForceExecution();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            channel.sendResponse(e);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to notify channel of error message for action [{}]", action), inner);
                        }
                    }
                });
            }
        } catch (Exception e) {
            try {
                channel.sendResponse(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to notify channel of error message for action [{}]", action), inner);
            }
        }
    }

    private boolean shouldTraceAction(String action) {
        if (tracerLogInclude.length > 0) {
            if (Regex.simpleMatch(tracerLogInclude, action) == false) {
                return false;
            }
        }
        if (tracerLogExclude.length > 0) {
            return !Regex.simpleMatch(tracerLogExclude, action);
        }
        return true;
    }

    public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
        return transport.addressesFromString(address, perAddressLimit);
    }

    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> requestFactory, String executor, TransportRequestHandler<Request> handler) {
        handler = interceptor.interceptHandler(action, executor, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(action, requestFactory, taskManager, handler, executor, false, true);
        registerRequestHandler(reg);
    }

    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> request, String executor, boolean forceExecution, boolean canTripCircuitBreaker, TransportRequestHandler<Request> handler) {
        handler = interceptor.interceptHandler(action, executor, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(action, request, taskManager, handler, executor, forceExecution, canTripCircuitBreaker);
        registerRequestHandler(reg);
    }

    private <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        synchronized (requestHandlerMutex) {
            if (requestHandlers.containsKey(reg.getAction())) {
                throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
            }
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
        }
    }

    protected RequestHandlerRegistry getRequestHandler(String action) {
        return requestHandlers.get(action);
    }

    protected class Adapter implements TransportServiceAdapter {

        final MeanMetric rxMetric = new MeanMetric();

        final MeanMetric txMetric = new MeanMetric();

        @Override
        public void addBytesReceived(long size) {
            rxMetric.inc(size);
        }

        @Override
        public void addBytesSent(long size) {
            txMetric.inc(size);
        }

        @Override
        public void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) {
            if (traceEnabled() && shouldTraceAction(action)) {
                traceRequestSent(node, requestId, action, options);
            }
        }

        protected boolean traceEnabled() {
            return tracerLog.isTraceEnabled();
        }

        @Override
        public void onResponseSent(long requestId, String action, TransportResponse response, TransportResponseOptions options) {
            if (traceEnabled() && shouldTraceAction(action)) {
                traceResponseSent(requestId, action);
            }
        }

        @Override
        public void onResponseSent(long requestId, String action, Exception e) {
            if (traceEnabled() && shouldTraceAction(action)) {
                traceResponseSent(requestId, action, e);
            }
        }

        protected void traceResponseSent(long requestId, String action, Exception e) {
            tracerLog.trace((org.apache.logging.log4j.util.Supplier<?>) () -> new ParameterizedMessage("[{}][{}] sent error response", requestId, action), e);
        }

        @Override
        public void onRequestReceived(long requestId, String action) {
            try {
                blockIncomingRequestsLatch.await();
            } catch (InterruptedException e) {
                logger.trace("interrupted while waiting for incoming requests block to be removed");
            }
            if (traceEnabled() && shouldTraceAction(action)) {
                traceReceivedRequest(requestId, action);
            }
        }

        @Override
        public RequestHandlerRegistry getRequestHandler(String action) {
            return requestHandlers.get(action);
        }

        @Override
        public TransportResponseHandler onResponseReceived(final long requestId) {
            RequestHolder holder = clientHandlers.remove(requestId);
            if (holder == null) {
                checkForTimeout(requestId);
                return null;
            }
            holder.cancelTimeout();
            if (traceEnabled() && shouldTraceAction(holder.action())) {
                traceReceivedResponse(requestId, holder.node(), holder.action());
            }
            return holder.handler();
        }

        protected void checkForTimeout(long requestId) {
            final DiscoveryNode sourceNode;
            final String action;
            assert clientHandlers.get(requestId) == null;
            TimeoutInfoHolder timeoutInfoHolder = timeoutInfoHandlers.remove(requestId);
            if (timeoutInfoHolder != null) {
                long time = System.currentTimeMillis();
                logger.warn("Received response for a request that has timed out, sent [{}ms] ago, timed out [{}ms] ago, " + "action [{}], node [{}], id [{}]", time - timeoutInfoHolder.sentTime(), time - timeoutInfoHolder.timeoutTime(), timeoutInfoHolder.action(), timeoutInfoHolder.node(), requestId);
                action = timeoutInfoHolder.action();
                sourceNode = timeoutInfoHolder.node();
            } else {
                logger.warn("Transport response handler not found of id [{}]", requestId);
                action = null;
                sourceNode = null;
            }
            if (traceEnabled() == false) {
                return;
            }
            if (action == null) {
                assert sourceNode == null;
                traceUnresolvedResponse(requestId);
            } else if (shouldTraceAction(action)) {
                traceReceivedResponse(requestId, sourceNode, action);
            }
        }

        @Override
        public void onNodeConnected(final DiscoveryNode node) {
            final Stream<TransportConnectionListener> listenersToNotify = TransportService.this.connectionListeners.stream();
            threadPool.generic().execute(() -> listenersToNotify.forEach(listener -> listener.onNodeConnected(node)));
        }

        @Override
        public void onConnectionOpened(DiscoveryNode node) {
            final Stream<TransportConnectionListener> listenersToNotify = TransportService.this.connectionListeners.stream();
            threadPool.generic().execute(() -> listenersToNotify.forEach(listener -> listener.onConnectionOpened(node)));
        }

        @Override
        public void onNodeDisconnected(final DiscoveryNode node) {
            try {
                threadPool.generic().execute(() -> {
                    for (final TransportConnectionListener connectionListener : connectionListeners) {
                        connectionListener.onNodeDisconnected(node);
                    }
                });
                for (Map.Entry<Long, RequestHolder> entry : clientHandlers.entrySet()) {
                    RequestHolder holder = entry.getValue();
                    if (holder.node().equals(node)) {
                        final RequestHolder holderToNotify = clientHandlers.remove(entry.getKey());
                        if (holderToNotify != null) {
                            threadPool.generic().execute(() -> holderToNotify.handler().handleException(new NodeDisconnectedException(node, holderToNotify.action())));
                        }
                    }
                }
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Rejected execution on NodeDisconnected", ex);
            }
        }

        protected void traceReceivedRequest(long requestId, String action) {
            tracerLog.trace("[{}][{}] received request", requestId, action);
        }

        protected void traceResponseSent(long requestId, String action) {
            tracerLog.trace("[{}][{}] sent response", requestId, action);
        }

        protected void traceReceivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
            tracerLog.trace("[{}][{}] received response from [{}]", requestId, action, sourceNode);
        }

        protected void traceUnresolvedResponse(long requestId) {
            tracerLog.trace("[{}] received response but can't resolve it to a request", requestId);
        }

        protected void traceRequestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
            tracerLog.trace("[{}][{}] sent to [{}] (timeout: [{}])", requestId, action, node, options.timeout());
        }
    }

    class TimeoutHandler implements Runnable {

        private final long requestId;

        private final long sentTime = System.currentTimeMillis();

        volatile ScheduledFuture future;

        TimeoutHandler(long requestId) {
            this.requestId = requestId;
        }

        @Override
        public void run() {
            final RequestHolder holder = clientHandlers.get(requestId);
            if (holder != null) {
                long timeoutTime = System.currentTimeMillis();
                timeoutInfoHandlers.put(requestId, new TimeoutInfoHolder(holder.node(), holder.action(), sentTime, timeoutTime));
                final RequestHolder removedHolder = clientHandlers.remove(requestId);
                if (removedHolder != null) {
                    assert removedHolder == holder : "two different holder instances for request [" + requestId + "]";
                    removedHolder.handler().handleException(new ReceiveTimeoutTransportException(holder.node(), holder.action(), "request_id [" + requestId + "] timed out after [" + (timeoutTime - sentTime) + "ms]"));
                } else {
                    timeoutInfoHandlers.remove(requestId);
                }
            }
        }

        public void cancel() {
            assert clientHandlers.get(requestId) == null : "cancel must be called after the requestId [" + requestId + "] has been removed from clientHandlers";
            FutureUtils.cancel(future);
        }
    }

    static class TimeoutInfoHolder {

        private final DiscoveryNode node;

        private final String action;

        private final long sentTime;

        private final long timeoutTime;

        TimeoutInfoHolder(DiscoveryNode node, String action, long sentTime, long timeoutTime) {
            this.node = node;
            this.action = action;
            this.sentTime = sentTime;
            this.timeoutTime = timeoutTime;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String action() {
            return action;
        }

        public long sentTime() {
            return sentTime;
        }

        public long timeoutTime() {
            return timeoutTime;
        }
    }

    static class RequestHolder<T extends TransportResponse> {

        private final TransportResponseHandler<T> handler;

        private final DiscoveryNode node;

        private final String action;

        private final TimeoutHandler timeoutHandler;

        RequestHolder(TransportResponseHandler<T> handler, DiscoveryNode node, String action, TimeoutHandler timeoutHandler) {
            this.handler = handler;
            this.node = node;
            this.action = action;
            this.timeoutHandler = timeoutHandler;
        }

        public TransportResponseHandler<T> handler() {
            return handler;
        }

        public DiscoveryNode node() {
            return this.node;
        }

        public String action() {
            return this.action;
        }

        public void cancelTimeout() {
            if (timeoutHandler != null) {
                timeoutHandler.cancel();
            }
        }
    }

    private static final class ContextRestoreResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        private final TransportResponseHandler<T> delegate;

        private final ThreadContext.StoredContext threadContext;

        private ContextRestoreResponseHandler(ThreadContext.StoredContext threadContext, TransportResponseHandler<T> delegate) {
            this.delegate = delegate;
            this.threadContext = threadContext;
        }

        @Override
        public T newInstance() {
            return delegate.newInstance();
        }

        @Override
        public void handleResponse(T response) {
            threadContext.restore();
            delegate.handleResponse(response);
        }

        @Override
        public void handleException(TransportException exp) {
            threadContext.restore();
            delegate.handleException(exp);
        }

        @Override
        public String executor() {
            return delegate.executor();
        }

        @Override
        public String toString() {
            return getClass().getName() + "/" + delegate.toString();
        }
    }

    static class DirectResponseChannel implements TransportChannel {

        final Logger logger;

        final DiscoveryNode localNode;

        private final String action;

        private final long requestId;

        final TransportServiceAdapter adapter;

        final ThreadPool threadPool;

        public DirectResponseChannel(Logger logger, DiscoveryNode localNode, String action, long requestId, TransportServiceAdapter adapter, ThreadPool threadPool) {
            this.logger = logger;
            this.localNode = localNode;
            this.action = action;
            this.requestId = requestId;
            this.adapter = adapter;
            this.threadPool = threadPool;
        }

        @Override
        public String action() {
            return action;
        }

        @Override
        public String getProfileName() {
            return DIRECT_RESPONSE_PROFILE;
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            sendResponse(response, TransportResponseOptions.EMPTY);
        }

        @Override
        public void sendResponse(final TransportResponse response, TransportResponseOptions options) throws IOException {
            final TransportResponseHandler handler = adapter.onResponseReceived(requestId);
            if (handler != null) {
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processResponse(handler, response);
                } else {
                    threadPool.executor(executor).execute(new Runnable() {

                        @SuppressWarnings({ "unchecked" })
                        @Override
                        public void run() {
                            processResponse(handler, response);
                        }
                    });
                }
            }
        }

        @SuppressWarnings("unchecked")
        protected void processResponse(TransportResponseHandler handler, TransportResponse response) {
            try {
                handler.handleResponse(response);
            } catch (Exception e) {
                processException(handler, wrapInRemote(new ResponseHandlerFailureTransportException(e)));
            }
        }

        @Override
        public void sendResponse(Exception exception) throws IOException {
            final TransportResponseHandler handler = adapter.onResponseReceived(requestId);
            if (handler != null) {
                final RemoteTransportException rtx = wrapInRemote(exception);
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processException(handler, rtx);
                } else {
                    threadPool.executor(handler.executor()).execute(new Runnable() {

                        @SuppressWarnings({ "unchecked" })
                        @Override
                        public void run() {
                            processException(handler, rtx);
                        }
                    });
                }
            }
        }

        protected RemoteTransportException wrapInRemote(Exception e) {
            if (e instanceof RemoteTransportException) {
                return (RemoteTransportException) e;
            }
            return new RemoteTransportException(localNode.getName(), localNode.getAddress(), action, e);
        }

        protected void processException(final TransportResponseHandler handler, final RemoteTransportException rtx) {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to handle exception for action [{}], handler [{}]", action, handler), e);
            }
        }

        @Override
        public long getRequestId() {
            return requestId;
        }

        @Override
        public String getChannelType() {
            return "direct";
        }
    }

    private boolean isLocalNode(DiscoveryNode discoveryNode) {
        return Objects.requireNonNull(discoveryNode, "discovery node must not be null").equals(localNode);
    }
}