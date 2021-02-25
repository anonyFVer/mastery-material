package org.elasticsearch.transport;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import static java.util.Collections.emptyList;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.settings.Setting.listSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;

public class TransportService extends AbstractLifecycleComponent implements TransportMessageListener, TransportConnectionListener {

    public static final Setting<Integer> CONNECTIONS_PER_NODE_RECOVERY = intSetting("transport.connections_per_node.recovery", 2, 1, Setting.Property.NodeScope);

    public static final Setting<Integer> CONNECTIONS_PER_NODE_BULK = intSetting("transport.connections_per_node.bulk", 3, 1, Setting.Property.NodeScope);

    public static final Setting<Integer> CONNECTIONS_PER_NODE_REG = intSetting("transport.connections_per_node.reg", 6, 1, Setting.Property.NodeScope);

    public static final Setting<Integer> CONNECTIONS_PER_NODE_STATE = intSetting("transport.connections_per_node.state", 1, 1, Setting.Property.NodeScope);

    public static final Setting<Integer> CONNECTIONS_PER_NODE_PING = intSetting("transport.connections_per_node.ping", 1, 1, Setting.Property.NodeScope);

    public static final Setting<TimeValue> TCP_CONNECT_TIMEOUT = timeSetting("transport.tcp.connect_timeout", NetworkService.TCP_CONNECT_TIMEOUT, Setting.Property.NodeScope);

    public static final String DIRECT_RESPONSE_PROFILE = ".direct";

    public static final String HANDSHAKE_ACTION_NAME = "internal:transport/handshake";

    private final CountDownLatch blockIncomingRequestsLatch = new CountDownLatch(1);

    protected final Transport transport;

    protected final ConnectionManager connectionManager;

    protected final ThreadPool threadPool;

    protected final ClusterName clusterName;

    protected final TaskManager taskManager;

    private final TransportInterceptor.AsyncSender asyncSender;

    private final Function<BoundTransportAddress, DiscoveryNode> localNodeFactory;

    private final boolean connectToRemoteCluster;

    private final Transport.ResponseHandlers responseHandlers;

    private final TransportInterceptor interceptor;

    final Map<Long, TimeoutInfoHolder> timeoutInfoHandlers = Collections.synchronizedMap(new LinkedHashMap<Long, TimeoutInfoHolder>(100, .75F, true) {

        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > 100;
        }
    });

    public static final TransportInterceptor NOOP_TRANSPORT_INTERCEPTOR = new TransportInterceptor() {
    };

    public static final Setting<List<String>> TRACE_LOG_INCLUDE_SETTING = listSetting("transport.tracer.include", emptyList(), Function.identity(), Property.Dynamic, Property.NodeScope);

    public static final Setting<List<String>> TRACE_LOG_EXCLUDE_SETTING = listSetting("transport.tracer.exclude", Arrays.asList("internal:discovery/zen/fd*", TransportLivenessAction.NAME), Function.identity(), Property.Dynamic, Property.NodeScope);

    private final Logger tracerLog;

    volatile String[] tracerLogInclude;

    volatile String[] tracerLogExclude;

    private final RemoteClusterService remoteClusterService;

    private final boolean validateConnections;

    volatile DiscoveryNode localNode = null;

    private final Transport.Connection localNodeConnection = new Transport.Connection() {

        @Override
        public DiscoveryNode getNode() {
            return localNode;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) throws TransportException {
            sendLocalRequest(requestId, action, request, options);
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
        }
    };

    public TransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor transportInterceptor, Function<BoundTransportAddress, DiscoveryNode> localNodeFactory, @Nullable ClusterSettings clusterSettings, Set<String> taskHeaders) {
        this(settings, transport, threadPool, transportInterceptor, localNodeFactory, clusterSettings, taskHeaders, new ConnectionManager(settings, transport, threadPool));
    }

    public TransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor transportInterceptor, Function<BoundTransportAddress, DiscoveryNode> localNodeFactory, @Nullable ClusterSettings clusterSettings, Set<String> taskHeaders, ConnectionManager connectionManager) {
        super(settings);
        this.validateConnections = TransportClient.CLIENT_TYPE.equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey())) == false || TransportClient.CLIENT_TRANSPORT_SNIFF.get(settings);
        this.transport = transport;
        this.threadPool = threadPool;
        this.localNodeFactory = localNodeFactory;
        this.connectionManager = connectionManager;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        setTracerLogInclude(TRACE_LOG_INCLUDE_SETTING.get(settings));
        setTracerLogExclude(TRACE_LOG_EXCLUDE_SETTING.get(settings));
        tracerLog = Loggers.getLogger(logger, ".tracer");
        taskManager = createTaskManager(settings, threadPool, taskHeaders);
        this.interceptor = transportInterceptor;
        this.asyncSender = interceptor.interceptSender(this::sendRequestInternal);
        this.connectToRemoteCluster = RemoteClusterService.ENABLE_REMOTE_CLUSTERS.get(settings);
        remoteClusterService = new RemoteClusterService(settings, this);
        responseHandlers = transport.getResponseHandlers();
        if (clusterSettings != null) {
            clusterSettings.addSettingsUpdateConsumer(TRACE_LOG_INCLUDE_SETTING, this::setTracerLogInclude);
            clusterSettings.addSettingsUpdateConsumer(TRACE_LOG_EXCLUDE_SETTING, this::setTracerLogExclude);
            if (connectToRemoteCluster) {
                remoteClusterService.listenForUpdates(clusterSettings);
            }
        }
        registerRequestHandler(HANDSHAKE_ACTION_NAME, () -> HandshakeRequest.INSTANCE, ThreadPool.Names.SAME, false, false, (request, channel, task) -> channel.sendResponse(new HandshakeResponse(localNode, clusterName, localNode.getVersion())));
    }

    public RemoteClusterService getRemoteClusterService() {
        return remoteClusterService;
    }

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    protected TaskManager createTaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders) {
        return new TaskManager(settings, threadPool, taskHeaders);
    }

    private ExecutorService getExecutorService() {
        return threadPool.generic();
    }

    void setTracerLogInclude(List<String> tracerLogInclude) {
        this.tracerLogInclude = tracerLogInclude.toArray(Strings.EMPTY_ARRAY);
    }

    void setTracerLogExclude(List<String> tracerLogExclude) {
        this.tracerLogExclude = tracerLogExclude.toArray(Strings.EMPTY_ARRAY);
    }

    @Override
    protected void doStart() {
        transport.addMessageListener(this);
        connectionManager.addListener(this);
        transport.start();
        if (transport.boundAddress() != null && logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
            for (Map.Entry<String, BoundTransportAddress> entry : transport.profileBoundAddresses().entrySet()) {
                logger.info("profile [{}]: {}", entry.getKey(), entry.getValue());
            }
        }
        localNode = localNodeFactory.apply(transport.boundAddress());
        if (connectToRemoteCluster) {
            remoteClusterService.initializeRemoteClusters();
        }
    }

    @Override
    protected void doStop() {
        try {
            IOUtils.close(connectionManager, remoteClusterService, transport::stop);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            for (final Transport.ResponseContext holderToNotify : responseHandlers.prune(h -> true)) {
                getExecutorService().execute(new AbstractRunnable() {

                    @Override
                    public void onRejection(Exception e) {
                        logger.debug(() -> new ParameterizedMessage("failed to notify response handler on rejection, action: {}", holderToNotify.action()), e);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(() -> new ParameterizedMessage("failed to notify response handler on exception, action: {}", holderToNotify.action()), e);
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

    @Override
    protected void doClose() throws IOException {
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
        return transport.getStats();
    }

    public BoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    public List<String> getLocalAddresses() {
        return transport.getLocalAddresses();
    }

    public boolean nodeConnected(DiscoveryNode node) {
        return isLocalNode(node) || connectionManager.nodeConnected(node);
    }

    public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
        connectToNode(node, null);
    }

    public void connectToNode(final DiscoveryNode node, ConnectionProfile connectionProfile) {
        if (isLocalNode(node)) {
            return;
        }
        connectionManager.connectToNode(node, connectionProfile, connectionValidator(node));
    }

    public CheckedBiConsumer<Transport.Connection, ConnectionProfile, IOException> connectionValidator(DiscoveryNode node) {
        return (newConnection, actualProfile) -> {
            final DiscoveryNode remote = handshake(newConnection, actualProfile.getHandshakeTimeout().millis(), cn -> true).discoveryNode;
            if (validateConnections && node.equals(remote) == false) {
                throw new ConnectTransportException(node, "handshake failed. unexpected remote node " + remote);
            }
        };
    }

    public Transport.Connection openConnection(final DiscoveryNode node, ConnectionProfile connectionProfile) throws IOException {
        if (isLocalNode(node)) {
            return localNodeConnection;
        } else {
            return connectionManager.openConnection(node, connectionProfile);
        }
    }

    public DiscoveryNode handshake(final Transport.Connection connection, final long handshakeTimeout) throws ConnectTransportException {
        return handshake(connection, handshakeTimeout, clusterName::equals).discoveryNode;
    }

    public HandshakeResponse handshake(final Transport.Connection connection, final long handshakeTimeout, Predicate<ClusterName> clusterNamePredicate) {
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
        if (!clusterNamePredicate.test(response.clusterName)) {
            throw new IllegalStateException("handshake failed, mismatched cluster name [" + response.clusterName + "] - " + node);
        } else if (response.version.isCompatible(localNode.getVersion()) == false) {
            throw new IllegalStateException("handshake failed, incompatible version [" + response.version + "] - " + node);
        }
        return response;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    static class HandshakeRequest extends TransportRequest {

        public static final HandshakeRequest INSTANCE = new HandshakeRequest();

        private HandshakeRequest() {
        }
    }

    public static class HandshakeResponse extends TransportResponse {

        private DiscoveryNode discoveryNode;

        private ClusterName clusterName;

        private Version version;

        HandshakeResponse() {
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

        public DiscoveryNode getDiscoveryNode() {
            return discoveryNode;
        }

        public ClusterName getClusterName() {
            return clusterName;
        }
    }

    public void disconnectFromNode(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return;
        }
        connectionManager.disconnectFromNode(node);
    }

    public void addConnectionListener(TransportConnectionListener listener) {
        connectionManager.addListener(listener);
    }

    public void removeConnectionListener(TransportConnectionListener listener) {
        connectionManager.removeListener(listener);
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
        try {
            asyncSender.sendRequest(connection, action, request, options, handler);
        } catch (NodeNotConnectedException ex) {
            handler.handleException(ex);
        }
    }

    public Transport.Connection getConnection(DiscoveryNode node) {
        if (isLocalNode(node)) {
            return localNodeConnection;
        } else {
            return connectionManager.getConnection(node);
        }
    }

    public final <T extends TransportResponse> void sendChildRequest(final DiscoveryNode node, final String action, final TransportRequest request, final Task parentTask, final TransportRequestOptions options, final TransportResponseHandler<T> handler) {
        try {
            Transport.Connection connection = getConnection(node);
            sendChildRequest(connection, action, request, parentTask, options, handler);
        } catch (NodeNotConnectedException ex) {
            handler.handleException(ex);
        }
    }

    public <T extends TransportResponse> void sendChildRequest(final Transport.Connection connection, final String action, final TransportRequest request, final Task parentTask, final TransportResponseHandler<T> handler) {
        sendChildRequest(connection, action, request, parentTask, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> void sendChildRequest(final Transport.Connection connection, final String action, final TransportRequest request, final Task parentTask, final TransportRequestOptions options, final TransportResponseHandler<T> handler) {
        request.setParentTask(localNode.getId(), parentTask.getId());
        try {
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
        Supplier<ThreadContext.StoredContext> storedContextSupplier = threadPool.getThreadContext().newRestorableContext(true);
        ContextRestoreResponseHandler<T> responseHandler = new ContextRestoreResponseHandler<>(storedContextSupplier, handler);
        final long requestId = responseHandlers.add(new Transport.ResponseContext<>(responseHandler, connection, action));
        final TimeoutHandler timeoutHandler;
        if (options.timeout() != null) {
            timeoutHandler = new TimeoutHandler(requestId, connection.getNode(), action);
            responseHandler.setTimeoutHandler(timeoutHandler);
        } else {
            timeoutHandler = null;
        }
        try {
            if (lifecycle.stoppedOrClosed()) {
                throw new TransportException("TransportService is closed stopped can't send request");
            }
            if (timeoutHandler != null) {
                assert options.timeout() != null;
                timeoutHandler.future = threadPool.schedule(options.timeout(), ThreadPool.Names.GENERIC, timeoutHandler);
            }
            connection.sendRequest(requestId, action, request, options);
        } catch (final Exception e) {
            final Transport.ResponseContext contextToNotify = responseHandlers.remove(requestId);
            if (contextToNotify != null) {
                if (timeoutHandler != null) {
                    timeoutHandler.cancel();
                }
                final SendRequestTransportException sendRequestException = new SendRequestTransportException(node, action, e);
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {

                    @Override
                    public void onRejection(Exception e) {
                        logger.debug(() -> new ParameterizedMessage("failed to notify response handler on rejection, action: {}", contextToNotify.action()), e);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(() -> new ParameterizedMessage("failed to notify response handler on exception, action: {}", contextToNotify.action()), e);
                    }

                    @Override
                    protected void doRun() throws Exception {
                        contextToNotify.handler().handleException(sendRequestException);
                    }
                });
            } else {
                logger.debug("Exception while sending request, handler likely already notified due to timeout", e);
            }
        }
    }

    private void sendLocalRequest(long requestId, final String action, final TransportRequest request, TransportRequestOptions options) {
        final DirectResponseChannel channel = new DirectResponseChannel(logger, localNode, action, requestId, this, threadPool);
        try {
            onRequestSent(localNode, requestId, action, request, options);
            onRequestReceived(requestId, action);
            final RequestHandlerRegistry reg = getRequestHandler(action);
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
                            logger.warn(() -> new ParameterizedMessage("failed to notify channel of error message for action [{}]", action), inner);
                        }
                    }
                });
            }
        } catch (Exception e) {
            try {
                channel.sendResponse(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.warn(() -> new ParameterizedMessage("failed to notify channel of error message for action [{}]", action), inner);
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

    public static final Set<String> VALID_ACTION_PREFIXES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("indices:admin", "indices:monitor", "indices:data/write", "indices:data/read", "indices:internal", "cluster:admin", "cluster:monitor", "cluster:internal", "internal:")));

    private void validateActionName(String actionName) {
        if (isValidActionName(actionName) == false) {
            logger.warn("invalid action name [" + actionName + "] must start with one of: " + TransportService.VALID_ACTION_PREFIXES);
        }
    }

    public static boolean isValidActionName(String actionName) {
        for (String prefix : VALID_ACTION_PREFIXES) {
            if (actionName.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> requestFactory, String executor, TransportRequestHandler<Request> handler) {
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, false, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(action, Streamable.newWriteableReader(requestFactory), taskManager, handler, executor, false, true);
        transport.registerRequestHandler(reg);
    }

    public <Request extends TransportRequest> void registerRequestHandler(String action, String executor, Writeable.Reader<Request> requestReader, TransportRequestHandler<Request> handler) {
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, false, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(action, requestReader, taskManager, handler, executor, false, true);
        transport.registerRequestHandler(reg);
    }

    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> request, String executor, boolean forceExecution, boolean canTripCircuitBreaker, TransportRequestHandler<Request> handler) {
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, forceExecution, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(action, Streamable.newWriteableReader(request), taskManager, handler, executor, forceExecution, canTripCircuitBreaker);
        transport.registerRequestHandler(reg);
    }

    public <Request extends TransportRequest> void registerRequestHandler(String action, String executor, boolean forceExecution, boolean canTripCircuitBreaker, Writeable.Reader<Request> requestReader, TransportRequestHandler<Request> handler) {
        validateActionName(action);
        handler = interceptor.interceptHandler(action, executor, forceExecution, handler);
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(action, requestReader, taskManager, handler, executor, forceExecution, canTripCircuitBreaker);
        transport.registerRequestHandler(reg);
    }

    public void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) {
        if (traceEnabled() && shouldTraceAction(action)) {
            traceRequestSent(node, requestId, action, options);
        }
    }

    protected boolean traceEnabled() {
        return tracerLog.isTraceEnabled();
    }

    public void onResponseSent(long requestId, String action, TransportResponse response, TransportResponseOptions options) {
        if (traceEnabled() && shouldTraceAction(action)) {
            traceResponseSent(requestId, action);
        }
    }

    public void onResponseSent(long requestId, String action, Exception e) {
        if (traceEnabled() && shouldTraceAction(action)) {
            traceResponseSent(requestId, action, e);
        }
    }

    protected void traceResponseSent(long requestId, String action, Exception e) {
        tracerLog.trace(() -> new ParameterizedMessage("[{}][{}] sent error response", requestId, action), e);
    }

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

    public RequestHandlerRegistry getRequestHandler(String action) {
        return transport.getRequestHandler(action);
    }

    @Override
    public void onResponseReceived(long requestId, Transport.ResponseContext holder) {
        if (holder == null) {
            checkForTimeout(requestId);
        } else if (traceEnabled() && shouldTraceAction(holder.action())) {
            traceReceivedResponse(requestId, holder.connection().getNode(), holder.action());
        }
    }

    private void checkForTimeout(long requestId) {
        final DiscoveryNode sourceNode;
        final String action;
        assert responseHandlers.contains(requestId) == false;
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
    public void onConnectionClosed(Transport.Connection connection) {
        try {
            List<Transport.ResponseContext> pruned = responseHandlers.prune(h -> h.connection().getCacheKey().equals(connection.getCacheKey()));
            getExecutorService().execute(() -> {
                for (Transport.ResponseContext holderToNotify : pruned) {
                    holderToNotify.handler().handleException(new NodeDisconnectedException(connection.getNode(), holderToNotify.action()));
                }
            });
        } catch (EsRejectedExecutionException ex) {
            logger.debug("Rejected execution on onConnectionClosed", ex);
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

    final class TimeoutHandler implements Runnable {

        private final long requestId;

        private final long sentTime = System.currentTimeMillis();

        private final String action;

        private final DiscoveryNode node;

        volatile ScheduledFuture future;

        TimeoutHandler(long requestId, DiscoveryNode node, String action) {
            this.requestId = requestId;
            this.node = node;
            this.action = action;
        }

        @Override
        public void run() {
            if (responseHandlers.contains(requestId)) {
                long timeoutTime = System.currentTimeMillis();
                timeoutInfoHandlers.put(requestId, new TimeoutInfoHolder(node, action, sentTime, timeoutTime));
                final Transport.ResponseContext holder = responseHandlers.remove(requestId);
                if (holder != null) {
                    assert holder.action().equals(action);
                    assert holder.connection().getNode().equals(node);
                    holder.handler().handleException(new ReceiveTimeoutTransportException(holder.connection().getNode(), holder.action(), "request_id [" + requestId + "] timed out after [" + (timeoutTime - sentTime) + "ms]"));
                } else {
                    timeoutInfoHandlers.remove(requestId);
                }
            }
        }

        public void cancel() {
            assert responseHandlers.contains(requestId) == false : "cancel must be called after the requestId [" + requestId + "] has been removed from clientHandlers";
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

    public static final class ContextRestoreResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        private final TransportResponseHandler<T> delegate;

        private final Supplier<ThreadContext.StoredContext> contextSupplier;

        private volatile TimeoutHandler handler;

        public ContextRestoreResponseHandler(Supplier<ThreadContext.StoredContext> contextSupplier, TransportResponseHandler<T> delegate) {
            this.delegate = delegate;
            this.contextSupplier = contextSupplier;
        }

        @Override
        public T read(StreamInput in) throws IOException {
            return delegate.read(in);
        }

        @Override
        public void handleResponse(T response) {
            if (handler != null) {
                handler.cancel();
            }
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleResponse(response);
            }
        }

        @Override
        public void handleException(TransportException exp) {
            if (handler != null) {
                handler.cancel();
            }
            try (ThreadContext.StoredContext ignore = contextSupplier.get()) {
                delegate.handleException(exp);
            }
        }

        @Override
        public String executor() {
            return delegate.executor();
        }

        @Override
        public String toString() {
            return getClass().getName() + "/" + delegate.toString();
        }

        void setTimeoutHandler(TimeoutHandler handler) {
            this.handler = handler;
        }
    }

    static class DirectResponseChannel implements TransportChannel {

        final Logger logger;

        final DiscoveryNode localNode;

        private final String action;

        private final long requestId;

        final TransportService service;

        final ThreadPool threadPool;

        DirectResponseChannel(Logger logger, DiscoveryNode localNode, String action, long requestId, TransportService service, ThreadPool threadPool) {
            this.logger = logger;
            this.localNode = localNode;
            this.action = action;
            this.requestId = requestId;
            this.service = service;
            this.threadPool = threadPool;
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
            service.onResponseSent(requestId, action, response, options);
            final TransportResponseHandler handler = service.responseHandlers.onResponseReceived(requestId, service);
            if (handler != null) {
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processResponse(handler, response);
                } else {
                    threadPool.executor(executor).execute(() -> processResponse(handler, response));
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
            service.onResponseSent(requestId, action, exception);
            final TransportResponseHandler handler = service.responseHandlers.onResponseReceived(requestId, service);
            if (handler != null) {
                final RemoteTransportException rtx = wrapInRemote(exception);
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processException(handler, rtx);
                } else {
                    threadPool.executor(handler.executor()).execute(() -> processException(handler, rtx));
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
                logger.error(() -> new ParameterizedMessage("failed to handle exception for action [{}], handler [{}]", action, handler), e);
            }
        }

        @Override
        public String getChannelType() {
            return "direct";
        }

        @Override
        public Version getVersion() {
            return localNode.getVersion();
        }
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    private boolean isLocalNode(DiscoveryNode discoveryNode) {
        return Objects.requireNonNull(discoveryNode, "discovery node must not be null").equals(localNode);
    }
}