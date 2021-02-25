package org.elasticsearch.test.transport;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.MockTcpTransport;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportServiceAdapter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public final class MockTransportService extends TransportService {

    private final Map<DiscoveryNode, List<Transport.Connection>> openConnections = new HashMap<>();

    public static class TestPlugin extends Plugin {

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING);
        }
    }

    public static MockTransportService createNewService(Settings settings, Version version, ThreadPool threadPool, @Nullable ClusterSettings clusterSettings) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        final Transport transport = new MockTcpTransport(settings, threadPool, BigArrays.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService(), namedWriteableRegistry, new NetworkService(settings, Collections.emptyList()), version);
        return new MockTransportService(settings, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR, clusterSettings);
    }

    private final Transport original;

    public MockTransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor interceptor, @Nullable ClusterSettings clusterSettings) {
        this(settings, transport, threadPool, interceptor, (boundAddress) -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), settings.get(Node.NODE_NAME_SETTING.getKey(), UUIDs.randomBase64UUID())), clusterSettings);
    }

    public MockTransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor interceptor, Function<BoundTransportAddress, DiscoveryNode> localNodeFactory, @Nullable ClusterSettings clusterSettings) {
        super(settings, new LookupTestTransport(transport), threadPool, interceptor, localNodeFactory, clusterSettings);
        this.original = transport;
    }

    public static TransportAddress[] extractTransportAddresses(TransportService transportService) {
        HashSet<TransportAddress> transportAddresses = new HashSet<>();
        BoundTransportAddress boundTransportAddress = transportService.boundAddress();
        transportAddresses.addAll(Arrays.asList(boundTransportAddress.boundAddresses()));
        transportAddresses.add(boundTransportAddress.publishAddress());
        return transportAddresses.toArray(new TransportAddress[transportAddresses.size()]);
    }

    @Override
    protected TaskManager createTaskManager() {
        if (MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.get(settings)) {
            return new MockTaskManager(settings);
        } else {
            return super.createTaskManager();
        }
    }

    public void clearAllRules() {
        transport().transports.clear();
    }

    public void clearRule(TransportService transportService) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            clearRule(transportAddress);
        }
    }

    public void clearRule(TransportAddress transportAddress) {
        Transport transport = transport().transports.remove(transportAddress);
        if (transport instanceof ClearableTransport) {
            ((ClearableTransport) transport).clearRule();
        }
    }

    public Transport original() {
        return original;
    }

    public void addFailToSendNoConnectRule(TransportService transportService) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            addFailToSendNoConnectRule(transportAddress);
        }
    }

    public void addFailToSendNoConnectRule(TransportAddress transportAddress) {
        addDelegate(transportAddress, new DelegateTransport(original) {

            @Override
            public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile) throws ConnectTransportException {
                throw new ConnectTransportException(node, "DISCONNECT: simulated");
            }

            @Override
            protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException {
                throw new ConnectTransportException(connection.getNode(), "DISCONNECT: simulated");
            }
        });
    }

    public void addFailToSendNoConnectRule(TransportService transportService, final String... blockedActions) {
        addFailToSendNoConnectRule(transportService, new HashSet<>(Arrays.asList(blockedActions)));
    }

    public void addFailToSendNoConnectRule(TransportAddress transportAddress, final String... blockedActions) {
        addFailToSendNoConnectRule(transportAddress, new HashSet<>(Arrays.asList(blockedActions)));
    }

    public void addFailToSendNoConnectRule(TransportService transportService, final Set<String> blockedActions) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            addFailToSendNoConnectRule(transportAddress, blockedActions);
        }
    }

    public void addFailToSendNoConnectRule(TransportAddress transportAddress, final Set<String> blockedActions) {
        addDelegate(transportAddress, new DelegateTransport(original) {

            @Override
            public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile) throws ConnectTransportException {
                original.connectToNode(node, connectionProfile);
            }

            @Override
            protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException {
                if (blockedActions.contains(action)) {
                    logger.info("--> preventing {} request", action);
                    throw new ConnectTransportException(connection.getNode(), "DISCONNECT: prevented " + action + " request");
                }
                connection.sendRequest(requestId, action, request, options);
            }
        });
    }

    public void addUnresponsiveRule(TransportService transportService) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            addUnresponsiveRule(transportAddress);
        }
    }

    public void addUnresponsiveRule(TransportAddress transportAddress) {
        addDelegate(transportAddress, new DelegateTransport(original) {

            @Override
            public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile) throws ConnectTransportException {
                throw new ConnectTransportException(node, "UNRESPONSIVE: simulated");
            }

            @Override
            protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException {
            }
        });
    }

    public void addUnresponsiveRule(TransportService transportService, final TimeValue duration) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            addUnresponsiveRule(transportAddress, duration);
        }
    }

    public void addUnresponsiveRule(TransportAddress transportAddress, final TimeValue duration) {
        final long startTime = System.currentTimeMillis();
        addDelegate(transportAddress, new ClearableTransport(original) {

            private final Queue<Runnable> requestsToSendWhenCleared = new LinkedBlockingDeque<Runnable>();

            private boolean cleared = false;

            TimeValue getDelay() {
                return new TimeValue(duration.millis() - (System.currentTimeMillis() - startTime));
            }

            @Override
            public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile) throws ConnectTransportException {
                TimeValue delay = getDelay();
                if (delay.millis() <= 0) {
                    original.connectToNode(node, connectionProfile);
                    return;
                }
                TimeValue connectingTimeout = NetworkService.TcpSettings.TCP_CONNECT_TIMEOUT.getDefault(Settings.EMPTY);
                try {
                    if (delay.millis() < connectingTimeout.millis()) {
                        Thread.sleep(delay.millis());
                        original.connectToNode(node, connectionProfile);
                    } else {
                        Thread.sleep(connectingTimeout.millis());
                        throw new ConnectTransportException(node, "UNRESPONSIVE: simulated");
                    }
                } catch (InterruptedException e) {
                    throw new ConnectTransportException(node, "UNRESPONSIVE: interrupted while sleeping", e);
                }
            }

            @Override
            protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException {
                TimeValue delay = getDelay();
                if (delay.millis() <= 0) {
                    connection.sendRequest(requestId, action, request, options);
                    return;
                }
                RequestHandlerRegistry reg = MockTransportService.this.getRequestHandler(action);
                BytesStreamOutput bStream = new BytesStreamOutput();
                request.writeTo(bStream);
                final TransportRequest clonedRequest = reg.newRequest();
                clonedRequest.readFrom(bStream.bytes().streamInput());
                Runnable runnable = new AbstractRunnable() {

                    AtomicBoolean requestSent = new AtomicBoolean();

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug("failed to send delayed request", e);
                    }

                    @Override
                    protected void doRun() throws IOException {
                        if (requestSent.compareAndSet(false, true)) {
                            connection.sendRequest(requestId, action, clonedRequest, options);
                        }
                    }
                };
                synchronized (this) {
                    if (cleared) {
                        runnable.run();
                    } else {
                        requestsToSendWhenCleared.add(runnable);
                        threadPool.schedule(delay, ThreadPool.Names.GENERIC, runnable);
                    }
                }
            }

            @Override
            public void clearRule() {
                synchronized (this) {
                    assert cleared == false;
                    cleared = true;
                    requestsToSendWhenCleared.forEach(Runnable::run);
                }
            }
        });
    }

    public boolean addDelegate(TransportService transportService, DelegateTransport transport) {
        boolean noRegistered = true;
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            noRegistered &= addDelegate(transportAddress, transport);
        }
        return noRegistered;
    }

    public boolean addDelegate(TransportAddress transportAddress, DelegateTransport transport) {
        return transport().transports.put(transportAddress, transport) == null;
    }

    private LookupTestTransport transport() {
        return (LookupTestTransport) transport;
    }

    private static class LookupTestTransport extends DelegateTransport {

        final ConcurrentMap<TransportAddress, Transport> transports = ConcurrentCollections.newConcurrentMap();

        LookupTestTransport(Transport transport) {
            super(transport);
        }

        private Transport getTransport(DiscoveryNode node) {
            Transport transport = transports.get(node.getAddress());
            if (transport != null) {
                return transport;
            }
            return this.transport;
        }

        @Override
        public boolean nodeConnected(DiscoveryNode node) {
            return getTransport(node).nodeConnected(node);
        }

        @Override
        public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile) throws ConnectTransportException {
            getTransport(node).connectToNode(node, connectionProfile);
        }

        @Override
        public void disconnectFromNode(DiscoveryNode node) {
            getTransport(node).disconnectFromNode(node);
        }

        @Override
        public Connection getConnection(DiscoveryNode node) {
            return getTransport(node).getConnection(node);
        }

        @Override
        public Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
            return getTransport(node).openConnection(node, profile);
        }
    }

    public static class DelegateTransport implements Transport {

        protected final Transport transport;

        public DelegateTransport(Transport transport) {
            this.transport = transport;
        }

        @Override
        public void transportServiceAdapter(TransportServiceAdapter service) {
            transport.transportServiceAdapter(service);
        }

        @Override
        public BoundTransportAddress boundAddress() {
            return transport.boundAddress();
        }

        @Override
        public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
            return transport.addressesFromString(address, perAddressLimit);
        }

        @Override
        public boolean nodeConnected(DiscoveryNode node) {
            return transport.nodeConnected(node);
        }

        @Override
        public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile) throws ConnectTransportException {
            transport.connectToNode(node, connectionProfile);
        }

        @Override
        public void disconnectFromNode(DiscoveryNode node) {
            transport.disconnectFromNode(node);
        }

        @Override
        public long serverOpen() {
            return transport.serverOpen();
        }

        @Override
        public List<String> getLocalAddresses() {
            return transport.getLocalAddresses();
        }

        @Override
        public long newRequestId() {
            return transport.newRequestId();
        }

        @Override
        public Connection getConnection(DiscoveryNode node) {
            return new FilteredConnection(transport.getConnection(node)) {

                @Override
                public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                    DelegateTransport.this.sendRequest(connection, requestId, action, request, options);
                }
            };
        }

        @Override
        public Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
            return new FilteredConnection(transport.openConnection(node, profile)) {

                @Override
                public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                    DelegateTransport.this.sendRequest(connection, requestId, action, request, options);
                }
            };
        }

        @Override
        public Lifecycle.State lifecycleState() {
            return transport.lifecycleState();
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {
            transport.addLifecycleListener(listener);
        }

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {
            transport.removeLifecycleListener(listener);
        }

        @Override
        public void start() {
            transport.start();
        }

        @Override
        public void stop() {
            transport.stop();
        }

        @Override
        public void close() {
            transport.close();
        }

        @Override
        public Map<String, BoundTransportAddress> profileBoundAddresses() {
            return transport.profileBoundAddresses();
        }

        protected void sendRequest(Transport.Connection connection, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException {
            connection.sendRequest(requestId, action, request, options);
        }
    }

    public abstract static class ClearableTransport extends DelegateTransport {

        public ClearableTransport(Transport transport) {
            super(transport);
        }

        public abstract void clearRule();
    }

    List<Tracer> activeTracers = new CopyOnWriteArrayList<>();

    public static class Tracer {

        public void receivedRequest(long requestId, String action) {
        }

        public void responseSent(long requestId, String action) {
        }

        public void responseSent(long requestId, String action, Throwable t) {
        }

        public void receivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
        }

        public void requestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
        }
    }

    public void addTracer(Tracer tracer) {
        activeTracers.add(tracer);
    }

    public boolean removeTracer(Tracer tracer) {
        return activeTracers.remove(tracer);
    }

    public void clearTracers() {
        activeTracers.clear();
    }

    @Override
    protected Adapter createAdapter() {
        return new MockAdapter();
    }

    class MockAdapter extends Adapter {

        @Override
        protected boolean traceEnabled() {
            return super.traceEnabled() || activeTracers.isEmpty() == false;
        }

        @Override
        protected void traceReceivedRequest(long requestId, String action) {
            super.traceReceivedRequest(requestId, action);
            for (Tracer tracer : activeTracers) {
                tracer.receivedRequest(requestId, action);
            }
        }

        @Override
        protected void traceResponseSent(long requestId, String action) {
            super.traceResponseSent(requestId, action);
            for (Tracer tracer : activeTracers) {
                tracer.responseSent(requestId, action);
            }
        }

        @Override
        protected void traceResponseSent(long requestId, String action, Exception e) {
            super.traceResponseSent(requestId, action, e);
            for (Tracer tracer : activeTracers) {
                tracer.responseSent(requestId, action, e);
            }
        }

        @Override
        protected void traceReceivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
            super.traceReceivedResponse(requestId, sourceNode, action);
            for (Tracer tracer : activeTracers) {
                tracer.receivedResponse(requestId, sourceNode, action);
            }
        }

        @Override
        protected void traceRequestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
            super.traceRequestSent(node, requestId, action, options);
            for (Tracer tracer : activeTracers) {
                tracer.requestSent(node, requestId, action, options);
            }
        }
    }

    private static class FilteredConnection implements Transport.Connection {

        protected final Transport.Connection connection;

        private FilteredConnection(Transport.Connection connection) {
            this.connection = connection;
        }

        @Override
        public DiscoveryNode getNode() {
            return connection.getNode();
        }

        @Override
        public Version getVersion() {
            return connection.getVersion();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
            connection.sendRequest(requestId, action, request, options);
        }

        @Override
        public void close() throws IOException {
            connection.close();
        }
    }

    public Transport getOriginalTransport() {
        Transport transport = transport();
        while (transport instanceof DelegateTransport) {
            transport = ((DelegateTransport) transport).transport;
        }
        return transport;
    }

    @Override
    public Transport.Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
        FilteredConnection filteredConnection = new FilteredConnection(super.openConnection(node, profile)) {

            final AtomicBoolean closed = new AtomicBoolean(false);

            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    if (closed.compareAndSet(false, true)) {
                        synchronized (openConnections) {
                            List<Transport.Connection> connections = openConnections.get(node);
                            boolean remove = connections.remove(this);
                            assert remove;
                            if (connections.isEmpty()) {
                                openConnections.remove(node);
                            }
                        }
                    }
                }
            }
        };
        synchronized (openConnections) {
            List<Transport.Connection> connections = openConnections.computeIfAbsent(node, (n) -> new CopyOnWriteArrayList<>());
            connections.add(filteredConnection);
        }
        return filteredConnection;
    }

    @Override
    protected void doClose() {
        super.doClose();
        synchronized (openConnections) {
            assert openConnections.size() == 0 : "still open connections: " + openConnections;
        }
    }
}