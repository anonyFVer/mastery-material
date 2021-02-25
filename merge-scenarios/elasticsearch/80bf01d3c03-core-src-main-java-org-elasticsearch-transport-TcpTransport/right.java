package org.elasticsearch.transport;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.compress.NotCompressedException;
import org.elasticsearch.common.io.ReleasableBytesStream;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractLifecycleRunnable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.CancelledKeyException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.settings.Setting.timeSetting;
import static org.elasticsearch.common.transport.NetworkExceptionHelper.isCloseConnectionException;
import static org.elasticsearch.common.transport.NetworkExceptionHelper.isConnectException;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public abstract class TcpTransport<Channel> extends AbstractLifecycleComponent implements Transport {

    public static final String TRANSPORT_SERVER_WORKER_THREAD_NAME_PREFIX = "transport_server_worker";

    public static final String TRANSPORT_SERVER_BOSS_THREAD_NAME_PREFIX = "transport_server_boss";

    public static final String TRANSPORT_CLIENT_WORKER_THREAD_NAME_PREFIX = "transport_client_worker";

    public static final String TRANSPORT_CLIENT_BOSS_THREAD_NAME_PREFIX = "transport_client_boss";

    public static final Setting<TimeValue> PING_SCHEDULE = timeSetting("transport.ping_schedule", TimeValue.timeValueSeconds(-1), Setting.Property.NodeScope);

    public static final Setting<Integer> CONNECTIONS_PER_NODE_RECOVERY = intSetting("transport.connections_per_node.recovery", 2, 1, Setting.Property.NodeScope);

    public static final Setting<Integer> CONNECTIONS_PER_NODE_BULK = intSetting("transport.connections_per_node.bulk", 3, 1, Setting.Property.NodeScope);

    public static final Setting<Integer> CONNECTIONS_PER_NODE_REG = intSetting("transport.connections_per_node.reg", 6, 1, Setting.Property.NodeScope);

    public static final Setting<Integer> CONNECTIONS_PER_NODE_STATE = intSetting("transport.connections_per_node.state", 1, 1, Setting.Property.NodeScope);

    public static final Setting<Integer> CONNECTIONS_PER_NODE_PING = intSetting("transport.connections_per_node.ping", 1, 1, Setting.Property.NodeScope);

    public static final Setting<TimeValue> TCP_CONNECT_TIMEOUT = timeSetting("transport.tcp.connect_timeout", NetworkService.TcpSettings.TCP_CONNECT_TIMEOUT, Setting.Property.NodeScope);

    public static final Setting<Boolean> TCP_NO_DELAY = boolSetting("transport.tcp_no_delay", NetworkService.TcpSettings.TCP_NO_DELAY, Setting.Property.NodeScope);

    public static final Setting<Boolean> TCP_KEEP_ALIVE = boolSetting("transport.tcp.keep_alive", NetworkService.TcpSettings.TCP_KEEP_ALIVE, Setting.Property.NodeScope);

    public static final Setting<Boolean> TCP_REUSE_ADDRESS = boolSetting("transport.tcp.reuse_address", NetworkService.TcpSettings.TCP_REUSE_ADDRESS, Setting.Property.NodeScope);

    public static final Setting<Boolean> TCP_BLOCKING_CLIENT = boolSetting("transport.tcp.blocking_client", NetworkService.TcpSettings.TCP_BLOCKING_CLIENT, Setting.Property.NodeScope);

    public static final Setting<Boolean> TCP_BLOCKING_SERVER = boolSetting("transport.tcp.blocking_server", NetworkService.TcpSettings.TCP_BLOCKING_SERVER, Setting.Property.NodeScope);

    public static final Setting<ByteSizeValue> TCP_SEND_BUFFER_SIZE = Setting.byteSizeSetting("transport.tcp.send_buffer_size", NetworkService.TcpSettings.TCP_SEND_BUFFER_SIZE, Setting.Property.NodeScope);

    public static final Setting<ByteSizeValue> TCP_RECEIVE_BUFFER_SIZE = Setting.byteSizeSetting("transport.tcp.receive_buffer_size", NetworkService.TcpSettings.TCP_RECEIVE_BUFFER_SIZE, Setting.Property.NodeScope);

    private static final long NINETY_PER_HEAP_SIZE = (long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.9);

    private static final int PING_DATA_SIZE = -1;

    protected final boolean blockingClient;

    private final CircuitBreakerService circuitBreakerService;

    protected final ScheduledPing scheduledPing;

    private final TimeValue pingSchedule;

    protected final ThreadPool threadPool;

    private final BigArrays bigArrays;

    protected final NetworkService networkService;

    protected volatile TransportServiceAdapter transportServiceAdapter;

    protected final ConcurrentMap<DiscoveryNode, NodeChannels> connectedNodes = newConcurrentMap();

    protected final Map<String, List<Channel>> serverChannels = newConcurrentMap();

    protected final ConcurrentMap<String, BoundTransportAddress> profileBoundAddresses = newConcurrentMap();

    protected final KeyedLock<String> connectionLock = new KeyedLock<>();

    private final NamedWriteableRegistry namedWriteableRegistry;

    protected final ReadWriteLock globalLock = new ReentrantReadWriteLock();

    protected final boolean compress;

    protected volatile BoundTransportAddress boundAddress;

    private final String transportName;

    protected final ConnectionProfile defaultConnectionProfile;

    private final ConcurrentMap<Long, HandshakeResponseHandler> pendingHandshakes = new ConcurrentHashMap<>();

    private final AtomicLong requestIdGenerator = new AtomicLong();

    private final CounterMetric numHandshakes = new CounterMetric();

    private static final String HANDSHAKE_ACTION_NAME = "internal:tcp/handshake";

    public TcpTransport(String transportName, Settings settings, ThreadPool threadPool, BigArrays bigArrays, CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry, NetworkService networkService) {
        super(settings);
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
        this.circuitBreakerService = circuitBreakerService;
        this.scheduledPing = new ScheduledPing();
        this.pingSchedule = PING_SCHEDULE.get(settings);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.compress = Transport.TRANSPORT_TCP_COMPRESS.get(settings);
        this.networkService = networkService;
        this.transportName = transportName;
        this.blockingClient = TCP_BLOCKING_CLIENT.get(settings);
        defaultConnectionProfile = buildDefaultConnectionProfile(settings);
    }

    static ConnectionProfile buildDefaultConnectionProfile(Settings settings) {
        int connectionsPerNodeRecovery = CONNECTIONS_PER_NODE_RECOVERY.get(settings);
        int connectionsPerNodeBulk = CONNECTIONS_PER_NODE_BULK.get(settings);
        int connectionsPerNodeReg = CONNECTIONS_PER_NODE_REG.get(settings);
        int connectionsPerNodeState = CONNECTIONS_PER_NODE_STATE.get(settings);
        int connectionsPerNodePing = CONNECTIONS_PER_NODE_PING.get(settings);
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.setConnectTimeout(TCP_CONNECT_TIMEOUT.get(settings));
        builder.addConnections(connectionsPerNodeBulk, TransportRequestOptions.Type.BULK);
        builder.addConnections(connectionsPerNodePing, TransportRequestOptions.Type.PING);
        builder.addConnections(DiscoveryNode.isMasterNode(settings) ? connectionsPerNodeState : 0, TransportRequestOptions.Type.STATE);
        builder.addConnections(DiscoveryNode.isDataNode(settings) ? connectionsPerNodeRecovery : 0, TransportRequestOptions.Type.RECOVERY);
        builder.addConnections(connectionsPerNodeReg, TransportRequestOptions.Type.REG);
        return builder.build();
    }

    @Override
    protected void doStart() {
        if (pingSchedule.millis() > 0) {
            threadPool.schedule(pingSchedule, ThreadPool.Names.GENERIC, scheduledPing);
        }
    }

    @Override
    public CircuitBreaker getInFlightRequestBreaker() {
        return circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
    }

    @Override
    public void transportServiceAdapter(TransportServiceAdapter service) {
        if (service.getRequestHandler(HANDSHAKE_ACTION_NAME) != null) {
            throw new IllegalStateException(HANDSHAKE_ACTION_NAME + " is a reserved request handler and must not be registered");
        }
        this.transportServiceAdapter = service;
    }

    private static class HandshakeResponseHandler<Channel> implements TransportResponseHandler<VersionHandshakeResponse> {

        final AtomicReference<Version> versionRef = new AtomicReference<>();

        final CountDownLatch latch = new CountDownLatch(1);

        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        final Channel channel;

        public HandshakeResponseHandler(Channel channel) {
            this.channel = channel;
        }

        @Override
        public VersionHandshakeResponse newInstance() {
            return new VersionHandshakeResponse();
        }

        @Override
        public void handleResponse(VersionHandshakeResponse response) {
            final boolean success = versionRef.compareAndSet(null, response.version);
            latch.countDown();
            assert success;
        }

        @Override
        public void handleException(TransportException exp) {
            final boolean success = exceptionRef.compareAndSet(null, exp);
            latch.countDown();
            assert success;
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    public class ScheduledPing extends AbstractLifecycleRunnable {

        private final BytesReference pingHeader;

        final CounterMetric successfulPings = new CounterMetric();

        final CounterMetric failedPings = new CounterMetric();

        public ScheduledPing() {
            super(lifecycle, logger);
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.writeByte((byte) 'E');
                out.writeByte((byte) 'S');
                out.writeInt(PING_DATA_SIZE);
                pingHeader = out.bytes();
            } catch (IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        @Override
        protected void doRunInLifecycle() throws Exception {
            for (Map.Entry<DiscoveryNode, NodeChannels> entry : connectedNodes.entrySet()) {
                DiscoveryNode node = entry.getKey();
                NodeChannels channels = entry.getValue();
                for (Channel channel : channels.getChannels()) {
                    try {
                        sendMessage(channel, pingHeader, successfulPings::inc);
                    } catch (Exception e) {
                        if (isOpen(channel)) {
                            logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to send ping transport message", node), e);
                            failedPings.inc();
                        } else {
                            logger.trace((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to send ping transport message (channel closed)", node), e);
                        }
                    }
                }
            }
        }

        public long getSuccessfulPings() {
            return successfulPings.count();
        }

        public long getFailedPings() {
            return failedPings.count();
        }

        @Override
        protected void onAfterInLifecycle() {
            try {
                threadPool.schedule(pingSchedule, ThreadPool.Names.GENERIC, this);
            } catch (EsRejectedExecutionException ex) {
                if (ex.isExecutorShutdown()) {
                    logger.debug("couldn't schedule new ping execution, executor is shutting down", ex);
                } else {
                    throw ex;
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (lifecycle.stoppedOrClosed()) {
                logger.trace("failed to send ping transport message", e);
            } else {
                logger.warn("failed to send ping transport message", e);
            }
        }
    }

    public final class NodeChannels implements Connection {

        private final Map<TransportRequestOptions.Type, ConnectionProfile.ConnectionTypeHandle> typeMapping;

        private final Channel[] channels;

        private final DiscoveryNode node;

        private final AtomicBoolean closed = new AtomicBoolean(false);

        private final Version version;

        public NodeChannels(DiscoveryNode node, Channel[] channels, ConnectionProfile connectionProfile) {
            this.node = node;
            this.channels = channels;
            assert channels.length == connectionProfile.getNumConnections() : "expected channels size to be == " + connectionProfile.getNumConnections() + " but was: [" + channels.length + "]";
            typeMapping = new EnumMap<>(TransportRequestOptions.Type.class);
            for (ConnectionProfile.ConnectionTypeHandle handle : connectionProfile.getHandles()) {
                for (TransportRequestOptions.Type type : handle.getTypes()) typeMapping.put(type, handle);
            }
            version = node.getVersion();
        }

        NodeChannels(NodeChannels channels, Version handshakeVersion) {
            this.node = channels.node;
            this.channels = channels.channels;
            this.typeMapping = channels.typeMapping;
            this.version = handshakeVersion;
        }

        @Override
        public Version getVersion() {
            return version;
        }

        public boolean hasChannel(Channel channel) {
            for (Channel channel1 : channels) {
                if (channel.equals(channel1)) {
                    return true;
                }
            }
            return false;
        }

        public List<Channel> getChannels() {
            return Arrays.asList(channels);
        }

        public Channel channel(TransportRequestOptions.Type type) {
            ConnectionProfile.ConnectionTypeHandle connectionTypeHandle = typeMapping.get(type);
            if (connectionTypeHandle == null) {
                throw new IllegalArgumentException("no type channel for [" + type + "]");
            }
            return connectionTypeHandle.getChannel(channels);
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                closeChannels(Arrays.stream(channels).filter(Objects::nonNull).collect(Collectors.toList()));
            }
        }

        @Override
        public DiscoveryNode getNode() {
            return this.node;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
            if (closed.get()) {
                throw new NodeNotConnectedException(node, "connection already closed");
            }
            Channel channel = channel(options.type());
            sendRequestToChannel(this.node, channel, requestId, action, request, options, getVersion(), (byte) 0);
        }
    }

    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return connectedNodes.containsKey(node);
    }

    @Override
    public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile) {
        connectionProfile = connectionProfile == null ? defaultConnectionProfile : connectionProfile;
        if (!lifecycle.started()) {
            throw new IllegalStateException("can't add nodes to a stopped transport");
        }
        if (node == null) {
            throw new ConnectTransportException(null, "can't connect to a null node");
        }
        globalLock.readLock().lock();
        try {
            try (Releasable ignored = connectionLock.acquire(node.getId())) {
                if (!lifecycle.started()) {
                    throw new IllegalStateException("can't add nodes to a stopped transport");
                }
                NodeChannels nodeChannels = connectedNodes.get(node);
                if (nodeChannels != null) {
                    return;
                }
                try {
                    try {
                        nodeChannels = openConnection(node, connectionProfile);
                    } catch (Exception e) {
                        logger.trace((Supplier<?>) () -> new ParameterizedMessage("failed to connect to [{}], cleaning dangling connections", node), e);
                        throw e;
                    }
                    connectedNodes.put(node, nodeChannels);
                    if (logger.isDebugEnabled()) {
                        logger.debug("connected to node [{}]", node);
                    }
                    transportServiceAdapter.onNodeConnected(node);
                } catch (ConnectTransportException e) {
                    throw e;
                } catch (Exception e) {
                    throw new ConnectTransportException(node, "general node connection failure", e);
                }
            }
        } finally {
            globalLock.readLock().unlock();
        }
    }

    @Override
    public final NodeChannels openConnection(DiscoveryNode node, ConnectionProfile connectionProfile) throws IOException {
        try {
            NodeChannels nodeChannels = connectToChannels(node, connectionProfile);
            final Channel channel = nodeChannels.getChannels().get(0);
            final TimeValue connectTimeout = connectionProfile.getConnectTimeout() == null ? defaultConnectionProfile.getConnectTimeout() : connectionProfile.getConnectTimeout();
            final TimeValue handshakeTimeout = connectionProfile.getHandshakeTimeout() == null ? connectTimeout : connectionProfile.getHandshakeTimeout();
            final Version version = executeHandshake(node, channel, handshakeTimeout);
            transportServiceAdapter.onConnectionOpened(node);
            return new NodeChannels(nodeChannels, version);
        } catch (ConnectTransportException e) {
            throw e;
        } catch (Exception e) {
            throw new ConnectTransportException(node, "general node connection failure", e);
        }
    }

    protected boolean disconnectFromNode(DiscoveryNode node, Channel channel, String reason) {
        NodeChannels nodeChannels = connectedNodes.get(node);
        if (nodeChannels != null && nodeChannels.hasChannel(channel)) {
            try (Releasable ignored = connectionLock.acquire(node.getId())) {
                nodeChannels = connectedNodes.get(node);
                if (nodeChannels != null && nodeChannels.hasChannel(channel)) {
                    connectedNodes.remove(node);
                    closeAndNotify(node, nodeChannels, reason);
                    return true;
                }
            }
        }
        return false;
    }

    private void closeAndNotify(DiscoveryNode node, NodeChannels nodeChannels, String reason) {
        try {
            logger.debug("disconnecting from [{}], {}", node, reason);
            IOUtils.closeWhileHandlingException(nodeChannels);
        } finally {
            logger.trace("disconnected from [{}], {}", node, reason);
            transportServiceAdapter.onNodeDisconnected(node);
        }
    }

    protected final void disconnectFromNodeChannel(final Channel channel, final Exception failure) {
        threadPool.generic().execute(() -> {
            try {
                try {
                    closeChannels(Collections.singletonList(channel));
                } finally {
                    for (DiscoveryNode node : connectedNodes.keySet()) {
                        if (disconnectFromNode(node, channel, ExceptionsHelper.detailedMessage(failure))) {
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                logger.warn("failed to close channel", e);
            } finally {
                onChannelClosed(channel);
            }
        });
    }

    @Override
    public NodeChannels getConnection(DiscoveryNode node) {
        NodeChannels nodeChannels = connectedNodes.get(node);
        if (nodeChannels == null) {
            throw new NodeNotConnectedException(node, "Node not connected");
        }
        return nodeChannels;
    }

    @Override
    public void disconnectFromNode(DiscoveryNode node) {
        try (Releasable ignored = connectionLock.acquire(node.getId())) {
            NodeChannels nodeChannels = connectedNodes.remove(node);
            if (nodeChannels != null) {
                closeAndNotify(node, nodeChannels, "due to explicit disconnect call");
            }
        }
    }

    protected Version getCurrentVersion() {
        return Version.CURRENT;
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return this.boundAddress;
    }

    @Override
    public Map<String, BoundTransportAddress> profileBoundAddresses() {
        return unmodifiableMap(new HashMap<>(profileBoundAddresses));
    }

    protected Map<String, Settings> buildProfileSettings() {
        Map<String, Settings> profiles = TransportSettings.TRANSPORT_PROFILES_SETTING.get(settings).getAsGroups(true);
        if (!profiles.containsKey(TransportSettings.DEFAULT_PROFILE)) {
            profiles = new HashMap<>(profiles);
            profiles.put(TransportSettings.DEFAULT_PROFILE, Settings.EMPTY);
        }
        Settings defaultSettings = profiles.get(TransportSettings.DEFAULT_PROFILE);
        Map<String, Settings> result = new HashMap<>();
        for (Map.Entry<String, Settings> entry : profiles.entrySet()) {
            Settings profileSettings = entry.getValue();
            String name = entry.getKey();
            if (!Strings.hasLength(name)) {
                logger.info("transport profile configured without a name. skipping profile with settings [{}]", profileSettings.toDelimitedString(','));
                continue;
            } else if (TransportSettings.DEFAULT_PROFILE.equals(name)) {
                profileSettings = Settings.builder().put(profileSettings).put("port", profileSettings.get("port", TransportSettings.PORT.get(this.settings))).build();
            } else if (profileSettings.get("port") == null) {
                logger.info("No port configured for profile [{}], not binding", name);
                continue;
            }
            Settings mergedSettings = Settings.builder().put(defaultSettings).put(profileSettings).build();
            result.put(name, mergedSettings);
        }
        return result;
    }

    @Override
    public List<String> getLocalAddresses() {
        List<String> local = new ArrayList<>();
        local.add("127.0.0.1");
        if (NetworkUtils.SUPPORTS_V6) {
            local.add("[::1]");
        }
        return local;
    }

    protected void bindServer(final String name, final Settings settings) {
        InetAddress[] hostAddresses;
        String[] bindHosts = settings.getAsArray("bind_host", null);
        try {
            hostAddresses = networkService.resolveBindHostAddresses(bindHosts);
        } catch (IOException e) {
            throw new BindTransportException("Failed to resolve host " + Arrays.toString(bindHosts) + "", e);
        }
        if (logger.isDebugEnabled()) {
            String[] addresses = new String[hostAddresses.length];
            for (int i = 0; i < hostAddresses.length; i++) {
                addresses[i] = NetworkAddress.format(hostAddresses[i]);
            }
            logger.debug("binding server bootstrap to: {}", (Object) addresses);
        }
        assert hostAddresses.length > 0;
        List<InetSocketAddress> boundAddresses = new ArrayList<>();
        for (InetAddress hostAddress : hostAddresses) {
            boundAddresses.add(bindToPort(name, hostAddress, settings.get("port")));
        }
        final BoundTransportAddress boundTransportAddress = createBoundTransportAddress(name, settings, boundAddresses);
        if (TransportSettings.DEFAULT_PROFILE.equals(name)) {
            this.boundAddress = boundTransportAddress;
        } else {
            profileBoundAddresses.put(name, boundTransportAddress);
        }
    }

    protected InetSocketAddress bindToPort(final String name, final InetAddress hostAddress, String port) {
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = portsRange.iterate(portNumber -> {
            try {
                Channel channel = bind(name, new InetSocketAddress(hostAddress, portNumber));
                synchronized (serverChannels) {
                    List<Channel> list = serverChannels.get(name);
                    if (list == null) {
                        list = new ArrayList<>();
                        serverChannels.put(name, list);
                    }
                    list.add(channel);
                    boundSocket.set(getLocalAddress(channel));
                }
            } catch (Exception e) {
                lastException.set(e);
                return false;
            }
            return true;
        });
        if (!success) {
            throw new BindTransportException("Failed to bind to [" + port + "]", lastException.get());
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Bound profile [{}] to address {{}}", name, NetworkAddress.format(boundSocket.get()));
        }
        return boundSocket.get();
    }

    private BoundTransportAddress createBoundTransportAddress(String name, Settings profileSettings, List<InetSocketAddress> boundAddresses) {
        String[] boundAddressesHostStrings = new String[boundAddresses.size()];
        TransportAddress[] transportBoundAddresses = new TransportAddress[boundAddresses.size()];
        for (int i = 0; i < boundAddresses.size(); i++) {
            InetSocketAddress boundAddress = boundAddresses.get(i);
            boundAddressesHostStrings[i] = boundAddress.getHostString();
            transportBoundAddresses[i] = new TransportAddress(boundAddress);
        }
        final String[] publishHosts;
        if (TransportSettings.DEFAULT_PROFILE.equals(name)) {
            publishHosts = TransportSettings.PUBLISH_HOST.get(settings).toArray(Strings.EMPTY_ARRAY);
        } else {
            publishHosts = profileSettings.getAsArray("publish_host", boundAddressesHostStrings);
        }
        final InetAddress publishInetAddress;
        try {
            publishInetAddress = networkService.resolvePublishHostAddresses(publishHosts);
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }
        final int publishPort = resolvePublishPort(name, settings, profileSettings, boundAddresses, publishInetAddress);
        final TransportAddress publishAddress = new TransportAddress(new InetSocketAddress(publishInetAddress, publishPort));
        return new BoundTransportAddress(transportBoundAddresses, publishAddress);
    }

    public static int resolvePublishPort(String profileName, Settings settings, Settings profileSettings, List<InetSocketAddress> boundAddresses, InetAddress publishInetAddress) {
        int publishPort;
        if (TransportSettings.DEFAULT_PROFILE.equals(profileName)) {
            publishPort = TransportSettings.PUBLISH_PORT.get(settings);
        } else {
            publishPort = profileSettings.getAsInt("publish_port", -1);
        }
        if (publishPort < 0) {
            for (InetSocketAddress boundAddress : boundAddresses) {
                InetAddress boundInetAddress = boundAddress.getAddress();
                if (boundInetAddress.isAnyLocalAddress() || boundInetAddress.equals(publishInetAddress)) {
                    publishPort = boundAddress.getPort();
                    break;
                }
            }
        }
        if (publishPort < 0) {
            final IntSet ports = new IntHashSet();
            for (InetSocketAddress boundAddress : boundAddresses) {
                ports.add(boundAddress.getPort());
            }
            if (ports.size() == 1) {
                publishPort = ports.iterator().next().value;
            }
        }
        if (publishPort < 0) {
            String profileExplanation = TransportSettings.DEFAULT_PROFILE.equals(profileName) ? "" : " for profile " + profileName;
            throw new BindTransportException("Failed to auto-resolve publish port" + profileExplanation + ", multiple bound addresses " + boundAddresses + " with distinct ports and none of them matched the publish address (" + publishInetAddress + "). " + "Please specify a unique port by setting " + TransportSettings.PORT.getKey() + " or " + TransportSettings.PUBLISH_PORT.getKey());
        }
        return publishPort;
    }

    @Override
    public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
        return parse(address, settings.get("transport.profiles.default.port", TransportSettings.PORT.get(settings)), perAddressLimit);
    }

    private static final Pattern BRACKET_PATTERN = Pattern.compile("^\\[(.*:.*)\\](?::([\\d\\-]*))?$");

    static TransportAddress[] parse(String hostPortString, String defaultPortRange, int perAddressLimit) throws UnknownHostException {
        Objects.requireNonNull(hostPortString);
        String host;
        String portString = null;
        if (hostPortString.startsWith("[")) {
            Matcher matcher = BRACKET_PATTERN.matcher(hostPortString);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid bracketed host/port range: " + hostPortString);
            }
            host = matcher.group(1);
            portString = matcher.group(2);
        } else {
            int colonPos = hostPortString.indexOf(':');
            if (colonPos >= 0 && hostPortString.indexOf(':', colonPos + 1) == -1) {
                host = hostPortString.substring(0, colonPos);
                portString = hostPortString.substring(colonPos + 1);
            } else {
                host = hostPortString;
                if (colonPos >= 0) {
                    throw new IllegalArgumentException("IPv6 addresses must be bracketed: " + hostPortString);
                }
            }
        }
        if (portString == null || portString.isEmpty()) {
            portString = defaultPortRange;
        }
        Set<InetAddress> addresses = new HashSet<>(Arrays.asList(InetAddress.getAllByName(host)));
        List<TransportAddress> transportAddresses = new ArrayList<>();
        int[] ports = new PortsRange(portString).ports();
        int limit = Math.min(ports.length, perAddressLimit);
        for (int i = 0; i < limit; i++) {
            for (InetAddress address : addresses) {
                transportAddresses.add(new TransportAddress(address, ports[i]));
            }
        }
        return transportAddresses.toArray(new TransportAddress[transportAddresses.size()]);
    }

    @Override
    protected final void doClose() {
    }

    @Override
    protected final void doStop() {
        final CountDownLatch latch = new CountDownLatch(1);
        threadPool.generic().execute(() -> {
            globalLock.writeLock().lock();
            try {
                for (Map.Entry<String, List<Channel>> entry : serverChannels.entrySet()) {
                    try {
                        closeChannels(entry.getValue());
                    } catch (Exception e) {
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage("Error closing serverChannel for profile [{}]", entry.getKey()), e);
                    }
                }
                for (Iterator<NodeChannels> it = connectedNodes.values().iterator(); it.hasNext(); ) {
                    NodeChannels nodeChannels = it.next();
                    it.remove();
                    IOUtils.closeWhileHandlingException(nodeChannels);
                }
                stopInternal();
            } finally {
                globalLock.writeLock().unlock();
                latch.countDown();
            }
        });
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected void onException(Channel channel, Exception e) throws IOException {
        if (!lifecycle.started()) {
            return;
        }
        if (isCloseConnectionException(e)) {
            logger.trace((Supplier<?>) () -> new ParameterizedMessage("close connection exception caught on transport layer [{}], disconnecting from relevant node", channel), e);
            disconnectFromNodeChannel(channel, e);
        } else if (isConnectException(e)) {
            logger.trace((Supplier<?>) () -> new ParameterizedMessage("connect exception caught on transport layer [{}]", channel), e);
            disconnectFromNodeChannel(channel, e);
        } else if (e instanceof BindException) {
            logger.trace((Supplier<?>) () -> new ParameterizedMessage("bind exception caught on transport layer [{}]", channel), e);
            disconnectFromNodeChannel(channel, e);
        } else if (e instanceof CancelledKeyException) {
            logger.trace((Supplier<?>) () -> new ParameterizedMessage("cancelled key exception caught on transport layer [{}], disconnecting from relevant node", channel), e);
            disconnectFromNodeChannel(channel, e);
        } else if (e instanceof TcpTransport.HttpOnTransportException) {
            if (isOpen(channel)) {
                final Runnable closeChannel = () -> {
                    try {
                        closeChannels(Collections.singletonList(channel));
                    } catch (IOException e1) {
                        logger.debug("failed to close httpOnTransport channel", e1);
                    }
                };
                boolean success = false;
                try {
                    sendMessage(channel, new BytesArray(e.getMessage().getBytes(StandardCharsets.UTF_8)), closeChannel);
                    success = true;
                } finally {
                    if (success == false) {
                        closeChannel.run();
                    }
                }
            }
        } else {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("exception caught on transport layer [{}], closing connection", channel), e);
            disconnectFromNodeChannel(channel, e);
        }
    }

    protected abstract InetSocketAddress getLocalAddress(Channel channel);

    protected abstract Channel bind(String name, InetSocketAddress address) throws IOException;

    protected abstract void closeChannels(List<Channel> channel) throws IOException;

    protected abstract void sendMessage(Channel channel, BytesReference reference, Runnable sendListener) throws IOException;

    protected abstract NodeChannels connectToChannels(DiscoveryNode node, ConnectionProfile connectionProfile) throws IOException;

    protected void stopInternal() {
    }

    public boolean canCompress(TransportRequest request) {
        return compress && (!(request instanceof BytesTransportRequest));
    }

    private void sendRequestToChannel(DiscoveryNode node, final Channel targetChannel, final long requestId, final String action, final TransportRequest request, TransportRequestOptions options, Version channelVersion, byte status) throws IOException, TransportException {
        if (compress) {
            options = TransportRequestOptions.builder(options).withCompress(true).build();
        }
        status = TransportStatus.setRequest(status);
        ReleasableBytesStreamOutput bStream = new ReleasableBytesStreamOutput(bigArrays);
        final Releasable toRelease = Releasables.releaseOnce(() -> Releasables.close(bStream.bytes()));
        boolean addedReleaseListener = false;
        StreamOutput stream = bStream;
        try {
            if (options.compress() && canCompress(request)) {
                status = TransportStatus.setCompress(status);
                stream = CompressorFactory.COMPRESSOR.streamOutput(stream);
            }
            Version version = Version.min(getCurrentVersion(), channelVersion);
            stream.setVersion(version);
            threadPool.getThreadContext().writeTo(stream);
            stream.writeString(action);
            BytesReference message = buildMessage(requestId, status, node.getVersion(), request, stream, bStream);
            final TransportRequestOptions finalOptions = options;
            Runnable onRequestSent = () -> {
                try {
                    toRelease.close();
                } finally {
                    transportServiceAdapter.onRequestSent(node, requestId, action, request, finalOptions);
                }
            };
            addedReleaseListener = internalSendMessage(targetChannel, message, onRequestSent);
        } finally {
            IOUtils.close(stream);
            if (!addedReleaseListener) {
                toRelease.close();
            }
        }
    }

    private boolean internalSendMessage(Channel targetChannel, BytesReference message, Runnable onRequestSent) throws IOException {
        boolean success;
        try {
            sendMessage(targetChannel, message, onRequestSent);
            success = true;
        } catch (IOException ex) {
            onException(targetChannel, ex);
            success = false;
        }
        return success;
    }

    public void sendErrorResponse(Version nodeVersion, Channel channel, final Exception error, final long requestId, final String action) throws IOException {
        try (BytesStreamOutput stream = new BytesStreamOutput()) {
            stream.setVersion(nodeVersion);
            RemoteTransportException tx = new RemoteTransportException(nodeName(), new TransportAddress(getLocalAddress(channel)), action, error);
            threadPool.getThreadContext().writeTo(stream);
            stream.writeException(tx);
            byte status = 0;
            status = TransportStatus.setResponse(status);
            status = TransportStatus.setError(status);
            final BytesReference bytes = stream.bytes();
            final BytesReference header = buildHeader(requestId, status, nodeVersion, bytes.length());
            Runnable onRequestSent = () -> transportServiceAdapter.onResponseSent(requestId, action, error);
            sendMessage(channel, new CompositeBytesReference(header, bytes), onRequestSent);
        }
    }

    public void sendResponse(Version nodeVersion, Channel channel, final TransportResponse response, final long requestId, final String action, TransportResponseOptions options) throws IOException {
        sendResponse(nodeVersion, channel, response, requestId, action, options, (byte) 0);
    }

    private void sendResponse(Version nodeVersion, Channel channel, final TransportResponse response, final long requestId, final String action, TransportResponseOptions options, byte status) throws IOException {
        if (compress) {
            options = TransportResponseOptions.builder(options).withCompress(true).build();
        }
        status = TransportStatus.setResponse(status);
        ReleasableBytesStreamOutput bStream = new ReleasableBytesStreamOutput(bigArrays);
        final Releasable toRelease = Releasables.releaseOnce(() -> Releasables.close(bStream.bytes()));
        boolean addedReleaseListener = false;
        StreamOutput stream = bStream;
        try {
            if (options.compress()) {
                status = TransportStatus.setCompress(status);
                stream = CompressorFactory.COMPRESSOR.streamOutput(stream);
            }
            threadPool.getThreadContext().writeTo(stream);
            stream.setVersion(nodeVersion);
            BytesReference reference = buildMessage(requestId, status, nodeVersion, response, stream, bStream);
            final TransportResponseOptions finalOptions = options;
            Runnable onRequestSent = () -> {
                try {
                    toRelease.close();
                } finally {
                    transportServiceAdapter.onResponseSent(requestId, action, response, finalOptions);
                }
            };
            addedReleaseListener = internalSendMessage(channel, reference, onRequestSent);
        } finally {
            try {
                IOUtils.close(stream);
            } finally {
                if (!addedReleaseListener) {
                    toRelease.close();
                }
            }
        }
    }

    final BytesReference buildHeader(long requestId, byte status, Version protocolVersion, int length) throws IOException {
        try (BytesStreamOutput headerOutput = new BytesStreamOutput(TcpHeader.HEADER_SIZE)) {
            headerOutput.setVersion(protocolVersion);
            TcpHeader.writeHeader(headerOutput, requestId, status, protocolVersion, length);
            final BytesReference bytes = headerOutput.bytes();
            assert bytes.length() == TcpHeader.HEADER_SIZE : "header size mismatch expected: " + TcpHeader.HEADER_SIZE + " but was: " + bytes.length();
            return bytes;
        }
    }

    private BytesReference buildMessage(long requestId, byte status, Version nodeVersion, TransportMessage message, StreamOutput stream, ReleasableBytesStream writtenBytes) throws IOException {
        final BytesReference zeroCopyBuffer;
        if (message instanceof BytesTransportRequest) {
            BytesTransportRequest bRequest = (BytesTransportRequest) message;
            assert nodeVersion.equals(bRequest.version());
            bRequest.writeThin(stream);
            zeroCopyBuffer = bRequest.bytes;
        } else {
            message.writeTo(stream);
            zeroCopyBuffer = BytesArray.EMPTY;
        }
        stream.close();
        final BytesReference messageBody = writtenBytes.bytes();
        final BytesReference header = buildHeader(requestId, status, stream.getVersion(), messageBody.length() + zeroCopyBuffer.length());
        return new CompositeBytesReference(header, messageBody, zeroCopyBuffer);
    }

    public static boolean validateMessageHeader(BytesReference buffer) throws IOException {
        final int sizeHeaderLength = TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;
        if (buffer.length() < sizeHeaderLength) {
            throw new IllegalStateException("message size must be >= to the header size");
        }
        int offset = 0;
        if (buffer.get(offset) != 'E' || buffer.get(offset + 1) != 'S') {
            if (bufferStartsWith(buffer, offset, "GET ") || bufferStartsWith(buffer, offset, "POST ") || bufferStartsWith(buffer, offset, "PUT ") || bufferStartsWith(buffer, offset, "HEAD ") || bufferStartsWith(buffer, offset, "DELETE ") || bufferStartsWith(buffer, offset, "OPTIONS ") || bufferStartsWith(buffer, offset, "PATCH ") || bufferStartsWith(buffer, offset, "TRACE ")) {
                throw new HttpOnTransportException("This is not a HTTP port");
            }
            throw new StreamCorruptedException("invalid internal transport message format, got (" + Integer.toHexString(buffer.get(offset) & 0xFF) + "," + Integer.toHexString(buffer.get(offset + 1) & 0xFF) + "," + Integer.toHexString(buffer.get(offset + 2) & 0xFF) + "," + Integer.toHexString(buffer.get(offset + 3) & 0xFF) + ")");
        }
        final int dataLen;
        try (StreamInput input = buffer.streamInput()) {
            input.skip(TcpHeader.MARKER_BYTES_SIZE);
            dataLen = input.readInt();
            if (dataLen == PING_DATA_SIZE) {
                return false;
            }
        }
        if (dataLen <= 0) {
            throw new StreamCorruptedException("invalid data length: " + dataLen);
        }
        if (dataLen > NINETY_PER_HEAP_SIZE) {
            throw new IllegalArgumentException("transport content length received [" + new ByteSizeValue(dataLen) + "] exceeded [" + new ByteSizeValue(NINETY_PER_HEAP_SIZE) + "]");
        }
        if (buffer.length() < dataLen + sizeHeaderLength) {
            throw new IllegalStateException("buffer must be >= to the message size but wasn't");
        }
        return true;
    }

    private static boolean bufferStartsWith(BytesReference buffer, int offset, String method) {
        char[] chars = method.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (buffer.get(offset + i) != chars[i]) {
                return false;
            }
        }
        return true;
    }

    public static class HttpOnTransportException extends ElasticsearchException {

        public HttpOnTransportException(String msg) {
            super(msg);
        }

        @Override
        public RestStatus status() {
            return RestStatus.BAD_REQUEST;
        }

        public HttpOnTransportException(StreamInput in) throws IOException {
            super(in);
        }
    }

    protected abstract boolean isOpen(Channel channel);

    public final void messageReceived(BytesReference reference, Channel channel, String profileName, InetSocketAddress remoteAddress, int messageLengthBytes) throws IOException {
        final int totalMessageSize = messageLengthBytes + TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;
        transportServiceAdapter.addBytesReceived(totalMessageSize);
        boolean hasMessageBytesToRead = (totalMessageSize - TcpHeader.HEADER_SIZE) > 0;
        StreamInput streamIn = reference.streamInput();
        boolean success = false;
        try (ThreadContext.StoredContext tCtx = threadPool.getThreadContext().stashContext()) {
            long requestId = streamIn.readLong();
            byte status = streamIn.readByte();
            Version version = Version.fromId(streamIn.readInt());
            if (TransportStatus.isCompress(status) && hasMessageBytesToRead && streamIn.available() > 0) {
                Compressor compressor;
                try {
                    final int bytesConsumed = TcpHeader.REQUEST_ID_SIZE + TcpHeader.STATUS_SIZE + TcpHeader.VERSION_ID_SIZE;
                    compressor = CompressorFactory.compressor(reference.slice(bytesConsumed, reference.length() - bytesConsumed));
                } catch (NotCompressedException ex) {
                    int maxToRead = Math.min(reference.length(), 10);
                    StringBuilder sb = new StringBuilder("stream marked as compressed, but no compressor found, first [").append(maxToRead).append("] content bytes out of [").append(reference.length()).append("] readable bytes with message size [").append(messageLengthBytes).append("] ").append("] are [");
                    for (int i = 0; i < maxToRead; i++) {
                        sb.append(reference.get(i)).append(",");
                    }
                    sb.append("]");
                    throw new IllegalStateException(sb.toString());
                }
                streamIn = compressor.streamInput(streamIn);
            }
            if (version.isCompatible(getCurrentVersion()) == false) {
                throw new IllegalStateException("Received message from unsupported version: [" + version + "] minimal compatible version is: [" + getCurrentVersion().minimumCompatibilityVersion() + "]");
            }
            streamIn = new NamedWriteableAwareStreamInput(streamIn, namedWriteableRegistry);
            streamIn.setVersion(version);
            threadPool.getThreadContext().readHeaders(streamIn);
            if (TransportStatus.isRequest(status)) {
                handleRequest(channel, profileName, streamIn, requestId, messageLengthBytes, version, remoteAddress, status);
            } else {
                final TransportResponseHandler<?> handler;
                if (TransportStatus.isHandshake(status)) {
                    handler = pendingHandshakes.remove(requestId);
                } else {
                    TransportResponseHandler theHandler = transportServiceAdapter.onResponseReceived(requestId);
                    if (theHandler == null && TransportStatus.isError(status)) {
                        handler = pendingHandshakes.remove(requestId);
                    } else {
                        handler = theHandler;
                    }
                }
                if (handler != null) {
                    if (TransportStatus.isError(status)) {
                        handlerResponseError(streamIn, handler);
                    } else {
                        handleResponse(remoteAddress, streamIn, handler);
                    }
                    final int nextByte = streamIn.read();
                    if (nextByte != -1) {
                        throw new IllegalStateException("Message not fully read (response) for requestId [" + requestId + "], handler [" + handler + "], error [" + TransportStatus.isError(status) + "]; resetting");
                    }
                }
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(streamIn);
            } else {
                IOUtils.closeWhileHandlingException(streamIn);
            }
        }
    }

    private void handleResponse(InetSocketAddress remoteAddress, final StreamInput stream, final TransportResponseHandler handler) {
        final TransportResponse response = handler.newInstance();
        response.remoteAddress(new TransportAddress(remoteAddress));
        try {
            response.readFrom(stream);
        } catch (Exception e) {
            handleException(handler, new TransportSerializationException("Failed to deserialize response of type [" + response.getClass().getName() + "]", e));
            return;
        }
        threadPool.executor(handler.executor()).execute(new AbstractRunnable() {

            @Override
            public void onFailure(Exception e) {
                handleException(handler, new ResponseHandlerFailureTransportException(e));
            }

            @Override
            protected void doRun() throws Exception {
                handler.handleResponse(response);
            }
        });
    }

    private void handlerResponseError(StreamInput stream, final TransportResponseHandler handler) {
        Exception error;
        try {
            error = stream.readException();
        } catch (Exception e) {
            error = new TransportSerializationException("Failed to deserialize exception response from stream", e);
        }
        handleException(handler, error);
    }

    private void handleException(final TransportResponseHandler handler, Throwable error) {
        if (!(error instanceof RemoteTransportException)) {
            error = new RemoteTransportException(error.getMessage(), error);
        }
        final RemoteTransportException rtx = (RemoteTransportException) error;
        threadPool.executor(handler.executor()).execute(() -> {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to handle exception response [{}]", handler), e);
            }
        });
    }

    protected String handleRequest(Channel channel, String profileName, final StreamInput stream, long requestId, int messageLengthBytes, Version version, InetSocketAddress remoteAddress, byte status) throws IOException {
        final String action = stream.readString();
        transportServiceAdapter.onRequestReceived(requestId, action);
        TransportChannel transportChannel = null;
        try {
            if (TransportStatus.isHandshake(status)) {
                final VersionHandshakeResponse response = new VersionHandshakeResponse(getCurrentVersion());
                sendResponse(version, channel, response, requestId, HANDSHAKE_ACTION_NAME, TransportResponseOptions.EMPTY, TransportStatus.setHandshake((byte) 0));
            } else {
                final RequestHandlerRegistry reg = transportServiceAdapter.getRequestHandler(action);
                if (reg == null) {
                    throw new ActionNotFoundTransportException(action);
                }
                if (reg.canTripCircuitBreaker()) {
                    getInFlightRequestBreaker().addEstimateBytesAndMaybeBreak(messageLengthBytes, "<transport_request>");
                } else {
                    getInFlightRequestBreaker().addWithoutBreaking(messageLengthBytes);
                }
                transportChannel = new TcpTransportChannel<>(this, channel, transportName, action, requestId, version, profileName, messageLengthBytes);
                final TransportRequest request = reg.newRequest();
                request.remoteAddress(new TransportAddress(remoteAddress));
                request.readFrom(stream);
                validateRequest(stream, requestId, action);
                threadPool.executor(reg.getExecutor()).execute(new RequestHandler(reg, request, transportChannel));
            }
        } catch (Exception e) {
            if (transportChannel == null) {
                transportChannel = new TcpTransportChannel<>(this, channel, transportName, action, requestId, version, profileName, 0);
            }
            try {
                transportChannel.sendResponse(e);
            } catch (IOException inner) {
                inner.addSuppressed(e);
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Failed to send error message back to client for action [{}]", action), inner);
            }
        }
        return action;
    }

    protected void validateRequest(StreamInput stream, long requestId, String action) throws IOException {
        final int nextByte = stream.read();
        if (nextByte != -1) {
            throw new IllegalStateException("Message not fully read (request) for requestId [" + requestId + "], action [" + action + "], available [" + stream.available() + "]; resetting");
        }
    }

    class RequestHandler extends AbstractRunnable {

        private final RequestHandlerRegistry reg;

        private final TransportRequest request;

        private final TransportChannel transportChannel;

        public RequestHandler(RequestHandlerRegistry reg, TransportRequest request, TransportChannel transportChannel) {
            this.reg = reg;
            this.request = request;
            this.transportChannel = transportChannel;
        }

        @SuppressWarnings({ "unchecked" })
        @Override
        protected void doRun() throws Exception {
            reg.processMessageReceived(request, transportChannel);
        }

        @Override
        public boolean isForceExecution() {
            return reg.isForceExecution();
        }

        @Override
        public void onFailure(Exception e) {
            if (lifecycleState() == Lifecycle.State.STARTED) {
                try {
                    transportChannel.sendResponse(e);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("Failed to send error message back to client for action [{}]", reg.getAction()), inner);
                }
            }
        }
    }

    private static final class VersionHandshakeResponse extends TransportResponse {

        private Version version;

        private VersionHandshakeResponse(Version version) {
            this.version = version;
        }

        private VersionHandshakeResponse() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            version = Version.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            assert version != null;
            Version.writeVersion(version, out);
        }
    }

    protected Version executeHandshake(DiscoveryNode node, Channel channel, TimeValue timeout) throws IOException, InterruptedException {
        numHandshakes.inc();
        final long requestId = newRequestId();
        final HandshakeResponseHandler handler = new HandshakeResponseHandler(channel);
        AtomicReference<Version> versionRef = handler.versionRef;
        AtomicReference<Exception> exceptionRef = handler.exceptionRef;
        pendingHandshakes.put(requestId, handler);
        boolean success = false;
        try {
            if (isOpen(channel) == false) {
                throw new IllegalStateException("handshake failed, channel already closed");
            }
            final Version minCompatVersion = getCurrentVersion().minimumCompatibilityVersion();
            sendRequestToChannel(node, channel, requestId, HANDSHAKE_ACTION_NAME, TransportRequest.Empty.INSTANCE, TransportRequestOptions.EMPTY, minCompatVersion, TransportStatus.setHandshake((byte) 0));
            if (handler.latch.await(timeout.millis(), TimeUnit.MILLISECONDS) == false) {
                throw new ConnectTransportException(node, "handshake_timeout[" + timeout + "]");
            }
            success = true;
            if (exceptionRef.get() != null) {
                throw new IllegalStateException("handshake failed", exceptionRef.get());
            } else {
                Version version = versionRef.get();
                if (getCurrentVersion().isCompatible(version) == false) {
                    throw new IllegalStateException("Received message from unsupported version: [" + version + "] minimal compatible version is: [" + getCurrentVersion().minimumCompatibilityVersion() + "]");
                }
                return version;
            }
        } finally {
            final TransportResponseHandler<?> removedHandler = pendingHandshakes.remove(requestId);
            assert success && removedHandler == null || success == false : "handler for requestId [" + requestId + "] is not been removed";
        }
    }

    final int getNumPendingHandshakes() {
        return pendingHandshakes.size();
    }

    final long getNumHandshakes() {
        return numHandshakes.count();
    }

    @Override
    public long newRequestId() {
        return requestIdGenerator.incrementAndGet();
    }

    protected final void onChannelClosed(Channel channel) {
        final Optional<Long> first = pendingHandshakes.entrySet().stream().filter((entry) -> entry.getValue().channel == channel).map((e) -> e.getKey()).findFirst();
        if (first.isPresent()) {
            final Long requestId = first.get();
            final HandshakeResponseHandler handler = pendingHandshakes.remove(requestId);
            if (handler != null) {
                handler.handleException(new TransportException("connection reset"));
            }
        }
    }
}