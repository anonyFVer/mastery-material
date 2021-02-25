package org.apache.dubbo.remoting.etcd.jetcd;

import io.etcd.jetcd.kv.PutResponse;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.etcd.RetryPolicy;
import org.apache.dubbo.remoting.etcd.StateListener;
import org.apache.dubbo.remoting.etcd.option.Constants;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.CloseableClient;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Observers;
import io.etcd.jetcd.common.exception.ErrorCode;
import io.etcd.jetcd.common.exception.EtcdException;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import static java.util.stream.Collectors.toList;

public class JEtcdClientWrapper {

    private Logger logger = LoggerFactory.getLogger(JEtcdClientWrapper.class);

    private final URL url;

    private volatile Client client;

    private volatile boolean started = false;

    private volatile boolean connectState = false;

    private ScheduledFuture future;

    private ScheduledExecutorService reconnectNotify;

    private AtomicReference<ManagedChannel> channel;

    private ConnectionStateListener connectionStateListener;

    private long expirePeriod;

    private CompletableFuture<Client> completableFuture;

    private RetryPolicy retryPolicy;

    private RuntimeException failed;

    private final ScheduledFuture<?> retryFuture;

    private final ScheduledExecutorService retryExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("Etcd3RegistryKeepAliveFailedRetryTimer", true));

    private final Set<String> failedRegistered = new ConcurrentHashSet<String>();

    private final Set<String> registeredPaths = new ConcurrentHashSet<>();

    private volatile CloseableClient keepAlive = null;

    private volatile long globalLeaseId;

    private volatile boolean cancelKeepAlive = false;

    public static final Charset UTF_8 = Charset.forName("UTF-8");

    public JEtcdClientWrapper(URL url) {
        this.url = url;
        this.expirePeriod = url.getParameter(Constants.SESSION_TIMEOUT_KEY, Constants.DEFAULT_KEEPALIVE_TIMEOUT) / 1000;
        if (expirePeriod <= 0) {
            this.expirePeriod = Constants.DEFAULT_KEEPALIVE_TIMEOUT / 1000;
        }
        this.channel = new AtomicReference<>();
        this.completableFuture = CompletableFuture.supplyAsync(() -> prepareClient(url));
        this.reconnectNotify = Executors.newScheduledThreadPool(1, new NamedThreadFactory("reconnectNotify", true));
        this.retryPolicy = new RetryNTimes(1, 1000, TimeUnit.MILLISECONDS);
        this.failed = new IllegalStateException("Etcd3 registry is not connected yet, url:" + url);
        int retryPeriod = url.getParameter(Constants.REGISTRY_RETRY_PERIOD_KEY, Constants.DEFAULT_REGISTRY_RETRY_PERIOD);
        this.retryFuture = retryExecutor.scheduleWithFixedDelay(() -> {
            try {
                retry();
            } catch (Throwable t) {
                logger.error("Unexpected error occur at failed retry, cause: " + t.getMessage(), t);
            }
        }, retryPeriod, retryPeriod, TimeUnit.MILLISECONDS);
    }

    private Client prepareClient(URL url) {
        int maxInboundSize = DEFAULT_INBOUND_SIZE;
        if (StringUtils.isNotEmpty(System.getProperty(GRPC_MAX_INBOUND_SIZE_KEY))) {
            maxInboundSize = Integer.valueOf(System.getProperty(GRPC_MAX_INBOUND_SIZE_KEY));
        }
        ClientBuilder clientBuilder = Client.builder().loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance()).endpoints(endPoints(url.getBackupAddress())).maxInboundMessageSize(maxInboundSize);
        return clientBuilder.build();
    }

    public Client getClient() {
        return client;
    }

    public ManagedChannel getChannel() {
        if (channel.get() == null || (channel.get().isShutdown() || channel.get().isTerminated())) {
            channel.set(newChannel(client));
        }
        return channel.get();
    }

    public List<String> getChildren(String path) {
        try {
            return RetryLoops.invokeWithRetry(() -> {
                requiredNotNull(client, failed);
                int len = path.length();
                return client.getKVClient().get(ByteSequence.from(path, UTF_8), GetOption.newBuilder().withPrefix(ByteSequence.from(path, UTF_8)).build()).get(DEFAULT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS).getKvs().stream().parallel().filter(pair -> {
                    String key = pair.getKey().toString(UTF_8);
                    int index = len, count = 0;
                    if (key.length() > len) {
                        for (; (index = key.indexOf(Constants.PATH_SEPARATOR, index)) != -1; ++index) {
                            if (count++ > 1)
                                break;
                        }
                    }
                    return count == 1;
                }).map(pair -> pair.getKey().toString(UTF_8)).collect(toList());
            }, retryPolicy);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public boolean isConnected() {
        return ConnectivityState.READY == (getChannel().getState(false)) || ConnectivityState.IDLE == (getChannel().getState(false));
    }

    public long createLease(long second) {
        try {
            return RetryLoops.invokeWithRetry(() -> {
                requiredNotNull(client, failed);
                return client.getLeaseClient().grant(second).get(DEFAULT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS).getID();
            }, retryPolicy);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void revokeLease(long lease) {
        try {
            RetryLoops.invokeWithRetry((Callable<Void>) () -> {
                requiredNotNull(client, failed);
                client.getLeaseClient().revoke(lease).get(DEFAULT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
                return null;
            }, retryPolicy);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public long createLease(long ttl, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (timeout <= 0) {
            return createLease(ttl);
        }
        requiredNotNull(client, failed);
        return client.getLeaseClient().grant(ttl).get(timeout, unit).getID();
    }

    public boolean checkExists(String path) {
        try {
            return RetryLoops.invokeWithRetry(() -> {
                requiredNotNull(client, failed);
                return client.getKVClient().get(ByteSequence.from(path, UTF_8), GetOption.newBuilder().withCountOnly(true).build()).get(DEFAULT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS).getCount() > 0;
            }, retryPolicy);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    protected Long find(String path) {
        try {
            return RetryLoops.invokeWithRetry(() -> {
                requiredNotNull(client, failed);
                return client.getKVClient().get(ByteSequence.from(path, UTF_8)).get(DEFAULT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS).getKvs().stream().mapToLong(keyValue -> Long.valueOf(keyValue.getValue().toString(UTF_8))).findFirst().getAsLong();
            }, retryPolicy);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void createPersistent(String path) {
        try {
            RetryLoops.invokeWithRetry((Callable<Void>) () -> {
                requiredNotNull(client, failed);
                client.getKVClient().put(ByteSequence.from(path, UTF_8), ByteSequence.from(String.valueOf(path.hashCode()), UTF_8)).get(DEFAULT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
                return null;
            }, retryPolicy);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public long createEphemeral(String path) {
        try {
            return RetryLoops.invokeWithRetry(() -> {
                requiredNotNull(client, failed);
                registeredPaths.add(path);
                keepAlive();
                final long leaseId = globalLeaseId;
                client.getKVClient().put(ByteSequence.from(path, UTF_8), ByteSequence.from(String.valueOf(leaseId), UTF_8), PutOption.newBuilder().withLeaseId(leaseId).build()).get(DEFAULT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
                return leaseId;
            }, retryPolicy);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void keepAlive(long lease) {
        this.keepAlive(lease, null);
    }

    @SuppressWarnings("unchecked")
    private <T> void keepAlive(long lease, Consumer<T> onFailed) {
        final StreamObserver<LeaseKeepAliveResponse> observer = new Observers.Builder().onError((e) -> {
            if (e instanceof EtcdException) {
                EtcdException error = (EtcdException) e;
                if (error.getErrorCode() == ErrorCode.NOT_FOUND) {
                    keepAlive0(onFailed);
                }
            }
        }).onCompleted(() -> {
            keepAlive0(onFailed);
        }).build();
        cancelKeepAlive();
        this.keepAlive = client.getLeaseClient().keepAlive(lease, observer);
    }

    private void keepAlive() throws Exception {
        if (keepAlive == null) {
            synchronized (this) {
                if (keepAlive == null) {
                    this.globalLeaseId = client.getLeaseClient().grant(expirePeriod).get(DEFAULT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS).getID();
                    keepAlive(globalLeaseId, (NULL) -> recovery());
                }
            }
        }
    }

    private <T> void keepAlive0(Consumer<T> onFailed) {
        if (onFailed != null) {
            long leaseId = globalLeaseId;
            try {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to keep alive for global lease '" + leaseId + "', waiting for retry again.");
                }
                onFailed.accept(null);
            } catch (Exception ignored) {
                logger.warn("Failed to recover from global lease expired or lease deadline exceeded. lease '" + leaseId + "'", ignored);
            }
        }
    }

    private void recovery() {
        try {
            if (cancelKeepAlive)
                return;
            cancelKeepAlive();
            Set<String> ephemeralPaths = new HashSet<String>(registeredPaths);
            if (!ephemeralPaths.isEmpty()) {
                for (String path : ephemeralPaths) {
                    try {
                        if (cancelKeepAlive)
                            return;
                        createEphemeral(path);
                        failedRegistered.remove(path);
                    } catch (Exception e) {
                        failedRegistered.add(path);
                        Status status = Status.fromThrowable(e);
                        if (status.getCode() == Status.Code.NOT_FOUND) {
                            cancelKeepAlive();
                        }
                    }
                }
            }
        } catch (Throwable t) {
            logger.warn("Unexpected error, failed to recover from global lease expired or deadline exceeded.", t);
        }
    }

    public void delete(String path) {
        try {
            RetryLoops.invokeWithRetry((Callable<Void>) () -> {
                requiredNotNull(client, failed);
                client.getKVClient().delete(ByteSequence.from(path, UTF_8)).get(DEFAULT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
                registeredPaths.remove(path);
                return null;
            }, retryPolicy);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            failedRegistered.remove(path);
        }
    }

    public String[] endPoints(String backupAddress) {
        String[] endpoints = backupAddress.split(Constants.COMMA_SEPARATOR);
        List<String> addresses = Arrays.stream(endpoints).map(address -> address.contains(Constants.HTTP_SUBFIX_KEY) ? address : Constants.HTTP_KEY + address).collect(toList());
        Collections.shuffle(addresses);
        return addresses.toArray(new String[0]);
    }

    public void start() {
        if (!started) {
            try {
                this.client = completableFuture.get(expirePeriod, TimeUnit.SECONDS);
                this.connectState = isConnected();
                this.started = true;
            } catch (Throwable t) {
                logger.error("Timeout! etcd3 server can not be connected in : " + expirePeriod + " seconds! url: " + url, t);
                completableFuture.whenComplete((c, e) -> {
                    this.client = c;
                    if (e != null) {
                        logger.error("Got an exception when trying to create etcd3 instance, can not connect to etcd3 server, url: " + url, e);
                    }
                });
            }
            try {
                this.future = reconnectNotify.scheduleWithFixedDelay(() -> {
                    boolean connected = isConnected();
                    if (connectState != connected) {
                        int notifyState = connected ? StateListener.CONNECTED : StateListener.DISCONNECTED;
                        if (connectionStateListener != null) {
                            try {
                                if (connected) {
                                    clearKeepAlive();
                                }
                                connectionStateListener.stateChanged(getClient(), notifyState);
                            } finally {
                                cancelKeepAlive = false;
                            }
                        }
                        connectState = connected;
                    }
                }, Constants.DEFAULT_REGISTRY_RECONNECT_PERIOD, Constants.DEFAULT_REGISTRY_RECONNECT_PERIOD, TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                logger.error("monitor reconnect status failed.", t);
            }
        }
    }

    private void cancelKeepAlive() {
        try {
            if (keepAlive != null) {
                keepAlive.close();
            }
        } finally {
            keepAlive = null;
        }
    }

    private void clearKeepAlive() {
        cancelKeepAlive = true;
        failedRegistered.clear();
        cancelKeepAlive();
    }

    protected void doClose() {
        try {
            cancelKeepAlive = true;
            if (globalLeaseId > 0) {
                revokeLease(this.globalLeaseId);
            }
        } catch (Exception e) {
            logger.warn("revoke global lease '" + globalLeaseId + "' failed, registry: " + url, e);
        }
        try {
            if (started && future != null) {
                started = false;
                future.cancel(true);
                reconnectNotify.shutdownNow();
            }
        } catch (Exception e) {
            logger.warn("stop reconnect Notify failed, registry: " + url, e);
        }
        try {
            retryFuture.cancel(true);
            retryExecutor.shutdownNow();
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        if (getClient() != null)
            getClient().close();
    }

    private ManagedChannel newChannel(Client client) {
        try {
            Field connectionField = client.getClass().getDeclaredField("connectionManager");
            if (!connectionField.isAccessible()) {
                connectionField.setAccessible(true);
            }
            Object connection = connectionField.get(client);
            Method channel = connection.getClass().getDeclaredMethod("getChannel");
            if (!channel.isAccessible()) {
                channel.setAccessible(true);
            }
            return (ManagedChannel) channel.invoke(connection);
        } catch (Exception e) {
            throw new RuntimeException("Failed to obtain connection channel from " + url.getBackupAddress(), e);
        }
    }

    public ConnectionStateListener getConnectionStateListener() {
        return connectionStateListener;
    }

    public void setConnectionStateListener(ConnectionStateListener connectionStateListener) {
        this.connectionStateListener = connectionStateListener;
    }

    public static void requiredNotNull(Object obj, RuntimeException exeception) {
        if (obj == null) {
            throw exeception;
        }
    }

    public String getKVValue(String key) {
        if (null == key) {
            return null;
        }
        CompletableFuture<GetResponse> responseFuture = this.client.getKVClient().get(ByteSequence.from(key, UTF_8));
        try {
            List<KeyValue> result = responseFuture.get(DEFAULT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS).getKvs();
            if (!result.isEmpty()) {
                return result.get(0).getValue().toString(UTF_8);
            }
        } catch (Exception e) {
        }
        return null;
    }

    public boolean put(String key, String value) {
        if (key == null || value == null) {
            return false;
        }
        CompletableFuture<PutResponse> putFuture = this.client.getKVClient().put(ByteSequence.from(key, UTF_8), ByteSequence.from(value, UTF_8));
        try {
            putFuture.get(DEFAULT_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
        }
        return false;
    }

    private void retry() {
        if (!failedRegistered.isEmpty()) {
            Set<String> failed = new HashSet<String>(failedRegistered);
            if (!failed.isEmpty()) {
                if (cancelKeepAlive)
                    return;
                if (logger.isWarnEnabled()) {
                    logger.warn("Retry failed register(keep alive) for path '" + failed + "', path size: " + failed.size());
                }
                try {
                    for (String path : failed) {
                        try {
                            if (cancelKeepAlive)
                                return;
                            createEphemeral(path);
                            failedRegistered.remove(path);
                        } catch (Throwable e) {
                            failedRegistered.add(path);
                            Status status = Status.fromThrowable(e);
                            if (status.getCode() == Status.Code.NOT_FOUND) {
                                cancelKeepAlive();
                            }
                            logger.warn("Failed to retry register(keep alive) for path '" + path + "', waiting for again, cause: " + e.getMessage(), e);
                        }
                    }
                } catch (Throwable t) {
                    logger.warn("Failed to retry register(keep alive) for path '" + failed + "', waiting for again, cause: " + t.getMessage(), t);
                }
            }
        }
    }

    public static final long DEFAULT_REQUEST_TIMEOUT = obtainRequestTimeout();

    public static final int DEFAULT_INBOUND_SIZE = 100 * 1024 * 1024;

    public static final String GRPC_MAX_INBOUND_SIZE_KEY = "grpc.max.inbound.size";

    public static final String ETCD_REQUEST_TIMEOUT_KEY = "etcd.request.timeout";

    private static int obtainRequestTimeout() {
        if (StringUtils.isNotEmpty(System.getProperty(ETCD_REQUEST_TIMEOUT_KEY))) {
            return Integer.valueOf(System.getProperty(ETCD_REQUEST_TIMEOUT_KEY));
        }
        return 10 * 1000;
    }
}