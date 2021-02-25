package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class MasterFaultDetection extends FaultDetection {

    private static final Logger logger = LogManager.getLogger(MasterFaultDetection.class);

    public static final String MASTER_PING_ACTION_NAME = "internal:discovery/zen/fd/master_ping";

    public interface Listener {

        void onMasterFailure(DiscoveryNode masterNode, Throwable cause, String reason);
    }

    private final MasterService masterService;

    private final java.util.function.Supplier<ClusterState> clusterStateSupplier;

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

    private volatile MasterPinger masterPinger;

    private final Object masterNodeMutex = new Object();

    private volatile DiscoveryNode masterNode;

    private volatile int retryCount;

    private final AtomicBoolean notifiedMasterFailure = new AtomicBoolean();

    public MasterFaultDetection(Settings settings, ThreadPool threadPool, TransportService transportService, java.util.function.Supplier<ClusterState> clusterStateSupplier, MasterService masterService, ClusterName clusterName) {
        super(settings, threadPool, transportService, clusterName);
        this.clusterStateSupplier = clusterStateSupplier;
        this.masterService = masterService;
        logger.debug("[master] uses ping_interval [{}], ping_timeout [{}], ping_retries [{}]", pingInterval, pingRetryTimeout, pingRetryCount);
        transportService.registerRequestHandler(MASTER_PING_ACTION_NAME, MasterPingRequest::new, ThreadPool.Names.SAME, false, false, new MasterPingRequestHandler());
    }

    public DiscoveryNode masterNode() {
        return this.masterNode;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    public void restart(DiscoveryNode masterNode, String reason) {
        synchronized (masterNodeMutex) {
            if (logger.isDebugEnabled()) {
                logger.debug("[master] restarting fault detection against master [{}], reason [{}]", masterNode, reason);
            }
            innerStop();
            innerStart(masterNode);
        }
    }

    private void innerStart(final DiscoveryNode masterNode) {
        this.masterNode = masterNode;
        this.retryCount = 0;
        this.notifiedMasterFailure.set(false);
        if (masterPinger != null) {
            masterPinger.stop();
        }
        this.masterPinger = new MasterPinger();
        threadPool.schedule(pingInterval, ThreadPool.Names.SAME, masterPinger);
    }

    public void stop(String reason) {
        synchronized (masterNodeMutex) {
            if (masterNode != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("[master] stopping fault detection against master [{}], reason [{}]", masterNode, reason);
                }
            }
            innerStop();
        }
    }

    private void innerStop() {
        this.retryCount = 0;
        if (masterPinger != null) {
            masterPinger.stop();
            masterPinger = null;
        }
        this.masterNode = null;
    }

    @Override
    public void close() {
        super.close();
        stop("closing");
        this.listeners.clear();
    }

    @Override
    protected void handleTransportDisconnect(DiscoveryNode node) {
        synchronized (masterNodeMutex) {
            if (!node.equals(this.masterNode)) {
                return;
            }
            if (connectOnNetworkDisconnect) {
                try {
                    transportService.connectToNode(node);
                    if (masterPinger != null) {
                        masterPinger.stop();
                    }
                    this.masterPinger = new MasterPinger();
                    threadPool.schedule(TimeValue.timeValueMillis(0), ThreadPool.Names.SAME, masterPinger);
                } catch (Exception e) {
                    logger.trace("[master] [{}] transport disconnected (with verified connect)", masterNode);
                    notifyMasterFailure(masterNode, null, "transport disconnected (with verified connect)");
                }
            } else {
                logger.trace("[master] [{}] transport disconnected", node);
                notifyMasterFailure(node, null, "transport disconnected");
            }
        }
    }

    private void notifyMasterFailure(final DiscoveryNode masterNode, final Throwable cause, final String reason) {
        if (notifiedMasterFailure.compareAndSet(false, true)) {
            try {
                threadPool.generic().execute(() -> {
                    for (Listener listener : listeners) {
                        listener.onMasterFailure(masterNode, cause, reason);
                    }
                });
            } catch (EsRejectedExecutionException e) {
                logger.error("master failure notification was rejected, it's highly likely the node is shutting down", e);
            }
            stop("master failure, " + reason);
        }
    }

    private class MasterPinger implements Runnable {

        private volatile boolean running = true;

        public void stop() {
            this.running = false;
        }

        @Override
        public void run() {
            if (!running) {
                return;
            }
            final DiscoveryNode masterToPing = masterNode;
            if (masterToPing == null) {
                threadPool.schedule(pingInterval, ThreadPool.Names.SAME, MasterPinger.this);
                return;
            }
            final MasterPingRequest request = new MasterPingRequest(clusterStateSupplier.get().nodes().getLocalNode(), masterToPing, clusterName);
            final TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.PING).withTimeout(pingRetryTimeout).build();
            transportService.sendRequest(masterToPing, MASTER_PING_ACTION_NAME, request, options, new TransportResponseHandler<MasterPingResponseResponse>() {

                @Override
                public MasterPingResponseResponse read(StreamInput in) throws IOException {
                    return new MasterPingResponseResponse(in);
                }

                @Override
                public void handleResponse(MasterPingResponseResponse response) {
                    if (!running) {
                        return;
                    }
                    MasterFaultDetection.this.retryCount = 0;
                    if (masterToPing.equals(MasterFaultDetection.this.masterNode())) {
                        threadPool.schedule(pingInterval, ThreadPool.Names.SAME, MasterPinger.this);
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    if (!running) {
                        return;
                    }
                    synchronized (masterNodeMutex) {
                        if (masterToPing.equals(MasterFaultDetection.this.masterNode())) {
                            if (exp instanceof ConnectTransportException || exp.getCause() instanceof ConnectTransportException) {
                                handleTransportDisconnect(masterToPing);
                                return;
                            } else if (exp.getCause() instanceof NotMasterException) {
                                logger.debug("[master] pinging a master {} that is no longer a master", masterNode);
                                notifyMasterFailure(masterToPing, exp, "no longer master");
                                return;
                            } else if (exp.getCause() instanceof ThisIsNotTheMasterYouAreLookingForException) {
                                logger.debug("[master] pinging a master {} that is not the master", masterNode);
                                notifyMasterFailure(masterToPing, exp, "not master");
                                return;
                            } else if (exp.getCause() instanceof NodeDoesNotExistOnMasterException) {
                                logger.debug("[master] pinging a master {} but we do not exists on it, act as if its master failure", masterNode);
                                notifyMasterFailure(masterToPing, exp, "do not exists on master, act as master failure");
                                return;
                            }
                            int retryCount = ++MasterFaultDetection.this.retryCount;
                            logger.trace(() -> new ParameterizedMessage("[master] failed to ping [{}], retry [{}] out of [{}]", masterNode, retryCount, pingRetryCount), exp);
                            if (retryCount >= pingRetryCount) {
                                logger.debug("[master] failed to ping [{}], tried [{}] times, each with maximum [{}] timeout", masterNode, pingRetryCount, pingRetryTimeout);
                                notifyMasterFailure(masterToPing, null, "failed to ping, tried [" + pingRetryCount + "] times, each with  maximum [" + pingRetryTimeout + "] timeout");
                            } else {
                                transportService.sendRequest(masterToPing, MASTER_PING_ACTION_NAME, request, options, this);
                            }
                        }
                    }
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    static class ThisIsNotTheMasterYouAreLookingForException extends IllegalStateException {

        ThisIsNotTheMasterYouAreLookingForException(String msg) {
            super(msg);
        }

        ThisIsNotTheMasterYouAreLookingForException() {
        }

        @Override
        public Throwable fillInStackTrace() {
            return null;
        }
    }

    static class NodeDoesNotExistOnMasterException extends IllegalStateException {

        @Override
        public Throwable fillInStackTrace() {
            return null;
        }
    }

    private class MasterPingRequestHandler implements TransportRequestHandler<MasterPingRequest> {

        @Override
        public void messageReceived(final MasterPingRequest request, final TransportChannel channel, Task task) throws Exception {
            final DiscoveryNodes nodes = clusterStateSupplier.get().nodes();
            if (!request.masterNode.equals(nodes.getLocalNode())) {
                throw new ThisIsNotTheMasterYouAreLookingForException();
            }
            if (request.clusterName != null && !request.clusterName.equals(clusterName)) {
                logger.trace("master fault detection ping request is targeted for a different [{}] cluster then us [{}]", request.clusterName, clusterName);
                throw new ThisIsNotTheMasterYouAreLookingForException("master fault detection ping request is targeted for a different [" + request.clusterName + "] cluster then us [" + clusterName + "]");
            }
            if (!nodes.isLocalNodeElectedMaster() || !nodes.nodeExists(request.sourceNode)) {
                logger.trace("checking ping from {} under a cluster state thread", request.sourceNode);
                masterService.submitStateUpdateTask("master ping (from: " + request.sourceNode + ")", new ClusterStateUpdateTask() {

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        DiscoveryNodes nodes = currentState.nodes();
                        if (!nodes.nodeExists(request.sourceNode)) {
                            throw new NodeDoesNotExistOnMasterException();
                        }
                        return currentState;
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        onFailure(source, new NotMasterException("local node is not master"));
                    }

                    @Override
                    public void onFailure(String source, @Nullable Exception e) {
                        if (e == null) {
                            e = new ElasticsearchException("unknown error while processing ping");
                        }
                        try {
                            channel.sendResponse(e);
                        } catch (IOException inner) {
                            inner.addSuppressed(e);
                            logger.warn("error while sending ping response", inner);
                        }
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        try {
                            channel.sendResponse(new MasterPingResponseResponse());
                        } catch (IOException e) {
                            logger.warn("error while sending ping response", e);
                        }
                    }
                });
            } else {
                channel.sendResponse(new MasterPingResponseResponse());
            }
        }
    }

    public static class MasterPingRequest extends TransportRequest {

        private DiscoveryNode sourceNode;

        private DiscoveryNode masterNode;

        private ClusterName clusterName;

        public MasterPingRequest() {
        }

        private MasterPingRequest(DiscoveryNode sourceNode, DiscoveryNode masterNode, ClusterName clusterName) {
            this.sourceNode = sourceNode;
            this.masterNode = masterNode;
            this.clusterName = clusterName;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            sourceNode = new DiscoveryNode(in);
            masterNode = new DiscoveryNode(in);
            clusterName = new ClusterName(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            sourceNode.writeTo(out);
            masterNode.writeTo(out);
            clusterName.writeTo(out);
        }
    }

    private static class MasterPingResponseResponse extends TransportResponse {

        private MasterPingResponseResponse() {
        }

        private MasterPingResponseResponse(StreamInput in) throws IOException {
            super(in);
        }
    }
}