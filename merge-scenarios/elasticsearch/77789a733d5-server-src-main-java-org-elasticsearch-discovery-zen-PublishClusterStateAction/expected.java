package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.IncompatibleClusterStateVersionException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.AckClusterStatePublishResponseHandler;
import org.elasticsearch.discovery.BlockingClusterStatePublishResponseHandler;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PublishClusterStateAction extends AbstractComponent {

    public static final String SEND_ACTION_NAME = "internal:discovery/zen/publish/send";

    public static final String COMMIT_ACTION_NAME = "internal:discovery/zen/publish/commit";

    public interface IncomingClusterStateListener {

        void onIncomingClusterState(ClusterState incomingState);

        void onClusterStateCommitted(String stateUUID, ActionListener<Void> processedListener);
    }

    private final TransportService transportService;

    private final NamedWriteableRegistry namedWriteableRegistry;

    private final IncomingClusterStateListener incomingClusterStateListener;

    private final DiscoverySettings discoverySettings;

    private final AtomicLong fullClusterStateReceivedCount = new AtomicLong();

    private final AtomicLong incompatibleClusterStateDiffReceivedCount = new AtomicLong();

    private final AtomicLong compatibleClusterStateDiffReceivedCount = new AtomicLong();

    public PublishClusterStateAction(TransportService transportService, NamedWriteableRegistry namedWriteableRegistry, IncomingClusterStateListener incomingClusterStateListener, DiscoverySettings discoverySettings) {
        this.transportService = transportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.incomingClusterStateListener = incomingClusterStateListener;
        this.discoverySettings = discoverySettings;
        transportService.registerRequestHandler(SEND_ACTION_NAME, BytesTransportRequest::new, ThreadPool.Names.SAME, false, false, new SendClusterStateRequestHandler());
        transportService.registerRequestHandler(COMMIT_ACTION_NAME, CommitClusterStateRequest::new, ThreadPool.Names.SAME, false, false, new CommitClusterStateRequestHandler());
    }

    public void publish(final ClusterChangedEvent clusterChangedEvent, final int minMasterNodes, final Discovery.AckListener ackListener) throws FailedToCommitClusterStateException {
        final DiscoveryNodes nodes;
        final SendingController sendingController;
        final Set<DiscoveryNode> nodesToPublishTo;
        final Map<Version, BytesReference> serializedStates;
        final Map<Version, BytesReference> serializedDiffs;
        final boolean sendFullVersion;
        try {
            nodes = clusterChangedEvent.state().nodes();
            nodesToPublishTo = new HashSet<>(nodes.getSize());
            DiscoveryNode localNode = nodes.getLocalNode();
            final int totalMasterNodes = nodes.getMasterNodes().size();
            for (final DiscoveryNode node : nodes) {
                if (node.equals(localNode) == false) {
                    nodesToPublishTo.add(node);
                }
            }
            sendFullVersion = !discoverySettings.getPublishDiff() || clusterChangedEvent.previousState() == null;
            serializedStates = new HashMap<>();
            serializedDiffs = new HashMap<>();
            buildDiffAndSerializeStates(clusterChangedEvent.state(), clusterChangedEvent.previousState(), nodesToPublishTo, sendFullVersion, serializedStates, serializedDiffs);
            final BlockingClusterStatePublishResponseHandler publishResponseHandler = new AckClusterStatePublishResponseHandler(nodesToPublishTo, ackListener);
            sendingController = new SendingController(clusterChangedEvent.state(), minMasterNodes, totalMasterNodes, publishResponseHandler);
        } catch (Exception e) {
            throw new FailedToCommitClusterStateException("unexpected error while preparing to publish", e);
        }
        try {
            innerPublish(clusterChangedEvent, nodesToPublishTo, sendingController, ackListener, sendFullVersion, serializedStates, serializedDiffs);
        } catch (FailedToCommitClusterStateException t) {
            throw t;
        } catch (Exception e) {
            if (sendingController.markAsFailed("unexpected error", e)) {
                throw new FailedToCommitClusterStateException("unexpected error", e);
            } else {
                throw e;
            }
        }
    }

    private void innerPublish(final ClusterChangedEvent clusterChangedEvent, final Set<DiscoveryNode> nodesToPublishTo, final SendingController sendingController, final Discovery.AckListener ackListener, final boolean sendFullVersion, final Map<Version, BytesReference> serializedStates, final Map<Version, BytesReference> serializedDiffs) {
        final ClusterState clusterState = clusterChangedEvent.state();
        final ClusterState previousState = clusterChangedEvent.previousState();
        final TimeValue publishTimeout = discoverySettings.getPublishTimeout();
        final long publishingStartInNanos = System.nanoTime();
        for (final DiscoveryNode node : nodesToPublishTo) {
            if (sendFullVersion || !previousState.nodes().nodeExists(node)) {
                sendFullClusterState(clusterState, serializedStates, node, publishTimeout, sendingController);
            } else {
                sendClusterStateDiff(clusterState, serializedDiffs, serializedStates, node, publishTimeout, sendingController);
            }
        }
        sendingController.waitForCommit(discoverySettings.getCommitTimeout());
        final long commitTime = System.nanoTime() - publishingStartInNanos;
        ackListener.onCommit(TimeValue.timeValueNanos(commitTime));
        try {
            long timeLeftInNanos = Math.max(0, publishTimeout.nanos() - commitTime);
            final BlockingClusterStatePublishResponseHandler publishResponseHandler = sendingController.getPublishResponseHandler();
            sendingController.setPublishingTimedOut(!publishResponseHandler.awaitAllNodes(TimeValue.timeValueNanos(timeLeftInNanos)));
            if (sendingController.getPublishingTimedOut()) {
                DiscoveryNode[] pendingNodes = publishResponseHandler.pendingNodes();
                if (pendingNodes.length > 0) {
                    logger.warn("timed out waiting for all nodes to process published state [{}] (timeout [{}], pending nodes: {})", clusterState.version(), publishTimeout, pendingNodes);
                }
            }
            Set<DiscoveryNode> failedNodes = publishResponseHandler.getFailedNodes();
            if (failedNodes.isEmpty() == false) {
                logger.warn("publishing cluster state with version [{}] failed for the following nodes: [{}]", clusterChangedEvent.state().version(), failedNodes);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void buildDiffAndSerializeStates(ClusterState clusterState, ClusterState previousState, Set<DiscoveryNode> nodesToPublishTo, boolean sendFullVersion, Map<Version, BytesReference> serializedStates, Map<Version, BytesReference> serializedDiffs) {
        Diff<ClusterState> diff = null;
        for (final DiscoveryNode node : nodesToPublishTo) {
            try {
                if (sendFullVersion || !previousState.nodes().nodeExists(node)) {
                    if (serializedStates.containsKey(node.getVersion()) == false) {
                        serializedStates.put(node.getVersion(), serializeFullClusterState(clusterState, node.getVersion()));
                    }
                } else {
                    if (diff == null) {
                        diff = clusterState.diff(previousState);
                    }
                    if (serializedDiffs.containsKey(node.getVersion()) == false) {
                        serializedDiffs.put(node.getVersion(), serializeDiffClusterState(diff, node.getVersion()));
                    }
                }
            } catch (IOException e) {
                throw new ElasticsearchException("failed to serialize cluster_state for publishing to node {}", e, node);
            }
        }
    }

    private void sendFullClusterState(ClusterState clusterState, Map<Version, BytesReference> serializedStates, DiscoveryNode node, TimeValue publishTimeout, SendingController sendingController) {
        BytesReference bytes = serializedStates.get(node.getVersion());
        if (bytes == null) {
            try {
                bytes = serializeFullClusterState(clusterState, node.getVersion());
                serializedStates.put(node.getVersion(), bytes);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to serialize cluster_state before publishing it to node {}", node), e);
                sendingController.onNodeSendFailed(node, e);
                return;
            }
        }
        sendClusterStateToNode(clusterState, bytes, node, publishTimeout, sendingController, false, serializedStates);
    }

    private void sendClusterStateDiff(ClusterState clusterState, Map<Version, BytesReference> serializedDiffs, Map<Version, BytesReference> serializedStates, DiscoveryNode node, TimeValue publishTimeout, SendingController sendingController) {
        BytesReference bytes = serializedDiffs.get(node.getVersion());
        assert bytes != null : "failed to find serialized diff for node " + node + " of version [" + node.getVersion() + "]";
        sendClusterStateToNode(clusterState, bytes, node, publishTimeout, sendingController, true, serializedStates);
    }

    private void sendClusterStateToNode(final ClusterState clusterState, BytesReference bytes, final DiscoveryNode node, final TimeValue publishTimeout, final SendingController sendingController, final boolean sendDiffs, final Map<Version, BytesReference> serializedStates) {
        try {
            TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE).withCompress(false).build();
            transportService.sendRequest(node, SEND_ACTION_NAME, new BytesTransportRequest(bytes, node.getVersion()), options, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    if (sendingController.getPublishingTimedOut()) {
                        logger.debug("node {} responded for cluster state [{}] (took longer than [{}])", node, clusterState.version(), publishTimeout);
                    }
                    sendingController.onNodeSendAck(node);
                }

                @Override
                public void handleException(TransportException exp) {
                    if (sendDiffs && exp.unwrapCause() instanceof IncompatibleClusterStateVersionException) {
                        logger.debug("resending full cluster state to node {} reason {}", node, exp.getDetailedMessage());
                        sendFullClusterState(clusterState, serializedStates, node, publishTimeout, sendingController);
                    } else {
                        logger.debug(() -> new ParameterizedMessage("failed to send cluster state to {}", node), exp);
                        sendingController.onNodeSendFailed(node, exp);
                    }
                }
            });
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("error sending cluster state to {}", node), e);
            sendingController.onNodeSendFailed(node, e);
        }
    }

    private void sendCommitToNode(final DiscoveryNode node, final ClusterState clusterState, final SendingController sendingController) {
        try {
            logger.trace("sending commit for cluster state (uuid: [{}], version [{}]) to [{}]", clusterState.stateUUID(), clusterState.version(), node);
            TransportRequestOptions options = TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE).build();
            transportService.sendRequest(node, COMMIT_ACTION_NAME, new CommitClusterStateRequest(clusterState.stateUUID()), options, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {

                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    if (sendingController.getPublishingTimedOut()) {
                        logger.debug("node {} responded to cluster state commit [{}]", node, clusterState.version());
                    }
                    sendingController.getPublishResponseHandler().onResponse(node);
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.debug(() -> new ParameterizedMessage("failed to commit cluster state (uuid [{}], version [{}]) to {}", clusterState.stateUUID(), clusterState.version(), node), exp);
                    sendingController.getPublishResponseHandler().onFailure(node, exp);
                }
            });
        } catch (Exception t) {
            logger.warn(() -> new ParameterizedMessage("error sending cluster state commit (uuid [{}], version [{}]) to {}", clusterState.stateUUID(), clusterState.version(), node), t);
            sendingController.getPublishResponseHandler().onFailure(node, t);
        }
    }

    public static BytesReference serializeFullClusterState(ClusterState clusterState, Version nodeVersion) throws IOException {
        BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.COMPRESSOR.streamOutput(bStream)) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(true);
            clusterState.writeTo(stream);
        }
        return bStream.bytes();
    }

    public static BytesReference serializeDiffClusterState(Diff diff, Version nodeVersion) throws IOException {
        BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = CompressorFactory.COMPRESSOR.streamOutput(bStream)) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(false);
            diff.writeTo(stream);
        }
        return bStream.bytes();
    }

    private Object lastSeenClusterStateMutex = new Object();

    private ClusterState lastSeenClusterState;

    protected void handleIncomingClusterStateRequest(BytesTransportRequest request, TransportChannel channel) throws IOException {
        Compressor compressor = CompressorFactory.compressor(request.bytes());
        StreamInput in = request.bytes().streamInput();
        final ClusterState incomingState;
        synchronized (lastSeenClusterStateMutex) {
            try {
                if (compressor != null) {
                    in = compressor.streamInput(in);
                }
                in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
                in.setVersion(request.version());
                if (in.readBoolean()) {
                    incomingState = ClusterState.readFrom(in, transportService.getLocalNode());
                    fullClusterStateReceivedCount.incrementAndGet();
                    logger.debug("received full cluster state version [{}] with size [{}]", incomingState.version(), request.bytes().length());
                } else if (lastSeenClusterState != null) {
                    Diff<ClusterState> diff = ClusterState.readDiffFrom(in, lastSeenClusterState.nodes().getLocalNode());
                    incomingState = diff.apply(lastSeenClusterState);
                    compatibleClusterStateDiffReceivedCount.incrementAndGet();
                    logger.debug("received diff cluster state version [{}] with uuid [{}], diff size [{}]", incomingState.version(), incomingState.stateUUID(), request.bytes().length());
                } else {
                    logger.debug("received diff for but don't have any local cluster state - requesting full state");
                    throw new IncompatibleClusterStateVersionException("have no local cluster state");
                }
            } catch (IncompatibleClusterStateVersionException e) {
                incompatibleClusterStateDiffReceivedCount.incrementAndGet();
                throw e;
            } catch (Exception e) {
                logger.warn("unexpected error while deserializing an incoming cluster state", e);
                throw e;
            } finally {
                IOUtils.close(in);
            }
            incomingClusterStateListener.onIncomingClusterState(incomingState);
            lastSeenClusterState = incomingState;
        }
        channel.sendResponse(TransportResponse.Empty.INSTANCE);
    }

    protected void handleCommitRequest(CommitClusterStateRequest request, final TransportChannel channel) {
        incomingClusterStateListener.onClusterStateCommitted(request.stateUUID, new ActionListener<Void>() {

            @Override
            public void onResponse(Void ignore) {
                try {
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                } catch (Exception e) {
                    logger.debug("failed to send response on cluster state processed", e);
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.debug("failed to send response on cluster state processed", inner);
                }
            }
        });
    }

    private class SendClusterStateRequestHandler implements TransportRequestHandler<BytesTransportRequest> {

        @Override
        public void messageReceived(BytesTransportRequest request, final TransportChannel channel, Task task) throws Exception {
            handleIncomingClusterStateRequest(request, channel);
        }
    }

    private class CommitClusterStateRequestHandler implements TransportRequestHandler<CommitClusterStateRequest> {

        @Override
        public void messageReceived(CommitClusterStateRequest request, final TransportChannel channel, Task task) throws Exception {
            handleCommitRequest(request, channel);
        }
    }

    protected static class CommitClusterStateRequest extends TransportRequest {

        String stateUUID;

        public CommitClusterStateRequest() {
        }

        public CommitClusterStateRequest(String stateUUID) {
            this.stateUUID = stateUUID;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            stateUUID = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(stateUUID);
        }
    }

    class SendingController {

        private final ClusterState clusterState;

        public BlockingClusterStatePublishResponseHandler getPublishResponseHandler() {
            return publishResponseHandler;
        }

        private final BlockingClusterStatePublishResponseHandler publishResponseHandler;

        final ArrayList<DiscoveryNode> sendAckedBeforeCommit = new ArrayList<>();

        final CountDownLatch committedOrFailedLatch;

        boolean committed;

        int neededMastersToCommit;

        int pendingMasterNodes;

        final AtomicBoolean publishingTimedOut = new AtomicBoolean();

        private SendingController(ClusterState clusterState, int minMasterNodes, int totalMasterNodes, BlockingClusterStatePublishResponseHandler publishResponseHandler) {
            this.clusterState = clusterState;
            this.publishResponseHandler = publishResponseHandler;
            this.neededMastersToCommit = Math.max(0, minMasterNodes - 1);
            this.pendingMasterNodes = totalMasterNodes - 1;
            if (this.neededMastersToCommit > this.pendingMasterNodes) {
                throw new FailedToCommitClusterStateException("not enough masters to ack sent cluster state." + "[{}] needed , have [{}]", neededMastersToCommit, pendingMasterNodes);
            }
            this.committed = neededMastersToCommit == 0;
            this.committedOrFailedLatch = new CountDownLatch(committed ? 0 : 1);
        }

        public void waitForCommit(TimeValue commitTimeout) {
            boolean timedout = false;
            try {
                timedout = committedOrFailedLatch.await(commitTimeout.millis(), TimeUnit.MILLISECONDS) == false;
            } catch (InterruptedException e) {
            }
            if (timedout) {
                markAsFailed("timed out waiting for commit (commit timeout [" + commitTimeout + "])");
            }
            if (isCommitted() == false) {
                throw new FailedToCommitClusterStateException("{} enough masters to ack sent cluster state. [{}] left", timedout ? "timed out while waiting for" : "failed to get", neededMastersToCommit);
            }
        }

        public synchronized boolean isCommitted() {
            return committed;
        }

        public synchronized void onNodeSendAck(DiscoveryNode node) {
            if (committed) {
                assert sendAckedBeforeCommit.isEmpty();
                sendCommitToNode(node, clusterState, this);
            } else if (committedOrFailed()) {
                logger.trace("ignoring ack from [{}] for cluster state version [{}]. already failed", node, clusterState.version());
            } else {
                sendAckedBeforeCommit.add(node);
                if (node.isMasterNode()) {
                    checkForCommitOrFailIfNoPending(node);
                }
            }
        }

        private synchronized boolean committedOrFailed() {
            return committedOrFailedLatch.getCount() == 0;
        }

        private synchronized void checkForCommitOrFailIfNoPending(DiscoveryNode masterNode) {
            logger.trace("master node {} acked cluster state version [{}]. processing ... (current pending [{}], needed [{}])", masterNode, clusterState.version(), pendingMasterNodes, neededMastersToCommit);
            neededMastersToCommit--;
            if (neededMastersToCommit == 0) {
                if (markAsCommitted()) {
                    for (DiscoveryNode nodeToCommit : sendAckedBeforeCommit) {
                        sendCommitToNode(nodeToCommit, clusterState, this);
                    }
                    sendAckedBeforeCommit.clear();
                }
            }
            decrementPendingMasterAcksAndChangeForFailure();
        }

        private synchronized void decrementPendingMasterAcksAndChangeForFailure() {
            pendingMasterNodes--;
            if (pendingMasterNodes == 0 && neededMastersToCommit > 0) {
                markAsFailed("no more pending master nodes, but failed to reach needed acks ([" + neededMastersToCommit + "] left)");
            }
        }

        public synchronized void onNodeSendFailed(DiscoveryNode node, Exception e) {
            if (node.isMasterNode()) {
                logger.trace("master node {} failed to ack cluster state version [{}]. " + "processing ... (current pending [{}], needed [{}])", node, clusterState.version(), pendingMasterNodes, neededMastersToCommit);
                decrementPendingMasterAcksAndChangeForFailure();
            }
            publishResponseHandler.onFailure(node, e);
        }

        private synchronized boolean markAsCommitted() {
            if (committedOrFailed()) {
                return committed;
            }
            logger.trace("committing version [{}]", clusterState.version());
            committed = true;
            committedOrFailedLatch.countDown();
            return true;
        }

        private synchronized boolean markAsFailed(String details, Exception reason) {
            if (committedOrFailed()) {
                return committed == false;
            }
            logger.trace(() -> new ParameterizedMessage("failed to commit version [{}]. {}", clusterState.version(), details), reason);
            committed = false;
            committedOrFailedLatch.countDown();
            return true;
        }

        private synchronized boolean markAsFailed(String reason) {
            if (committedOrFailed()) {
                return committed == false;
            }
            logger.trace("failed to commit version [{}]. {}", clusterState.version(), reason);
            committed = false;
            committedOrFailedLatch.countDown();
            return true;
        }

        public boolean getPublishingTimedOut() {
            return publishingTimedOut.get();
        }

        public void setPublishingTimedOut(boolean isTimedOut) {
            publishingTimedOut.set(isTimedOut);
        }
    }

    public PublishClusterStateStats stats() {
        return new PublishClusterStateStats(fullClusterStateReceivedCount.get(), incompatibleClusterStateDiffReceivedCount.get(), compatibleClusterStateDiffReceivedCount.get());
    }
}