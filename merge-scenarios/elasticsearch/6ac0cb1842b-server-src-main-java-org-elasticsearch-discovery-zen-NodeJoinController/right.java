package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoverySettings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class NodeJoinController {

    private static final Logger logger = LogManager.getLogger(NodeJoinController.class);

    private final MasterService masterService;

    private final JoinTaskExecutor joinTaskExecutor;

    private ElectionContext electionContext = null;

    public NodeJoinController(MasterService masterService, AllocationService allocationService, ElectMasterService electMaster) {
        this.masterService = masterService;
        joinTaskExecutor = new JoinTaskExecutor(allocationService, electMaster, logger);
    }

    public void waitToBeElectedAsMaster(int requiredMasterJoins, TimeValue timeValue, final ElectionCallback callback) {
        final CountDownLatch done = new CountDownLatch(1);
        final ElectionCallback wrapperCallback = new ElectionCallback() {

            @Override
            public void onElectedAsMaster(ClusterState state) {
                done.countDown();
                callback.onElectedAsMaster(state);
            }

            @Override
            public void onFailure(Throwable t) {
                done.countDown();
                callback.onFailure(t);
            }
        };
        ElectionContext myElectionContext = null;
        try {
            synchronized (this) {
                assert electionContext != null : "waitToBeElectedAsMaster is called we are not accumulating joins";
                myElectionContext = electionContext;
                electionContext.onAttemptToBeElected(requiredMasterJoins, wrapperCallback);
                checkPendingJoinsAndElectIfNeeded();
            }
            try {
                if (done.await(timeValue.millis(), TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
            }
            if (logger.isTraceEnabled()) {
                final int pendingNodes = myElectionContext.getPendingMasterJoinsCount();
                logger.trace("timed out waiting to be elected. waited [{}]. pending master node joins [{}]", timeValue, pendingNodes);
            }
            failContextIfNeeded(myElectionContext, "timed out waiting to be elected");
        } catch (Exception e) {
            logger.error("unexpected failure while waiting for incoming joins", e);
            if (myElectionContext != null) {
                failContextIfNeeded(myElectionContext, "unexpected failure while waiting for pending joins [" + e.getMessage() + "]");
            }
        }
    }

    private synchronized void failContextIfNeeded(final ElectionContext context, final String reason) {
        if (electionContext == context) {
            stopElectionContext(reason);
        }
    }

    public synchronized void startElectionContext() {
        logger.trace("starting an election context, will accumulate joins");
        assert electionContext == null : "double startElectionContext() calls";
        electionContext = new ElectionContext();
    }

    public void stopElectionContext(String reason) {
        logger.trace("stopping election ([{}])", reason);
        synchronized (this) {
            assert electionContext != null : "stopElectionContext() called but not accumulating";
            electionContext.closeAndProcessPending(reason);
            electionContext = null;
        }
    }

    public synchronized void handleJoinRequest(final DiscoveryNode node, final MembershipAction.JoinCallback callback) {
        if (electionContext != null) {
            electionContext.addIncomingJoin(node, callback);
            checkPendingJoinsAndElectIfNeeded();
        } else {
            masterService.submitStateUpdateTask("zen-disco-node-join", node, ClusterStateTaskConfig.build(Priority.URGENT), joinTaskExecutor, new JoinTaskListener(callback, logger));
        }
    }

    private synchronized void checkPendingJoinsAndElectIfNeeded() {
        assert electionContext != null : "election check requested but no active context";
        final int pendingMasterJoins = electionContext.getPendingMasterJoinsCount();
        if (electionContext.isEnoughPendingJoins(pendingMasterJoins) == false) {
            if (logger.isTraceEnabled()) {
                logger.trace("not enough joins for election. Got [{}], required [{}]", pendingMasterJoins, electionContext.requiredMasterJoins);
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("have enough joins for election. Got [{}], required [{}]", pendingMasterJoins, electionContext.requiredMasterJoins);
            }
            electionContext.closeAndBecomeMaster();
            electionContext = null;
        }
    }

    public interface ElectionCallback {

        void onElectedAsMaster(ClusterState state);

        void onFailure(Throwable t);
    }

    class ElectionContext {

        private ElectionCallback callback = null;

        private int requiredMasterJoins = -1;

        private final Map<DiscoveryNode, List<MembershipAction.JoinCallback>> joinRequestAccumulator = new HashMap<>();

        final AtomicBoolean closed = new AtomicBoolean();

        public synchronized void onAttemptToBeElected(int requiredMasterJoins, ElectionCallback callback) {
            ensureOpen();
            assert this.requiredMasterJoins < 0;
            assert this.callback == null;
            this.requiredMasterJoins = requiredMasterJoins;
            this.callback = callback;
        }

        public synchronized void addIncomingJoin(DiscoveryNode node, MembershipAction.JoinCallback callback) {
            ensureOpen();
            joinRequestAccumulator.computeIfAbsent(node, n -> new ArrayList<>()).add(callback);
        }

        public synchronized boolean isEnoughPendingJoins(int pendingMasterJoins) {
            final boolean hasEnough;
            if (requiredMasterJoins < 0) {
                hasEnough = false;
            } else {
                assert callback != null : "requiredMasterJoins is set but not the callback";
                hasEnough = pendingMasterJoins >= requiredMasterJoins;
            }
            return hasEnough;
        }

        private Map<DiscoveryNode, ClusterStateTaskListener> getPendingAsTasks() {
            Map<DiscoveryNode, ClusterStateTaskListener> tasks = new HashMap<>();
            joinRequestAccumulator.entrySet().stream().forEach(e -> tasks.put(e.getKey(), new JoinTaskListener(e.getValue(), logger)));
            return tasks;
        }

        public synchronized int getPendingMasterJoinsCount() {
            int pendingMasterJoins = 0;
            for (DiscoveryNode node : joinRequestAccumulator.keySet()) {
                if (node.isMasterNode()) {
                    pendingMasterJoins++;
                }
            }
            return pendingMasterJoins;
        }

        public synchronized void closeAndBecomeMaster() {
            assert callback != null : "becoming a master but the callback is not yet set";
            assert isEnoughPendingJoins(getPendingMasterJoinsCount()) : "becoming a master but pending joins of " + getPendingMasterJoinsCount() + " are not enough. needs [" + requiredMasterJoins + "];";
            innerClose();
            Map<DiscoveryNode, ClusterStateTaskListener> tasks = getPendingAsTasks();
            final String source = "zen-disco-elected-as-master ([" + tasks.size() + "] nodes joined)";
            tasks.put(BECOME_MASTER_TASK, (source1, e) -> {
            });
            tasks.put(FINISH_ELECTION_TASK, electionFinishedListener);
            masterService.submitStateUpdateTasks(source, tasks, ClusterStateTaskConfig.build(Priority.URGENT), joinTaskExecutor);
        }

        public synchronized void closeAndProcessPending(String reason) {
            innerClose();
            Map<DiscoveryNode, ClusterStateTaskListener> tasks = getPendingAsTasks();
            final String source = "zen-disco-election-stop [" + reason + "]";
            tasks.put(FINISH_ELECTION_TASK, electionFinishedListener);
            masterService.submitStateUpdateTasks(source, tasks, ClusterStateTaskConfig.build(Priority.URGENT), joinTaskExecutor);
        }

        private void innerClose() {
            if (closed.getAndSet(true)) {
                throw new AlreadyClosedException("election context is already closed");
            }
        }

        private void ensureOpen() {
            if (closed.get()) {
                throw new AlreadyClosedException("election context is already closed");
            }
        }

        private synchronized ElectionCallback getCallback() {
            return callback;
        }

        private void onElectedAsMaster(ClusterState state) {
            assert MasterService.assertMasterUpdateThread();
            assert state.nodes().isLocalNodeElectedMaster() : "onElectedAsMaster called but local node is not master";
            ElectionCallback callback = getCallback();
            if (callback != null) {
                callback.onElectedAsMaster(state);
            }
        }

        private void onFailure(Throwable t) {
            assert MasterService.assertMasterUpdateThread();
            ElectionCallback callback = getCallback();
            if (callback != null) {
                callback.onFailure(t);
            }
        }

        private final ClusterStateTaskListener electionFinishedListener = new ClusterStateTaskListener() {

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (newState.nodes().isLocalNodeElectedMaster()) {
                    ElectionContext.this.onElectedAsMaster(newState);
                } else {
                    onFailure(source, new NotMasterException("election stopped [" + source + "]"));
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                ElectionContext.this.onFailure(e);
            }
        };
    }

    static class JoinTaskListener implements ClusterStateTaskListener {

        final List<MembershipAction.JoinCallback> callbacks;

        private final Logger logger;

        JoinTaskListener(MembershipAction.JoinCallback callback, Logger logger) {
            this(Collections.singletonList(callback), logger);
        }

        JoinTaskListener(List<MembershipAction.JoinCallback> callbacks, Logger logger) {
            this.callbacks = callbacks;
            this.logger = logger;
        }

        @Override
        public void onFailure(String source, Exception e) {
            for (MembershipAction.JoinCallback callback : callbacks) {
                try {
                    callback.onFailure(e);
                } catch (Exception inner) {
                    logger.error(() -> new ParameterizedMessage("error handling task failure [{}]", e), inner);
                }
            }
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            for (MembershipAction.JoinCallback callback : callbacks) {
                try {
                    callback.onSuccess();
                } catch (Exception e) {
                    logger.error(() -> new ParameterizedMessage("unexpected error during [{}]", source), e);
                }
            }
        }
    }

    public static final DiscoveryNode BECOME_MASTER_TASK = new DiscoveryNode("_BECOME_MASTER_TASK_", new TransportAddress(TransportAddress.META_ADDRESS, 0), Collections.emptyMap(), Collections.emptySet(), Version.CURRENT) {

        @Override
        public String toString() {
            return "";
        }
    };

    public static final DiscoveryNode FINISH_ELECTION_TASK = new DiscoveryNode("_FINISH_ELECTION_", new TransportAddress(TransportAddress.META_ADDRESS, 0), Collections.emptyMap(), Collections.emptySet(), Version.CURRENT) {

        @Override
        public String toString() {
            return "";
        }
    };

    public static class JoinTaskExecutor implements ClusterStateTaskExecutor<DiscoveryNode> {

        private final AllocationService allocationService;

        private final ElectMasterService electMasterService;

        private final Logger logger;

        public JoinTaskExecutor(AllocationService allocationService, ElectMasterService electMasterService, Logger logger) {
            this.allocationService = allocationService;
            this.electMasterService = electMasterService;
            this.logger = logger;
        }

        @Override
        public ClusterTasksResult<DiscoveryNode> execute(ClusterState currentState, List<DiscoveryNode> joiningNodes) throws Exception {
            final ClusterTasksResult.Builder<DiscoveryNode> results = ClusterTasksResult.builder();
            final DiscoveryNodes currentNodes = currentState.nodes();
            boolean nodesChanged = false;
            ClusterState.Builder newState;
            if (joiningNodes.size() == 1 && joiningNodes.get(0).equals(FINISH_ELECTION_TASK)) {
                return results.successes(joiningNodes).build(currentState);
            } else if (currentNodes.getMasterNode() == null && joiningNodes.contains(BECOME_MASTER_TASK)) {
                assert joiningNodes.contains(FINISH_ELECTION_TASK) : "becoming a master but election is not finished " + joiningNodes;
                newState = becomeMasterAndTrimConflictingNodes(currentState, joiningNodes);
                nodesChanged = true;
            } else if (currentNodes.isLocalNodeElectedMaster() == false) {
                logger.trace("processing node joins, but we are not the master. current master: {}", currentNodes.getMasterNode());
                throw new NotMasterException("Node [" + currentNodes.getLocalNode() + "] not master for join request");
            } else {
                newState = ClusterState.builder(currentState);
            }
            DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(newState.nodes());
            assert nodesBuilder.isLocalNodeElectedMaster();
            Version minClusterNodeVersion = newState.nodes().getMinNodeVersion();
            Version maxClusterNodeVersion = newState.nodes().getMaxNodeVersion();
            final boolean enforceMajorVersion = currentState.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false;
            for (final DiscoveryNode node : joiningNodes) {
                if (node.equals(BECOME_MASTER_TASK) || node.equals(FINISH_ELECTION_TASK)) {
                } else if (currentNodes.nodeExists(node)) {
                    logger.debug("received a join request for an existing node [{}]", node);
                } else {
                    try {
                        if (enforceMajorVersion) {
                            MembershipAction.ensureMajorVersionBarrier(node.getVersion(), minClusterNodeVersion);
                        }
                        MembershipAction.ensureNodesCompatibility(node.getVersion(), minClusterNodeVersion, maxClusterNodeVersion);
                        MembershipAction.ensureIndexCompatibility(node.getVersion(), currentState.getMetaData());
                        nodesBuilder.add(node);
                        nodesChanged = true;
                        minClusterNodeVersion = Version.min(minClusterNodeVersion, node.getVersion());
                        maxClusterNodeVersion = Version.max(maxClusterNodeVersion, node.getVersion());
                    } catch (IllegalArgumentException | IllegalStateException e) {
                        results.failure(node, e);
                        continue;
                    }
                }
                results.success(node);
            }
            if (nodesChanged) {
                newState.nodes(nodesBuilder);
                return results.build(allocationService.reroute(newState.build(), "node_join"));
            } else {
                return results.build(newState.build());
            }
        }

        private ClusterState.Builder becomeMasterAndTrimConflictingNodes(ClusterState currentState, List<DiscoveryNode> joiningNodes) {
            assert currentState.nodes().getMasterNodeId() == null : currentState;
            DiscoveryNodes currentNodes = currentState.nodes();
            DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentNodes);
            nodesBuilder.masterNodeId(currentState.nodes().getLocalNodeId());
            for (final DiscoveryNode joiningNode : joiningNodes) {
                final DiscoveryNode nodeWithSameId = nodesBuilder.get(joiningNode.getId());
                if (nodeWithSameId != null && nodeWithSameId.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameId, joiningNode);
                    nodesBuilder.remove(nodeWithSameId.getId());
                }
                final DiscoveryNode nodeWithSameAddress = currentNodes.findByAddress(joiningNode.getAddress());
                if (nodeWithSameAddress != null && nodeWithSameAddress.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameAddress, joiningNode);
                    nodesBuilder.remove(nodeWithSameAddress.getId());
                }
            }
            ClusterState tmpState = ClusterState.builder(currentState).nodes(nodesBuilder).blocks(ClusterBlocks.builder().blocks(currentState.blocks()).removeGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID)).build();
            return ClusterState.builder(allocationService.deassociateDeadNodes(tmpState, false, "removed dead nodes on election"));
        }

        @Override
        public boolean runOnlyOnMaster() {
            return false;
        }

        @Override
        public void clusterStatePublished(ClusterChangedEvent event) {
            electMasterService.logMinimumMasterNodesWarningIfNecessary(event.previousState(), event.state());
        }
    }
}