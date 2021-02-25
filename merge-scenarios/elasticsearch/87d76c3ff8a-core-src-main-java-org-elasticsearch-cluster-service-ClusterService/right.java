package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.threadpool.ThreadPool;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class ClusterService extends AbstractLifecycleComponent {

    public static final Setting<TimeValue> CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING = Setting.positiveTimeSetting("cluster.service.slow_task_logging_threshold", TimeValue.timeValueSeconds(30), Property.Dynamic, Property.NodeScope);

    public static final String UPDATE_THREAD_NAME = "clusterService#updateTask";

    private final ThreadPool threadPool;

    private final ClusterName clusterName;

    private BiConsumer<ClusterChangedEvent, Discovery.AckListener> clusterStatePublisher;

    private final OperationRouting operationRouting;

    private final ClusterSettings clusterSettings;

    private TimeValue slowTaskLoggingThreshold;

    private volatile PrioritizedEsThreadPoolExecutor updateTasksExecutor;

    private final Collection<ClusterStateListener> priorityClusterStateListeners = new CopyOnWriteArrayList<>();

    private final Collection<ClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<>();

    private final Collection<ClusterStateListener> lastClusterStateListeners = new CopyOnWriteArrayList<>();

    private final Map<ClusterStateTaskExecutor, List<UpdateTask>> updateTasksPerExecutor = new HashMap<>();

    private final Collection<ClusterStateListener> postAppliedListeners = new CopyOnWriteArrayList<>();

    private final Iterable<ClusterStateListener> preAppliedListeners = Iterables.concat(priorityClusterStateListeners, clusterStateListeners, lastClusterStateListeners);

    private final LocalNodeMasterListeners localNodeMasterListeners;

    private final Queue<NotifyTimeout> onGoingTimeouts = ConcurrentCollections.newQueue();

    private volatile ClusterState clusterState;

    private final ClusterBlocks.Builder initialBlocks;

    private NodeConnectionsService nodeConnectionsService;

    public ClusterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        super(settings);
        this.operationRouting = new OperationRouting(settings, clusterSettings);
        this.threadPool = threadPool;
        this.clusterSettings = clusterSettings;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.clusterState = ClusterState.builder(clusterName).build();
        this.clusterSettings.addSettingsUpdateConsumer(CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING, this::setSlowTaskLoggingThreshold);
        this.slowTaskLoggingThreshold = CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);
        localNodeMasterListeners = new LocalNodeMasterListeners(threadPool);
        initialBlocks = ClusterBlocks.builder();
    }

    private void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    public synchronized void setClusterStatePublisher(BiConsumer<ClusterChangedEvent, Discovery.AckListener> publisher) {
        clusterStatePublisher = publisher;
    }

    public synchronized void setLocalNode(DiscoveryNode localNode) {
        assert clusterState.nodes().getLocalNodeId() == null : "local node is already set";
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes()).add(localNode).localNodeId(localNode.getId());
        this.clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
    }

    public synchronized void setNodeConnectionsService(NodeConnectionsService nodeConnectionsService) {
        assert this.nodeConnectionsService == null : "nodeConnectionsService is already set";
        this.nodeConnectionsService = nodeConnectionsService;
    }

    public synchronized void addInitialStateBlock(ClusterBlock block) throws IllegalStateException {
        if (lifecycle.started()) {
            throw new IllegalStateException("can't set initial block when started");
        }
        initialBlocks.addGlobalBlock(block);
    }

    public synchronized void removeInitialStateBlock(ClusterBlock block) throws IllegalStateException {
        removeInitialStateBlock(block.id());
    }

    public synchronized void removeInitialStateBlock(int blockId) throws IllegalStateException {
        if (lifecycle.started()) {
            throw new IllegalStateException("can't set initial block when started");
        }
        initialBlocks.removeGlobalBlock(blockId);
    }

    @Override
    protected synchronized void doStart() {
        Objects.requireNonNull(clusterStatePublisher, "please set a cluster state publisher before starting");
        Objects.requireNonNull(clusterState.nodes().getLocalNode(), "please set the local node before starting");
        Objects.requireNonNull(nodeConnectionsService, "please set the node connection service before starting");
        add(localNodeMasterListeners);
        this.clusterState = ClusterState.builder(clusterState).blocks(initialBlocks).build();
        this.updateTasksExecutor = EsExecutors.newSinglePrioritizing(UPDATE_THREAD_NAME, daemonThreadFactory(settings, UPDATE_THREAD_NAME), threadPool.getThreadContext());
        this.clusterState = ClusterState.builder(clusterState).blocks(initialBlocks).build();
    }

    @Override
    protected synchronized void doStop() {
        for (NotifyTimeout onGoingTimeout : onGoingTimeouts) {
            onGoingTimeout.cancel();
            try {
                onGoingTimeout.cancel();
                onGoingTimeout.listener.onClose();
            } catch (Exception ex) {
                logger.debug("failed to notify listeners on shutdown", ex);
            }
        }
        ThreadPool.terminate(updateTasksExecutor, 10, TimeUnit.SECONDS);
        postAppliedListeners.stream().filter(listener -> listener instanceof TimeoutClusterStateListener).map(listener -> (TimeoutClusterStateListener) listener).forEach(TimeoutClusterStateListener::onClose);
        remove(localNodeMasterListeners);
    }

    @Override
    protected synchronized void doClose() {
    }

    public DiscoveryNode localNode() {
        DiscoveryNode localNode = clusterState.getNodes().getLocalNode();
        if (localNode == null) {
            throw new IllegalStateException("No local node found. Is the node started?");
        }
        return localNode;
    }

    public OperationRouting operationRouting() {
        return operationRouting;
    }

    public ClusterState state() {
        return this.clusterState;
    }

    public void addFirst(ClusterStateListener listener) {
        priorityClusterStateListeners.add(listener);
    }

    public void addLast(ClusterStateListener listener) {
        lastClusterStateListeners.add(listener);
    }

    public void add(ClusterStateListener listener) {
        clusterStateListeners.add(listener);
    }

    public void remove(ClusterStateListener listener) {
        clusterStateListeners.remove(listener);
        priorityClusterStateListeners.remove(listener);
        lastClusterStateListeners.remove(listener);
        postAppliedListeners.remove(listener);
        for (Iterator<NotifyTimeout> it = onGoingTimeouts.iterator(); it.hasNext(); ) {
            NotifyTimeout timeout = it.next();
            if (timeout.listener.equals(listener)) {
                timeout.cancel();
                it.remove();
            }
        }
    }

    public void add(LocalNodeMasterListener listener) {
        localNodeMasterListeners.add(listener);
    }

    public void remove(LocalNodeMasterListener listener) {
        localNodeMasterListeners.remove(listener);
    }

    public void add(@Nullable final TimeValue timeout, final TimeoutClusterStateListener listener) {
        if (lifecycle.stoppedOrClosed()) {
            listener.onClose();
            return;
        }
        try {
            updateTasksExecutor.execute(new SourcePrioritizedRunnable(Priority.HIGH, "_add_listener_") {

                @Override
                public void run() {
                    if (timeout != null) {
                        NotifyTimeout notifyTimeout = new NotifyTimeout(listener, timeout);
                        notifyTimeout.future = threadPool.schedule(timeout, ThreadPool.Names.GENERIC, notifyTimeout);
                        onGoingTimeouts.add(notifyTimeout);
                    }
                    postAppliedListeners.add(listener);
                    listener.postAdded();
                }
            });
        } catch (EsRejectedExecutionException e) {
            if (lifecycle.stoppedOrClosed()) {
                listener.onClose();
            } else {
                throw e;
            }
        }
    }

    public void submitStateUpdateTask(final String source, final ClusterStateUpdateTask updateTask) {
        submitStateUpdateTask(source, updateTask, updateTask, updateTask, updateTask);
    }

    public <T> void submitStateUpdateTask(final String source, final T task, final ClusterStateTaskConfig config, final ClusterStateTaskExecutor<T> executor, final ClusterStateTaskListener listener) {
        submitStateUpdateTasks(source, Collections.singletonMap(task, listener), config, executor);
    }

    public <T> void submitStateUpdateTasks(final String source, final Map<T, ClusterStateTaskListener> tasks, final ClusterStateTaskConfig config, final ClusterStateTaskExecutor<T> executor) {
        if (!lifecycle.started()) {
            return;
        }
        if (tasks.isEmpty()) {
            return;
        }
        try {
            final IdentityHashMap<T, ClusterStateTaskListener> tasksIdentity = new IdentityHashMap<>(tasks);
            final List<UpdateTask<T>> updateTasks = tasksIdentity.entrySet().stream().map(entry -> new UpdateTask<>(source, entry.getKey(), config, executor, safe(entry.getValue(), logger))).collect(Collectors.toList());
            synchronized (updateTasksPerExecutor) {
                List<UpdateTask> existingTasks = updateTasksPerExecutor.computeIfAbsent(executor, k -> new ArrayList<>());
                for (@SuppressWarnings("unchecked") UpdateTask<T> existing : existingTasks) {
                    if (tasksIdentity.containsKey(existing.task)) {
                        throw new IllegalStateException("task [" + executor.describeTasks(Collections.singletonList(existing.task)) + "] with source [" + source + "] is already queued");
                    }
                }
                existingTasks.addAll(updateTasks);
            }
            final UpdateTask<T> firstTask = updateTasks.get(0);
            if (config.timeout() != null) {
                updateTasksExecutor.execute(firstTask, threadPool.scheduler(), config.timeout(), () -> threadPool.generic().execute(() -> {
                    for (UpdateTask<T> task : updateTasks) {
                        if (task.processed.getAndSet(true) == false) {
                            logger.debug("cluster state update task [{}] timed out after [{}]", source, config.timeout());
                            task.listener.onFailure(source, new ProcessClusterEventTimeoutException(config.timeout(), source));
                        }
                    }
                }));
            } else {
                updateTasksExecutor.execute(firstTask);
            }
        } catch (EsRejectedExecutionException e) {
            if (!lifecycle.stoppedOrClosed()) {
                throw e;
            }
        }
    }

    public List<PendingClusterTask> pendingTasks() {
        PrioritizedEsThreadPoolExecutor.Pending[] pendings = updateTasksExecutor.getPending();
        List<PendingClusterTask> pendingClusterTasks = new ArrayList<>(pendings.length);
        for (PrioritizedEsThreadPoolExecutor.Pending pending : pendings) {
            final String source;
            final long timeInQueue;
            final Object task = pending.task;
            if (task == null) {
                continue;
            } else if (task instanceof SourcePrioritizedRunnable) {
                SourcePrioritizedRunnable runnable = (SourcePrioritizedRunnable) task;
                source = runnable.source();
                timeInQueue = runnable.getAgeInMillis();
            } else {
                assert false : "expected SourcePrioritizedRunnable got " + task.getClass();
                source = "unknown [" + task.getClass() + "]";
                timeInQueue = 0;
            }
            pendingClusterTasks.add(new PendingClusterTask(pending.insertionOrder, pending.priority, new Text(source), timeInQueue, pending.executing));
        }
        return pendingClusterTasks;
    }

    public int numberOfPendingTasks() {
        return updateTasksExecutor.getNumberOfPendingTasks();
    }

    public TimeValue getMaxTaskWaitTime() {
        return updateTasksExecutor.getMaxTaskWaitTime();
    }

    public static boolean assertClusterStateThread() {
        assert Thread.currentThread().getName().contains(ClusterService.UPDATE_THREAD_NAME) : "not called from the cluster state update thread";
        return true;
    }

    public static boolean assertNotClusterStateUpdateThread(String reason) {
        assert Thread.currentThread().getName().contains(UPDATE_THREAD_NAME) == false : "Expected current thread [" + Thread.currentThread() + "] to not be the cluster state update thread. Reason: [" + reason + "]";
        return true;
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    abstract static class SourcePrioritizedRunnable extends PrioritizedRunnable {

        protected final String source;

        public SourcePrioritizedRunnable(Priority priority, String source) {
            super(priority);
            this.source = source;
        }

        public String source() {
            return source;
        }
    }

    <T> void runTasksForExecutor(ClusterStateTaskExecutor<T> executor) {
        final ArrayList<UpdateTask<T>> toExecute = new ArrayList<>();
        final Map<String, ArrayList<T>> processTasksBySource = new HashMap<>();
        synchronized (updateTasksPerExecutor) {
            List<UpdateTask> pending = updateTasksPerExecutor.remove(executor);
            if (pending != null) {
                for (UpdateTask<T> task : pending) {
                    if (task.processed.getAndSet(true) == false) {
                        logger.trace("will process {}", task.toString(executor));
                        toExecute.add(task);
                        processTasksBySource.computeIfAbsent(task.source, s -> new ArrayList<>()).add(task.task);
                    } else {
                        logger.trace("skipping {}, already processed", task.toString(executor));
                    }
                }
            }
        }
        if (toExecute.isEmpty()) {
            return;
        }
        final String tasksSummary = processTasksBySource.entrySet().stream().map(entry -> {
            String tasks = executor.describeTasks(entry.getValue());
            return tasks.isEmpty() ? entry.getKey() : entry.getKey() + "[" + tasks + "]";
        }).reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        if (!lifecycle.started()) {
            logger.debug("processing [{}]: ignoring, cluster_service not started", tasksSummary);
            return;
        }
        logger.debug("processing [{}]: execute", tasksSummary);
        ClusterState previousClusterState = clusterState;
        if (!previousClusterState.nodes().isLocalNodeElectedMaster() && executor.runOnlyOnMaster()) {
            logger.debug("failing [{}]: local node is no longer master", tasksSummary);
            toExecute.stream().forEach(task -> task.listener.onNoLongerMaster(task.source));
            return;
        }
        ClusterStateTaskExecutor.BatchResult<T> batchResult;
        long startTimeNS = currentTimeInNanos();
        try {
            List<T> inputs = toExecute.stream().map(tUpdateTask -> tUpdateTask.task).collect(Collectors.toList());
            batchResult = executor.execute(previousClusterState, inputs);
        } catch (Exception e) {
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            if (logger.isTraceEnabled()) {
                logger.trace((Supplier<?>) () -> new ParameterizedMessage("failed to execute cluster state update in [{}], state:\nversion [{}], source [{}]\n{}{}{}", executionTime, previousClusterState.version(), tasksSummary, previousClusterState.nodes(), previousClusterState.routingTable(), previousClusterState.getRoutingNodes()), e);
            }
            warnAboutSlowTaskIfNeeded(executionTime, tasksSummary);
            batchResult = ClusterStateTaskExecutor.BatchResult.<T>builder().failures(toExecute.stream().map(updateTask -> updateTask.task)::iterator, e).build(previousClusterState);
        }
        assert batchResult.executionResults != null;
        assert batchResult.executionResults.size() == toExecute.size() : String.format(Locale.ROOT, "expected [%d] task result%s but was [%d]", toExecute.size(), toExecute.size() == 1 ? "" : "s", batchResult.executionResults.size());
        boolean assertsEnabled = false;
        assert (assertsEnabled = true);
        if (assertsEnabled) {
            for (UpdateTask<T> updateTask : toExecute) {
                assert batchResult.executionResults.containsKey(updateTask.task) : "missing task result for " + updateTask.toString(executor);
            }
        }
        ClusterState newClusterState = batchResult.resultingState;
        final ArrayList<UpdateTask<T>> proccessedListeners = new ArrayList<>();
        for (UpdateTask<T> updateTask : toExecute) {
            assert batchResult.executionResults.containsKey(updateTask.task) : "missing " + updateTask.toString(executor);
            final ClusterStateTaskExecutor.TaskResult executionResult = batchResult.executionResults.get(updateTask.task);
            executionResult.handle(() -> proccessedListeners.add(updateTask), ex -> {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("cluster state update task {} failed", updateTask.toString(executor)), ex);
                updateTask.listener.onFailure(updateTask.source, ex);
            });
        }
        if (previousClusterState == newClusterState) {
            for (UpdateTask<T> task : proccessedListeners) {
                if (task.listener instanceof AckedClusterStateTaskListener) {
                    ((AckedClusterStateTaskListener) task.listener).onAllNodesAcked(null);
                }
                task.listener.clusterStateProcessed(task.source, previousClusterState, newClusterState);
            }
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            logger.debug("processing [{}]: took [{}] no change in cluster_state", tasksSummary, executionTime);
            warnAboutSlowTaskIfNeeded(executionTime, tasksSummary);
            return;
        }
        try {
            ArrayList<Discovery.AckListener> ackListeners = new ArrayList<>();
            if (newClusterState.nodes().isLocalNodeElectedMaster()) {
                Builder builder = ClusterState.builder(newClusterState).incrementVersion();
                if (previousClusterState.routingTable() != newClusterState.routingTable()) {
                    builder.routingTable(RoutingTable.builder(newClusterState.routingTable()).version(newClusterState.routingTable().version() + 1).build());
                }
                if (previousClusterState.metaData() != newClusterState.metaData()) {
                    builder.metaData(MetaData.builder(newClusterState.metaData()).version(newClusterState.metaData().version() + 1));
                }
                newClusterState = builder.build();
                for (UpdateTask<T> task : proccessedListeners) {
                    if (task.listener instanceof AckedClusterStateTaskListener) {
                        final AckedClusterStateTaskListener ackedListener = (AckedClusterStateTaskListener) task.listener;
                        if (ackedListener.ackTimeout() == null || ackedListener.ackTimeout().millis() == 0) {
                            ackedListener.onAckTimeout();
                        } else {
                            try {
                                ackListeners.add(new AckCountDownListener(ackedListener, newClusterState.version(), newClusterState.nodes(), threadPool));
                            } catch (EsRejectedExecutionException ex) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Couldn't schedule timeout thread - node might be shutting down", ex);
                                }
                                ackedListener.onAckTimeout();
                            }
                        }
                    }
                }
            }
            final Discovery.AckListener ackListener = new DelegetingAckListener(ackListeners);
            newClusterState.status(ClusterState.ClusterStateStatus.BEING_APPLIED);
            if (logger.isTraceEnabled()) {
                logger.trace("cluster state updated, source [{}]\n{}", tasksSummary, newClusterState);
            } else if (logger.isDebugEnabled()) {
                logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), tasksSummary);
            }
            ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(tasksSummary, newClusterState, previousClusterState);
            final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
            if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                String summary = nodesDelta.shortSummary();
                if (summary.length() > 0) {
                    logger.info("{}, reason: {}", summary, tasksSummary);
                }
            }
            nodeConnectionsService.connectToNodes(clusterChangedEvent.nodesDelta().addedNodes());
            if (newClusterState.nodes().isLocalNodeElectedMaster()) {
                logger.debug("publishing cluster state version [{}]", newClusterState.version());
                try {
                    clusterStatePublisher.accept(clusterChangedEvent, ackListener);
                } catch (Discovery.FailedToCommitClusterStateException t) {
                    final long version = newClusterState.version();
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("failing [{}]: failed to commit cluster state version [{}]", tasksSummary, version), t);
                    nodeConnectionsService.disconnectFromNodes(clusterChangedEvent.nodesDelta().addedNodes());
                    proccessedListeners.forEach(task -> task.listener.onFailure(task.source, t));
                    return;
                }
            }
            clusterState = newClusterState;
            logger.debug("set local cluster state to version {}", newClusterState.version());
            try {
                if (clusterChangedEvent.state().blocks().disableStatePersistence() == false && clusterChangedEvent.metaDataChanged()) {
                    final Settings incomingSettings = clusterChangedEvent.state().metaData().settings();
                    clusterSettings.applySettings(incomingSettings);
                }
            } catch (Exception ex) {
                logger.warn("failed to apply cluster settings", ex);
            }
            for (ClusterStateListener listener : preAppliedListeners) {
                try {
                    listener.clusterChanged(clusterChangedEvent);
                } catch (Exception ex) {
                    logger.warn("failed to notify ClusterStateListener", ex);
                }
            }
            nodeConnectionsService.disconnectFromNodes(clusterChangedEvent.nodesDelta().removedNodes());
            newClusterState.status(ClusterState.ClusterStateStatus.APPLIED);
            for (ClusterStateListener listener : postAppliedListeners) {
                try {
                    listener.clusterChanged(clusterChangedEvent);
                } catch (Exception ex) {
                    logger.warn("failed to notify ClusterStateListener", ex);
                }
            }
            if (newClusterState.nodes().isLocalNodeElectedMaster()) {
                try {
                    ackListener.onNodeAck(newClusterState.nodes().getLocalNode(), null);
                } catch (Exception e) {
                    final DiscoveryNode localNode = newClusterState.nodes().getLocalNode();
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("error while processing ack for master node [{}]", localNode), e);
                }
            }
            for (UpdateTask<T> task : proccessedListeners) {
                task.listener.clusterStateProcessed(task.source, previousClusterState, newClusterState);
            }
            try {
                executor.clusterStatePublished(clusterChangedEvent);
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("exception thrown while notifying executor of new cluster state publication [{}]", tasksSummary), e);
            }
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            logger.debug("processing [{}]: took [{}] done applying updated cluster_state (version: {}, uuid: {})", tasksSummary, executionTime, newClusterState.version(), newClusterState.stateUUID());
            warnAboutSlowTaskIfNeeded(executionTime, tasksSummary);
        } catch (Exception e) {
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            final long version = newClusterState.version();
            final String stateUUID = newClusterState.stateUUID();
            final String fullState = newClusterState.toString();
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to apply updated cluster state in [{}]:\nversion [{}], uuid [{}], source [{}]\n{}", executionTime, version, stateUUID, tasksSummary, fullState), e);
        }
    }

    protected long currentTimeInNanos() {
        return System.nanoTime();
    }

    private static SafeClusterStateTaskListener safe(ClusterStateTaskListener listener, Logger logger) {
        if (listener instanceof AckedClusterStateTaskListener) {
            return new SafeAckedClusterStateTaskListener((AckedClusterStateTaskListener) listener, logger);
        } else {
            return new SafeClusterStateTaskListener(listener, logger);
        }
    }

    private static class SafeClusterStateTaskListener implements ClusterStateTaskListener {

        private final ClusterStateTaskListener listener;

        private final Logger logger;

        public SafeClusterStateTaskListener(ClusterStateTaskListener listener, Logger logger) {
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public void onFailure(String source, Exception e) {
            try {
                listener.onFailure(source, e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error((Supplier<?>) () -> new ParameterizedMessage("exception thrown by listener notifying of failure from [{}]", source), inner);
            }
        }

        @Override
        public void onNoLongerMaster(String source) {
            try {
                listener.onNoLongerMaster(source);
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("exception thrown by listener while notifying no longer master from [{}]", source), e);
            }
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            try {
                listener.clusterStateProcessed(source, oldState, newState);
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("exception thrown by listener while notifying of cluster state processed from [{}], old cluster state:\n" + "{}\nnew cluster state:\n{}", source, oldState, newState), e);
            }
        }
    }

    private static class SafeAckedClusterStateTaskListener extends SafeClusterStateTaskListener implements AckedClusterStateTaskListener {

        private final AckedClusterStateTaskListener listener;

        private final Logger logger;

        public SafeAckedClusterStateTaskListener(AckedClusterStateTaskListener listener, Logger logger) {
            super(listener, logger);
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return listener.mustAck(discoveryNode);
        }

        @Override
        public void onAllNodesAcked(@Nullable Exception e) {
            try {
                listener.onAllNodesAcked(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error("exception thrown by listener while notifying on all nodes acked", inner);
            }
        }

        @Override
        public void onAckTimeout() {
            try {
                listener.onAckTimeout();
            } catch (Exception e) {
                logger.error("exception thrown by listener while notifying on ack timeout", e);
            }
        }

        @Override
        public TimeValue ackTimeout() {
            return listener.ackTimeout();
        }
    }

    class UpdateTask<T> extends SourcePrioritizedRunnable {

        public final T task;

        public final ClusterStateTaskConfig config;

        public final ClusterStateTaskExecutor<T> executor;

        public final ClusterStateTaskListener listener;

        public final AtomicBoolean processed = new AtomicBoolean();

        UpdateTask(String source, T task, ClusterStateTaskConfig config, ClusterStateTaskExecutor<T> executor, ClusterStateTaskListener listener) {
            super(config.priority(), source);
            this.task = task;
            this.config = config;
            this.executor = executor;
            this.listener = listener;
        }

        @Override
        public void run() {
            if (processed.get() == false) {
                runTasksForExecutor(executor);
            }
        }

        public String toString(ClusterStateTaskExecutor<T> executor) {
            String taskDescription = executor.describeTasks(Collections.singletonList(task));
            if (taskDescription.isEmpty()) {
                return "[" + source + "]";
            } else {
                return "[" + source + "[" + taskDescription + "]]";
            }
        }
    }

    private void warnAboutSlowTaskIfNeeded(TimeValue executionTime, String source) {
        if (executionTime.getMillis() > slowTaskLoggingThreshold.getMillis()) {
            logger.warn("cluster state update task [{}] took [{}] above the warn threshold of {}", source, executionTime, slowTaskLoggingThreshold);
        }
    }

    class NotifyTimeout implements Runnable {

        final TimeoutClusterStateListener listener;

        final TimeValue timeout;

        volatile ScheduledFuture future;

        NotifyTimeout(TimeoutClusterStateListener listener, TimeValue timeout) {
            this.listener = listener;
            this.timeout = timeout;
        }

        public void cancel() {
            FutureUtils.cancel(future);
        }

        @Override
        public void run() {
            if (future != null && future.isCancelled()) {
                return;
            }
            if (lifecycle.stoppedOrClosed()) {
                listener.onClose();
            } else {
                listener.onTimeout(this.timeout);
            }
        }
    }

    private static class LocalNodeMasterListeners implements ClusterStateListener {

        private final List<LocalNodeMasterListener> listeners = new CopyOnWriteArrayList<>();

        private final ThreadPool threadPool;

        private volatile boolean master = false;

        private LocalNodeMasterListeners(ThreadPool threadPool) {
            this.threadPool = threadPool;
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (!master && event.localNodeMaster()) {
                master = true;
                for (LocalNodeMasterListener listener : listeners) {
                    Executor executor = threadPool.executor(listener.executorName());
                    executor.execute(new OnMasterRunnable(listener));
                }
                return;
            }
            if (master && !event.localNodeMaster()) {
                master = false;
                for (LocalNodeMasterListener listener : listeners) {
                    Executor executor = threadPool.executor(listener.executorName());
                    executor.execute(new OffMasterRunnable(listener));
                }
            }
        }

        private void add(LocalNodeMasterListener listener) {
            listeners.add(listener);
        }

        private void remove(LocalNodeMasterListener listener) {
            listeners.remove(listener);
        }

        private void clear() {
            listeners.clear();
        }
    }

    private static class OnMasterRunnable implements Runnable {

        private final LocalNodeMasterListener listener;

        private OnMasterRunnable(LocalNodeMasterListener listener) {
            this.listener = listener;
        }

        @Override
        public void run() {
            listener.onMaster();
        }
    }

    private static class OffMasterRunnable implements Runnable {

        private final LocalNodeMasterListener listener;

        private OffMasterRunnable(LocalNodeMasterListener listener) {
            this.listener = listener;
        }

        @Override
        public void run() {
            listener.offMaster();
        }
    }

    private static class DelegetingAckListener implements Discovery.AckListener {

        private final List<Discovery.AckListener> listeners;

        private DelegetingAckListener(List<Discovery.AckListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            for (Discovery.AckListener listener : listeners) {
                listener.onNodeAck(node, e);
            }
        }

        @Override
        public void onTimeout() {
            throw new UnsupportedOperationException("no timeout delegation");
        }
    }

    private static class AckCountDownListener implements Discovery.AckListener {

        private static final Logger logger = Loggers.getLogger(AckCountDownListener.class);

        private final AckedClusterStateTaskListener ackedTaskListener;

        private final CountDown countDown;

        private final DiscoveryNodes nodes;

        private final long clusterStateVersion;

        private final Future<?> ackTimeoutCallback;

        private Exception lastFailure;

        AckCountDownListener(AckedClusterStateTaskListener ackedTaskListener, long clusterStateVersion, DiscoveryNodes nodes, ThreadPool threadPool) {
            this.ackedTaskListener = ackedTaskListener;
            this.clusterStateVersion = clusterStateVersion;
            this.nodes = nodes;
            int countDown = 0;
            for (DiscoveryNode node : nodes) {
                if (ackedTaskListener.mustAck(node)) {
                    countDown++;
                }
            }
            countDown = Math.max(1, countDown);
            logger.trace("expecting {} acknowledgements for cluster_state update (version: {})", countDown, clusterStateVersion);
            this.countDown = new CountDown(countDown);
            this.ackTimeoutCallback = threadPool.schedule(ackedTaskListener.ackTimeout(), ThreadPool.Names.GENERIC, new Runnable() {

                @Override
                public void run() {
                    onTimeout();
                }
            });
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            if (!ackedTaskListener.mustAck(node)) {
                if (!node.equals(nodes.getMasterNode())) {
                    return;
                }
            }
            if (e == null) {
                logger.trace("ack received from node [{}], cluster_state update (version: {})", node, clusterStateVersion);
            } else {
                this.lastFailure = e;
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("ack received from node [{}], cluster_state update (version: {})", node, clusterStateVersion), e);
            }
            if (countDown.countDown()) {
                logger.trace("all expected nodes acknowledged cluster_state update (version: {})", clusterStateVersion);
                FutureUtils.cancel(ackTimeoutCallback);
                ackedTaskListener.onAllNodesAcked(lastFailure);
            }
        }

        @Override
        public void onTimeout() {
            if (countDown.fastForward()) {
                logger.trace("timeout waiting for acknowledgement for cluster_state update (version: {})", clusterStateVersion);
                ackedTaskListener.onAckTimeout();
            }
        }
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    public Settings getSettings() {
        return settings;
    }
}