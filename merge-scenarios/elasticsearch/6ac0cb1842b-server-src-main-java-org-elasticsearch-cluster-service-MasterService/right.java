package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Assertions;
import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.threadpool.ThreadPool;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import static org.elasticsearch.cluster.service.ClusterService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class MasterService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(MasterService.class);

    public static final String MASTER_UPDATE_THREAD_NAME = "masterService#updateTask";

    private final String nodeName;

    private BiConsumer<ClusterChangedEvent, Discovery.AckListener> clusterStatePublisher;

    private java.util.function.Supplier<ClusterState> clusterStateSupplier;

    private volatile TimeValue slowTaskLoggingThreshold;

    protected final ThreadPool threadPool;

    private volatile PrioritizedEsThreadPoolExecutor threadPoolExecutor;

    private volatile Batcher taskBatcher;

    public MasterService(String nodeName, Settings settings, ThreadPool threadPool) {
        super(settings);
        this.nodeName = nodeName;
        this.slowTaskLoggingThreshold = CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);
        this.threadPool = threadPool;
    }

    public void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    public synchronized void setClusterStatePublisher(BiConsumer<ClusterChangedEvent, Discovery.AckListener> publisher) {
        clusterStatePublisher = publisher;
    }

    public synchronized void setClusterStateSupplier(java.util.function.Supplier<ClusterState> clusterStateSupplier) {
        this.clusterStateSupplier = clusterStateSupplier;
    }

    @Override
    protected synchronized void doStart() {
        Objects.requireNonNull(clusterStatePublisher, "please set a cluster state publisher before starting");
        Objects.requireNonNull(clusterStateSupplier, "please set a cluster state supplier before starting");
        threadPoolExecutor = EsExecutors.newSinglePrioritizing(nodeName + "/" + MASTER_UPDATE_THREAD_NAME, daemonThreadFactory(nodeName, MASTER_UPDATE_THREAD_NAME), threadPool.getThreadContext(), threadPool.scheduler());
        taskBatcher = new Batcher(logger, threadPoolExecutor);
    }

    class Batcher extends TaskBatcher {

        Batcher(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor) {
            super(logger, threadExecutor);
        }

        @Override
        protected void onTimeout(List<? extends BatchedTask> tasks, TimeValue timeout) {
            threadPool.generic().execute(() -> tasks.forEach(task -> ((UpdateTask) task).listener.onFailure(task.source, new ProcessClusterEventTimeoutException(timeout, task.source))));
        }

        @Override
        protected void run(Object batchingKey, List<? extends BatchedTask> tasks, String tasksSummary) {
            ClusterStateTaskExecutor<Object> taskExecutor = (ClusterStateTaskExecutor<Object>) batchingKey;
            List<UpdateTask> updateTasks = (List<UpdateTask>) tasks;
            runTasks(new TaskInputs(taskExecutor, updateTasks, tasksSummary));
        }

        class UpdateTask extends BatchedTask {

            final ClusterStateTaskListener listener;

            UpdateTask(Priority priority, String source, Object task, ClusterStateTaskListener listener, ClusterStateTaskExecutor<?> executor) {
                super(priority, source, executor, task);
                this.listener = listener;
            }

            @Override
            public String describeTasks(List<? extends BatchedTask> tasks) {
                return ((ClusterStateTaskExecutor<Object>) batchingKey).describeTasks(tasks.stream().map(BatchedTask::getTask).collect(Collectors.toList()));
            }
        }
    }

    @Override
    protected synchronized void doStop() {
        ThreadPool.terminate(threadPoolExecutor, 10, TimeUnit.SECONDS);
    }

    @Override
    protected synchronized void doClose() {
    }

    ClusterState state() {
        return clusterStateSupplier.get();
    }

    public static boolean assertMasterUpdateThread() {
        assert Thread.currentThread().getName().contains(MASTER_UPDATE_THREAD_NAME) : "not called from the master service thread";
        return true;
    }

    public static boolean assertNotMasterUpdateThread(String reason) {
        assert Thread.currentThread().getName().contains(MASTER_UPDATE_THREAD_NAME) == false : "Expected current thread [" + Thread.currentThread() + "] to not be the master service thread. Reason: [" + reason + "]";
        return true;
    }

    protected void runTasks(TaskInputs taskInputs) {
        final String summary = taskInputs.summary;
        if (!lifecycle.started()) {
            logger.debug("processing [{}]: ignoring, master service not started", summary);
            return;
        }
        logger.debug("processing [{}]: execute", summary);
        final ClusterState previousClusterState = state();
        if (!previousClusterState.nodes().isLocalNodeElectedMaster() && taskInputs.runOnlyWhenMaster()) {
            logger.debug("failing [{}]: local node is no longer master", summary);
            taskInputs.onNoLongerMaster();
            return;
        }
        long startTimeNS = currentTimeInNanos();
        TaskOutputs taskOutputs = calculateTaskOutputs(taskInputs, previousClusterState, startTimeNS);
        taskOutputs.notifyFailedTasks();
        if (taskOutputs.clusterStateUnchanged()) {
            taskOutputs.notifySuccessfulTasksOnUnchangedClusterState();
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            logger.debug("processing [{}]: took [{}] no change in cluster state", summary, executionTime);
            warnAboutSlowTaskIfNeeded(executionTime, summary);
        } else {
            ClusterState newClusterState = taskOutputs.newClusterState;
            if (logger.isTraceEnabled()) {
                logger.trace("cluster state updated, source [{}]\n{}", summary, newClusterState);
            } else if (logger.isDebugEnabled()) {
                logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), summary);
            }
            try {
                ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(summary, newClusterState, previousClusterState);
                final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
                if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                    String nodeSummary = nodesDelta.shortSummary();
                    if (nodeSummary.length() > 0) {
                        logger.info("{}, reason: {}", summary, nodeSummary);
                    }
                }
                logger.debug("publishing cluster state version [{}]", newClusterState.version());
                try {
                    clusterStatePublisher.accept(clusterChangedEvent, taskOutputs.createAckListener(threadPool, newClusterState));
                } catch (Discovery.FailedToCommitClusterStateException t) {
                    final long version = newClusterState.version();
                    logger.warn(() -> new ParameterizedMessage("failing [{}]: failed to commit cluster state version [{}]", summary, version), t);
                    taskOutputs.publishingFailed(t);
                    return;
                }
                taskOutputs.processedDifferentClusterState(previousClusterState, newClusterState);
                try {
                    taskOutputs.clusterStatePublished(clusterChangedEvent);
                } catch (Exception e) {
                    logger.error(() -> new ParameterizedMessage("exception thrown while notifying executor of new cluster state publication [{}]", summary), e);
                }
                TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
                logger.debug("processing [{}]: took [{}] done publishing updated cluster state (version: {}, uuid: {})", summary, executionTime, newClusterState.version(), newClusterState.stateUUID());
                warnAboutSlowTaskIfNeeded(executionTime, summary);
            } catch (Exception e) {
                TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
                final long version = newClusterState.version();
                final String stateUUID = newClusterState.stateUUID();
                final String fullState = newClusterState.toString();
                logger.warn(() -> new ParameterizedMessage("failed to publish updated cluster state in [{}]:\nversion [{}], uuid [{}], source [{}]\n{}", executionTime, version, stateUUID, summary, fullState), e);
            }
        }
    }

    public TaskOutputs calculateTaskOutputs(TaskInputs taskInputs, ClusterState previousClusterState, long startTimeNS) {
        ClusterTasksResult<Object> clusterTasksResult = executeTasks(taskInputs, startTimeNS, previousClusterState);
        ClusterState newClusterState = patchVersions(previousClusterState, clusterTasksResult);
        return new TaskOutputs(taskInputs, previousClusterState, newClusterState, getNonFailedTasks(taskInputs, clusterTasksResult), clusterTasksResult.executionResults);
    }

    private ClusterState patchVersions(ClusterState previousClusterState, ClusterTasksResult<?> executionResult) {
        ClusterState newClusterState = executionResult.resultingState;
        if (previousClusterState != newClusterState) {
            Builder builder = ClusterState.builder(newClusterState).incrementVersion();
            if (previousClusterState.routingTable() != newClusterState.routingTable()) {
                builder.routingTable(RoutingTable.builder(newClusterState.routingTable()).version(newClusterState.routingTable().version() + 1).build());
            }
            if (previousClusterState.metaData() != newClusterState.metaData()) {
                builder.metaData(MetaData.builder(newClusterState.metaData()).version(newClusterState.metaData().version() + 1));
            }
            newClusterState = builder.build();
        }
        return newClusterState;
    }

    public <T extends ClusterStateTaskConfig & ClusterStateTaskExecutor<T> & ClusterStateTaskListener> void submitStateUpdateTask(String source, T updateTask) {
        submitStateUpdateTask(source, updateTask, updateTask, updateTask, updateTask);
    }

    public <T> void submitStateUpdateTask(String source, T task, ClusterStateTaskConfig config, ClusterStateTaskExecutor<T> executor, ClusterStateTaskListener listener) {
        submitStateUpdateTasks(source, Collections.singletonMap(task, listener), config, executor);
    }

    class TaskOutputs {

        public final TaskInputs taskInputs;

        public final ClusterState previousClusterState;

        public final ClusterState newClusterState;

        public final List<Batcher.UpdateTask> nonFailedTasks;

        public final Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults;

        TaskOutputs(TaskInputs taskInputs, ClusterState previousClusterState, ClusterState newClusterState, List<Batcher.UpdateTask> nonFailedTasks, Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults) {
            this.taskInputs = taskInputs;
            this.previousClusterState = previousClusterState;
            this.newClusterState = newClusterState;
            this.nonFailedTasks = nonFailedTasks;
            this.executionResults = executionResults;
        }

        public void publishingFailed(Discovery.FailedToCommitClusterStateException t) {
            nonFailedTasks.forEach(task -> task.listener.onFailure(task.source(), t));
        }

        public void processedDifferentClusterState(ClusterState previousClusterState, ClusterState newClusterState) {
            nonFailedTasks.forEach(task -> task.listener.clusterStateProcessed(task.source(), previousClusterState, newClusterState));
        }

        public void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
            taskInputs.executor.clusterStatePublished(clusterChangedEvent);
        }

        public Discovery.AckListener createAckListener(ThreadPool threadPool, ClusterState newClusterState) {
            return new DelegatingAckListener(nonFailedTasks.stream().filter(task -> task.listener instanceof AckedClusterStateTaskListener).map(task -> new AckCountDownListener((AckedClusterStateTaskListener) task.listener, newClusterState.version(), newClusterState.nodes(), threadPool)).collect(Collectors.toList()));
        }

        public boolean clusterStateUnchanged() {
            return previousClusterState == newClusterState;
        }

        public void notifyFailedTasks() {
            for (Batcher.UpdateTask updateTask : taskInputs.updateTasks) {
                assert executionResults.containsKey(updateTask.task) : "missing " + updateTask;
                final ClusterStateTaskExecutor.TaskResult taskResult = executionResults.get(updateTask.task);
                if (taskResult.isSuccess() == false) {
                    updateTask.listener.onFailure(updateTask.source(), taskResult.getFailure());
                }
            }
        }

        public void notifySuccessfulTasksOnUnchangedClusterState() {
            nonFailedTasks.forEach(task -> {
                if (task.listener instanceof AckedClusterStateTaskListener) {
                    ((AckedClusterStateTaskListener) task.listener).onAllNodesAcked(null);
                }
                task.listener.clusterStateProcessed(task.source(), newClusterState, newClusterState);
            });
        }
    }

    public List<PendingClusterTask> pendingTasks() {
        return Arrays.stream(threadPoolExecutor.getPending()).map(pending -> {
            assert pending.task instanceof SourcePrioritizedRunnable : "thread pool executor should only use SourcePrioritizedRunnable instances but found: " + pending.task.getClass().getName();
            SourcePrioritizedRunnable task = (SourcePrioritizedRunnable) pending.task;
            return new PendingClusterTask(pending.insertionOrder, pending.priority, new Text(task.source()), task.getAgeInMillis(), pending.executing);
        }).collect(Collectors.toList());
    }

    public int numberOfPendingTasks() {
        return threadPoolExecutor.getNumberOfPendingTasks();
    }

    public TimeValue getMaxTaskWaitTime() {
        return threadPoolExecutor.getMaxTaskWaitTime();
    }

    private SafeClusterStateTaskListener safe(ClusterStateTaskListener listener, Supplier<ThreadContext.StoredContext> contextSupplier) {
        if (listener instanceof AckedClusterStateTaskListener) {
            return new SafeAckedClusterStateTaskListener((AckedClusterStateTaskListener) listener, contextSupplier, logger);
        } else {
            return new SafeClusterStateTaskListener(listener, contextSupplier, logger);
        }
    }

    private static class SafeClusterStateTaskListener implements ClusterStateTaskListener {

        private final ClusterStateTaskListener listener;

        protected final Supplier<ThreadContext.StoredContext> context;

        private final Logger logger;

        SafeClusterStateTaskListener(ClusterStateTaskListener listener, Supplier<ThreadContext.StoredContext> context, Logger logger) {
            this.listener = listener;
            this.context = context;
            this.logger = logger;
        }

        @Override
        public void onFailure(String source, Exception e) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.onFailure(source, e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error(() -> new ParameterizedMessage("exception thrown by listener notifying of failure from [{}]", source), inner);
            }
        }

        @Override
        public void onNoLongerMaster(String source) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.onNoLongerMaster(source);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("exception thrown by listener while notifying no longer master from [{}]", source), e);
            }
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.clusterStateProcessed(source, oldState, newState);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("exception thrown by listener while notifying of cluster state processed from [{}], old cluster state:\n" + "{}\nnew cluster state:\n{}", source, oldState, newState), e);
            }
        }
    }

    private static class SafeAckedClusterStateTaskListener extends SafeClusterStateTaskListener implements AckedClusterStateTaskListener {

        private final AckedClusterStateTaskListener listener;

        private final Logger logger;

        SafeAckedClusterStateTaskListener(AckedClusterStateTaskListener listener, Supplier<ThreadContext.StoredContext> context, Logger logger) {
            super(listener, context, logger);
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return listener.mustAck(discoveryNode);
        }

        @Override
        public void onAllNodesAcked(@Nullable Exception e) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.onAllNodesAcked(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error("exception thrown by listener while notifying on all nodes acked", inner);
            }
        }

        @Override
        public void onAckTimeout() {
            try (ThreadContext.StoredContext ignore = context.get()) {
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

    protected void warnAboutSlowTaskIfNeeded(TimeValue executionTime, String source) {
        if (executionTime.getMillis() > slowTaskLoggingThreshold.getMillis()) {
            logger.warn("cluster state update task [{}] took [{}] above the warn threshold of {}", source, executionTime, slowTaskLoggingThreshold);
        }
    }

    private static class DelegatingAckListener implements Discovery.AckListener {

        private final List<Discovery.AckListener> listeners;

        private DelegatingAckListener(List<Discovery.AckListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void onCommit(TimeValue commitTime) {
            for (Discovery.AckListener listener : listeners) {
                listener.onCommit(commitTime);
            }
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            for (Discovery.AckListener listener : listeners) {
                listener.onNodeAck(node, e);
            }
        }
    }

    private static class AckCountDownListener implements Discovery.AckListener {

        private static final Logger logger = LogManager.getLogger(AckCountDownListener.class);

        private final AckedClusterStateTaskListener ackedTaskListener;

        private final CountDown countDown;

        private final DiscoveryNode masterNode;

        private final ThreadPool threadPool;

        private final long clusterStateVersion;

        private volatile Future<?> ackTimeoutCallback;

        private Exception lastFailure;

        AckCountDownListener(AckedClusterStateTaskListener ackedTaskListener, long clusterStateVersion, DiscoveryNodes nodes, ThreadPool threadPool) {
            this.ackedTaskListener = ackedTaskListener;
            this.clusterStateVersion = clusterStateVersion;
            this.threadPool = threadPool;
            this.masterNode = nodes.getMasterNode();
            int countDown = 0;
            for (DiscoveryNode node : nodes) {
                if (node.equals(masterNode) || ackedTaskListener.mustAck(node)) {
                    countDown++;
                }
            }
            logger.trace("expecting {} acknowledgements for cluster_state update (version: {})", countDown, clusterStateVersion);
            this.countDown = new CountDown(countDown + 1);
        }

        @Override
        public void onCommit(TimeValue commitTime) {
            TimeValue ackTimeout = ackedTaskListener.ackTimeout();
            if (ackTimeout == null) {
                ackTimeout = TimeValue.ZERO;
            }
            final TimeValue timeLeft = TimeValue.timeValueNanos(Math.max(0, ackTimeout.nanos() - commitTime.nanos()));
            if (timeLeft.nanos() == 0L) {
                onTimeout();
            } else if (countDown.countDown()) {
                finish();
            } else {
                this.ackTimeoutCallback = threadPool.schedule(timeLeft, ThreadPool.Names.GENERIC, this::onTimeout);
                if (countDown.isCountedDown()) {
                    FutureUtils.cancel(ackTimeoutCallback);
                }
            }
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            if (node.equals(masterNode) == false && ackedTaskListener.mustAck(node) == false) {
                return;
            }
            if (e == null) {
                logger.trace("ack received from node [{}], cluster_state update (version: {})", node, clusterStateVersion);
            } else {
                this.lastFailure = e;
                logger.debug(() -> new ParameterizedMessage("ack received from node [{}], cluster_state update (version: {})", node, clusterStateVersion), e);
            }
            if (countDown.countDown()) {
                finish();
            }
        }

        private void finish() {
            logger.trace("all expected nodes acknowledged cluster_state update (version: {})", clusterStateVersion);
            FutureUtils.cancel(ackTimeoutCallback);
            ackedTaskListener.onAllNodesAcked(lastFailure);
        }

        public void onTimeout() {
            if (countDown.fastForward()) {
                logger.trace("timeout waiting for acknowledgement for cluster_state update (version: {})", clusterStateVersion);
                ackedTaskListener.onAckTimeout();
            }
        }
    }

    protected ClusterTasksResult<Object> executeTasks(TaskInputs taskInputs, long startTimeNS, ClusterState previousClusterState) {
        ClusterTasksResult<Object> clusterTasksResult;
        try {
            List<Object> inputs = taskInputs.updateTasks.stream().map(tUpdateTask -> tUpdateTask.task).collect(Collectors.toList());
            clusterTasksResult = taskInputs.executor.execute(previousClusterState, inputs);
            if (previousClusterState != clusterTasksResult.resultingState && previousClusterState.nodes().isLocalNodeElectedMaster() && (clusterTasksResult.resultingState.nodes().isLocalNodeElectedMaster() == false)) {
                throw new AssertionError("update task submitted to MasterService cannot remove master");
            }
        } catch (Exception e) {
            TimeValue executionTime = TimeValue.timeValueMillis(Math.max(0, TimeValue.nsecToMSec(currentTimeInNanos() - startTimeNS)));
            if (logger.isTraceEnabled()) {
                logger.trace(() -> new ParameterizedMessage("failed to execute cluster state update in [{}], state:\nversion [{}], source [{}]\n{}{}{}", executionTime, previousClusterState.version(), taskInputs.summary, previousClusterState.nodes(), previousClusterState.routingTable(), previousClusterState.getRoutingNodes()), e);
            }
            warnAboutSlowTaskIfNeeded(executionTime, taskInputs.summary);
            clusterTasksResult = ClusterTasksResult.builder().failures(taskInputs.updateTasks.stream().map(updateTask -> updateTask.task)::iterator, e).build(previousClusterState);
        }
        assert clusterTasksResult.executionResults != null;
        assert clusterTasksResult.executionResults.size() == taskInputs.updateTasks.size() : String.format(Locale.ROOT, "expected [%d] task result%s but was [%d]", taskInputs.updateTasks.size(), taskInputs.updateTasks.size() == 1 ? "" : "s", clusterTasksResult.executionResults.size());
        if (Assertions.ENABLED) {
            ClusterTasksResult<Object> finalClusterTasksResult = clusterTasksResult;
            taskInputs.updateTasks.forEach(updateTask -> {
                assert finalClusterTasksResult.executionResults.containsKey(updateTask.task) : "missing task result for " + updateTask;
            });
        }
        return clusterTasksResult;
    }

    public List<Batcher.UpdateTask> getNonFailedTasks(TaskInputs taskInputs, ClusterTasksResult<Object> clusterTasksResult) {
        return taskInputs.updateTasks.stream().filter(updateTask -> {
            assert clusterTasksResult.executionResults.containsKey(updateTask.task) : "missing " + updateTask;
            final ClusterStateTaskExecutor.TaskResult taskResult = clusterTasksResult.executionResults.get(updateTask.task);
            return taskResult.isSuccess();
        }).collect(Collectors.toList());
    }

    protected class TaskInputs {

        public final String summary;

        public final List<Batcher.UpdateTask> updateTasks;

        public final ClusterStateTaskExecutor<Object> executor;

        TaskInputs(ClusterStateTaskExecutor<Object> executor, List<Batcher.UpdateTask> updateTasks, String summary) {
            this.summary = summary;
            this.executor = executor;
            this.updateTasks = updateTasks;
        }

        public boolean runOnlyWhenMaster() {
            return executor.runOnlyOnMaster();
        }

        public void onNoLongerMaster() {
            updateTasks.forEach(task -> task.listener.onNoLongerMaster(task.source()));
        }
    }

    public <T> void submitStateUpdateTasks(final String source, final Map<T, ClusterStateTaskListener> tasks, final ClusterStateTaskConfig config, final ClusterStateTaskExecutor<T> executor) {
        if (!lifecycle.started()) {
            return;
        }
        final ThreadContext threadContext = threadPool.getThreadContext();
        final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.markAsSystemContext();
            List<Batcher.UpdateTask> safeTasks = tasks.entrySet().stream().map(e -> taskBatcher.new UpdateTask(config.priority(), source, e.getKey(), safe(e.getValue(), supplier), executor)).collect(Collectors.toList());
            taskBatcher.submitTasks(safeTasks, config.timeout());
        } catch (EsRejectedExecutionException e) {
            if (!lifecycle.stoppedOrClosed()) {
                throw e;
            }
        }
    }

    protected long currentTimeInNanos() {
        return System.nanoTime();
    }
}