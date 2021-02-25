package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.TimeoutClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import java.util.Collections;
import java.util.Map;

public class ClusterService extends AbstractLifecycleComponent {

    private final MasterService masterService;

    private final ClusterApplierService clusterApplierService;

    public static final Setting<TimeValue> CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING = Setting.positiveTimeSetting("cluster.service.slow_task_logging_threshold", TimeValue.timeValueSeconds(30), Property.Dynamic, Property.NodeScope);

    private final ClusterName clusterName;

    private final OperationRouting operationRouting;

    private final ClusterSettings clusterSettings;

    public ClusterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        super(settings);
        this.masterService = new MasterService(settings, threadPool);
        this.operationRouting = new OperationRouting(settings, clusterSettings);
        this.clusterSettings = clusterSettings;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.clusterSettings.addSettingsUpdateConsumer(CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING, this::setSlowTaskLoggingThreshold);
        this.clusterApplierService = new ClusterApplierService(settings, clusterSettings, threadPool);
    }

    private void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        masterService.setSlowTaskLoggingThreshold(slowTaskLoggingThreshold);
        clusterApplierService.setSlowTaskLoggingThreshold(slowTaskLoggingThreshold);
    }

    public synchronized void setNodeConnectionsService(NodeConnectionsService nodeConnectionsService) {
        clusterApplierService.setNodeConnectionsService(nodeConnectionsService);
    }

    @Override
    protected synchronized void doStart() {
        clusterApplierService.start();
        masterService.start();
    }

    @Override
    protected synchronized void doStop() {
        masterService.stop();
        clusterApplierService.stop();
    }

    @Override
    protected synchronized void doClose() {
        masterService.close();
        clusterApplierService.close();
    }

    public DiscoveryNode localNode() {
        DiscoveryNode localNode = state().getNodes().getLocalNode();
        if (localNode == null) {
            throw new IllegalStateException("No local node found. Is the node started?");
        }
        return localNode;
    }

    public OperationRouting operationRouting() {
        return operationRouting;
    }

    public ClusterState state() {
        return clusterApplierService.state();
    }

    public void addHighPriorityApplier(ClusterStateApplier applier) {
        clusterApplierService.addHighPriorityApplier(applier);
    }

    public void addLowPriorityApplier(ClusterStateApplier applier) {
        clusterApplierService.addLowPriorityApplier(applier);
    }

    public void addStateApplier(ClusterStateApplier applier) {
        clusterApplierService.addStateApplier(applier);
    }

    public void removeApplier(ClusterStateApplier applier) {
        clusterApplierService.removeApplier(applier);
    }

    public void addListener(ClusterStateListener listener) {
        clusterApplierService.addListener(listener);
    }

    public void removeListener(ClusterStateListener listener) {
        clusterApplierService.removeListener(listener);
    }

    public void removeTimeoutListener(TimeoutClusterStateListener listener) {
        clusterApplierService.removeTimeoutListener(listener);
    }

    public void addLocalNodeMasterListener(LocalNodeMasterListener listener) {
        clusterApplierService.addLocalNodeMasterListener(listener);
    }

    public void removeLocalNodeMasterListener(LocalNodeMasterListener listener) {
        clusterApplierService.removeLocalNodeMasterListener(listener);
    }

    public void addTimeoutListener(@Nullable final TimeValue timeout, final TimeoutClusterStateListener listener) {
        clusterApplierService.addTimeoutListener(timeout, listener);
    }

    public MasterService getMasterService() {
        return masterService;
    }

    public ClusterApplierService getClusterApplierService() {
        return clusterApplierService;
    }

    public static boolean assertClusterOrMasterStateThread() {
        assert Thread.currentThread().getName().contains(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME) || Thread.currentThread().getName().contains(MasterService.MASTER_UPDATE_THREAD_NAME) : "not called from the master/cluster state update thread";
        return true;
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    public Settings getSettings() {
        return settings;
    }

    public <T extends ClusterStateTaskConfig & ClusterStateTaskExecutor<T> & ClusterStateTaskListener> void submitStateUpdateTask(String source, T updateTask) {
        submitStateUpdateTask(source, updateTask, updateTask, updateTask, updateTask);
    }

    public <T> void submitStateUpdateTask(String source, T task, ClusterStateTaskConfig config, ClusterStateTaskExecutor<T> executor, ClusterStateTaskListener listener) {
        submitStateUpdateTasks(source, Collections.singletonMap(task, listener), config, executor);
    }

    public <T> void submitStateUpdateTasks(final String source, final Map<T, ClusterStateTaskListener> tasks, final ClusterStateTaskConfig config, final ClusterStateTaskExecutor<T> executor) {
        masterService.submitStateUpdateTasks(source, tasks, config, executor);
    }
}