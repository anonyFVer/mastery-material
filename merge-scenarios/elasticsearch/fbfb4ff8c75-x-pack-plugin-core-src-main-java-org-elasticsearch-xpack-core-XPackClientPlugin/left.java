package org.elasticsearch.xpack.core;

import org.elasticsearch.action.GenericAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.license.DeleteLicenseAction;
import org.elasticsearch.license.GetBasicStatusAction;
import org.elasticsearch.license.GetLicenseAction;
import org.elasticsearch.license.GetTrialStatusAction;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicensesMetaData;
import org.elasticsearch.license.PostStartBasicAction;
import org.elasticsearch.license.PostStartTrialAction;
import org.elasticsearch.license.PutLicenseAction;
import org.elasticsearch.persistent.CompletionPersistentTaskAction;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksNodeService;
import org.elasticsearch.persistent.RemovePersistentTaskAction;
import org.elasticsearch.persistent.StartPersistentTaskAction;
import org.elasticsearch.persistent.UpdatePersistentTaskStatusAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.action.XPackInfoAction;
import org.elasticsearch.xpack.core.action.XPackUsageAction;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.core.graph.GraphFeatureSetUsage;
import org.elasticsearch.xpack.core.graph.action.GraphExploreAction;
import org.elasticsearch.xpack.core.indexlifecycle.AllocateAction;
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.core.indexlifecycle.ForceMergeAction;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleFeatureSetUsage;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.ReadOnlyAction;
import org.elasticsearch.xpack.core.indexlifecycle.ReplicasAction;
import org.elasticsearch.xpack.core.indexlifecycle.RolloverAction;
import org.elasticsearch.xpack.core.indexlifecycle.ShrinkAction;
import org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.logstash.LogstashFeatureSetUsage;
import org.elasticsearch.xpack.core.ml.MachineLearningFeatureSetUsage;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarEventAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.DeleteFilterAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarsAction;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.GetOverallBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.action.MlInfoAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.action.PostCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutCalendarAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateProcessAction;
import org.elasticsearch.xpack.core.ml.action.ValidateDetectorAction;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskStatus;
import org.elasticsearch.xpack.core.monitoring.MonitoringFeatureSetUsage;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xpack.core.rollup.RollupFeatureSetUsage;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.DeleteRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupCapsAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupJobsAction;
import org.elasticsearch.xpack.core.rollup.action.PutRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;
import org.elasticsearch.xpack.core.rollup.action.StartRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.StopRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;
import org.elasticsearch.xpack.core.security.SecurityFeatureSetUsage;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.SecuritySettings;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.core.security.action.role.GetRolesAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.RefreshTokenAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.core.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledAction;
import org.elasticsearch.xpack.core.security.authc.TokenMetaData;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AllExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AnyExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExceptExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.core.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.action.GetCertificateInfoAction;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeAction;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeInfoAction;
import org.elasticsearch.xpack.core.watcher.WatcherFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.WatcherMetaData;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.delete.DeleteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class XPackClientPlugin extends Plugin implements ActionPlugin, NetworkPlugin {

    private final Settings settings;

    public XPackClientPlugin(final Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        ArrayList<Setting<?>> settings = new ArrayList<>();
        settings.add(Setting.groupSetting("license.", Setting.Property.NodeScope));
        settings.addAll(XPackSettings.getAllSettings());
        settings.add(LicenseService.SELF_GENERATED_LICENSE_TYPE);
        settings.add(Setting.simpleString("index.xpack.version", Setting.Property.IndexScope));
        return settings;
    }

    @Override
    public Settings additionalSettings() {
        return additionalSettings(settings, XPackSettings.SECURITY_ENABLED.get(settings), XPackPlugin.transportClientMode(settings));
    }

    static Settings additionalSettings(final Settings settings, final boolean enabled, final boolean transportClientMode) {
        if (enabled && transportClientMode) {
            final Settings.Builder builder = Settings.builder();
            builder.put(SecuritySettings.addTransportSettings(settings));
            builder.put(SecuritySettings.addUserSettings(settings));
            builder.put(ThreadContext.PREFIX + "." + "has_xpack", true);
            return builder.build();
        } else {
            return Settings.EMPTY;
        }
    }

    @Override
    public List<GenericAction> getClientActions() {
        return Arrays.asList(DeprecationInfoAction.INSTANCE, GraphExploreAction.INSTANCE, GetJobsAction.INSTANCE, GetJobsStatsAction.INSTANCE, MlInfoAction.INSTANCE, PutJobAction.INSTANCE, UpdateJobAction.INSTANCE, DeleteJobAction.INSTANCE, OpenJobAction.INSTANCE, GetFiltersAction.INSTANCE, PutFilterAction.INSTANCE, DeleteFilterAction.INSTANCE, KillProcessAction.INSTANCE, GetBucketsAction.INSTANCE, GetInfluencersAction.INSTANCE, GetOverallBucketsAction.INSTANCE, GetRecordsAction.INSTANCE, PostDataAction.INSTANCE, CloseJobAction.INSTANCE, FinalizeJobExecutionAction.INSTANCE, FlushJobAction.INSTANCE, ValidateDetectorAction.INSTANCE, ValidateJobConfigAction.INSTANCE, GetCategoriesAction.INSTANCE, GetModelSnapshotsAction.INSTANCE, RevertModelSnapshotAction.INSTANCE, UpdateModelSnapshotAction.INSTANCE, GetDatafeedsAction.INSTANCE, GetDatafeedsStatsAction.INSTANCE, PutDatafeedAction.INSTANCE, UpdateDatafeedAction.INSTANCE, DeleteDatafeedAction.INSTANCE, PreviewDatafeedAction.INSTANCE, StartDatafeedAction.INSTANCE, StopDatafeedAction.INSTANCE, IsolateDatafeedAction.INSTANCE, DeleteModelSnapshotAction.INSTANCE, UpdateProcessAction.INSTANCE, DeleteExpiredDataAction.INSTANCE, ForecastJobAction.INSTANCE, GetCalendarsAction.INSTANCE, PutCalendarAction.INSTANCE, DeleteCalendarAction.INSTANCE, DeleteCalendarEventAction.INSTANCE, UpdateCalendarJobAction.INSTANCE, GetCalendarEventsAction.INSTANCE, PostCalendarEventsAction.INSTANCE, PersistJobAction.INSTANCE, ClearRealmCacheAction.INSTANCE, ClearRolesCacheAction.INSTANCE, GetUsersAction.INSTANCE, PutUserAction.INSTANCE, DeleteUserAction.INSTANCE, GetRolesAction.INSTANCE, PutRoleAction.INSTANCE, DeleteRoleAction.INSTANCE, ChangePasswordAction.INSTANCE, AuthenticateAction.INSTANCE, SetEnabledAction.INSTANCE, HasPrivilegesAction.INSTANCE, GetRoleMappingsAction.INSTANCE, PutRoleMappingAction.INSTANCE, DeleteRoleMappingAction.INSTANCE, CreateTokenAction.INSTANCE, InvalidateTokenAction.INSTANCE, GetCertificateInfoAction.INSTANCE, RefreshTokenAction.INSTANCE, IndexUpgradeInfoAction.INSTANCE, IndexUpgradeAction.INSTANCE, PutWatchAction.INSTANCE, DeleteWatchAction.INSTANCE, GetWatchAction.INSTANCE, WatcherStatsAction.INSTANCE, AckWatchAction.INSTANCE, ActivateWatchAction.INSTANCE, WatcherServiceAction.INSTANCE, ExecuteWatchAction.INSTANCE, PutLicenseAction.INSTANCE, GetLicenseAction.INSTANCE, DeleteLicenseAction.INSTANCE, PostStartTrialAction.INSTANCE, GetTrialStatusAction.INSTANCE, PostStartBasicAction.INSTANCE, GetBasicStatusAction.INSTANCE, XPackInfoAction.INSTANCE, XPackUsageAction.INSTANCE, RollupSearchAction.INSTANCE, PutRollupJobAction.INSTANCE, StartRollupJobAction.INSTANCE, StopRollupJobAction.INSTANCE, DeleteRollupJobAction.INSTANCE, GetRollupJobsAction.INSTANCE, GetRollupCapsAction.INSTANCE, DeleteLifecycleAction.INSTANCE, GetLifecycleAction.INSTANCE, PutLifecycleAction.INSTANCE, ExplainLifecycleAction.INSTANCE);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.GRAPH, GraphFeatureSetUsage::new), new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.LOGSTASH, LogstashFeatureSetUsage::new), new NamedWriteableRegistry.Entry(MetaData.Custom.class, "ml", MlMetadata::new), new NamedWriteableRegistry.Entry(NamedDiff.class, "ml", MlMetadata.MlMetadataDiff::new), new NamedWriteableRegistry.Entry(PersistentTaskParams.class, StartDatafeedAction.TASK_NAME, StartDatafeedAction.DatafeedParams::new), new NamedWriteableRegistry.Entry(PersistentTaskParams.class, OpenJobAction.TASK_NAME, OpenJobAction.JobParams::new), new NamedWriteableRegistry.Entry(Task.Status.class, JobTaskStatus.NAME, JobTaskStatus::new), new NamedWriteableRegistry.Entry(Task.Status.class, DatafeedState.NAME, DatafeedState::fromStream), new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.MACHINE_LEARNING, MachineLearningFeatureSetUsage::new), new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.MONITORING, MonitoringFeatureSetUsage::new), new NamedWriteableRegistry.Entry(ClusterState.Custom.class, TokenMetaData.TYPE, TokenMetaData::new), new NamedWriteableRegistry.Entry(NamedDiff.class, TokenMetaData.TYPE, TokenMetaData::readDiffFrom), new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.SECURITY, SecurityFeatureSetUsage::new), new NamedWriteableRegistry.Entry(RoleMapperExpression.class, AllExpression.NAME, AllExpression::new), new NamedWriteableRegistry.Entry(RoleMapperExpression.class, AnyExpression.NAME, AnyExpression::new), new NamedWriteableRegistry.Entry(RoleMapperExpression.class, FieldExpression.NAME, FieldExpression::new), new NamedWriteableRegistry.Entry(RoleMapperExpression.class, ExceptExpression.NAME, ExceptExpression::new), new NamedWriteableRegistry.Entry(MetaData.Custom.class, WatcherMetaData.TYPE, WatcherMetaData::new), new NamedWriteableRegistry.Entry(NamedDiff.class, WatcherMetaData.TYPE, WatcherMetaData::readDiffFrom), new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.WATCHER, WatcherFeatureSetUsage::new), new NamedWriteableRegistry.Entry(MetaData.Custom.class, LicensesMetaData.TYPE, LicensesMetaData::new), new NamedWriteableRegistry.Entry(NamedDiff.class, LicensesMetaData.TYPE, LicensesMetaData::readDiffFrom), new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.ROLLUP, RollupFeatureSetUsage::new), new NamedWriteableRegistry.Entry(PersistentTaskParams.class, RollupJob.NAME, RollupJob::new), new NamedWriteableRegistry.Entry(Task.Status.class, RollupJobStatus.NAME, RollupJobStatus::new), new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.INDEX_LIFECYCLE, IndexLifecycleFeatureSetUsage::new), new NamedWriteableRegistry.Entry(MetaData.Custom.class, IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata::new), new NamedWriteableRegistry.Entry(NamedDiff.class, IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.IndexLifecycleMetadataDiff::new), new NamedWriteableRegistry.Entry(LifecycleType.class, TimeseriesLifecycleType.TYPE, (in) -> TimeseriesLifecycleType.INSTANCE), new NamedWriteableRegistry.Entry(LifecycleAction.class, AllocateAction.NAME, AllocateAction::new), new NamedWriteableRegistry.Entry(LifecycleAction.class, ForceMergeAction.NAME, ForceMergeAction::new), new NamedWriteableRegistry.Entry(LifecycleAction.class, ReadOnlyAction.NAME, ReadOnlyAction::new), new NamedWriteableRegistry.Entry(LifecycleAction.class, ReplicasAction.NAME, ReplicasAction::new), new NamedWriteableRegistry.Entry(LifecycleAction.class, RolloverAction.NAME, RolloverAction::new), new NamedWriteableRegistry.Entry(LifecycleAction.class, ShrinkAction.NAME, ShrinkAction::new), new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new));
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField("ml"), parser -> MlMetadata.METADATA_PARSER.parse(parser, null).build()), new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(StartDatafeedAction.TASK_NAME), StartDatafeedAction.DatafeedParams::fromXContent), new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(OpenJobAction.TASK_NAME), OpenJobAction.JobParams::fromXContent), new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(DatafeedState.NAME), DatafeedState::fromXContent), new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(JobTaskStatus.NAME), JobTaskStatus::fromXContent), new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(WatcherMetaData.TYPE), WatcherMetaData::fromXContent), new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(LicensesMetaData.TYPE), LicensesMetaData::fromXContent), new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(RollupField.TASK_NAME), RollupJob::fromXContent), new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(RollupJobStatus.NAME), RollupJobStatus::fromXContent), new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(IndexLifecycleMetadata.TYPE), parser -> IndexLifecycleMetadata.PARSER.parse(parser, null)), new NamedXContentRegistry.Entry(LifecycleType.class, new ParseField(TimeseriesLifecycleType.TYPE), (p, c) -> TimeseriesLifecycleType.INSTANCE), new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse), new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse), new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReadOnlyAction.NAME), ReadOnlyAction::parse), new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReplicasAction.NAME), ReplicasAction::parse), new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse), new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse), new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(final Settings settings, final ThreadPool threadPool, final BigArrays bigArrays, final PageCacheRecycler pageCacheRecycler, final CircuitBreakerService circuitBreakerService, final NamedWriteableRegistry namedWriteableRegistry, final NetworkService networkService) {
        if (XPackPlugin.transportClientMode(settings) == false || XPackSettings.SECURITY_ENABLED.get(settings) == false) {
            return Collections.emptyMap();
        }
        final SSLService sslService;
        try {
            sslService = new SSLService(settings, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return Collections.singletonMap(SecurityField.NAME4, () -> new SecurityNetty4Transport(settings, threadPool, networkService, bigArrays, namedWriteableRegistry, circuitBreakerService, sslService));
    }
}