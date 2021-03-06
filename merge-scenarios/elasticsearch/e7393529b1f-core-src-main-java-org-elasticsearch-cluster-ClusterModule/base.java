package org.elasticsearch.cluster;

import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.index.NodeMappingRefreshAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateFilter;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexAliasesService;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.metadata.MetaDataUpdateSettingsService;
import org.elasticsearch.cluster.routing.DelayedAllocationService;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeVersionAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.RebalanceOnlyWhenActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ExtensionPoint;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.tasks.TaskResultsService;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ClusterModule extends AbstractModule {

    public static final String EVEN_SHARD_COUNT_ALLOCATOR = "even_shard";

    public static final String BALANCED_ALLOCATOR = "balanced";

    public static final Setting<String> SHARDS_ALLOCATOR_TYPE_SETTING = new Setting<>("cluster.routing.allocation.type", BALANCED_ALLOCATOR, Function.identity(), Property.NodeScope);

    private final Settings settings;

    private final ExtensionPoint.SelectedType<ShardsAllocator> shardsAllocators = new ExtensionPoint.SelectedType<>("shards_allocator", ShardsAllocator.class);

    private final ExtensionPoint.ClassSet<IndexTemplateFilter> indexTemplateFilters = new ExtensionPoint.ClassSet<>("index_template_filter", IndexTemplateFilter.class);

    private final ClusterService clusterService;

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    final Collection<AllocationDecider> allocationDeciders;

    Class<? extends ClusterInfoService> clusterInfoServiceImpl = InternalClusterInfoService.class;

    public ClusterModule(Settings settings, ClusterService clusterService, List<ClusterPlugin> clusterPlugins) {
        this.settings = settings;
        this.allocationDeciders = createAllocationDeciders(settings, clusterService.getClusterSettings(), clusterPlugins);
        registerShardsAllocator(ClusterModule.BALANCED_ALLOCATOR, BalancedShardsAllocator.class);
        registerShardsAllocator(ClusterModule.EVEN_SHARD_COUNT_ALLOCATOR, BalancedShardsAllocator.class);
        this.clusterService = clusterService;
        indexNameExpressionResolver = new IndexNameExpressionResolver(settings);
    }

    public void registerShardsAllocator(String name, Class<? extends ShardsAllocator> clazz) {
        shardsAllocators.registerExtension(name, clazz);
    }

    public void registerIndexTemplateFilter(Class<? extends IndexTemplateFilter> indexTemplateFilter) {
        indexTemplateFilters.registerExtension(indexTemplateFilter);
    }

    public IndexNameExpressionResolver getIndexNameExpressionResolver() {
        return indexNameExpressionResolver;
    }

    public static Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings, List<ClusterPlugin> clusterPlugins) {
        Map<Class, AllocationDecider> deciders = new HashMap<>();
        addAllocationDecider(deciders, new MaxRetryAllocationDecider(settings));
        addAllocationDecider(deciders, new SameShardAllocationDecider(settings));
        addAllocationDecider(deciders, new FilterAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ReplicaAfterPrimaryActiveAllocationDecider(settings));
        addAllocationDecider(deciders, new ThrottlingAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new RebalanceOnlyWhenActiveAllocationDecider(settings));
        addAllocationDecider(deciders, new ClusterRebalanceAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ConcurrentRebalanceAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new EnableAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new AwarenessAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ShardsLimitAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new NodeVersionAllocationDecider(settings));
        addAllocationDecider(deciders, new DiskThresholdDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new SnapshotInProgressAllocationDecider(settings, clusterSettings));
        clusterPlugins.stream().flatMap(p -> p.createAllocationDeciders(settings, clusterSettings).stream()).forEach(d -> addAllocationDecider(deciders, d));
        return deciders.values();
    }

    private static void addAllocationDecider(Map<Class, AllocationDecider> deciders, AllocationDecider decider) {
        if (deciders.put(decider.getClass(), decider) != null) {
            throw new IllegalArgumentException("Cannot specify allocation decider [" + decider.getClass().getName() + "] twice");
        }
    }

    @Override
    protected void configure() {
        String shardsAllocatorType = shardsAllocators.bindType(binder(), settings, ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), ClusterModule.BALANCED_ALLOCATOR);
        if (shardsAllocatorType.equals(ClusterModule.EVEN_SHARD_COUNT_ALLOCATOR)) {
            final ESLogger logger = Loggers.getLogger(getClass(), settings);
            logger.warn("{} allocator has been removed in 2.0 using {} instead", ClusterModule.EVEN_SHARD_COUNT_ALLOCATOR, ClusterModule.BALANCED_ALLOCATOR);
        }
        indexTemplateFilters.bind(binder());
        bind(ClusterInfoService.class).to(clusterInfoServiceImpl).asEagerSingleton();
        bind(GatewayAllocator.class).asEagerSingleton();
        bind(AllocationService.class).asEagerSingleton();
        bind(ClusterService.class).toInstance(clusterService);
        bind(NodeConnectionsService.class).asEagerSingleton();
        bind(MetaDataCreateIndexService.class).asEagerSingleton();
        bind(MetaDataDeleteIndexService.class).asEagerSingleton();
        bind(MetaDataIndexStateService.class).asEagerSingleton();
        bind(MetaDataMappingService.class).asEagerSingleton();
        bind(MetaDataIndexAliasesService.class).asEagerSingleton();
        bind(MetaDataUpdateSettingsService.class).asEagerSingleton();
        bind(MetaDataIndexTemplateService.class).asEagerSingleton();
        bind(IndexNameExpressionResolver.class).toInstance(indexNameExpressionResolver);
        bind(RoutingService.class).asEagerSingleton();
        bind(DelayedAllocationService.class).asEagerSingleton();
        bind(ShardStateAction.class).asEagerSingleton();
        bind(NodeMappingRefreshAction.class).asEagerSingleton();
        bind(MappingUpdatedAction.class).asEagerSingleton();
        bind(TaskResultsService.class).asEagerSingleton();
        bind(AllocationDeciders.class).toInstance(new AllocationDeciders(settings, allocationDeciders));
    }
}