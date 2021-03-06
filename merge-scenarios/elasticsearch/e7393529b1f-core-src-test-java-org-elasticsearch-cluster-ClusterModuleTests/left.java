package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.metadata.IndexTemplateFilter;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.plugins.ClusterPlugin;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ClusterModuleTests extends ModuleTestCase {

    private ClusterService clusterService = new ClusterService(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null);

    static class FakeAllocationDecider extends AllocationDecider {

        protected FakeAllocationDecider(Settings settings) {
            super(settings);
        }
    }

    static class FakeShardsAllocator implements ShardsAllocator {

        @Override
        public void allocate(RoutingAllocation allocation) {
        }

        @Override
        public Map<DiscoveryNode, Float> weighShard(RoutingAllocation allocation, ShardRouting shard) {
            return new HashMap<>();
        }
    }

    public void testRegisterClusterDynamicSettingDuplicate() {
        try {
            new SettingsModule(Settings.EMPTY, EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Cannot register setting [" + EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey() + "] twice");
        }
    }

    public void testRegisterClusterDynamicSetting() {
        SettingsModule module = new SettingsModule(Settings.EMPTY, Setting.boolSetting("foo.bar", false, Property.Dynamic, Property.NodeScope));
        assertInstanceBinding(module, ClusterSettings.class, service -> service.hasDynamicSetting("foo.bar"));
    }

    public void testRegisterIndexDynamicSettingDuplicate() {
        try {
            new SettingsModule(Settings.EMPTY, EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Cannot register setting [" + EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey() + "] twice");
        }
    }

    public void testRegisterIndexDynamicSetting() {
        SettingsModule module = new SettingsModule(Settings.EMPTY, Setting.boolSetting("index.foo.bar", false, Property.Dynamic, Property.IndexScope));
        assertInstanceBinding(module, IndexScopedSettings.class, service -> service.hasDynamicSetting("index.foo.bar"));
    }

    public void testRegisterAllocationDeciderDuplicate() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ClusterModule(Settings.EMPTY, clusterService, Collections.singletonList(new ClusterPlugin() {

            @Override
            public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
                return Collections.singletonList(new EnableAllocationDecider(settings, clusterSettings));
            }
        })));
        assertEquals(e.getMessage(), "Cannot specify allocation decider [" + EnableAllocationDecider.class.getName() + "] twice");
    }

    public void testRegisterAllocationDecider() {
        ClusterModule module = new ClusterModule(Settings.EMPTY, clusterService, Collections.singletonList(new ClusterPlugin() {

            @Override
            public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
                return Collections.singletonList(new FakeAllocationDecider(settings));
            }
        }));
        assertTrue(module.allocationDeciders.stream().anyMatch(d -> d.getClass().equals(FakeAllocationDecider.class)));
    }

    public void testRegisterShardsAllocator() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), "custom").build();
        ClusterModule module = new ClusterModule(settings, clusterService, Collections.emptyList());
        module.registerShardsAllocator("custom", FakeShardsAllocator.class);
        assertBinding(module, ShardsAllocator.class, FakeShardsAllocator.class);
    }

    public void testRegisterShardsAllocatorAlreadyRegistered() {
        ClusterModule module = new ClusterModule(Settings.EMPTY, clusterService, Collections.emptyList());
        try {
            module.registerShardsAllocator(ClusterModule.BALANCED_ALLOCATOR, FakeShardsAllocator.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [shards_allocator] more than once for [balanced]");
        }
    }

    public void testUnknownShardsAllocator() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), "dne").build();
        ClusterModule module = new ClusterModule(settings, clusterService, Collections.emptyList());
        assertBindingFailure(module, "Unknown [shards_allocator]");
    }

    public void testEvenShardsAllocatorBackcompat() {
        Settings settings = Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), ClusterModule.EVEN_SHARD_COUNT_ALLOCATOR).build();
        ClusterModule module = new ClusterModule(settings, clusterService, Collections.emptyList());
        assertBinding(module, ShardsAllocator.class, BalancedShardsAllocator.class);
    }
}