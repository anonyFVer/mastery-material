package org.elasticsearch.test.discovery;

import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.ZenPing;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import static org.elasticsearch.discovery.zen.SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;

public class TestZenDiscovery extends ZenDiscovery {

    public static final Setting<Boolean> USE_MOCK_PINGS = Setting.boolSetting("discovery.zen.use_mock_pings", true, Setting.Property.NodeScope);

    public static class TestPlugin extends Plugin implements DiscoveryPlugin {

        protected final Settings settings;

        public TestPlugin(Settings settings) {
            this.settings = settings;
        }

        @Override
        public Map<String, Supplier<Discovery>> getDiscoveryTypes(ThreadPool threadPool, TransportService transportService, NamedWriteableRegistry namedWriteableRegistry, MasterService masterService, ClusterApplier clusterApplier, ClusterSettings clusterSettings, UnicastHostsProvider hostsProvider, AllocationService allocationService) {
            return Collections.singletonMap("test-zen", () -> new TestZenDiscovery(Settings.builder().put(settings).putList(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey()).build(), threadPool, transportService, namedWriteableRegistry, masterService, clusterApplier, clusterSettings, hostsProvider, allocationService));
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(USE_MOCK_PINGS);
        }

        @Override
        public Settings additionalSettings() {
            return Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "test-zen").putList(DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey()).build();
        }
    }

    private TestZenDiscovery(Settings settings, ThreadPool threadPool, TransportService transportService, NamedWriteableRegistry namedWriteableRegistry, MasterService masterService, ClusterApplier clusterApplier, ClusterSettings clusterSettings, UnicastHostsProvider hostsProvider, AllocationService allocationService) {
        super(settings, threadPool, transportService, namedWriteableRegistry, masterService, clusterApplier, clusterSettings, hostsProvider, allocationService, Collections.emptyList());
    }

    @Override
    protected ZenPing newZenPing(Settings settings, ThreadPool threadPool, TransportService transportService, UnicastHostsProvider hostsProvider) {
        if (USE_MOCK_PINGS.get(settings)) {
            return new MockZenPing(settings, this);
        } else {
            return super.newZenPing(settings, threadPool, transportService, hostsProvider);
        }
    }

    public ZenPing getZenPing() {
        return zenPing;
    }
}