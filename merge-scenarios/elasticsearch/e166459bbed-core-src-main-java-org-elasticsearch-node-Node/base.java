package org.elasticsearch.node;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.MasterNodeChangePredicate;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.gateway.GatewayModule;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.http.HttpServer;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.tribe.TribeService;
import org.elasticsearch.watcher.ResourceWatcherService;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Node implements Closeable {

    public static final Setting<Boolean> WRITE_PORTS_FIELD_SETTING = Setting.boolSetting("node.portsfile", false, Property.NodeScope);

    public static final Setting<Boolean> NODE_DATA_SETTING = Setting.boolSetting("node.data", true, Property.NodeScope);

    public static final Setting<Boolean> NODE_MASTER_SETTING = Setting.boolSetting("node.master", true, Property.NodeScope);

    public static final Setting<Boolean> NODE_INGEST_SETTING = Setting.boolSetting("node.ingest", true, Property.NodeScope);

    public static final Setting<Boolean> NODE_LOCAL_STORAGE_SETTING = Setting.boolSetting("node.local_storage", true, Property.NodeScope);

    public static final Setting<String> NODE_NAME_SETTING = Setting.simpleString("node.name", Property.NodeScope);

    public static final Setting<Settings> NODE_ATTRIBUTES = Setting.groupSetting("node.attr.", Property.NodeScope);

    public static final Setting<String> BREAKER_TYPE_KEY = new Setting<>("indices.breaker.type", "hierarchy", (s) -> {
        switch(s) {
            case "hierarchy":
            case "none":
                return s;
            default:
                throw new IllegalArgumentException("indices.breaker.type must be one of [hierarchy, none] but was: " + s);
        }
    }, Setting.Property.NodeScope);

    public static final Settings addNodeNameIfNeeded(Settings settings, final String nodeId) {
        if (NODE_NAME_SETTING.exists(settings)) {
            return settings;
        }
        return Settings.builder().put(settings).put(NODE_NAME_SETTING.getKey(), nodeId.substring(0, 7)).build();
    }

    private static final String CLIENT_TYPE = "node";

    private final Lifecycle lifecycle = new Lifecycle();

    private final Injector injector;

    private final Settings settings;

    private final Environment environment;

    private final NodeEnvironment nodeEnvironment;

    private final PluginsService pluginsService;

    private final NodeClient client;

    private final Collection<LifecycleComponent> pluginLifecycleComponents;

    public Node(Settings preparedSettings) {
        this(InternalSettingsPreparer.prepareEnvironment(preparedSettings, null));
    }

    public Node(Environment environment) {
        this(environment, Collections.emptyList());
    }

    protected Node(final Environment environment, Collection<Class<? extends Plugin>> classpathPlugins) {
        final List<Closeable> resourcesToClose = new ArrayList<>();
        boolean success = false;
        {
            ESLogger logger = Loggers.getLogger(Node.class, NODE_NAME_SETTING.get(environment.settings()));
            logger.info("initializing ...");
        }
        try {
            Settings tmpSettings = Settings.builder().put(environment.settings()).put(Client.CLIENT_TYPE_SETTING_S.getKey(), CLIENT_TYPE).build();
            tmpSettings = TribeService.processSettings(tmpSettings);
            try {
                nodeEnvironment = new NodeEnvironment(tmpSettings, environment);
                resourcesToClose.add(nodeEnvironment);
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to created node environment", ex);
            }
            final boolean hadPredefinedNodeName = NODE_NAME_SETTING.exists(tmpSettings);
            tmpSettings = addNodeNameIfNeeded(tmpSettings, nodeEnvironment.nodeId());
            ESLogger logger = Loggers.getLogger(Node.class, tmpSettings);
            if (hadPredefinedNodeName == false) {
                logger.info("node name [{}] derived from node ID; set [{}] to override", NODE_NAME_SETTING.get(tmpSettings), NODE_NAME_SETTING.getKey());
            }
            final String displayVersion = Version.CURRENT + (Build.CURRENT.isSnapshot() ? "-SNAPSHOT" : "");
            final JvmInfo jvmInfo = JvmInfo.jvmInfo();
            logger.info("version[{}], pid[{}], build[{}/{}], OS[{}/{}/{}], JVM[{}/{}/{}/{}]", displayVersion, jvmInfo.pid(), Build.CURRENT.shortHash(), Build.CURRENT.date(), Constants.OS_NAME, Constants.OS_VERSION, Constants.OS_ARCH, Constants.JVM_VENDOR, Constants.JVM_NAME, Constants.JAVA_VERSION, Constants.JVM_VERSION);
            if (logger.isDebugEnabled()) {
                logger.debug("using config [{}], data [{}], logs [{}], plugins [{}]", environment.configFile(), Arrays.toString(environment.dataFiles()), environment.logsFile(), environment.pluginsFile());
            }
            if (JsonXContent.unquotedFieldNamesSet) {
                DeprecationLogger dLogger = new DeprecationLogger(logger);
                dLogger.deprecated("[{}] has been set, but will be removed in Elasticsearch 6.0.0", JsonXContent.JSON_ALLOW_UNQUOTED_FIELD_NAMES);
            }
            this.pluginsService = new PluginsService(tmpSettings, environment.modulesFile(), environment.pluginsFile(), classpathPlugins);
            this.settings = pluginsService.updatedSettings();
            this.environment = new Environment(this.settings);
            Environment.assertEquivalent(environment, this.environment);
            final List<ExecutorBuilder<?>> executorBuilders = pluginsService.getExecutorBuilders(settings);
            final ThreadPool threadPool = new ThreadPool(settings, executorBuilders.toArray(new ExecutorBuilder[0]));
            resourcesToClose.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
            DeprecationLogger.setThreadContext(threadPool.getThreadContext());
            resourcesToClose.add(() -> DeprecationLogger.removeThreadContext(threadPool.getThreadContext()));
            final List<Setting<?>> additionalSettings = new ArrayList<>();
            final List<String> additionalSettingsFilter = new ArrayList<>();
            additionalSettings.addAll(pluginsService.getPluginSettings());
            additionalSettingsFilter.addAll(pluginsService.getPluginSettingsFilter());
            for (final ExecutorBuilder<?> builder : threadPool.builders()) {
                additionalSettings.addAll(builder.getRegisteredSettings());
            }
            final ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, threadPool);
            final ScriptModule scriptModule = ScriptModule.create(settings, this.environment, resourceWatcherService, pluginsService.filterPlugins(ScriptPlugin.class));
            AnalysisModule analysisModule = new AnalysisModule(this.environment, pluginsService.filterPlugins(AnalysisPlugin.class));
            additionalSettings.addAll(scriptModule.getSettings());
            final SettingsModule settingsModule = new SettingsModule(this.settings, additionalSettings, additionalSettingsFilter);
            scriptModule.registerClusterSettingsListeners(settingsModule.getClusterSettings());
            resourcesToClose.add(resourceWatcherService);
            final NetworkService networkService = new NetworkService(settings, getCustomNameResolvers(pluginsService.filterPlugins(DiscoveryPlugin.class)));
            final ClusterService clusterService = new ClusterService(settings, settingsModule.getClusterSettings(), threadPool);
            clusterService.add(scriptModule.getScriptService());
            resourcesToClose.add(clusterService);
            final TribeService tribeService = new TribeService(settings, clusterService, nodeEnvironment.nodeId());
            resourcesToClose.add(tribeService);
            final IngestService ingestService = new IngestService(settings, threadPool, this.environment, scriptModule.getScriptService(), pluginsService.filterPlugins(IngestPlugin.class));
            ModulesBuilder modules = new ModulesBuilder();
            for (Module pluginModule : pluginsService.createGuiceModules()) {
                modules.add(pluginModule);
            }
            final MonitorService monitorService = new MonitorService(settings, nodeEnvironment, threadPool);
            modules.add(new NodeModule(this, monitorService));
            NetworkModule networkModule = new NetworkModule(networkService, settings, false);
            modules.add(networkModule);
            modules.add(new DiscoveryModule(this.settings));
            ClusterModule clusterModule = new ClusterModule(settings, clusterService, pluginsService.filterPlugins(ClusterPlugin.class));
            modules.add(clusterModule);
            IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class));
            modules.add(indicesModule);
            SearchModule searchModule = new SearchModule(settings, false, pluginsService.filterPlugins(SearchPlugin.class));
            modules.add(searchModule);
            modules.add(new ActionModule(DiscoveryNode.isIngestNode(settings), false, settings, clusterModule.getIndexNameExpressionResolver(), settingsModule.getClusterSettings(), pluginsService.filterPlugins(ActionPlugin.class)));
            modules.add(new GatewayModule());
            modules.add(new RepositoriesModule(this.environment, pluginsService.filterPlugins(RepositoryPlugin.class)));
            pluginsService.processModules(modules);
            CircuitBreakerService circuitBreakerService = createCircuitBreakerService(settingsModule.getSettings(), settingsModule.getClusterSettings());
            resourcesToClose.add(circuitBreakerService);
            BigArrays bigArrays = createBigArrays(settings, circuitBreakerService);
            resourcesToClose.add(bigArrays);
            modules.add(settingsModule);
            List<NamedWriteableRegistry.Entry> namedWriteables = Stream.of(networkModule.getNamedWriteables().stream(), indicesModule.getNamedWriteables().stream(), searchModule.getNamedWriteables().stream(), pluginsService.filterPlugins(Plugin.class).stream().flatMap(p -> p.getNamedWriteables().stream())).flatMap(Function.identity()).collect(Collectors.toList());
            final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
            final MetaStateService metaStateService = new MetaStateService(settings, nodeEnvironment);
            final IndicesService indicesService = new IndicesService(settings, pluginsService, nodeEnvironment, settingsModule.getClusterSettings(), analysisModule.getAnalysisRegistry(), searchModule.getQueryParserRegistry(), clusterModule.getIndexNameExpressionResolver(), indicesModule.getMapperRegistry(), namedWriteableRegistry, threadPool, settingsModule.getIndexScopedSettings(), circuitBreakerService, metaStateService);
            client = new NodeClient(settings, threadPool);
            Collection<Object> pluginComponents = pluginsService.filterPlugins(Plugin.class).stream().flatMap(p -> p.createComponents(client, clusterService, threadPool, resourceWatcherService, scriptModule.getScriptService(), searchModule.getSearchRequestParsers()).stream()).collect(Collectors.toList());
            Collection<UnaryOperator<Map<String, MetaData.Custom>>> customMetaDataUpgraders = pluginsService.filterPlugins(Plugin.class).stream().map(Plugin::getCustomMetaDataUpgrader).collect(Collectors.toList());
            final MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(customMetaDataUpgraders);
            modules.add(b -> {
                b.bind(PluginsService.class).toInstance(pluginsService);
                b.bind(Client.class).toInstance(client);
                b.bind(NodeClient.class).toInstance(client);
                b.bind(Environment.class).toInstance(this.environment);
                b.bind(ThreadPool.class).toInstance(threadPool);
                b.bind(NodeEnvironment.class).toInstance(nodeEnvironment);
                b.bind(TribeService.class).toInstance(tribeService);
                b.bind(ResourceWatcherService.class).toInstance(resourceWatcherService);
                b.bind(CircuitBreakerService.class).toInstance(circuitBreakerService);
                b.bind(BigArrays.class).toInstance(bigArrays);
                b.bind(ScriptService.class).toInstance(scriptModule.getScriptService());
                b.bind(AnalysisRegistry.class).toInstance(analysisModule.getAnalysisRegistry());
                b.bind(IngestService.class).toInstance(ingestService);
                b.bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
                b.bind(MetaDataUpgrader.class).toInstance(metaDataUpgrader);
                b.bind(MetaStateService.class).toInstance(metaStateService);
                b.bind(IndicesService.class).toInstance(indicesService);
                Class<? extends SearchService> searchServiceImpl = pickSearchServiceImplementation();
                if (searchServiceImpl == SearchService.class) {
                    b.bind(SearchService.class).asEagerSingleton();
                } else {
                    b.bind(SearchService.class).to(searchServiceImpl).asEagerSingleton();
                }
                pluginComponents.stream().forEach(p -> b.bind((Class) p.getClass()).toInstance(p));
            });
            injector = modules.createInjector();
            List<LifecycleComponent> pluginLifecycleComponents = pluginComponents.stream().filter(p -> p instanceof LifecycleComponent).map(p -> (LifecycleComponent) p).collect(Collectors.toList());
            pluginLifecycleComponents.addAll(pluginsService.getGuiceServiceClasses().stream().map(injector::getInstance).collect(Collectors.toList()));
            resourcesToClose.addAll(pluginLifecycleComponents);
            this.pluginLifecycleComponents = Collections.unmodifiableList(pluginLifecycleComponents);
            client.intialize(injector.getInstance(new Key<Map<GenericAction, TransportAction>>() {
            }));
            logger.info("initialized");
            success = true;
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to bind service", ex);
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(resourcesToClose);
            }
        }
    }

    public Settings settings() {
        return this.settings;
    }

    public Client client() {
        return client;
    }

    public Environment getEnvironment() {
        return environment;
    }

    public NodeEnvironment getNodeEnvironment() {
        return nodeEnvironment;
    }

    public Node start() {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        ESLogger logger = Loggers.getLogger(Node.class, NODE_NAME_SETTING.get(settings));
        logger.info("starting ...");
        injector.getInstance(Discovery.class).setAllocationService(injector.getInstance(AllocationService.class));
        pluginLifecycleComponents.forEach(LifecycleComponent::start);
        injector.getInstance(MappingUpdatedAction.class).setClient(client);
        injector.getInstance(IndicesService.class).start();
        injector.getInstance(IndicesClusterStateService.class).start();
        injector.getInstance(IndicesTTLService.class).start();
        injector.getInstance(SnapshotsService.class).start();
        injector.getInstance(SnapshotShardsService.class).start();
        injector.getInstance(RoutingService.class).start();
        injector.getInstance(SearchService.class).start();
        injector.getInstance(MonitorService.class).start();
        injector.getInstance(RestController.class).start();
        final ClusterService clusterService = injector.getInstance(ClusterService.class);
        final NodeConnectionsService nodeConnectionsService = injector.getInstance(NodeConnectionsService.class);
        nodeConnectionsService.start();
        clusterService.setNodeConnectionsService(nodeConnectionsService);
        injector.getInstance(GatewayAllocator.class).setReallocation(clusterService, injector.getInstance(RoutingService.class));
        injector.getInstance(ResourceWatcherService.class).start();
        injector.getInstance(GatewayService.class).start();
        Discovery discovery = injector.getInstance(Discovery.class);
        clusterService.addInitialStateBlock(discovery.getDiscoverySettings().getNoMasterBlock());
        clusterService.setClusterStatePublisher(discovery::publish);
        final TribeService tribeService = injector.getInstance(TribeService.class);
        tribeService.start();
        TransportService transportService = injector.getInstance(TransportService.class);
        transportService.getTaskManager().setTaskResultsService(injector.getInstance(TaskResultsService.class));
        transportService.start();
        validateNodeBeforeAcceptingRequests(settings, transportService.boundAddress());
        DiscoveryNode localNode = DiscoveryNode.createLocal(settings, transportService.boundAddress().publishAddress(), injector.getInstance(NodeEnvironment.class).nodeId());
        clusterService.setLocalNode(localNode);
        transportService.setLocalNode(localNode);
        clusterService.add(transportService.getTaskManager());
        clusterService.start();
        discovery.start();
        transportService.acceptIncomingRequests();
        discovery.startInitialJoin();
        if (DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.get(settings).millis() > 0) {
            final ThreadPool thread = injector.getInstance(ThreadPool.class);
            ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, thread.getThreadContext());
            if (observer.observedState().nodes().getMasterNodeId() == null) {
                final CountDownLatch latch = new CountDownLatch(1);
                observer.waitForNextChange(new ClusterStateObserver.Listener() {

                    @Override
                    public void onNewClusterState(ClusterState state) {
                        latch.countDown();
                    }

                    @Override
                    public void onClusterServiceClose() {
                        latch.countDown();
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        logger.warn("timed out while waiting for initial discovery state - timeout: {}", DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.get(settings));
                        latch.countDown();
                    }
                }, MasterNodeChangePredicate.INSTANCE, DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.get(settings));
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new ElasticsearchTimeoutException("Interrupted while waiting for initial discovery state");
                }
            }
        }
        if (NetworkModule.HTTP_ENABLED.get(settings)) {
            injector.getInstance(HttpServer.class).start();
        }
        tribeService.startNodes();
        if (WRITE_PORTS_FIELD_SETTING.get(settings)) {
            if (NetworkModule.HTTP_ENABLED.get(settings)) {
                HttpServerTransport http = injector.getInstance(HttpServerTransport.class);
                writePortsFile("http", http.boundAddress());
            }
            TransportService transport = injector.getInstance(TransportService.class);
            writePortsFile("transport", transport.boundAddress());
        }
        logger.info("started");
        return this;
    }

    private Node stop() {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        ESLogger logger = Loggers.getLogger(Node.class, NODE_NAME_SETTING.get(settings));
        logger.info("stopping ...");
        injector.getInstance(TribeService.class).stop();
        injector.getInstance(ResourceWatcherService.class).stop();
        if (NetworkModule.HTTP_ENABLED.get(settings)) {
            injector.getInstance(HttpServer.class).stop();
        }
        injector.getInstance(SnapshotsService.class).stop();
        injector.getInstance(SnapshotShardsService.class).stop();
        injector.getInstance(IndicesClusterStateService.class).stop();
        injector.getInstance(IndicesTTLService.class).stop();
        injector.getInstance(RoutingService.class).stop();
        injector.getInstance(ClusterService.class).stop();
        injector.getInstance(Discovery.class).stop();
        injector.getInstance(NodeConnectionsService.class).stop();
        injector.getInstance(MonitorService.class).stop();
        injector.getInstance(GatewayService.class).stop();
        injector.getInstance(SearchService.class).stop();
        injector.getInstance(RestController.class).stop();
        injector.getInstance(TransportService.class).stop();
        pluginLifecycleComponents.forEach(LifecycleComponent::stop);
        injector.getInstance(IndicesService.class).stop();
        logger.info("stopped");
        return this;
    }

    @Override
    public synchronized void close() throws IOException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        ESLogger logger = Loggers.getLogger(Node.class, NODE_NAME_SETTING.get(settings));
        logger.info("closing ...");
        List<Closeable> toClose = new ArrayList<>();
        StopWatch stopWatch = new StopWatch("node_close");
        toClose.add(() -> stopWatch.start("tribe"));
        toClose.add(injector.getInstance(TribeService.class));
        toClose.add(() -> stopWatch.stop().start("node_service"));
        toClose.add(injector.getInstance(NodeService.class));
        toClose.add(() -> stopWatch.stop().start("http"));
        if (NetworkModule.HTTP_ENABLED.get(settings)) {
            toClose.add(injector.getInstance(HttpServer.class));
        }
        toClose.add(() -> stopWatch.stop().start("snapshot_service"));
        toClose.add(injector.getInstance(SnapshotsService.class));
        toClose.add(injector.getInstance(SnapshotShardsService.class));
        toClose.add(() -> stopWatch.stop().start("client"));
        Releasables.close(injector.getInstance(Client.class));
        toClose.add(() -> stopWatch.stop().start("indices_cluster"));
        toClose.add(injector.getInstance(IndicesClusterStateService.class));
        toClose.add(() -> stopWatch.stop().start("indices"));
        toClose.add(injector.getInstance(IndicesTTLService.class));
        toClose.add(injector.getInstance(IndicesService.class));
        toClose.add(injector.getInstance(IndicesStore.class));
        toClose.add(() -> stopWatch.stop().start("routing"));
        toClose.add(injector.getInstance(RoutingService.class));
        toClose.add(() -> stopWatch.stop().start("cluster"));
        toClose.add(injector.getInstance(ClusterService.class));
        toClose.add(() -> stopWatch.stop().start("node_connections_service"));
        toClose.add(injector.getInstance(NodeConnectionsService.class));
        toClose.add(() -> stopWatch.stop().start("discovery"));
        toClose.add(injector.getInstance(Discovery.class));
        toClose.add(() -> stopWatch.stop().start("monitor"));
        toClose.add(injector.getInstance(MonitorService.class));
        toClose.add(() -> stopWatch.stop().start("gateway"));
        toClose.add(injector.getInstance(GatewayService.class));
        toClose.add(() -> stopWatch.stop().start("search"));
        toClose.add(injector.getInstance(SearchService.class));
        toClose.add(() -> stopWatch.stop().start("rest"));
        toClose.add(injector.getInstance(RestController.class));
        toClose.add(() -> stopWatch.stop().start("transport"));
        toClose.add(injector.getInstance(TransportService.class));
        for (LifecycleComponent plugin : pluginLifecycleComponents) {
            toClose.add(() -> stopWatch.stop().start("plugin(" + plugin.getClass().getName() + ")"));
            toClose.add(plugin);
        }
        toClose.addAll(pluginsService.filterPlugins(Closeable.class));
        toClose.add(() -> stopWatch.stop().start("script"));
        toClose.add(injector.getInstance(ScriptService.class));
        toClose.add(() -> stopWatch.stop().start("thread_pool"));
        toClose.add(() -> injector.getInstance(ThreadPool.class).shutdown());
        toClose.add(() -> {
            try {
                injector.getInstance(ThreadPool.class).awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
        });
        toClose.add(() -> stopWatch.stop().start("thread_pool_force_shutdown"));
        toClose.add(() -> injector.getInstance(ThreadPool.class).shutdownNow());
        toClose.add(() -> stopWatch.stop());
        toClose.add(injector.getInstance(NodeEnvironment.class));
        toClose.add(injector.getInstance(BigArrays.class));
        if (logger.isTraceEnabled()) {
            logger.trace("Close times for each service:\n{}", stopWatch.prettyPrint());
        }
        IOUtils.close(toClose);
        logger.info("closed");
    }

    public boolean isClosed() {
        return lifecycle.closed();
    }

    public Injector injector() {
        return this.injector;
    }

    @SuppressWarnings("unused")
    protected void validateNodeBeforeAcceptingRequests(Settings settings, BoundTransportAddress boundTransportAddress) {
    }

    private void writePortsFile(String type, BoundTransportAddress boundAddress) {
        Path tmpPortsFile = environment.logsFile().resolve(type + ".ports.tmp");
        try (BufferedWriter writer = Files.newBufferedWriter(tmpPortsFile, Charset.forName("UTF-8"))) {
            for (TransportAddress address : boundAddress.boundAddresses()) {
                InetAddress inetAddress = InetAddress.getByName(address.getAddress());
                if (inetAddress instanceof Inet6Address && inetAddress.isLinkLocalAddress()) {
                    continue;
                }
                writer.write(NetworkAddress.format(new InetSocketAddress(inetAddress, address.getPort())) + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write ports file", e);
        }
        Path portsFile = environment.logsFile().resolve(type + ".ports");
        try {
            Files.move(tmpPortsFile, portsFile, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to rename ports file", e);
        }
    }

    protected PluginsService getPluginsService() {
        return pluginsService;
    }

    public static CircuitBreakerService createCircuitBreakerService(Settings settings, ClusterSettings clusterSettings) {
        String type = BREAKER_TYPE_KEY.get(settings);
        if (type.equals("hierarchy")) {
            return new HierarchyCircuitBreakerService(settings, clusterSettings);
        } else if (type.equals("none")) {
            return new NoneCircuitBreakerService();
        } else {
            throw new IllegalArgumentException("Unknown circuit breaker type [" + type + "]");
        }
    }

    BigArrays createBigArrays(Settings settings, CircuitBreakerService circuitBreakerService) {
        return new BigArrays(settings, circuitBreakerService);
    }

    protected Class<? extends SearchService> pickSearchServiceImplementation() {
        return SearchService.class;
    }

    private List<NetworkService.CustomNameResolver> getCustomNameResolvers(List<DiscoveryPlugin> discoveryPlugins) {
        List<NetworkService.CustomNameResolver> customNameResolvers = new ArrayList<>();
        for (DiscoveryPlugin discoveryPlugin : discoveryPlugins) {
            NetworkService.CustomNameResolver customNameResolver = discoveryPlugin.getCustomNameResolver(settings);
            if (customNameResolver != null) {
                customNameResolvers.add(customNameResolver);
            }
        }
        return customNameResolvers;
    }
}