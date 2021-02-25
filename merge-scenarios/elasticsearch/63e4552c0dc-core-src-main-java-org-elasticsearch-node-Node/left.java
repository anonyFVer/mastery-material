package org.elasticsearch.node;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.DeprecationLogger;
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
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
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
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.store.IndicesStore;
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
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static java.util.stream.Collectors.toList;

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
            Logger logger = Loggers.getLogger(Node.class, NODE_NAME_SETTING.get(environment.settings()));
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
            Logger logger = Loggers.getLogger(Node.class, tmpSettings);
            final String nodeId = nodeEnvironment.nodeId();
            tmpSettings = addNodeNameIfNeeded(tmpSettings, nodeId);
            final String nodeName = NODE_NAME_SETTING.get(tmpSettings);
            if (hadPredefinedNodeName == false) {
                logger.info("node name [{}] derived from node ID [{}]; set [{}] to override", nodeName, nodeId, NODE_NAME_SETTING.getKey());
            } else {
                logger.info("node name [{}], node ID [{}]", nodeName, nodeId);
            }
            final JvmInfo jvmInfo = JvmInfo.jvmInfo();
            logger.info("version[{}], pid[{}], build[{}/{}], OS[{}/{}/{}], JVM[{}/{}/{}/{}]", displayVersion(Version.CURRENT, Build.CURRENT.isSnapshot()), jvmInfo.pid(), Build.CURRENT.shortHash(), Build.CURRENT.date(), Constants.OS_NAME, Constants.OS_VERSION, Constants.OS_ARCH, Constants.JVM_VENDOR, Constants.JVM_NAME, Constants.JAVA_VERSION, Constants.JVM_VERSION);
            warnIfPreRelease(Version.CURRENT, Build.CURRENT.isSnapshot(), logger);
            if (logger.isDebugEnabled()) {
                logger.debug("using config [{}], data [{}], logs [{}], plugins [{}]", environment.configFile(), Arrays.toString(environment.dataFiles()), environment.logsFile(), environment.pluginsFile());
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
            client = new NodeClient(settings, threadPool);
            final ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, threadPool);
            final ScriptModule scriptModule = ScriptModule.create(settings, this.environment, resourceWatcherService, pluginsService.filterPlugins(ScriptPlugin.class));
            AnalysisModule analysisModule = new AnalysisModule(this.environment, pluginsService.filterPlugins(AnalysisPlugin.class));
            additionalSettings.addAll(scriptModule.getSettings());
            final SettingsModule settingsModule = new SettingsModule(this.settings, additionalSettings, additionalSettingsFilter);
            scriptModule.registerClusterSettingsListeners(settingsModule.getClusterSettings());
            resourcesToClose.add(resourceWatcherService);
            final NetworkService networkService = new NetworkService(settings, getCustomNameResolvers(pluginsService.filterPlugins(DiscoveryPlugin.class)));
            final ClusterService clusterService = new ClusterService(settings, settingsModule.getClusterSettings(), threadPool);
            clusterService.addListener(scriptModule.getScriptService());
            resourcesToClose.add(clusterService);
            final IngestService ingestService = new IngestService(settings, threadPool, this.environment, scriptModule.getScriptService(), analysisModule.getAnalysisRegistry(), pluginsService.filterPlugins(IngestPlugin.class));
            final ClusterInfoService clusterInfoService = newClusterInfoService(settings, clusterService, threadPool, client);
            ModulesBuilder modules = new ModulesBuilder();
            for (Module pluginModule : pluginsService.createGuiceModules()) {
                modules.add(pluginModule);
            }
            final MonitorService monitorService = new MonitorService(settings, nodeEnvironment, threadPool);
            modules.add(new NodeModule(this, monitorService));
            ClusterModule clusterModule = new ClusterModule(settings, clusterService, pluginsService.filterPlugins(ClusterPlugin.class));
            modules.add(clusterModule);
            IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class));
            modules.add(indicesModule);
            SearchModule searchModule = new SearchModule(settings, false, pluginsService.filterPlugins(SearchPlugin.class));
            ActionModule actionModule = new ActionModule(false, settings, clusterModule.getIndexNameExpressionResolver(), settingsModule.getClusterSettings(), threadPool, pluginsService.filterPlugins(ActionPlugin.class));
            modules.add(actionModule);
            modules.add(new GatewayModule());
            CircuitBreakerService circuitBreakerService = createCircuitBreakerService(settingsModule.getSettings(), settingsModule.getClusterSettings());
            resourcesToClose.add(circuitBreakerService);
            BigArrays bigArrays = createBigArrays(settings, circuitBreakerService);
            resourcesToClose.add(bigArrays);
            modules.add(settingsModule);
            List<NamedWriteableRegistry.Entry> namedWriteables = Stream.of(NetworkModule.getNamedWriteables().stream(), indicesModule.getNamedWriteables().stream(), searchModule.getNamedWriteables().stream(), pluginsService.filterPlugins(Plugin.class).stream().flatMap(p -> p.getNamedWriteables().stream()), ClusterModule.getNamedWriteables().stream()).flatMap(Function.identity()).collect(Collectors.toList());
            final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
            NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(Stream.of(NetworkModule.getNamedXContents().stream(), searchModule.getNamedXContents().stream(), pluginsService.filterPlugins(Plugin.class).stream().flatMap(p -> p.getNamedXContent().stream()), ClusterModule.getNamedXWriteables().stream()).flatMap(Function.identity()).collect(toList()));
            final TribeService tribeService = new TribeService(settings, clusterService, nodeId, namedWriteableRegistry, s -> newTribeClientNode(s, classpathPlugins));
            resourcesToClose.add(tribeService);
            modules.add(new RepositoriesModule(this.environment, pluginsService.filterPlugins(RepositoryPlugin.class), xContentRegistry));
            final MetaStateService metaStateService = new MetaStateService(settings, nodeEnvironment, xContentRegistry);
            final IndicesService indicesService = new IndicesService(settings, pluginsService, nodeEnvironment, xContentRegistry, settingsModule.getClusterSettings(), analysisModule.getAnalysisRegistry(), clusterModule.getIndexNameExpressionResolver(), indicesModule.getMapperRegistry(), namedWriteableRegistry, threadPool, settingsModule.getIndexScopedSettings(), circuitBreakerService, bigArrays, scriptModule.getScriptService(), clusterService, client, metaStateService);
            Collection<Object> pluginComponents = pluginsService.filterPlugins(Plugin.class).stream().flatMap(p -> p.createComponents(client, clusterService, threadPool, resourceWatcherService, scriptModule.getScriptService(), xContentRegistry).stream()).collect(Collectors.toList());
            Collection<UnaryOperator<Map<String, MetaData.Custom>>> customMetaDataUpgraders = pluginsService.filterPlugins(Plugin.class).stream().map(Plugin::getCustomMetaDataUpgrader).collect(Collectors.toList());
            final NetworkModule networkModule = new NetworkModule(settings, false, pluginsService.filterPlugins(NetworkPlugin.class), threadPool, bigArrays, circuitBreakerService, namedWriteableRegistry, xContentRegistry, networkService);
            final MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(customMetaDataUpgraders);
            final Transport transport = networkModule.getTransportSupplier().get();
            final TransportService transportService = newTransportService(settings, transport, threadPool, networkModule.getTransportInterceptor(), settingsModule.getClusterSettings());
            final Consumer<Binder> httpBind;
            if (networkModule.isHttpEnabled()) {
                HttpServerTransport httpServerTransport = networkModule.getHttpServerTransportSupplier().get();
                HttpServer httpServer = new HttpServer(settings, httpServerTransport, actionModule.getRestController(), client, circuitBreakerService);
                httpBind = b -> {
                    b.bind(HttpServer.class).toInstance(httpServer);
                    b.bind(HttpServerTransport.class).toInstance(httpServerTransport);
                };
            } else {
                httpBind = b -> {
                    b.bind(HttpServer.class).toProvider(Providers.of(null));
                };
            }
            final DiscoveryModule discoveryModule = new DiscoveryModule(this.settings, threadPool, transportService, namedWriteableRegistry, networkService, clusterService, pluginsService.filterPlugins(DiscoveryPlugin.class));
            modules.add(b -> {
                b.bind(NamedXContentRegistry.class).toInstance(xContentRegistry);
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
                b.bind(SearchService.class).toInstance(newSearchService(clusterService, indicesService, threadPool, scriptModule.getScriptService(), bigArrays, searchModule.getFetchPhase()));
                b.bind(SearchTransportService.class).toInstance(new SearchTransportService(settings, settingsModule.getClusterSettings(), transportService));
                b.bind(SearchPhaseController.class).toInstance(new SearchPhaseController(settings, bigArrays, scriptModule.getScriptService()));
                b.bind(Transport.class).toInstance(transport);
                b.bind(TransportService.class).toInstance(transportService);
                b.bind(NetworkService.class).toInstance(networkService);
                b.bind(UpdateHelper.class).toInstance(new UpdateHelper(settings, scriptModule.getScriptService()));
                b.bind(MetaDataIndexUpgradeService.class).toInstance(new MetaDataIndexUpgradeService(settings, xContentRegistry, indicesModule.getMapperRegistry(), settingsModule.getIndexScopedSettings()));
                b.bind(ClusterInfoService.class).toInstance(clusterInfoService);
                b.bind(Discovery.class).toInstance(discoveryModule.getDiscovery());
                {
                    RecoverySettings recoverySettings = new RecoverySettings(settings, settingsModule.getClusterSettings());
                    processRecoverySettings(settingsModule.getClusterSettings(), recoverySettings);
                    b.bind(PeerRecoverySourceService.class).toInstance(new PeerRecoverySourceService(settings, transportService, indicesService, recoverySettings, clusterService));
                    b.bind(PeerRecoveryTargetService.class).toInstance(new PeerRecoveryTargetService(settings, threadPool, transportService, recoverySettings, clusterService));
                }
                httpBind.accept(b);
                pluginComponents.stream().forEach(p -> b.bind((Class) p.getClass()).toInstance(p));
            });
            injector = modules.createInjector();
            List<LifecycleComponent> pluginLifecycleComponents = pluginComponents.stream().filter(p -> p instanceof LifecycleComponent).map(p -> (LifecycleComponent) p).collect(Collectors.toList());
            pluginLifecycleComponents.addAll(pluginsService.getGuiceServiceClasses().stream().map(injector::getInstance).collect(Collectors.toList()));
            resourcesToClose.addAll(pluginLifecycleComponents);
            this.pluginLifecycleComponents = Collections.unmodifiableList(pluginLifecycleComponents);
            client.initialize(injector.getInstance(new Key<Map<GenericAction, TransportAction>>() {
            }), () -> clusterService.localNode().getId());
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

    static void warnIfPreRelease(final Version version, final boolean isSnapshot, final Logger logger) {
        if (!version.isRelease() || isSnapshot) {
            logger.warn("version [{}] is a pre-release version of Elasticsearch and is not suitable for production", displayVersion(version, isSnapshot));
        }
    }

    private static String displayVersion(final Version version, final boolean isSnapshot) {
        return version + (isSnapshot ? "-SNAPSHOT" : "");
    }

    protected TransportService newTransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor interceptor, ClusterSettings clusterSettings) {
        return new TransportService(settings, transport, threadPool, interceptor, clusterSettings);
    }

    protected void processRecoverySettings(ClusterSettings clusterSettings, RecoverySettings recoverySettings) {
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

    public Node start() throws NodeValidationException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        Logger logger = Loggers.getLogger(Node.class, NODE_NAME_SETTING.get(settings));
        logger.info("starting ...");
        injector.getInstance(Discovery.class).setAllocationService(injector.getInstance(AllocationService.class));
        pluginLifecycleComponents.forEach(LifecycleComponent::start);
        injector.getInstance(MappingUpdatedAction.class).setClient(client);
        injector.getInstance(IndicesService.class).start();
        injector.getInstance(IndicesClusterStateService.class).start();
        injector.getInstance(SnapshotsService.class).start();
        injector.getInstance(SnapshotShardsService.class).start();
        injector.getInstance(RoutingService.class).start();
        injector.getInstance(SearchService.class).start();
        injector.getInstance(MonitorService.class).start();
        final ClusterService clusterService = injector.getInstance(ClusterService.class);
        final NodeConnectionsService nodeConnectionsService = injector.getInstance(NodeConnectionsService.class);
        nodeConnectionsService.start();
        clusterService.setNodeConnectionsService(nodeConnectionsService);
        injector.getInstance(GatewayAllocator.class).setReallocation(clusterService, injector.getInstance(RoutingService.class));
        injector.getInstance(ResourceWatcherService.class).start();
        injector.getInstance(GatewayService.class).start();
        Discovery discovery = injector.getInstance(Discovery.class);
        clusterService.setDiscoverySettings(discovery.getDiscoverySettings());
        clusterService.addInitialStateBlock(discovery.getDiscoverySettings().getNoMasterBlock());
        clusterService.setClusterStatePublisher(discovery::publish);
        final TribeService tribeService = injector.getInstance(TribeService.class);
        tribeService.start();
        TransportService transportService = injector.getInstance(TransportService.class);
        transportService.getTaskManager().setTaskResultsService(injector.getInstance(TaskResultsService.class));
        transportService.start();
        validateNodeBeforeAcceptingRequests(settings, transportService.boundAddress(), pluginsService.filterPlugins(Plugin.class).stream().flatMap(p -> p.getBootstrapChecks().stream()).collect(Collectors.toList()));
        DiscoveryNode localNode = DiscoveryNode.createLocal(settings, transportService.boundAddress().publishAddress(), injector.getInstance(NodeEnvironment.class).nodeId());
        clusterService.setLocalNode(localNode);
        transportService.setLocalNode(localNode);
        clusterService.addStateApplier(transportService.getTaskManager());
        clusterService.start();
        discovery.start();
        transportService.acceptIncomingRequests();
        discovery.startInitialJoin();
        final TimeValue initialStateTimeout = DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.get(settings);
        if (initialStateTimeout.millis() > 0) {
            final ThreadPool thread = injector.getInstance(ThreadPool.class);
            ClusterState clusterState = clusterService.state();
            ClusterStateObserver observer = new ClusterStateObserver(clusterState, clusterService, null, logger, thread.getThreadContext());
            if (clusterState.nodes().getMasterNodeId() == null) {
                logger.debug("waiting to join the cluster. timeout [{}]", initialStateTimeout);
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
                        logger.warn("timed out while waiting for initial discovery state - timeout: {}", initialStateTimeout);
                        latch.countDown();
                    }
                }, state -> state.nodes().getMasterNodeId() != null, initialStateTimeout);
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
        SearchTransportService searchTransportService = injector.getInstance(SearchTransportService.class);
        searchTransportService.start();
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
        Logger logger = Loggers.getLogger(Node.class, NODE_NAME_SETTING.get(settings));
        logger.info("stopping ...");
        injector.getInstance(TribeService.class).stop();
        injector.getInstance(ResourceWatcherService.class).stop();
        if (NetworkModule.HTTP_ENABLED.get(settings)) {
            injector.getInstance(HttpServer.class).stop();
        }
        injector.getInstance(SnapshotsService.class).stop();
        injector.getInstance(SnapshotShardsService.class).stop();
        injector.getInstance(IndicesClusterStateService.class).stop();
        injector.getInstance(Discovery.class).stop();
        injector.getInstance(RoutingService.class).stop();
        injector.getInstance(ClusterService.class).stop();
        injector.getInstance(NodeConnectionsService.class).stop();
        injector.getInstance(MonitorService.class).stop();
        injector.getInstance(GatewayService.class).stop();
        injector.getInstance(SearchService.class).stop();
        injector.getInstance(TransportService.class).stop();
        injector.getInstance(SearchTransportService.class).stop();
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
        Logger logger = Loggers.getLogger(Node.class, NODE_NAME_SETTING.get(settings));
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
        toClose.add(() -> stopWatch.stop().start("transport"));
        toClose.add(injector.getInstance(TransportService.class));
        toClose.add(() -> stopWatch.stop().start("search_transport_service"));
        toClose.add(injector.getInstance(SearchTransportService.class));
        for (LifecycleComponent plugin : pluginLifecycleComponents) {
            toClose.add(() -> stopWatch.stop().start("plugin(" + plugin.getClass().getName() + ")"));
            toClose.add(plugin);
        }
        toClose.addAll(pluginsService.filterPlugins(Plugin.class));
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
    protected void validateNodeBeforeAcceptingRequests(final Settings settings, final BoundTransportAddress boundTransportAddress, List<BootstrapCheck> bootstrapChecks) throws NodeValidationException {
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

    protected SearchService newSearchService(ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ScriptService scriptService, BigArrays bigArrays, FetchPhase fetchPhase) {
        return new SearchService(clusterService, indicesService, threadPool, scriptService, bigArrays, fetchPhase);
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

    protected Node newTribeClientNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
        return new Node(new Environment(settings), classpathPlugins);
    }

    protected ClusterInfoService newClusterInfoService(Settings settings, ClusterService clusterService, ThreadPool threadPool, NodeClient client) {
        return new InternalClusterInfoService(settings, clusterService, threadPool, client);
    }
}