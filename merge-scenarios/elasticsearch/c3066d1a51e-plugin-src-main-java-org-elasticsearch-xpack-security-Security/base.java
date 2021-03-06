package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.tribe.TribeService;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.extensions.XPackExtension;
import org.elasticsearch.xpack.extensions.XPackExtensionsService;
import org.elasticsearch.xpack.security.action.SecurityActionModule;
import org.elasticsearch.xpack.security.action.filter.SecurityActionFilter;
import org.elasticsearch.xpack.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.security.action.realm.TransportClearRealmCacheAction;
import org.elasticsearch.xpack.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.security.action.role.GetRolesAction;
import org.elasticsearch.xpack.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.security.action.role.TransportClearRolesCacheAction;
import org.elasticsearch.xpack.security.action.role.TransportDeleteRoleAction;
import org.elasticsearch.xpack.security.action.role.TransportGetRolesAction;
import org.elasticsearch.xpack.security.action.role.TransportPutRoleAction;
import org.elasticsearch.xpack.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.security.action.rolemapping.TransportDeleteRoleMappingAction;
import org.elasticsearch.xpack.security.action.rolemapping.TransportGetRoleMappingsAction;
import org.elasticsearch.xpack.security.action.rolemapping.TransportPutRoleMappingAction;
import org.elasticsearch.xpack.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.security.action.token.TransportCreateTokenAction;
import org.elasticsearch.xpack.security.action.token.TransportInvalidateTokenAction;
import org.elasticsearch.xpack.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.security.action.user.PutUserAction;
import org.elasticsearch.xpack.security.action.user.SetEnabledAction;
import org.elasticsearch.xpack.security.action.user.TransportAuthenticateAction;
import org.elasticsearch.xpack.security.action.user.TransportChangePasswordAction;
import org.elasticsearch.xpack.security.action.user.TransportDeleteUserAction;
import org.elasticsearch.xpack.security.action.user.TransportGetUsersAction;
import org.elasticsearch.xpack.security.action.user.TransportHasPrivilegesAction;
import org.elasticsearch.xpack.security.action.user.TransportPutUserAction;
import org.elasticsearch.xpack.security.action.user.TransportSetEnabledAction;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail;
import org.elasticsearch.xpack.security.audit.index.IndexNameResolver;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.security.authc.InternalRealms;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.RealmSettings;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenMetaData;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealm;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl.ExpressionParser;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.SecuritySearchOperationListener;
import org.elasticsearch.xpack.security.authz.accesscontrol.OptOutQueryCache;
import org.elasticsearch.xpack.security.authz.accesscontrol.SecurityIndexSearcherWrapper;
import org.elasticsearch.xpack.security.authz.accesscontrol.SetSecurityUserProcessor;
import org.elasticsearch.xpack.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.elasticsearch.xpack.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.rest.SecurityRestFilter;
import org.elasticsearch.xpack.security.rest.action.RestAuthenticateAction;
import org.elasticsearch.xpack.security.rest.action.oauth2.RestGetTokenAction;
import org.elasticsearch.xpack.security.rest.action.oauth2.RestInvalidateTokenAction;
import org.elasticsearch.xpack.security.rest.action.realm.RestClearRealmCacheAction;
import org.elasticsearch.xpack.security.rest.action.role.RestClearRolesCacheAction;
import org.elasticsearch.xpack.security.rest.action.role.RestDeleteRoleAction;
import org.elasticsearch.xpack.security.rest.action.role.RestGetRolesAction;
import org.elasticsearch.xpack.security.rest.action.role.RestPutRoleAction;
import org.elasticsearch.xpack.security.rest.action.rolemapping.RestDeleteRoleMappingAction;
import org.elasticsearch.xpack.security.rest.action.rolemapping.RestGetRoleMappingsAction;
import org.elasticsearch.xpack.security.rest.action.rolemapping.RestPutRoleMappingAction;
import org.elasticsearch.xpack.security.rest.action.user.RestChangePasswordAction;
import org.elasticsearch.xpack.security.rest.action.user.RestDeleteUserAction;
import org.elasticsearch.xpack.security.rest.action.user.RestGetUsersAction;
import org.elasticsearch.xpack.security.rest.action.user.RestHasPrivilegesAction;
import org.elasticsearch.xpack.security.rest.action.user.RestPutUserAction;
import org.elasticsearch.xpack.security.rest.action.user.RestSetEnabledAction;
import org.elasticsearch.xpack.security.support.IndexLifecycleManager;
import org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4HttpServerTransport;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.ssl.SSLBootstrapCheck;
import org.elasticsearch.xpack.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.template.TemplateUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.security.SecurityLifecycleService.SECURITY_TEMPLATE_NAME;

public class Security implements ActionPlugin, IngestPlugin, NetworkPlugin, ClusterPlugin {

    private static final Logger logger = Loggers.getLogger(XPackPlugin.class);

    public static final String NAME4 = XPackPlugin.SECURITY + "4";

    public static final Setting<Optional<String>> USER_SETTING = new Setting<>(setting("user"), (String) null, Optional::ofNullable, Property.NodeScope);

    static final Setting<List<String>> AUDIT_OUTPUTS_SETTING = Setting.listSetting(setting("audit.outputs"), s -> s.getAsMap().containsKey(setting("audit.outputs")) ? Collections.emptyList() : Collections.singletonList(LoggingAuditTrail.NAME), Function.identity(), Property.NodeScope);

    private final Settings settings;

    private final Environment env;

    private final boolean enabled;

    private final boolean transportClientMode;

    private final XPackLicenseState licenseState;

    private final SSLService sslService;

    private final SetOnce<TransportInterceptor> securityInterceptor = new SetOnce<>();

    private final SetOnce<IPFilter> ipFilter = new SetOnce<>();

    private final SetOnce<AuthenticationService> authcService = new SetOnce<>();

    private final SetOnce<AuditTrailService> auditTrailService = new SetOnce<>();

    private final SetOnce<SecurityContext> securityContext = new SetOnce<>();

    private final SetOnce<ThreadContext> threadContext = new SetOnce<>();

    private final SetOnce<TokenService> tokenService = new SetOnce<>();

    private final List<BootstrapCheck> bootstrapChecks;

    public Security(Settings settings, Environment env, XPackLicenseState licenseState, SSLService sslService) throws IOException, GeneralSecurityException {
        this.settings = settings;
        this.env = env;
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
        this.enabled = XPackSettings.SECURITY_ENABLED.get(settings);
        if (enabled && transportClientMode == false) {
            validateAutoCreateIndex(settings);
        }
        this.licenseState = licenseState;
        this.sslService = sslService;
        if (enabled) {
            final List<BootstrapCheck> checks = new ArrayList<>();
            checks.addAll(Arrays.asList(new SSLBootstrapCheck(sslService, env), new TokenSSLBootstrapCheck(), new PkiRealmBootstrapCheck(sslService)));
            checks.addAll(InternalRealms.getBootstrapChecks(settings));
            this.bootstrapChecks = Collections.unmodifiableList(checks);
        } else {
            this.bootstrapChecks = Collections.emptyList();
        }
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();
        if (enabled == false || transportClientMode) {
            modules.add(b -> b.bind(IPFilter.class).toProvider(Providers.of(null)));
        }
        if (transportClientMode) {
            if (enabled == false) {
                return modules;
            }
            modules.add(b -> {
                b.bind(SSLService.class).toInstance(sslService);
            });
            return modules;
        }
        modules.add(b -> XPackPlugin.bindFeatureSet(b, SecurityFeatureSet.class));
        if (enabled == false) {
            modules.add(b -> {
                b.bind(Realms.class).toProvider(Providers.of(null));
                b.bind(CompositeRolesStore.class).toProvider(Providers.of(null));
                b.bind(NativeRoleMappingStore.class).toProvider(Providers.of(null));
                b.bind(AuditTrailService.class).toInstance(new AuditTrailService(settings, Collections.emptyList(), licenseState));
            });
            return modules;
        }
        modules.add(b -> {
            if (XPackSettings.AUDIT_ENABLED.get(settings)) {
                b.bind(AuditTrail.class).to(AuditTrailService.class);
            }
        });
        modules.add(new SecurityActionModule(settings));
        return modules;
    }

    public Collection<Object> createComponents(InternalClient client, ThreadPool threadPool, ClusterService clusterService, ResourceWatcherService resourceWatcherService, List<XPackExtension> extensions) throws Exception {
        if (enabled == false) {
            return Collections.emptyList();
        }
        threadContext.set(threadPool.getThreadContext());
        List<Object> components = new ArrayList<>();
        securityContext.set(new SecurityContext(settings, threadPool.getThreadContext()));
        components.add(securityContext.get());
        IndexAuditTrail indexAuditTrail = null;
        Set<AuditTrail> auditTrails = new LinkedHashSet<>();
        if (XPackSettings.AUDIT_ENABLED.get(settings)) {
            List<String> outputs = AUDIT_OUTPUTS_SETTING.get(settings);
            if (outputs.isEmpty()) {
                throw new IllegalArgumentException("Audit logging is enabled but there are zero output types in " + XPackSettings.AUDIT_ENABLED.getKey());
            }
            for (String output : outputs) {
                switch(output) {
                    case LoggingAuditTrail.NAME:
                        auditTrails.add(new LoggingAuditTrail(settings, clusterService, threadPool));
                        break;
                    case IndexAuditTrail.NAME:
                        indexAuditTrail = new IndexAuditTrail(settings, client, threadPool, clusterService);
                        auditTrails.add(indexAuditTrail);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown audit trail output [" + output + "]");
                }
            }
        }
        final AuditTrailService auditTrailService = new AuditTrailService(settings, auditTrails.stream().collect(Collectors.toList()), licenseState);
        components.add(auditTrailService);
        this.auditTrailService.set(auditTrailService);
        final SecurityLifecycleService securityLifecycleService = new SecurityLifecycleService(settings, clusterService, threadPool, client, indexAuditTrail);
        final TokenService tokenService = new TokenService(settings, Clock.systemUTC(), client, securityLifecycleService, clusterService);
        this.tokenService.set(tokenService);
        components.add(tokenService);
        final NativeUsersStore nativeUsersStore = new NativeUsersStore(settings, client, securityLifecycleService);
        final NativeRoleMappingStore nativeRoleMappingStore = new NativeRoleMappingStore(settings, client, securityLifecycleService);
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        final ReservedRealm reservedRealm = new ReservedRealm(env, settings, nativeUsersStore, anonymousUser, securityLifecycleService, threadPool.getThreadContext());
        Map<String, Realm.Factory> realmFactories = new HashMap<>();
        realmFactories.putAll(InternalRealms.getFactories(threadPool, resourceWatcherService, sslService, nativeUsersStore, nativeRoleMappingStore, securityLifecycleService));
        for (XPackExtension extension : extensions) {
            Map<String, Realm.Factory> newRealms = extension.getRealms(resourceWatcherService);
            for (Map.Entry<String, Realm.Factory> entry : newRealms.entrySet()) {
                if (realmFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Realm type [" + entry.getKey() + "] is already registered");
                }
            }
        }
        final Realms realms = new Realms(settings, env, realmFactories, licenseState, threadPool.getThreadContext(), reservedRealm);
        components.add(nativeUsersStore);
        components.add(nativeRoleMappingStore);
        components.add(realms);
        components.add(reservedRealm);
        AuthenticationFailureHandler failureHandler = null;
        String extensionName = null;
        for (XPackExtension extension : extensions) {
            AuthenticationFailureHandler extensionFailureHandler = extension.getAuthenticationFailureHandler();
            if (extensionFailureHandler != null && failureHandler != null) {
                throw new IllegalStateException("Extensions [" + extensionName + "] and [" + extension.name() + "] " + "both set an authentication failure handler");
            }
            failureHandler = extensionFailureHandler;
            extensionName = extension.name();
        }
        if (failureHandler == null) {
            logger.debug("Using default authentication failure handler");
            failureHandler = new DefaultAuthenticationFailureHandler();
        } else {
            logger.debug("Using authentication failure handler from extension [" + extensionName + "]");
        }
        authcService.set(new AuthenticationService(settings, realms, auditTrailService, failureHandler, threadPool, anonymousUser, tokenService));
        components.add(authcService.get());
        final FileRolesStore fileRolesStore = new FileRolesStore(settings, env, resourceWatcherService, licenseState);
        final NativeRolesStore nativeRolesStore = new NativeRolesStore(settings, client, licenseState, securityLifecycleService);
        final ReservedRolesStore reservedRolesStore = new ReservedRolesStore();
        List<BiConsumer<Set<String>, ActionListener<Set<RoleDescriptor>>>> rolesProviders = new ArrayList<>();
        for (XPackExtension extension : extensions) {
            rolesProviders.addAll(extension.getRolesProviders(settings, resourceWatcherService));
        }
        final CompositeRolesStore allRolesStore = new CompositeRolesStore(settings, fileRolesStore, nativeRolesStore, reservedRolesStore, rolesProviders, threadPool.getThreadContext(), licenseState);
        securityLifecycleService.addSecurityIndexHealthChangeListener(allRolesStore::onSecurityIndexHealthChange);
        licenseState.addListener(allRolesStore::invalidateAll);
        final AuthorizationService authzService = new AuthorizationService(settings, allRolesStore, clusterService, auditTrailService, failureHandler, threadPool, anonymousUser);
        components.add(nativeRolesStore);
        components.add(reservedRolesStore);
        components.add(allRolesStore);
        components.add(authzService);
        components.add(securityLifecycleService);
        ipFilter.set(new IPFilter(settings, auditTrailService, clusterService.getClusterSettings(), licenseState));
        components.add(ipFilter.get());
        DestructiveOperations destructiveOperations = new DestructiveOperations(settings, clusterService.getClusterSettings());
        securityInterceptor.set(new SecurityServerTransportInterceptor(settings, threadPool, authcService.get(), authzService, licenseState, sslService, securityContext.get(), destructiveOperations));
        return components;
    }

    public Settings additionalSettings() {
        if (enabled == false) {
            return Settings.EMPTY;
        }
        return additionalSettings(settings, transportClientMode);
    }

    static Settings additionalSettings(Settings settings, boolean transportClientMode) {
        final Settings.Builder settingsBuilder = Settings.builder();
        if (NetworkModule.TRANSPORT_TYPE_SETTING.exists(settings)) {
            final String transportType = NetworkModule.TRANSPORT_TYPE_SETTING.get(settings);
            if (NAME4.equals(transportType) == false) {
                throw new IllegalArgumentException("transport type setting [" + NetworkModule.TRANSPORT_TYPE_KEY + "] must be [" + NAME4 + "] but is [" + transportType + "]");
            }
        } else {
            settingsBuilder.put(NetworkModule.TRANSPORT_TYPE_KEY, NAME4);
        }
        if (NetworkModule.HTTP_TYPE_SETTING.exists(settings)) {
            final String httpType = NetworkModule.HTTP_TYPE_SETTING.get(settings);
            if (httpType.equals(NAME4)) {
                SecurityNetty4HttpServerTransport.overrideSettings(settingsBuilder, settings);
            } else {
                throw new IllegalArgumentException("http type setting [" + NetworkModule.HTTP_TYPE_KEY + "] must be [" + NAME4 + "] but is [" + httpType + "]");
            }
        } else {
            settingsBuilder.put(NetworkModule.HTTP_TYPE_KEY, NAME4);
            SecurityNetty4HttpServerTransport.overrideSettings(settingsBuilder, settings);
        }
        addUserSettings(settings, settingsBuilder);
        addTribeSettings(settings, settingsBuilder);
        return settingsBuilder.build();
    }

    public static List<Setting<?>> getSettings(boolean transportClientMode, @Nullable XPackExtensionsService extensionsService) {
        List<Setting<?>> settingsList = new ArrayList<>();
        settingsList.add(USER_SETTING);
        if (transportClientMode) {
            return settingsList;
        }
        IPFilter.addSettings(settingsList);
        settingsList.add(AUDIT_OUTPUTS_SETTING);
        LoggingAuditTrail.registerSettings(settingsList);
        IndexAuditTrail.registerSettings(settingsList);
        AnonymousUser.addSettings(settingsList);
        RealmSettings.addSettings(settingsList, extensionsService == null ? null : extensionsService.getExtensions());
        NativeRolesStore.addSettings(settingsList);
        ReservedRealm.addSettings(settingsList);
        AuthenticationService.addSettings(settingsList);
        AuthorizationService.addSettings(settingsList);
        settingsList.add(CompositeRolesStore.CACHE_SIZE_SETTING);
        settingsList.add(FieldPermissionsCache.CACHE_SIZE_SETTING);
        settingsList.add(TokenService.TOKEN_EXPIRATION);
        settingsList.add(TokenService.DELETE_INTERVAL);
        settingsList.add(TokenService.DELETE_TIMEOUT);
        settingsList.add(SecurityServerTransportInterceptor.TRANSPORT_TYPE_PROFILE_SETTING);
        settingsList.addAll(SSLConfigurationSettings.getProfileSettings());
        settingsList.add(Setting.listSetting(setting("hide_settings"), Collections.emptyList(), Function.identity(), Property.NodeScope, Property.Filtered));
        return settingsList;
    }

    public List<String> getSettingsFilter(@Nullable XPackExtensionsService extensionsService) {
        ArrayList<String> settingsFilter = new ArrayList<>();
        String[] asArray = settings.getAsArray(setting("hide_settings"));
        for (String pattern : asArray) {
            settingsFilter.add(pattern);
        }
        final List<XPackExtension> extensions = extensionsService == null ? Collections.emptyList() : extensionsService.getExtensions();
        settingsFilter.addAll(RealmSettings.getSettingsFilter(extensions));
        settingsFilter.add("transport.profiles.*." + setting("*"));
        return settingsFilter;
    }

    public List<BootstrapCheck> getBootstrapChecks() {
        return bootstrapChecks;
    }

    public void onIndexModule(IndexModule module) {
        if (enabled) {
            assert licenseState != null;
            if (XPackSettings.DLS_FLS_ENABLED.get(settings)) {
                module.setSearcherWrapper(indexService -> new SecurityIndexSearcherWrapper(indexService.getIndexSettings(), shardId -> indexService.newQueryShardContext(shardId.id(), null, () -> {
                    throw new IllegalArgumentException("permission filters are not allowed to use the current timestamp");
                }, null), indexService.cache().bitsetFilterCache(), indexService.getThreadPool().getThreadContext(), licenseState, indexService.getScriptService()));
                module.forceQueryCacheProvider((settings, cache) -> new OptOutQueryCache(settings, cache, threadContext.get()));
            }
            module.addSearchOperationListener(new SecuritySearchOperationListener(threadContext.get(), licenseState, auditTrailService.get()));
        }
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled == false) {
            return emptyList();
        }
        return Arrays.asList(new ActionHandler<>(ClearRealmCacheAction.INSTANCE, TransportClearRealmCacheAction.class), new ActionHandler<>(ClearRolesCacheAction.INSTANCE, TransportClearRolesCacheAction.class), new ActionHandler<>(GetUsersAction.INSTANCE, TransportGetUsersAction.class), new ActionHandler<>(PutUserAction.INSTANCE, TransportPutUserAction.class), new ActionHandler<>(DeleteUserAction.INSTANCE, TransportDeleteUserAction.class), new ActionHandler<>(GetRolesAction.INSTANCE, TransportGetRolesAction.class), new ActionHandler<>(PutRoleAction.INSTANCE, TransportPutRoleAction.class), new ActionHandler<>(DeleteRoleAction.INSTANCE, TransportDeleteRoleAction.class), new ActionHandler<>(ChangePasswordAction.INSTANCE, TransportChangePasswordAction.class), new ActionHandler<>(AuthenticateAction.INSTANCE, TransportAuthenticateAction.class), new ActionHandler<>(SetEnabledAction.INSTANCE, TransportSetEnabledAction.class), new ActionHandler<>(HasPrivilegesAction.INSTANCE, TransportHasPrivilegesAction.class), new ActionHandler<>(GetRoleMappingsAction.INSTANCE, TransportGetRoleMappingsAction.class), new ActionHandler<>(PutRoleMappingAction.INSTANCE, TransportPutRoleMappingAction.class), new ActionHandler<>(DeleteRoleMappingAction.INSTANCE, TransportDeleteRoleMappingAction.class), new ActionHandler<>(CreateTokenAction.INSTANCE, TransportCreateTokenAction.class), new ActionHandler<>(InvalidateTokenAction.INSTANCE, TransportInvalidateTokenAction.class));
    }

    @Override
    public List<Class<? extends ActionFilter>> getActionFilters() {
        if (enabled == false) {
            return emptyList();
        }
        if (transportClientMode == false) {
            return singletonList(SecurityActionFilter.class);
        }
        return emptyList();
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
        if (enabled == false) {
            return emptyList();
        }
        return Arrays.asList(new RestAuthenticateAction(settings, restController, securityContext.get(), licenseState), new RestClearRealmCacheAction(settings, restController, licenseState), new RestClearRolesCacheAction(settings, restController, licenseState), new RestGetUsersAction(settings, restController, licenseState), new RestPutUserAction(settings, restController, licenseState), new RestDeleteUserAction(settings, restController, licenseState), new RestGetRolesAction(settings, restController, licenseState), new RestPutRoleAction(settings, restController, licenseState), new RestDeleteRoleAction(settings, restController, licenseState), new RestChangePasswordAction(settings, restController, securityContext.get(), licenseState), new RestSetEnabledAction(settings, restController, licenseState), new RestHasPrivilegesAction(settings, restController, securityContext.get(), licenseState), new RestGetRoleMappingsAction(settings, restController, licenseState), new RestPutRoleMappingAction(settings, restController, licenseState), new RestDeleteRoleMappingAction(settings, restController, licenseState), new RestGetTokenAction(settings, restController, licenseState), new RestInvalidateTokenAction(settings, restController, licenseState));
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.singletonMap(SetSecurityUserProcessor.TYPE, new SetSecurityUserProcessor.Factory(parameters.threadContext));
    }

    private static void addUserSettings(Settings settings, Settings.Builder settingsBuilder) {
        String authHeaderSettingName = ThreadContext.PREFIX + "." + UsernamePasswordToken.BASIC_AUTH_HEADER;
        if (settings.get(authHeaderSettingName) != null) {
            return;
        }
        Optional<String> userOptional = USER_SETTING.get(settings);
        userOptional.ifPresent(userSetting -> {
            final int i = userSetting.indexOf(":");
            if (i < 0 || i == userSetting.length() - 1) {
                throw new IllegalArgumentException("invalid [" + USER_SETTING.getKey() + "] setting. must be in the form of " + "\"<username>:<password>\"");
            }
            String username = userSetting.substring(0, i);
            String password = userSetting.substring(i + 1);
            settingsBuilder.put(authHeaderSettingName, UsernamePasswordToken.basicAuthHeaderValue(username, new SecureString(password)));
        });
    }

    private static void addTribeSettings(Settings settings, Settings.Builder settingsBuilder) {
        Map<String, Settings> tribesSettings = settings.getGroups("tribe", true);
        if (tribesSettings.isEmpty()) {
            return;
        }
        final Map<String, String> settingsMap = settings.getAsMap();
        for (Map.Entry<String, Settings> tribeSettings : tribesSettings.entrySet()) {
            String tribePrefix = "tribe." + tribeSettings.getKey() + ".";
            if (TribeService.TRIBE_SETTING_KEYS.stream().anyMatch(s -> s.startsWith(tribePrefix))) {
                continue;
            }
            final String tribeEnabledSetting = tribePrefix + XPackSettings.SECURITY_ENABLED.getKey();
            if (settings.get(tribeEnabledSetting) != null) {
                boolean enabled = XPackSettings.SECURITY_ENABLED.get(tribeSettings.getValue());
                if (!enabled) {
                    throw new IllegalStateException("tribe setting [" + tribeEnabledSetting + "] must be set to true but the value is [" + settings.get(tribeEnabledSetting) + "]");
                }
            } else {
                settingsBuilder.put(tribeEnabledSetting, true);
            }
            for (Map.Entry<String, String> entry : settingsMap.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith("xpack.security.")) {
                    settingsBuilder.put(tribePrefix + key, entry.getValue());
                }
            }
        }
        Map<String, Settings> realmsSettings = settings.getGroups(setting("authc.realms"), true);
        final boolean hasNativeRealm = XPackSettings.RESERVED_REALM_ENABLED_SETTING.get(settings) || realmsSettings.isEmpty() || realmsSettings.entrySet().stream().anyMatch((e) -> NativeRealm.TYPE.equals(e.getValue().get("type")) && e.getValue().getAsBoolean("enabled", true));
        if (hasNativeRealm) {
            if (TribeService.ON_CONFLICT_SETTING.get(settings).startsWith("prefer_") == false) {
                throw new IllegalArgumentException("use of security on tribe nodes requires setting [tribe.on_conflict] to specify the " + "name of the tribe to prefer such as [prefer_t1] as the security index can exist in multiple tribes but only one" + " can be used by the tribe node");
            }
        }
    }

    public static String settingPrefix() {
        return XPackPlugin.featureSettingPrefix(XPackPlugin.SECURITY) + ".";
    }

    public static String setting(String setting) {
        assert setting != null && setting.startsWith(".") == false;
        return settingPrefix() + setting;
    }

    static boolean indexAuditLoggingEnabled(Settings settings) {
        if (XPackSettings.AUDIT_ENABLED.get(settings)) {
            List<String> outputs = AUDIT_OUTPUTS_SETTING.get(settings);
            for (String output : outputs) {
                if (output.equals(IndexAuditTrail.NAME)) {
                    return true;
                }
            }
        }
        return false;
    }

    static void validateAutoCreateIndex(Settings settings) {
        String value = settings.get("action.auto_create_index");
        if (value == null) {
            return;
        }
        final boolean indexAuditingEnabled = Security.indexAuditLoggingEnabled(settings);
        final String auditIndex;
        if (indexAuditingEnabled) {
            auditIndex = "," + IndexAuditTrail.INDEX_NAME_PREFIX + "*";
        } else {
            auditIndex = "";
        }
        String securityIndices = SecurityLifecycleService.indexNames().stream().collect(Collectors.joining(","));
        String errorMessage = LoggerMessageFormat.format("the [action.auto_create_index] setting value [{}] is too" + " restrictive. disable [action.auto_create_index] or set it to " + "[{}{}]", (Object) value, securityIndices, auditIndex);
        if (Booleans.isFalse(value)) {
            throw new IllegalArgumentException(errorMessage);
        }
        if (Booleans.isTrue(value)) {
            return;
        }
        String[] matches = Strings.commaDelimitedListToStringArray(value);
        List<String> indices = new ArrayList<>();
        indices.addAll(SecurityLifecycleService.indexNames());
        if (indexAuditingEnabled) {
            DateTime now = new DateTime(DateTimeZone.UTC);
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now, IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusDays(1), IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusMonths(1), IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusMonths(2), IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusMonths(3), IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusMonths(4), IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusMonths(5), IndexNameResolver.Rollover.DAILY));
            indices.add(IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, now.plusMonths(6), IndexNameResolver.Rollover.DAILY));
        }
        for (String index : indices) {
            boolean matched = false;
            for (String match : matches) {
                char c = match.charAt(0);
                if (c == '-') {
                    if (Regex.simpleMatch(match.substring(1), index)) {
                        throw new IllegalArgumentException(errorMessage);
                    }
                } else if (c == '+') {
                    if (Regex.simpleMatch(match.substring(1), index)) {
                        matched = true;
                        break;
                    }
                } else {
                    if (Regex.simpleMatch(match, index)) {
                        matched = true;
                        break;
                    }
                }
            }
            if (!matched) {
                throw new IllegalArgumentException(errorMessage);
            }
        }
        if (indexAuditingEnabled) {
            logger.warn("the [action.auto_create_index] setting is configured to be restrictive [{}]. " + " for the next 6 months audit indices are allowed to be created, but please make sure" + " that any future history indices after 6 months with the pattern " + "[.security_audit_log*] are allowed to be created", value);
        }
    }

    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        if (transportClientMode || enabled == false) {
            return Collections.emptyList();
        }
        return Collections.singletonList(new TransportInterceptor() {

            @Override
            public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, String executor, boolean forceExecution, TransportRequestHandler<T> actualHandler) {
                assert securityInterceptor.get() != null;
                return securityInterceptor.get().interceptHandler(action, executor, forceExecution, actualHandler);
            }

            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                assert securityInterceptor.get() != null;
                return securityInterceptor.get().interceptSender(sender);
            }
        });
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays, CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry, NetworkService networkService) {
        if (enabled == false) {
            return Collections.emptyMap();
        }
        return Collections.singletonMap(Security.NAME4, () -> new SecurityNetty4Transport(settings, threadPool, networkService, bigArrays, namedWriteableRegistry, circuitBreakerService, ipFilter.get(), sslService));
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays, CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry, NamedXContentRegistry xContentRegistry, NetworkService networkService, HttpServerTransport.Dispatcher dispatcher) {
        if (enabled == false) {
            return Collections.emptyMap();
        }
        return Collections.singletonMap(Security.NAME4, () -> new SecurityNetty4HttpServerTransport(settings, networkService, bigArrays, ipFilter.get(), sslService, threadPool, xContentRegistry, dispatcher));
    }

    @Override
    public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
        if (enabled == false || transportClientMode) {
            return null;
        }
        final boolean ssl = HTTP_SSL_ENABLED.get(settings);
        Settings httpSSLSettings = SSLService.getHttpTransportSSLSettings(settings);
        boolean extractClientCertificate = ssl && sslService.isSSLClientAuthEnabled(httpSSLSettings);
        return handler -> new SecurityRestFilter(licenseState, threadContext, authcService.get(), handler, extractClientCertificate);
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedWriteableRegistry.Entry(ClusterState.Custom.class, TokenMetaData.TYPE, TokenMetaData::new));
        entries.add(new NamedWriteableRegistry.Entry(NamedDiff.class, TokenMetaData.TYPE, TokenMetaData::readDiffFrom));
        entries.addAll(Arrays.asList(ExpressionParser.NAMED_WRITEABLES));
        return entries;
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
        if (enabled && transportClientMode == false) {
            return Collections.singletonList(new FixedExecutorBuilder(settings, TokenService.THREAD_POOL_NAME, 1, 1000, "xpack.security.authc.token.thread_pool"));
        }
        return Collections.emptyList();
    }

    public UnaryOperator<Map<String, IndexTemplateMetaData>> getIndexTemplateMetaDataUpgrader() {
        return templates -> {
            final byte[] securityTemplate = TemplateUtils.loadTemplate("/" + SECURITY_TEMPLATE_NAME + ".json", Version.CURRENT.toString(), IndexLifecycleManager.TEMPLATE_VERSION_PATTERN).getBytes(StandardCharsets.UTF_8);
            final XContent xContent = XContentFactory.xContent(XContentType.JSON);
            try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, securityTemplate)) {
                templates.put(SECURITY_TEMPLATE_NAME, IndexTemplateMetaData.Builder.fromXContent(parser, SECURITY_TEMPLATE_NAME));
            } catch (IOException e) {
                logger.error("Error loading security template [{}] as part of metadata upgrading", SECURITY_TEMPLATE_NAME);
            }
            return templates;
        };
    }

    @Override
    public Map<String, Supplier<ClusterState.Custom>> getInitialClusterStateCustomSupplier() {
        if (enabled) {
            return Collections.singletonMap(TokenMetaData.TYPE, () -> tokenService.get().getTokenMetaData());
        } else {
            return Collections.emptyMap();
        }
    }
}