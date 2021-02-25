package org.apache.dubbo.config;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.monitor.MonitorFactory;
import org.apache.dubbo.monitor.MonitorService;
import org.apache.dubbo.registry.RegistryService;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.InvokerListener;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.support.MockInvoker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.CLUSTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_PROTOCOL;
import static org.apache.dubbo.common.constants.CommonConstants.FILE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PID_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RELEASE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SHUTDOWN_WAIT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SHUTDOWN_WAIT_SECONDS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_PROTOCOL;
import static org.apache.dubbo.common.constants.RegistryConstants.SERVICE_REGISTRY_PROTOCOL;
import static org.apache.dubbo.common.extension.ExtensionLoader.getExtensionLoader;
import static org.apache.dubbo.common.utils.UrlUtils.isServiceDiscoveryRegistryType;
import static org.apache.dubbo.config.Constants.DUBBO_IP_TO_REGISTRY;
import static org.apache.dubbo.config.Constants.LAYER_KEY;
import static org.apache.dubbo.config.Constants.LISTENER_KEY;
import static org.apache.dubbo.monitor.Constants.LOGSTAT_PROTOCOL;
import static org.apache.dubbo.registry.Constants.REGISTER_IP_KEY;
import static org.apache.dubbo.registry.Constants.REGISTER_KEY;
import static org.apache.dubbo.registry.Constants.SUBSCRIBE_KEY;
import static org.apache.dubbo.remoting.Constants.DUBBO_VERSION_KEY;
import static org.apache.dubbo.rpc.Constants.INVOKER_LISTENER_KEY;
import static org.apache.dubbo.rpc.Constants.LOCAL_KEY;
import static org.apache.dubbo.rpc.Constants.PROXY_KEY;
import static org.apache.dubbo.rpc.Constants.REFERENCE_FILTER_KEY;
import static org.apache.dubbo.rpc.Constants.RETURN_PREFIX;
import static org.apache.dubbo.rpc.Constants.THROW_PREFIX;
import static org.apache.dubbo.rpc.cluster.Constants.REFER_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.TAG_KEY;

public abstract class AbstractInterfaceConfig extends AbstractMethodConfig {

    private static final long serialVersionUID = -1559314110797223229L;

    protected String local;

    protected String stub;

    protected MonitorConfig monitor;

    protected String proxy;

    protected String cluster;

    protected String filter;

    protected String listener;

    protected String owner;

    protected Integer connections;

    protected String layer;

    protected ApplicationConfig application;

    protected ModuleConfig module;

    protected List<RegistryConfig> registries;

    protected String registryIds;

    protected String onconnect;

    protected String ondisconnect;

    protected MetricsConfig metrics;

    protected MetadataReportConfig metadataReportConfig;

    protected ConfigCenterConfig configCenter;

    private Integer callbacks;

    private String scope;

    protected String tag;

    public void checkRegistry() {
        loadRegistriesFromBackwardConfig();
        convertRegistryIdsToRegistries();
        for (RegistryConfig registryConfig : registries) {
            if (!registryConfig.isValid()) {
                throw new IllegalStateException("No registry config found or it's not a valid config! " + "The registry config is: " + registryConfig);
            }
        }
    }

    @SuppressWarnings("deprecation")
    public void checkApplication() {
        createApplicationIfAbsent();
        if (!application.isValid()) {
            throw new IllegalStateException("No application config found or it's not a valid config! " + "Please add <dubbo:application name=\"...\" /> to your spring config.");
        }
        ApplicationModel.setApplication(application.getName());
        String wait = ConfigUtils.getProperty(SHUTDOWN_WAIT_KEY);
        if (wait != null && wait.trim().length() > 0) {
            System.setProperty(SHUTDOWN_WAIT_KEY, wait.trim());
        } else {
            wait = ConfigUtils.getProperty(SHUTDOWN_WAIT_SECONDS_KEY);
            if (wait != null && wait.trim().length() > 0) {
                System.setProperty(SHUTDOWN_WAIT_SECONDS_KEY, wait.trim());
            }
        }
    }

    public void checkMonitor() {
        createMonitorIfAbsent();
        if (!monitor.isValid()) {
            logger.info("There's no valid monitor config found, if you want to open monitor statistics for Dubbo, " + "please make sure your monitor is configured properly.");
        }
    }

    private void createMonitorIfAbsent() {
        if (this.monitor != null) {
            return;
        }
        ConfigManager configManager = ConfigManager.getInstance();
        setMonitor(configManager.getMonitor().orElseGet(() -> {
            MonitorConfig monitorConfig = new MonitorConfig();
            monitorConfig.refresh();
            return monitorConfig;
        }));
    }

    public void checkMetadataReport() {
        if (metadataReportConfig == null) {
            ConfigManager configManager = ConfigManager.getInstance();
            if (CollectionUtils.isNotEmpty(configManager.getMetadataConfigs())) {
                setMetadataReportConfig(configManager.getMetadataConfigs().toArray(new MetadataReportConfig[configManager.getMetadataConfigs().size()])[0]);
            }
        }
        if (metadataReportConfig == null) {
            setMetadataReportConfig(new MetadataReportConfig());
        }
        metadataReportConfig.refresh();
        if (!metadataReportConfig.isValid()) {
            logger.warn("There's no valid metadata config found, if you are using the simplified mode of registry url, " + "please make sure you have a metadata address configured properly.");
        }
    }

    protected List<URL> loadRegistries(boolean provider) {
        List<URL> registryList = new ArrayList<URL>();
        if (CollectionUtils.isNotEmpty(registries)) {
            for (RegistryConfig config : registries) {
                String address = config.getAddress();
                if (StringUtils.isEmpty(address)) {
                    address = ANYHOST_VALUE;
                }
                if (!RegistryConfig.NO_AVAILABLE.equalsIgnoreCase(address)) {
                    Map<String, String> map = new HashMap<String, String>();
                    appendParameters(map, application);
                    appendParameters(map, config);
                    map.put(PATH_KEY, RegistryService.class.getName());
                    appendRuntimeParameters(map);
                    if (!map.containsKey(PROTOCOL_KEY)) {
                        map.put(PROTOCOL_KEY, DUBBO_PROTOCOL);
                    }
                    List<URL> urls = UrlUtils.parseURLs(address, map);
                    for (URL url : urls) {
                        url = URLBuilder.from(url).addParameter(REGISTRY_KEY, url.getProtocol()).setProtocol(extractRegistryType(url)).build();
                        if ((provider && url.getParameter(REGISTER_KEY, true)) || (!provider && url.getParameter(SUBSCRIBE_KEY, true))) {
                            registryList.add(url);
                        }
                    }
                }
            }
        }
        return registryList;
    }

    private String extractRegistryType(URL url) {
        return isServiceDiscoveryRegistryType(url) ? SERVICE_REGISTRY_PROTOCOL : REGISTRY_PROTOCOL;
    }

    protected URL loadMonitor(URL registryURL) {
        checkMonitor();
        Map<String, String> map = new HashMap<String, String>();
        map.put(INTERFACE_KEY, MonitorService.class.getName());
        appendRuntimeParameters(map);
        String hostToRegistry = ConfigUtils.getSystemProperty(DUBBO_IP_TO_REGISTRY);
        if (StringUtils.isEmpty(hostToRegistry)) {
            hostToRegistry = NetUtils.getLocalHost();
        } else if (NetUtils.isInvalidLocalHost(hostToRegistry)) {
            throw new IllegalArgumentException("Specified invalid registry ip from property:" + DUBBO_IP_TO_REGISTRY + ", value:" + hostToRegistry);
        }
        map.put(REGISTER_IP_KEY, hostToRegistry);
        appendParameters(map, monitor);
        appendParameters(map, application);
        String address = monitor.getAddress();
        String sysaddress = System.getProperty("dubbo.monitor.address");
        if (sysaddress != null && sysaddress.length() > 0) {
            address = sysaddress;
        }
        if (ConfigUtils.isNotEmpty(address)) {
            if (!map.containsKey(PROTOCOL_KEY)) {
                if (getExtensionLoader(MonitorFactory.class).hasExtension(LOGSTAT_PROTOCOL)) {
                    map.put(PROTOCOL_KEY, LOGSTAT_PROTOCOL);
                } else {
                    map.put(PROTOCOL_KEY, DUBBO_PROTOCOL);
                }
            }
            return UrlUtils.parseURL(address, map);
        } else if ((REGISTRY_PROTOCOL.equals(monitor.getProtocol()) || SERVICE_REGISTRY_PROTOCOL.equals(monitor.getProtocol())) && registryURL != null) {
            return URLBuilder.from(registryURL).setProtocol(DUBBO_PROTOCOL).addParameter(PROTOCOL_KEY, monitor.getProtocol()).addParameterAndEncoded(REFER_KEY, StringUtils.toQueryString(map)).build();
        }
        return null;
    }

    public static void appendRuntimeParameters(Map<String, String> map) {
        map.put(DUBBO_VERSION_KEY, Version.getProtocolVersion());
        map.put(RELEASE_KEY, Version.getVersion());
        map.put(TIMESTAMP_KEY, String.valueOf(System.currentTimeMillis()));
        if (ConfigUtils.getPid() > 0) {
            map.put(PID_KEY, String.valueOf(ConfigUtils.getPid()));
        }
    }

    public void checkInterfaceAndMethods(Class<?> interfaceClass, List<MethodConfig> methods) {
        Assert.notNull(interfaceClass, new IllegalStateException("interface not allow null!"));
        if (!interfaceClass.isInterface()) {
            throw new IllegalStateException("The interface class " + interfaceClass + " is not a interface!");
        }
        if (CollectionUtils.isNotEmpty(methods)) {
            for (MethodConfig methodBean : methods) {
                methodBean.setService(interfaceClass.getName());
                methodBean.setServiceId(this.getId());
                methodBean.refresh();
                String methodName = methodBean.getName();
                if (StringUtils.isEmpty(methodName)) {
                    throw new IllegalStateException("<dubbo:method> name attribute is required! Please check: " + "<dubbo:service interface=\"" + interfaceClass.getName() + "\" ... >" + "<dubbo:method name=\"\" ... /></<dubbo:reference>");
                }
                boolean hasMethod = Arrays.stream(interfaceClass.getMethods()).anyMatch(method -> method.getName().equals(methodName));
                if (!hasMethod) {
                    throw new IllegalStateException("The interface " + interfaceClass.getName() + " not found method " + methodName);
                }
            }
        }
    }

    public void checkMock(Class<?> interfaceClass) {
        if (ConfigUtils.isEmpty(mock)) {
            return;
        }
        String normalizedMock = MockInvoker.normalizeMock(mock);
        if (normalizedMock.startsWith(RETURN_PREFIX)) {
            normalizedMock = normalizedMock.substring(RETURN_PREFIX.length()).trim();
            try {
                MockInvoker.parseMockValue(normalizedMock);
            } catch (Exception e) {
                throw new IllegalStateException("Illegal mock return in <dubbo:service/reference ... " + "mock=\"" + mock + "\" />");
            }
        } else if (normalizedMock.startsWith(THROW_PREFIX)) {
            normalizedMock = normalizedMock.substring(THROW_PREFIX.length()).trim();
            if (ConfigUtils.isNotEmpty(normalizedMock)) {
                try {
                    MockInvoker.getThrowable(normalizedMock);
                } catch (Exception e) {
                    throw new IllegalStateException("Illegal mock throw in <dubbo:service/reference ... " + "mock=\"" + mock + "\" />");
                }
            }
        } else {
            MockInvoker.getMockObject(normalizedMock, interfaceClass);
        }
    }

    public void checkStubAndLocal(Class<?> interfaceClass) {
        if (ConfigUtils.isNotEmpty(local)) {
            Class<?> localClass = ConfigUtils.isDefault(local) ? ReflectUtils.forName(interfaceClass.getName() + "Local") : ReflectUtils.forName(local);
            verify(interfaceClass, localClass);
        }
        if (ConfigUtils.isNotEmpty(stub)) {
            Class<?> localClass = ConfigUtils.isDefault(stub) ? ReflectUtils.forName(interfaceClass.getName() + "Stub") : ReflectUtils.forName(stub);
            verify(interfaceClass, localClass);
        }
    }

    private void verify(Class<?> interfaceClass, Class<?> localClass) {
        if (!interfaceClass.isAssignableFrom(localClass)) {
            throw new IllegalStateException("The local implementation class " + localClass.getName() + " not implement interface " + interfaceClass.getName());
        }
        try {
            ReflectUtils.findConstructor(localClass, interfaceClass);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("No such constructor \"public " + localClass.getSimpleName() + "(" + interfaceClass.getName() + ")\" in local implementation class " + localClass.getName());
        }
    }

    private void convertRegistryIdsToRegistries() {
        computeValidRegistryIds();
        if (StringUtils.isEmpty(registryIds)) {
            if (CollectionUtils.isEmpty(registries)) {
                List<RegistryConfig> registryConfigs = ConfigManager.getInstance().getDefaultRegistries();
                if (registryConfigs.isEmpty()) {
                    registryConfigs = new ArrayList<>();
                    RegistryConfig registryConfig = new RegistryConfig();
                    registryConfig.refresh();
                    registryConfigs.add(registryConfig);
                }
                setRegistries(registryConfigs);
            }
        } else {
            String[] ids = COMMA_SPLIT_PATTERN.split(registryIds);
            List<RegistryConfig> tmpRegistries = CollectionUtils.isNotEmpty(registries) ? registries : new ArrayList<>();
            Arrays.stream(ids).forEach(id -> {
                if (tmpRegistries.stream().noneMatch(reg -> reg.getId().equals(id))) {
                    ConfigManager.getInstance().getRegistry(id).ifPresent(tmpRegistries::add);
                }
            });
            if (tmpRegistries.size() > ids.length) {
                throw new IllegalStateException("Too much registries found, the registries assigned to this service " + "are :" + registryIds + ", but got " + tmpRegistries.size() + " registries!");
            }
            setRegistries(tmpRegistries);
        }
    }

    protected void computeValidRegistryIds() {
        if (StringUtils.isEmpty(getRegistryIds())) {
            if (getApplication() != null && StringUtils.isNotEmpty(getApplication().getRegistryIds())) {
                setRegistryIds(getApplication().getRegistryIds());
            }
        }
    }

    private void loadRegistriesFromBackwardConfig() {
        if (registries == null || registries.isEmpty()) {
            String address = ConfigUtils.getProperty("dubbo.registry.address");
            if (address != null && address.length() > 0) {
                List<RegistryConfig> tmpRegistries = new ArrayList<RegistryConfig>();
                String[] as = address.split("\\s*[|]+\\s*");
                for (String a : as) {
                    RegistryConfig registryConfig = new RegistryConfig();
                    registryConfig.setAddress(a);
                    registryConfig.refresh();
                    tmpRegistries.add(registryConfig);
                }
                setRegistries(tmpRegistries);
            }
        }
    }

    @Deprecated
    public String getLocal() {
        return local;
    }

    @Deprecated
    public void setLocal(Boolean local) {
        if (local == null) {
            setLocal((String) null);
        } else {
            setLocal(local.toString());
        }
    }

    @Deprecated
    public void setLocal(String local) {
        checkName(LOCAL_KEY, local);
        this.local = local;
    }

    public String getStub() {
        return stub;
    }

    public void setStub(Boolean stub) {
        if (stub == null) {
            setStub((String) null);
        } else {
            setStub(stub.toString());
        }
    }

    public void setStub(String stub) {
        checkName("stub", stub);
        this.stub = stub;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        checkExtension(Cluster.class, CLUSTER_KEY, cluster);
        this.cluster = cluster;
    }

    public String getProxy() {
        return proxy;
    }

    public void setProxy(String proxy) {
        checkExtension(ProxyFactory.class, PROXY_KEY, proxy);
        this.proxy = proxy;
    }

    public Integer getConnections() {
        return connections;
    }

    public void setConnections(Integer connections) {
        this.connections = connections;
    }

    @Parameter(key = REFERENCE_FILTER_KEY, append = true)
    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        checkMultiExtension(Filter.class, FILE_KEY, filter);
        this.filter = filter;
    }

    @Parameter(key = INVOKER_LISTENER_KEY, append = true)
    public String getListener() {
        return listener;
    }

    public void setListener(String listener) {
        checkMultiExtension(InvokerListener.class, LISTENER_KEY, listener);
        this.listener = listener;
    }

    public String getLayer() {
        return layer;
    }

    public void setLayer(String layer) {
        checkNameHasSymbol(LAYER_KEY, layer);
        this.layer = layer;
    }

    public ApplicationConfig getApplication() {
        return application;
    }

    public void setApplication(ApplicationConfig application) {
        this.application = application;
    }

    private void createApplicationIfAbsent() {
        if (this.application != null) {
            this.application.refresh();
            return;
        }
        ConfigManager configManager = ConfigManager.getInstance();
        setApplication(configManager.getApplication().orElseGet(() -> {
            ApplicationConfig applicationConfig = new ApplicationConfig();
            applicationConfig.refresh();
            return applicationConfig;
        }));
    }

    public ModuleConfig getModule() {
        return module;
    }

    public void setModule(ModuleConfig module) {
        this.module = module;
    }

    public RegistryConfig getRegistry() {
        return CollectionUtils.isEmpty(registries) ? null : registries.get(0);
    }

    public void setRegistry(RegistryConfig registry) {
        List<RegistryConfig> registries = new ArrayList<RegistryConfig>(1);
        registries.add(registry);
        setRegistries(registries);
    }

    public List<RegistryConfig> getRegistries() {
        return registries;
    }

    @SuppressWarnings({ "unchecked" })
    public void setRegistries(List<? extends RegistryConfig> registries) {
        this.registries = (List<RegistryConfig>) registries;
    }

    @Parameter(excluded = true)
    public String getRegistryIds() {
        return registryIds;
    }

    public void setRegistryIds(String registryIds) {
        this.registryIds = registryIds;
    }

    public MonitorConfig getMonitor() {
        return monitor;
    }

    public void setMonitor(String monitor) {
        setMonitor(new MonitorConfig(monitor));
    }

    public void setMonitor(MonitorConfig monitor) {
        this.monitor = monitor;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        checkMultiName("owner", owner);
        this.owner = owner;
    }

    public ConfigCenterConfig getConfigCenter() {
        return configCenter;
    }

    public void setConfigCenter(ConfigCenterConfig configCenter) {
        this.configCenter = configCenter;
    }

    public Integer getCallbacks() {
        return callbacks;
    }

    public void setCallbacks(Integer callbacks) {
        this.callbacks = callbacks;
    }

    public String getOnconnect() {
        return onconnect;
    }

    public void setOnconnect(String onconnect) {
        this.onconnect = onconnect;
    }

    public String getOndisconnect() {
        return ondisconnect;
    }

    public void setOndisconnect(String ondisconnect) {
        this.ondisconnect = ondisconnect;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public MetadataReportConfig getMetadataReportConfig() {
        return metadataReportConfig;
    }

    public void setMetadataReportConfig(MetadataReportConfig metadataReportConfig) {
        this.metadataReportConfig = metadataReportConfig;
    }

    public MetricsConfig getMetrics() {
        return metrics;
    }

    public void setMetrics(MetricsConfig metrics) {
        this.metrics = metrics;
    }

    @Parameter(key = TAG_KEY, useKeyAsProperty = false)
    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }
}