package org.apache.dubbo.registry.client;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.metadata.ServiceNameMapping;
import org.apache.dubbo.metadata.WritableMetadataService;
import org.apache.dubbo.metadata.store.InMemoryWritableMetadataService;
import org.apache.dubbo.metadata.store.RemoteWritableMetadataService;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils;
import org.apache.dubbo.registry.client.metadata.SubscribedURLsSynthesizer;
import org.apache.dubbo.registry.client.metadata.proxy.MetadataServiceProxyFactory;
import org.apache.dubbo.registry.client.selector.ServiceInstanceSelector;
import org.apache.dubbo.registry.support.FailbackRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;
import static org.apache.dubbo.common.URLBuilder.from;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_PROTOCOL;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PID_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_TYPE_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.SERVICE_REGISTRY_TYPE;
import static org.apache.dubbo.common.constants.RegistryConstants.SUBSCRIBED_SERVICE_NAMES_KEY;
import static org.apache.dubbo.common.extension.ExtensionLoader.getExtensionLoader;
import static org.apache.dubbo.common.function.ThrowableAction.execute;
import static org.apache.dubbo.common.utils.CollectionUtils.isEmpty;
import static org.apache.dubbo.common.utils.CollectionUtils.isNotEmpty;
import static org.apache.dubbo.common.utils.DubboServiceLoader.loadServices;
import static org.apache.dubbo.common.utils.StringUtils.isBlank;
import static org.apache.dubbo.metadata.WritableMetadataService.DEFAULT_EXTENSION;
import static org.apache.dubbo.registry.client.ServiceDiscoveryFactory.getExtension;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getExportedServicesRevision;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getMetadataStorageType;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getProtocolPort;

public class ServiceDiscoveryRegistry extends FailbackRegistry {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final ServiceDiscovery serviceDiscovery;

    private final Set<String> subscribedServices;

    private final ServiceNameMapping serviceNameMapping;

    private final WritableMetadataService writableMetadataService;

    private final Set<String> registeredListeners = new LinkedHashSet<>();

    private final List<SubscribedURLsSynthesizer> subscribedURLsSynthesizers;

    private final Map<String, Map<String, List<URL>>> serviceExportedURLsCache = new LinkedHashMap<>();

    public ServiceDiscoveryRegistry(URL registryURL) {
        super(registryURL);
        this.serviceDiscovery = createServiceDiscovery(registryURL);
        this.subscribedServices = getSubscribedServices(registryURL);
        this.serviceNameMapping = ServiceNameMapping.getDefaultExtension();
        String metadataStorageType = getMetadataStorageType(registryURL);
        this.writableMetadataService = WritableMetadataService.getExtension(metadataStorageType);
        this.subscribedURLsSynthesizers = initSubscribedURLsSynthesizers();
    }

    public static Set<String> getSubscribedServices(URL registryURL) {
        String subscribedServiceNames = registryURL.getParameter(SUBSCRIBED_SERVICE_NAMES_KEY);
        return isBlank(subscribedServiceNames) ? emptySet() : unmodifiableSet(of(subscribedServiceNames.split(",")).map(String::trim).filter(StringUtils::isNotEmpty).collect(toSet()));
    }

    protected ServiceDiscovery createServiceDiscovery(URL registryURL) {
        ServiceDiscovery originalServiceDiscovery = getServiceDiscovery(registryURL);
        ServiceDiscovery serviceDiscovery = enhanceEventPublishing(originalServiceDiscovery);
        execute(() -> {
            serviceDiscovery.initialize(registryURL.addParameter(INTERFACE_KEY, ServiceDiscovery.class.getName()).removeParameter(REGISTRY_TYPE_KEY));
        });
        return serviceDiscovery;
    }

    private List<SubscribedURLsSynthesizer> initSubscribedURLsSynthesizers() {
        return loadServices(SubscribedURLsSynthesizer.class, this.getClass().getClassLoader());
    }

    private ServiceDiscovery getServiceDiscovery(URL registryURL) {
        ServiceDiscoveryFactory factory = getExtension(registryURL);
        return factory.getServiceDiscovery(registryURL);
    }

    private ServiceDiscovery enhanceEventPublishing(ServiceDiscovery original) {
        return new EventPublishingServiceDiscovery(original);
    }

    protected boolean shouldRegister(URL providerURL) {
        String side = providerURL.getParameter(SIDE_KEY);
        boolean should = PROVIDER_SIDE.equals(side);
        if (!should) {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("The URL[%s] should not be registered.", providerURL.toString()));
            }
        }
        return should;
    }

    protected boolean shouldSubscribe(URL subscribedURL) {
        return !shouldRegister(subscribedURL);
    }

    @Override
    public final void register(URL url) {
        if (!shouldRegister(url)) {
            return;
        }
        super.register(url);
    }

    @Override
    public void doRegister(URL url) {
        if (writableMetadataService.exportURL(url)) {
            if (logger.isInfoEnabled()) {
                logger.info(format("The URL[%s] registered successfully.", url.toString()));
            }
        } else {
            if (logger.isWarnEnabled()) {
                logger.info(format("The URL[%s] has been registered.", url.toString()));
            }
        }
    }

    @Override
    public final void unregister(URL url) {
        if (!shouldRegister(url)) {
            return;
        }
        super.unregister(url);
    }

    @Override
    public void doUnregister(URL url) {
        if (writableMetadataService.unexportURL(url)) {
            if (logger.isInfoEnabled()) {
                logger.info(format("The URL[%s] deregistered successfully.", url.toString()));
            }
        } else {
            if (logger.isWarnEnabled()) {
                logger.info(format("The URL[%s] has been deregistered.", url.toString()));
            }
        }
    }

    @Override
    public final void subscribe(URL url, NotifyListener listener) {
        if (!shouldSubscribe(url)) {
            return;
        }
        super.subscribe(url, listener);
    }

    @Override
    public void doSubscribe(URL url, NotifyListener listener) {
        subscribeURLs(url, listener);
    }

    @Override
    public final void unsubscribe(URL url, NotifyListener listener) {
        if (!shouldSubscribe(url)) {
            return;
        }
        super.unsubscribe(url, listener);
    }

    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {
        writableMetadataService.unsubscribeURL(url);
    }

    @Override
    public boolean isAvailable() {
        return !serviceDiscovery.getServices().isEmpty();
    }

    @Override
    public void destroy() {
        super.destroy();
        execute(() -> {
            serviceDiscovery.destroy();
        });
    }

    protected void subscribeURLs(URL url, NotifyListener listener) {
        writableMetadataService.subscribeURL(url);
        Set<String> serviceNames = getServices(url);
        serviceNames.forEach(serviceName -> subscribeURLs(url, listener, serviceName));
    }

    protected void subscribeURLs(URL url, NotifyListener listener, String serviceName) {
        List<ServiceInstance> serviceInstances = serviceDiscovery.getInstances(serviceName);
        subscribeURLs(url, listener, serviceName, serviceInstances);
        registerServiceInstancesChangedListener(url, new ServiceInstancesChangedListener(serviceName) {

            @Override
            public void onEvent(ServiceInstancesChangedEvent event) {
                subscribeURLs(url, listener, event.getServiceName(), new ArrayList<>(event.getServiceInstances()));
            }
        });
    }

    private void registerServiceInstancesChangedListener(URL url, ServiceInstancesChangedListener listener) {
        String listenerId = createListenerId(url, listener);
        if (registeredListeners.add(listenerId)) {
            serviceDiscovery.addServiceInstancesChangedListener(listener);
        }
    }

    private String createListenerId(URL url, ServiceInstancesChangedListener listener) {
        return listener.getServiceName() + ":" + url.toString(VERSION_KEY, GROUP_KEY, PROTOCOL_KEY);
    }

    protected void subscribeURLs(URL subscribedURL, NotifyListener listener, String serviceName, Collection<ServiceInstance> serviceInstances) {
        if (isEmpty(serviceInstances)) {
            logger.warn(format("There is no instance in service[name : %s]", serviceName));
            return;
        }
        List<URL> subscribedURLs = new LinkedList<>();
        subscribedURLs.addAll(getExportedURLs(subscribedURL, serviceInstances));
        if (subscribedURLs.isEmpty()) {
            subscribedURLs.addAll(synthesizeSubscribedURLs(subscribedURL, serviceInstances));
        }
        listener.notify(subscribedURLs);
    }

    private List<URL> getExportedURLs(URL subscribedURL, Collection<ServiceInstance> instances) {
        List<ServiceInstance> serviceInstances = instances.stream().filter(ServiceInstance::isEnabled).filter(ServiceInstance::isHealthy).filter(ServiceInstanceMetadataUtils::isDubboServiceInstance).collect(Collectors.toList());
        int size = serviceInstances.size();
        if (size == 0) {
            return emptyList();
        }
        expungeStaleRevisionExportedURLs(serviceInstances);
        initTemplateURLs(subscribedURL, serviceInstances);
        List<URL> subscribedURLs = cloneExportedURLs(subscribedURL, serviceInstances);
        serviceInstances.clear();
        return subscribedURLs;
    }

    private void initTemplateURLs(URL subscribedURL, List<ServiceInstance> serviceInstances) {
        for (int i = 0; i < serviceInstances.size(); i++) {
            ServiceInstance selectedInstance = selectServiceInstance(serviceInstances);
            List<URL> templateURLs = getTemplateExportedURLs(subscribedURL, selectedInstance);
            if (isNotEmpty(templateURLs)) {
                break;
            } else {
                serviceInstances.remove(selectedInstance);
            }
        }
    }

    private void expungeStaleRevisionExportedURLs(List<ServiceInstance> serviceInstances) {
        if (isEmpty(serviceInstances)) {
            return;
        }
        String serviceName = serviceInstances.get(0).getServiceName();
        synchronized (this) {
            Map<String, List<URL>> revisionExportedURLs = serviceExportedURLsCache.computeIfAbsent(serviceName, s -> new HashMap<>());
            if (revisionExportedURLs.isEmpty()) {
                return;
            }
            Set<String> existedRevisions = revisionExportedURLs.keySet();
            Set<String> currentRevisions = serviceInstances.stream().map(ServiceInstanceMetadataUtils::getExportedServicesRevision).collect(Collectors.toSet());
            Set<String> staleRevisions = new HashSet<>(existedRevisions);
            staleRevisions.removeAll(currentRevisions);
            staleRevisions.forEach(revisionExportedURLs::remove);
        }
    }

    private List<URL> cloneExportedURLs(URL subscribedURL, Collection<ServiceInstance> serviceInstances) {
        if (isEmpty(serviceInstances)) {
            return emptyList();
        }
        List<URL> clonedExportedURLs = new LinkedList<>();
        serviceInstances.forEach(serviceInstance -> {
            String host = serviceInstance.getHost();
            getTemplateExportedURLs(subscribedURL, serviceInstance).stream().map(templateURL -> templateURL.removeParameter(TIMESTAMP_KEY)).map(templateURL -> templateURL.removeParameter(PID_KEY)).map(templateURL -> {
                String protocol = templateURL.getProtocol();
                int port = getProtocolPort(serviceInstance, protocol);
                if (Objects.equals(templateURL.getHost(), host) && Objects.equals(templateURL.getPort(), port)) {
                    return templateURL;
                }
                URLBuilder clonedURLBuilder = from(templateURL).setHost(host).setPort(port);
                return clonedURLBuilder.build();
            }).forEach(clonedExportedURLs::add);
        });
        return clonedExportedURLs;
    }

    private ServiceInstance selectServiceInstance(List<ServiceInstance> serviceInstances) {
        int size = serviceInstances.size();
        if (size == 0) {
            return null;
        } else if (size == 1) {
            return serviceInstances.get(0);
        }
        ServiceInstanceSelector selector = getExtensionLoader(ServiceInstanceSelector.class).getAdaptiveExtension();
        return selector.select(getUrl(), serviceInstances);
    }

    private List<URL> getTemplateExportedURLs(URL subscribedURL, ServiceInstance selectedInstance) {
        List<URL> exportedURLs = getRevisionExportedURLs(selectedInstance);
        if (isEmpty(exportedURLs)) {
            return emptyList();
        }
        return filterSubscribedURLs(subscribedURL, exportedURLs);
    }

    private List<URL> filterSubscribedURLs(URL subscribedURL, List<URL> exportedURLs) {
        return exportedURLs.stream().filter(url -> isSameServiceInterface(subscribedURL, url)).filter(url -> isSameParameter(subscribedURL, url, VERSION_KEY)).filter(url -> isSameParameter(subscribedURL, url, GROUP_KEY)).filter(url -> isCompatibleProtocol(subscribedURL, url)).collect(Collectors.toList());
    }

    private boolean isSameServiceInterface(URL one, URL another) {
        return Objects.equals(one.getServiceInterface(), another.getServiceInterface());
    }

    private boolean isSameParameter(URL one, URL another, String key) {
        return Objects.equals(one.getParameter(key), another.getParameter(key));
    }

    private boolean isCompatibleProtocol(URL one, URL another) {
        String protocol = one.getParameter(PROTOCOL_KEY);
        return isCompatibleProtocol(protocol, another);
    }

    private boolean isCompatibleProtocol(String protocol, URL targetURL) {
        return protocol == null || Objects.equals(protocol, targetURL.getParameter(PROTOCOL_KEY)) || Objects.equals(protocol, targetURL.getProtocol());
    }

    private List<URL> getRevisionExportedURLs(ServiceInstance providerServiceInstance) {
        if (providerServiceInstance == null) {
            return emptyList();
        }
        String serviceName = providerServiceInstance.getServiceName();
        String revision = getExportedServicesRevision(providerServiceInstance);
        List<URL> exportedURLs = null;
        synchronized (this) {
            Map<String, List<URL>> exportedURLsMap = serviceExportedURLsCache.computeIfAbsent(serviceName, s -> new LinkedHashMap());
            exportedURLs = exportedURLsMap.get(revision);
            boolean firstGet = false;
            if (exportedURLs == null) {
                if (!exportedURLsMap.isEmpty()) {
                    if (logger.isWarnEnabled()) {
                        logger.warn(format("The ServiceInstance[id: %s, host : %s , port : %s] has different revision : %s" + ", please make sure the service [name : %s] is changing or not.", providerServiceInstance.getId(), providerServiceInstance.getHost(), providerServiceInstance.getPort(), revision, providerServiceInstance.getServiceName()));
                    }
                } else {
                    firstGet = true;
                }
                exportedURLs = getExportedURLs(providerServiceInstance);
                if (exportedURLs != null) {
                    exportedURLsMap.put(revision, exportedURLs);
                    if (logger.isDebugEnabled()) {
                        logger.debug(format("Getting the exported URLs[size : %s, first : %s] from the target service " + "instance [id: %s , service : %s , host : %s , port : %s , revision : %s]", exportedURLs.size(), firstGet, providerServiceInstance.getId(), providerServiceInstance.getServiceName(), providerServiceInstance.getHost(), providerServiceInstance.getPort(), revision));
                    }
                }
            }
        }
        return exportedURLs != null ? new ArrayList<>(exportedURLs) : null;
    }

    private List<URL> getExportedURLs(ServiceInstance providerServiceInstance) {
        List<URL> exportedURLs = null;
        String metadataStorageType = getMetadataStorageType(providerServiceInstance);
        try {
            MetadataService metadataService = MetadataServiceProxyFactory.getExtension(metadataStorageType == null ? DEFAULT_EXTENSION : metadataStorageType).getProxy(providerServiceInstance);
            SortedSet<String> urls = metadataService.getExportedURLs();
            exportedURLs = urls.stream().map(URL::valueOf).collect(Collectors.toList());
        } catch (Throwable e) {
            if (logger.isErrorEnabled()) {
                logger.error(format("It's failed to get the exported URLs from the target service instance[%s]", providerServiceInstance), e);
            }
            exportedURLs = null;
        }
        return exportedURLs;
    }

    protected List<URL> getExportedURLs(URL subscribedURL, ServiceInstance providerServiceInstance) {
        List<URL> exportedURLs = emptyList();
        String serviceInterface = subscribedURL.getServiceInterface();
        String group = subscribedURL.getParameter(GROUP_KEY);
        String version = subscribedURL.getParameter(VERSION_KEY);
        String protocol = subscribedURL.getParameter(PROTOCOL_KEY);
        String metadataStorageType = getMetadataStorageType(providerServiceInstance);
        try {
            MetadataService metadataService = MetadataServiceProxyFactory.getExtension(metadataStorageType == null ? DEFAULT_EXTENSION : metadataStorageType).getProxy(providerServiceInstance);
            SortedSet<String> urls = metadataService.getExportedURLs(serviceInterface, group, version, protocol);
            exportedURLs = urls.stream().map(URL::valueOf).collect(Collectors.toList());
        } catch (Throwable e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
        }
        return exportedURLs;
    }

    private Collection<? extends URL> synthesizeSubscribedURLs(URL subscribedURL, Collection<ServiceInstance> serviceInstances) {
        return subscribedURLsSynthesizers.stream().filter(synthesizer -> synthesizer.supports(subscribedURL)).map(synthesizer -> synthesizer.synthesize(subscribedURL, serviceInstances)).flatMap(Collection::stream).collect(Collectors.toList());
    }

    protected Set<String> getServices(URL subscribedURL) {
        Set<String> serviceNames = getSubscribedServices();
        if (isEmpty(serviceNames)) {
            serviceNames = findMappedServices(subscribedURL);
        }
        return serviceNames;
    }

    public Set<String> getSubscribedServices() {
        return subscribedServices;
    }

    protected Set<String> findMappedServices(URL subscribedURL) {
        String serviceInterface = subscribedURL.getServiceInterface();
        String group = subscribedURL.getParameter(GROUP_KEY);
        String version = subscribedURL.getParameter(VERSION_KEY);
        String protocol = subscribedURL.getParameter(PROTOCOL_KEY, DUBBO_PROTOCOL);
        return serviceNameMapping.get(serviceInterface, group, version, protocol);
    }

    public static ServiceDiscoveryRegistry create(URL registryURL) {
        return supports(registryURL) ? new ServiceDiscoveryRegistry(registryURL) : null;
    }

    public static boolean supports(URL registryURL) {
        return SERVICE_REGISTRY_TYPE.equalsIgnoreCase(registryURL.getParameter(REGISTRY_TYPE_KEY));
    }
}