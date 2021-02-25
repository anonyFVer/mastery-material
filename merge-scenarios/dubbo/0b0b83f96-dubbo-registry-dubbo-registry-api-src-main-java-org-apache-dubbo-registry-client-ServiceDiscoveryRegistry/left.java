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
import org.apache.dubbo.registry.client.metadata.proxy.MetadataServiceProxyFactory;
import org.apache.dubbo.registry.client.selector.ServiceInstanceSelector;
import org.apache.dubbo.registry.support.FailbackRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.stream.Stream.of;
import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_PROTOCOL;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_CHAR_SEPERATOR;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_TYPE_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.SERVICE_REGISTRY_TYPE;
import static org.apache.dubbo.common.constants.RegistryConstants.SUBSCRIBED_PROTOCOL_DEFAULT;
import static org.apache.dubbo.common.constants.RegistryConstants.SUBSCRIBED_SERVICE_NAMES_KEY;
import static org.apache.dubbo.common.extension.ExtensionLoader.getExtensionLoader;
import static org.apache.dubbo.common.function.ThrowableAction.execute;
import static org.apache.dubbo.common.utils.CollectionUtils.isEmpty;
import static org.apache.dubbo.common.utils.CollectionUtils.isEmptyMap;
import static org.apache.dubbo.common.utils.CollectionUtils.isNotEmpty;
import static org.apache.dubbo.common.utils.StringUtils.isBlank;
import static org.apache.dubbo.metadata.WritableMetadataService.DEFAULT_EXTENSION;
import static org.apache.dubbo.registry.client.ServiceDiscoveryFactory.getExtension;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getExportedServicesRevision;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getMetadataServiceURLsParams;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getMetadataStorageType;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getProviderHost;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getProviderPort;

public class ServiceDiscoveryRegistry extends FailbackRegistry {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final ServiceDiscovery serviceDiscovery;

    private final Map<String, String> subscribedServices;

    private final ServiceNameMapping serviceNameMapping;

    private final WritableMetadataService writableMetadataService;

    private final Set<String> listenedServices = new LinkedHashSet<>();

    public ServiceDiscoveryRegistry(URL registryURL) {
        super(registryURL);
        this.serviceDiscovery = createServiceDiscovery(registryURL);
        this.subscribedServices = getSubscribedServices(registryURL);
        this.serviceNameMapping = ServiceNameMapping.getDefaultExtension();
        String metadataStorageType = getMetadataStorageType(registryURL);
        this.writableMetadataService = WritableMetadataService.getExtension(metadataStorageType);
    }

    public static Map<String, String> getSubscribedServices(URL registryURL) {
        Map<String, String> services = new HashMap<>();
        String subscribedServiceNames = registryURL.getParameter(SUBSCRIBED_SERVICE_NAMES_KEY);
        if (isBlank(subscribedServiceNames)) {
            return services;
        } else {
            of(COMMA_SPLIT_PATTERN.split(subscribedServiceNames)).map(String::trim).filter(StringUtils::isNotEmpty).forEach(serviceProtocol -> {
                String[] arr = serviceProtocol.split(GROUP_CHAR_SEPERATOR);
                if (arr.length > 1) {
                    services.put(arr[0], arr[1]);
                } else {
                    services.put(arr[0], SUBSCRIBED_PROTOCOL_DEFAULT);
                }
            });
        }
        return services;
    }

    protected ServiceDiscovery createServiceDiscovery(URL registryURL) {
        ServiceDiscovery originalServiceDiscovery = getServiceDiscovery(registryURL);
        ServiceDiscovery serviceDiscovery = enhanceEventPublishing(originalServiceDiscovery);
        execute(() -> {
            serviceDiscovery.initialize(registryURL.addParameter(INTERFACE_KEY, ServiceDiscovery.class.getName()).removeParameter(REGISTRY_TYPE_KEY));
        });
        return serviceDiscovery;
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
        Map<String, String> services = getServices(url);
        services.forEach((name, proto) -> subscribeURLs(url, listener, name));
    }

    protected void subscribeURLs(URL url, NotifyListener listener, String serviceName) {
        List<ServiceInstance> serviceInstances = serviceDiscovery.getInstances(serviceName);
        subscribeURLs(url, listener, serviceName, serviceInstances);
        registerServiceInstancesChangedListener(new ServiceInstancesChangedListener(serviceName, subscribedServices.get(serviceName)) {

            @Override
            public void onEvent(ServiceInstancesChangedEvent event) {
                subscribeURLs(url, listener, event.getServiceName(), new ArrayList<>(event.getServiceInstances()));
            }
        });
    }

    private void registerServiceInstancesChangedListener(ServiceInstancesChangedListener listener) {
        if (listenedServices.add(listener.getServiceName())) {
            serviceDiscovery.addServiceInstancesChangedListener(listener);
        }
    }

    protected void subscribeURLs(URL subscribedURL, NotifyListener listener, String serviceName, Collection<ServiceInstance> serviceInstances) {
        if (isEmpty(serviceInstances)) {
            logger.warn(format("There is no instance in service[name : %s]", serviceName));
            return;
        }
        List<URL> subscribedURLs = getSubscribedURLs(subscribedURL, serviceInstances, serviceName);
        listener.notify(subscribedURLs);
    }

    private List<URL> getSubscribedURLs(URL subscribedURL, Collection<ServiceInstance> instances, String serviceName) {
        List<URL> subscribedURLs = new LinkedList<>();
        List<ServiceInstance> serviceInstances = instances.stream().filter(ServiceInstance::isEnabled).filter(ServiceInstance::isHealthy).collect(Collectors.toList());
        Map<String, List<URL>> revisionURLsCache = new HashMap<>();
        if (ServiceInstanceMetadataUtils.isDubboServiceInstance(serviceInstances.get(0))) {
            for (int i = 0; i < serviceInstances.size(); i++) {
                ServiceInstance selectedInstance = selectServiceInstance(serviceInstances);
                List<URL> templateURLs = getTemplateURLs(subscribedURL, selectedInstance, revisionURLsCache);
                if (isNotEmpty(templateURLs)) {
                    subscribedURLs.addAll(templateURLs);
                    serviceInstances.remove(selectedInstance);
                    break;
                }
            }
            List<URL> clonedURLs = cloneSubscribedURLs(subscribedURL, serviceInstances, revisionURLsCache);
            subscribedURLs.addAll(clonedURLs);
            revisionURLsCache.clear();
        } else {
            for (ServiceInstance instance : serviceInstances) {
                URLBuilder builder = new URLBuilder(subscribedServices.get(serviceName), instance.getHost(), instance.getPort(), subscribedURL.getServiceInterface(), instance.getMetadata());
                builder.addParameter(APPLICATION_KEY, serviceName);
                subscribedURLs.add(builder.build());
            }
        }
        serviceInstances.clear();
        return subscribedURLs;
    }

    private List<URL> cloneSubscribedURLs(URL subscribedURL, Collection<ServiceInstance> serviceInstances, Map<String, List<URL>> revisionURLsCache) {
        if (!revisionURLsCache.isEmpty()) {
            List<URL> clonedURLs = new LinkedList<>();
            Iterator<ServiceInstance> iterator = serviceInstances.iterator();
            while (iterator.hasNext()) {
                ServiceInstance serviceInstance = iterator.next();
                List<URL> templateURLs = getTemplateURLs(subscribedURL, serviceInstance, revisionURLsCache);
                Map<String, Map<String, Object>> serviceURLsParams = getMetadataServiceURLsParams(serviceInstance);
                templateURLs.forEach(templateURL -> {
                    String protocol = templateURL.getProtocol();
                    Map<String, Object> serviceURLParams = serviceURLsParams.get(protocol);
                    String host = getProviderHost(serviceURLParams);
                    Integer port = getProviderPort(serviceURLParams);
                    URL newSubscribedURL = new URL(protocol, host, port, templateURL.getParameters());
                    clonedURLs.add(newSubscribedURL);
                });
            }
            return clonedURLs;
        }
        return Collections.emptyList();
    }

    private ServiceInstance selectServiceInstance(List<ServiceInstance> serviceInstances) {
        ServiceInstanceSelector selector = getExtensionLoader(ServiceInstanceSelector.class).getAdaptiveExtension();
        return selector.select(getUrl(), serviceInstances);
    }

    protected List<URL> getTemplateURLs(URL subscribedURL, ServiceInstance selectedInstance, Map<String, List<URL>> revisionURLsCache) {
        String revision = getExportedServicesRevision(selectedInstance);
        List<URL> templateURLs = revisionURLsCache.get(revision);
        if (isEmpty(templateURLs)) {
            if (!revisionURLsCache.isEmpty()) {
                if (logger.isWarnEnabled()) {
                    logger.warn(format("The ServiceInstance[id: %s, host : %s , port : %s] has different revision : %s" + ", please make sure the service [name : %s] is changing or not.", selectedInstance.getId(), selectedInstance.getHost(), selectedInstance.getPort(), revision, selectedInstance.getServiceName()));
                }
            }
            templateURLs = getProviderExportedURLs(subscribedURL, selectedInstance);
            revisionURLsCache.put(revision, templateURLs);
        }
        return templateURLs;
    }

    protected List<URL> getProviderExportedURLs(URL subscribedURL, ServiceInstance providerInstance) {
        List<URL> exportedURLs = emptyList();
        String serviceInterface = subscribedURL.getServiceInterface();
        String group = subscribedURL.getParameter(GROUP_KEY);
        String version = subscribedURL.getParameter(VERSION_KEY);
        String protocol = subscribedURL.getParameter(PROTOCOL_KEY);
        String metadataStorageType = getMetadataStorageType(providerInstance);
        try {
            MetadataService metadataService = MetadataServiceProxyFactory.getExtension(metadataStorageType == null ? DEFAULT_EXTENSION : metadataStorageType).getProxy(providerInstance);
            SortedSet<String> urls = metadataService.getExportedURLs(serviceInterface, group, version, protocol);
            exportedURLs = urls.stream().map(URL::valueOf).collect(Collectors.toList());
        } catch (Throwable e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
        }
        return exportedURLs;
    }

    protected Map<String, String> getServices(URL subscribedURL) {
        Map<String, String> services = getSubscribedServices();
        if (isEmptyMap(services)) {
            services = findMappedServices(subscribedURL);
        }
        return services;
    }

    public Map<String, String> getSubscribedServices() {
        return subscribedServices;
    }

    protected Map<String, String> findMappedServices(URL subscribedURL) {
        String serviceInterface = subscribedURL.getServiceInterface();
        String group = subscribedURL.getParameter(GROUP_KEY);
        String version = subscribedURL.getParameter(VERSION_KEY);
        String protocol = subscribedURL.getParameter(PROTOCOL_KEY, DUBBO_PROTOCOL);
        Map<String, String> services = new LinkedHashMap<>();
        serviceNameMapping.get(serviceInterface, group, version, protocol).forEach(s -> {
            services.put(s, protocol);
        });
        return services;
    }

    public static ServiceDiscoveryRegistry create(URL registryURL) {
        return supports(registryURL) ? new ServiceDiscoveryRegistry(registryURL) : null;
    }

    public static boolean supports(URL registryURL) {
        return SERVICE_REGISTRY_TYPE.equalsIgnoreCase(registryURL.getParameter(REGISTRY_TYPE_KEY));
    }
}