package org.apache.dubbo.registry.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.metadata.ServiceNameMapping;
import org.apache.dubbo.metadata.WritableMetadataService;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.client.ServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceDiscoveryFactory;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.metadata.proxy.MetadataServiceProxyFactory;
import org.apache.dubbo.registry.client.selector.ServiceInstanceSelector;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_PROTOCOL;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.METADATA_DEFAULT;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_TYPE_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.SERVICE_REGISTRY_TYPE;
import static org.apache.dubbo.common.constants.RegistryConstants.SUBSCRIBED_SERVICE_NAMES_KEY;
import static org.apache.dubbo.common.extension.ExtensionLoader.getExtensionLoader;
import static org.apache.dubbo.common.utils.CollectionUtils.isEmpty;
import static org.apache.dubbo.common.utils.CollectionUtils.isNotEmpty;
import static org.apache.dubbo.common.utils.StringUtils.isBlank;
import static org.apache.dubbo.metadata.report.support.Constants.METADATA_REPORT_KEY;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getExportedServicesRevision;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getMetadataServiceURLsParams;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getProviderHost;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getProviderPort;

public class ServiceOrientedRegistry extends FailbackRegistry {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final ServiceDiscovery serviceDiscovery;

    private final Set<String> subscribedServices;

    private final ServiceNameMapping serviceNameMapping;

    private final WritableMetadataService writableMetadataService;

    private final MetadataServiceProxyFactory metadataServiceProxyFactory;

    public ServiceOrientedRegistry(URL registryURL) {
        super(registryURL);
        this.serviceDiscovery = buildServiceDiscovery(registryURL);
        this.subscribedServices = buildSubscribedServices(registryURL);
        this.serviceNameMapping = ServiceNameMapping.getDefaultExtension();
        String metadata = registryURL.getParameter(METADATA_REPORT_KEY, METADATA_DEFAULT);
        this.writableMetadataService = WritableMetadataService.getExtension(metadata);
        this.metadataServiceProxyFactory = MetadataServiceProxyFactory.getExtension(metadata);
    }

    private Set<String> buildSubscribedServices(URL url) {
        String subscribedServiceNames = url.getParameter(SUBSCRIBED_SERVICE_NAMES_KEY);
        return isBlank(subscribedServiceNames) ? emptySet() : unmodifiableSet(of(subscribedServiceNames.split(",")).map(String::trim).filter(StringUtils::isNotEmpty).collect(toSet()));
    }

    private ServiceDiscovery buildServiceDiscovery(URL url) {
        ServiceDiscoveryFactory factory = ExtensionLoader.getExtensionLoader(ServiceDiscoveryFactory.class).getAdaptiveExtension();
        ServiceDiscovery serviceDiscovery = factory.getDiscovery(url);
        serviceDiscovery.start();
        return serviceDiscovery;
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
    public void doRegister(URL url) {
        if (!shouldRegister(url)) {
            return;
        }
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
    public void doUnregister(URL url) {
        if (!shouldRegister(url)) {
            return;
        }
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
    public void doSubscribe(URL url, NotifyListener listener) {
        if (!shouldSubscribe(url)) {
            return;
        }
        subscribeURLs(url, listener);
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
        serviceDiscovery.stop();
    }

    protected void subscribeURLs(URL url, NotifyListener listener) {
        writableMetadataService.subscribeURL(url);
        Set<String> serviceNames = getServices(url);
        serviceNames.forEach(serviceName -> subscribeURLs(url, listener, serviceName));
    }

    protected void subscribeURLs(URL url, NotifyListener listener, String serviceName) {
        List<ServiceInstance> serviceInstances = serviceDiscovery.getInstances(serviceName);
        subscribeURLs(url, listener, serviceName, serviceInstances);
        serviceDiscovery.addServiceInstancesChangedListener(serviceName, event -> {
            subscribeURLs(url, listener, event.getServiceName(), new ArrayList<>(event.getServiceInstances()));
        });
    }

    protected void subscribeURLs(URL subscribedURL, NotifyListener listener, String serviceName, Collection<ServiceInstance> serviceInstances) {
        if (isEmpty(serviceInstances)) {
            logger.warn(format("There is no instance in service[name : %s]", serviceName));
            return;
        }
        List<URL> subscribedURLs = getSubscribedURLs(subscribedURL, serviceInstances);
        listener.notify(subscribedURLs);
    }

    private List<URL> getSubscribedURLs(URL subscribedURL, Collection<ServiceInstance> instances) {
        List<URL> subscribedURLs = new LinkedList<>();
        List<ServiceInstance> serviceInstances = instances.stream().filter(ServiceInstance::isEnabled).filter(ServiceInstance::isHealthy).collect(Collectors.toList());
        Map<String, List<URL>> revisionURLsCache = new HashMap<>();
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
        try {
            MetadataService metadataService = metadataServiceProxyFactory.getProxy(providerInstance);
            List<String> urls = metadataService.getExportedURLs(serviceInterface, group, version, protocol);
            exportedURLs = urls.stream().map(URL::valueOf).collect(Collectors.toList());
        } catch (Throwable e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
        }
        return exportedURLs;
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

    public static ServiceOrientedRegistry create(URL registryURL) {
        return supports(registryURL) ? new ServiceOrientedRegistry(registryURL) : null;
    }

    public static boolean supports(URL registryURL) {
        return SERVICE_REGISTRY_TYPE.equalsIgnoreCase(registryURL.getParameter(REGISTRY_TYPE_KEY));
    }

    public ServiceDiscovery getServiceDiscovery() {
        return serviceDiscovery;
    }
}