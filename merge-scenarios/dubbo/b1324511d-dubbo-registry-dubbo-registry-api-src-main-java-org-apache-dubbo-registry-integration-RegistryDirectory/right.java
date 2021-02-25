package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.configcenter.ConfigChangeEvent;
import org.apache.dubbo.configcenter.DynamicConfiguration;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.ConfiguratorFactory;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.RouterChain;
import org.apache.dubbo.rpc.cluster.RouterFactory;
import org.apache.dubbo.rpc.cluster.directory.AbstractDirectory;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.protocol.InvokerWrapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import static org.apache.dubbo.common.Constants.APP_DYNAMIC_CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.Constants.CATEGORY_KEY;
import static org.apache.dubbo.common.Constants.CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.Constants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.Constants.DYNAMIC_CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.Constants.PROVIDERS_CATEGORY;
import static org.apache.dubbo.common.Constants.ROUTERS_CATEGORY;
import static org.apache.dubbo.common.Constants.ROUTE_PROTOCOL;
import static org.apache.dubbo.common.utils.UrlUtils.classifyUrls;

public class RegistryDirectory<T> extends AbstractDirectory<T> implements NotifyListener {

    private static final Logger logger = LoggerFactory.getLogger(RegistryDirectory.class);

    private static final Cluster cluster = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();

    private static final RouterFactory routerFactory = ExtensionLoader.getExtensionLoader(RouterFactory.class).getAdaptiveExtension();

    private static final ConfiguratorFactory configuratorFactory = ExtensionLoader.getExtensionLoader(ConfiguratorFactory.class).getAdaptiveExtension();

    private final String serviceKey;

    private final Class<T> serviceType;

    private final Map<String, String> queryMap;

    private final URL directoryUrl;

    private final String[] serviceMethods;

    private final boolean multiGroup;

    private Protocol protocol;

    private Registry registry;

    private volatile boolean forbidden = false;

    private volatile URL overrideDirectoryUrl;

    private volatile List<Configurator> configurators;

    private volatile Map<String, Invoker<T>> urlInvokerMap;

    private volatile List<Invoker<T>> invokers;

    private volatile Set<URL> cachedInvokerUrls;

    private static final ConsumerConfigurationListener consumerConfigurationListener = new ConsumerConfigurationListener();

    private ReferenceConfigurationListener serviceConfigurationListener;

    public RegistryDirectory(Class<T> serviceType, URL url) {
        super(url);
        if (serviceType == null) {
            throw new IllegalArgumentException("service type is null.");
        }
        if (url.getServiceKey() == null || url.getServiceKey().length() == 0) {
            throw new IllegalArgumentException("registry serviceKey is null.");
        }
        this.serviceType = serviceType;
        this.serviceKey = url.getServiceKey();
        this.queryMap = StringUtils.parseQueryString(url.getParameterAndDecoded(Constants.REFER_KEY));
        this.overrideDirectoryUrl = this.directoryUrl = turnRegistryUrlToConsumerUrl(url);
        String group = directoryUrl.getParameter(Constants.GROUP_KEY, "");
        this.multiGroup = group != null && ("*".equals(group) || group.contains(","));
        String methods = queryMap.get(Constants.METHODS_KEY);
        this.serviceMethods = methods == null ? null : Constants.COMMA_SPLIT_PATTERN.split(methods);
    }

    private URL turnRegistryUrlToConsumerUrl(URL url) {
        String isDefault = url.getParameter(Constants.DEFAULT_KEY);
        if (StringUtils.isNotEmpty(isDefault)) {
            queryMap.put(Constants.REGISTRY_KEY + "." + Constants.DEFAULT_KEY, isDefault);
        }
        return url.setPath(url.getServiceInterface()).clearParameters().addParameters(queryMap).removeParameter(Constants.MONITOR_KEY);
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public void subscribe(URL url) {
        setConsumerUrl(url);
        consumerConfigurationListener.addNotifyListener(this);
        serviceConfigurationListener = new ReferenceConfigurationListener(url);
        registry.subscribe(url, this);
    }

    @Override
    public void destroy() {
        if (isDestroyed()) {
            return;
        }
        try {
            if (getConsumerUrl() != null && registry != null && registry.isAvailable()) {
                registry.unsubscribe(getConsumerUrl(), this);
            }
            DynamicConfiguration.getDynamicConfiguration().removeListener(ApplicationModel.getApplication(), consumerConfigurationListener);
        } catch (Throwable t) {
            logger.warn("unexpected error when unsubscribe service " + serviceKey + "from registry" + registry.getUrl(), t);
        }
        super.destroy();
        try {
            destroyAllInvokers();
        } catch (Throwable t) {
            logger.warn("Failed to destroy service " + serviceKey, t);
        }
    }

    @Override
    public synchronized void notify(List<URL> urls) {
        List<URL> categoryUrls = urls.stream().filter(this::isValidCategory).filter(this::isNotCompatibleFor26x).collect(Collectors.toList());
        this.configurators = Configurator.toConfigurators(classifyUrls(categoryUrls, url -> url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY).equals(CONFIGURATORS_CATEGORY))).orElse(configurators);
        toRouters(classifyUrls(categoryUrls, url -> {
            return url.getProtocol().equals(ROUTE_PROTOCOL) || url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY).equals(ROUTERS_CATEGORY);
        })).ifPresent(this::addRouters);
        refreshOverrideAndInvoker(classifyUrls(categoryUrls, url -> url.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY).equals(PROVIDERS_CATEGORY)));
    }

    public void refreshOverrideAndInvoker(List<URL> urls) {
        overrideDirectoryUrl();
        refreshInvoker(urls);
    }

    private void refreshInvoker(List<URL> invokerUrls) {
        if (invokerUrls != null && invokerUrls.size() == 1 && invokerUrls.get(0) != null && Constants.EMPTY_PROTOCOL.equals(invokerUrls.get(0).getProtocol())) {
            this.forbidden = true;
            this.invokers = null;
            routerChain.notifyFullInvokers(this.invokers, getConsumerUrl());
            destroyAllInvokers();
        } else {
            this.forbidden = false;
            Map<String, Invoker<T>> oldUrlInvokerMap = this.urlInvokerMap;
            if (invokerUrls == null) {
                invokerUrls = new ArrayList<>();
            }
            if (invokerUrls.isEmpty() && this.cachedInvokerUrls != null) {
                invokerUrls.addAll(this.cachedInvokerUrls);
            } else {
                this.cachedInvokerUrls = new HashSet<>();
                this.cachedInvokerUrls.addAll(invokerUrls);
            }
            if (invokerUrls.isEmpty()) {
                return;
            }
            Map<String, Invoker<T>> newUrlInvokerMap = toInvokers(invokerUrls);
            if (newUrlInvokerMap == null || newUrlInvokerMap.size() == 0) {
                logger.error(new IllegalStateException("urls to invokers error .invokerUrls.size :" + invokerUrls.size() + ", invoker.size :0. urls :" + invokerUrls.toString()));
                return;
            }
            List<Invoker<T>> newInvokers = Collections.unmodifiableList(new ArrayList<>(newUrlInvokerMap.values()));
            routerChain.notifyFullInvokers(newInvokers, getConsumerUrl());
            this.invokers = multiGroup ? toMergeInvokerList(newInvokers) : newInvokers;
            this.urlInvokerMap = newUrlInvokerMap;
            try {
                destroyUnusedInvokers(oldUrlInvokerMap, newUrlInvokerMap);
            } catch (Exception e) {
                logger.warn("destroyUnusedInvokers error. ", e);
            }
        }
    }

    private List<Invoker<T>> toMergeInvokerList(List<Invoker<T>> invokers) {
        List<Invoker<T>> mergedInvokers = new ArrayList<>();
        Map<String, List<Invoker<T>>> groupMap = new HashMap<String, List<Invoker<T>>>();
        for (Invoker<T> invoker : invokers) {
            String group = invoker.getUrl().getParameter(Constants.GROUP_KEY, "");
            groupMap.computeIfAbsent(group, k -> new ArrayList<>());
            groupMap.get(group).add(invoker);
        }
        if (groupMap.size() == 1) {
            mergedInvokers.addAll(groupMap.values().iterator().next());
        } else if (groupMap.size() > 1) {
            for (List<Invoker<T>> groupList : groupMap.values()) {
                StaticDirectory<T> staticDirectory = new StaticDirectory<>(groupList);
                staticDirectory.buildRouterChain();
                mergedInvokers.add(cluster.join(staticDirectory));
            }
        } else {
            mergedInvokers = invokers;
        }
        return mergedInvokers;
    }

    private Optional<List<Router>> toRouters(List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            return Optional.empty();
        }
        List<Router> routers = new ArrayList<Router>();
        for (URL url : urls) {
            if (Constants.EMPTY_PROTOCOL.equals(url.getProtocol())) {
                continue;
            }
            String routerType = url.getParameter(Constants.ROUTER_KEY);
            if (routerType != null && routerType.length() > 0) {
                url = url.setProtocol(routerType);
            }
            try {
                Router router = routerFactory.getRouter(url);
                router.addRouterChain(routerChain);
                if (!routers.contains(router))
                    routers.add(router);
            } catch (Throwable t) {
                logger.error("convert router url to router error, url: " + url, t);
            }
        }
        return Optional.of(routers);
    }

    private Map<String, Invoker<T>> toInvokers(List<URL> urls) {
        Map<String, Invoker<T>> newUrlInvokerMap = new HashMap<String, Invoker<T>>();
        if (urls == null || urls.isEmpty()) {
            return newUrlInvokerMap;
        }
        Set<String> keys = new HashSet<String>();
        String queryProtocols = this.queryMap.get(Constants.PROTOCOL_KEY);
        for (URL providerUrl : urls) {
            if (queryProtocols != null && queryProtocols.length() > 0) {
                boolean accept = false;
                String[] acceptProtocols = queryProtocols.split(",");
                for (String acceptProtocol : acceptProtocols) {
                    if (providerUrl.getProtocol().equals(acceptProtocol)) {
                        accept = true;
                        break;
                    }
                }
                if (!accept) {
                    continue;
                }
            }
            if (Constants.EMPTY_PROTOCOL.equals(providerUrl.getProtocol())) {
                continue;
            }
            if (!ExtensionLoader.getExtensionLoader(Protocol.class).hasExtension(providerUrl.getProtocol())) {
                logger.error(new IllegalStateException("Unsupported protocol " + providerUrl.getProtocol() + " in notified url: " + providerUrl + " from registry " + getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost() + ", supported protocol: " + ExtensionLoader.getExtensionLoader(Protocol.class).getSupportedExtensions()));
                continue;
            }
            URL url = mergeUrl(providerUrl);
            String key = url.toFullString();
            if (keys.contains(key)) {
                continue;
            }
            keys.add(key);
            Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap;
            Invoker<T> invoker = localUrlInvokerMap == null ? null : localUrlInvokerMap.get(key);
            if (invoker == null) {
                try {
                    boolean enabled = true;
                    if (url.hasParameter(Constants.DISABLED_KEY)) {
                        enabled = !url.getParameter(Constants.DISABLED_KEY, false);
                    } else {
                        enabled = url.getParameter(Constants.ENABLED_KEY, true);
                    }
                    if (enabled) {
                        invoker = new InvokerDelegate<T>(protocol.refer(serviceType, url), url, providerUrl);
                    }
                } catch (Throwable t) {
                    logger.error("Failed to refer invoker for interface:" + serviceType + ",url:(" + url + ")" + t.getMessage(), t);
                }
                if (invoker != null) {
                    newUrlInvokerMap.put(key, invoker);
                }
            } else {
                newUrlInvokerMap.put(key, invoker);
            }
        }
        keys.clear();
        return newUrlInvokerMap;
    }

    private URL mergeUrl(URL providerUrl) {
        providerUrl = ClusterUtils.mergeUrl(providerUrl, queryMap);
        providerUrl = overrideWithConfigurator(providerUrl);
        providerUrl = providerUrl.addParameter(Constants.CHECK_KEY, String.valueOf(false));
        this.overrideDirectoryUrl = this.overrideDirectoryUrl.addParametersIfAbsent(providerUrl.getParameters());
        if ((providerUrl.getPath() == null || providerUrl.getPath().length() == 0) && "dubbo".equals(providerUrl.getProtocol())) {
            String path = directoryUrl.getParameter(Constants.INTERFACE_KEY);
            if (path != null) {
                int i = path.indexOf('/');
                if (i >= 0) {
                    path = path.substring(i + 1);
                }
                i = path.lastIndexOf(':');
                if (i >= 0) {
                    path = path.substring(0, i);
                }
                providerUrl = providerUrl.setPath(path);
            }
        }
        return providerUrl;
    }

    private URL overrideWithConfigurator(URL providerUrl) {
        List<Configurator> localConfigurators = this.configurators;
        if (localConfigurators != null && !localConfigurators.isEmpty()) {
            for (Configurator configurator : localConfigurators) {
                providerUrl = configurator.configure(providerUrl);
            }
        }
        List<Configurator> localAppDynamicConfigurators = consumerConfigurationListener.getConfigurators();
        if (localAppDynamicConfigurators != null && !localAppDynamicConfigurators.isEmpty()) {
            for (Configurator configurator : localAppDynamicConfigurators) {
                providerUrl = configurator.configure(providerUrl);
            }
        }
        List<Configurator> localDynamicConfigurators = serviceConfigurationListener.getConfigurators();
        if (localDynamicConfigurators != null && !localDynamicConfigurators.isEmpty()) {
            for (Configurator configurator : localDynamicConfigurators) {
                providerUrl = configurator.configure(providerUrl);
            }
        }
        return providerUrl;
    }

    private Map<String, List<Invoker<T>>> toMethodInvokers(Map<String, Invoker<T>> invokersMap) {
        Map<String, List<Invoker<T>>> newMethodInvokerMap = new HashMap<String, List<Invoker<T>>>();
        List<Invoker<T>> invokersList = new ArrayList<Invoker<T>>();
        if (invokersMap != null && invokersMap.size() > 0) {
            for (Invoker<T> invoker : invokersMap.values()) {
                String parameter = invoker.getUrl().getParameter(Constants.METHODS_KEY);
                if (parameter != null && parameter.length() > 0) {
                    String[] methods = Constants.COMMA_SPLIT_PATTERN.split(parameter);
                    if (methods != null && methods.length > 0) {
                        for (String method : methods) {
                            if (method != null && method.length() > 0 && !Constants.ANY_VALUE.equals(method)) {
                                List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
                                if (methodInvokers == null) {
                                    methodInvokers = new ArrayList<Invoker<T>>();
                                    newMethodInvokerMap.put(method, methodInvokers);
                                }
                                methodInvokers.add(invoker);
                            }
                        }
                    }
                }
                invokersList.add(invoker);
            }
        }
        newMethodInvokerMap.put(Constants.ANY_VALUE, invokersList);
        for (String method : new HashSet<String>(newMethodInvokerMap.keySet())) {
            List<Invoker<T>> methodInvokers = newMethodInvokerMap.get(method);
            Collections.sort(methodInvokers, InvokerComparator.getComparator());
            newMethodInvokerMap.put(method, Collections.unmodifiableList(methodInvokers));
        }
        return Collections.unmodifiableMap(newMethodInvokerMap);
    }

    private void destroyAllInvokers() {
        Map<String, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap;
        if (localUrlInvokerMap != null) {
            for (Invoker<T> invoker : new ArrayList<Invoker<T>>(localUrlInvokerMap.values())) {
                try {
                    invoker.destroy();
                } catch (Throwable t) {
                    logger.warn("Failed to destroy service " + serviceKey + " to provider " + invoker.getUrl(), t);
                }
            }
            localUrlInvokerMap.clear();
        }
        invokers = null;
    }

    private void destroyUnusedInvokers(Map<String, Invoker<T>> oldUrlInvokerMap, Map<String, Invoker<T>> newUrlInvokerMap) {
        if (newUrlInvokerMap == null || newUrlInvokerMap.size() == 0) {
            destroyAllInvokers();
            return;
        }
        List<String> deleted = null;
        if (oldUrlInvokerMap != null) {
            Collection<Invoker<T>> newInvokers = newUrlInvokerMap.values();
            for (Map.Entry<String, Invoker<T>> entry : oldUrlInvokerMap.entrySet()) {
                if (!newInvokers.contains(entry.getValue())) {
                    if (deleted == null) {
                        deleted = new ArrayList<String>();
                    }
                    deleted.add(entry.getKey());
                }
            }
        }
        if (deleted != null) {
            for (String url : deleted) {
                if (url != null) {
                    Invoker<T> invoker = oldUrlInvokerMap.remove(url);
                    if (invoker != null) {
                        try {
                            invoker.destroy();
                            if (logger.isDebugEnabled()) {
                                logger.debug("destroy invoker[" + invoker.getUrl() + "] success. ");
                            }
                        } catch (Exception e) {
                            logger.warn("destroy invoker[" + invoker.getUrl() + "] faild. " + e.getMessage(), e);
                        }
                    }
                }
            }
        }
    }

    @Override
    public List<Invoker<T>> doList(Invocation invocation) {
        if (forbidden) {
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION, "No provider available from registry " + getUrl().getAddress() + " for service " + getConsumerUrl().getServiceKey() + " on consumer " + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion() + ", please check status of providers(disabled, not registered or in blacklist).");
        }
        if (multiGroup) {
            return this.invokers;
        }
        List<Invoker<T>> invokers = null;
        try {
            invokers = routerChain.route(getConsumerUrl(), invocation);
        } catch (Throwable t) {
            logger.error("Failed to execute router: " + getUrl() + ", cause: " + t.getMessage(), t);
        }
        return invokers == null ? new ArrayList<>(0) : invokers;
    }

    @Override
    public Class<T> getInterface() {
        return serviceType;
    }

    @Override
    public URL getUrl() {
        return this.overrideDirectoryUrl;
    }

    @Override
    public boolean isAvailable() {
        if (isDestroyed()) {
            return false;
        }
        Map<String, Invoker<T>> localUrlInvokerMap = urlInvokerMap;
        if (localUrlInvokerMap != null && localUrlInvokerMap.size() > 0) {
            for (Invoker<T> invoker : new ArrayList<Invoker<T>>(localUrlInvokerMap.values())) {
                if (invoker.isAvailable()) {
                    return true;
                }
            }
        }
        return false;
    }

    public void buildRouterChain(URL url) {
        this.setRouterChain(RouterChain.buildChain(url));
    }

    public Map<String, Invoker<T>> getUrlInvokerMap() {
        return urlInvokerMap;
    }

    public List<Invoker<T>> getInvokers() {
        return invokers;
    }

    private static class InvokerComparator implements Comparator<Invoker<?>> {

        private static final InvokerComparator comparator = new InvokerComparator();

        private InvokerComparator() {
        }

        public static InvokerComparator getComparator() {
            return comparator;
        }

        @Override
        public int compare(Invoker<?> o1, Invoker<?> o2) {
            return o1.getUrl().toString().compareTo(o2.getUrl().toString());
        }
    }

    private boolean isValidCategory(URL url) {
        String category = url.getParameter(CATEGORY_KEY, DEFAULT_CATEGORY);
        if ((ROUTERS_CATEGORY.equals(category) || ROUTE_PROTOCOL.equals(url.getProtocol())) || PROVIDERS_CATEGORY.equals(category) || CONFIGURATORS_CATEGORY.equals(category) || DYNAMIC_CONFIGURATORS_CATEGORY.equals(category) || APP_DYNAMIC_CONFIGURATORS_CATEGORY.equals(category)) {
            return true;
        }
        logger.warn("Unsupported category " + category + " in notified url: " + url + " from registry " + getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost());
        return false;
    }

    private boolean isNotCompatibleFor26x(URL url) {
        return StringUtils.isEmpty(url.getParameter(Constants.COMPATIBLE_CONFIG_KEY));
    }

    private void overrideDirectoryUrl() {
        this.overrideDirectoryUrl = directoryUrl;
        List<Configurator> localConfigurators = this.configurators;
        doOverrideUrl(localConfigurators);
        List<Configurator> localAppDynamicConfigurators = consumerConfigurationListener.getConfigurators();
        doOverrideUrl(localAppDynamicConfigurators);
        List<Configurator> localDynamicConfigurators = serviceConfigurationListener.getConfigurators();
        doOverrideUrl(localDynamicConfigurators);
    }

    private void doOverrideUrl(List<Configurator> configurators) {
        if (configurators != null && !configurators.isEmpty()) {
            for (Configurator configurator : configurators) {
                this.overrideDirectoryUrl = configurator.configure(overrideDirectoryUrl);
            }
        }
    }

    private static class InvokerDelegate<T> extends InvokerWrapper<T> {

        private URL providerUrl;

        public InvokerDelegate(Invoker<T> invoker, URL url, URL providerUrl) {
            super(invoker, url);
            this.providerUrl = providerUrl;
        }

        public URL getProviderUrl() {
            return providerUrl;
        }
    }

    private class ReferenceConfigurationListener extends AbstractConfiguratorListener {

        private URL url;

        ReferenceConfigurationListener(URL url) {
            this.url = url;
            this.init();
        }

        private synchronized void init() {
            String key = url.getEncodedServiceKey() + Constants.CONFIGURATORS_SUFFIX;
            DynamicConfiguration.getDynamicConfiguration().addListener(key, this);
            String rawConfig = DynamicConfiguration.getDynamicConfiguration().getConfig(key);
            if (rawConfig != null) {
                this.process(new ConfigChangeEvent(key, rawConfig));
            }
        }

        @Override
        protected void notifyOverrides() {
            RegistryDirectory.this.refreshInvoker(null);
        }
    }

    private static class ConsumerConfigurationListener extends AbstractConfiguratorListener {

        List<RegistryDirectory> listeners = new ArrayList<>();

        void addNotifyListener(RegistryDirectory listener) {
            this.listeners.add(listener);
        }

        ConsumerConfigurationListener() {
            this.init();
        }

        private synchronized void init() {
            String appKey = ApplicationModel.getApplication() + Constants.CONFIGURATORS_SUFFIX;
            DynamicConfiguration.getDynamicConfiguration().addListener(appKey, this);
            String appRawConfig = DynamicConfiguration.getDynamicConfiguration().getConfig(appKey);
            if (appRawConfig != null) {
                process(new ConfigChangeEvent(appKey, appRawConfig));
            }
        }

        @Override
        protected void notifyOverrides() {
            listeners.forEach(listener -> listener.refreshInvoker(null));
        }
    }
}