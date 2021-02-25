package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.config.context.Environment;
import org.apache.dubbo.governance.ConfigChangeEvent;
import org.apache.dubbo.governance.ConfigChangeType;
import org.apache.dubbo.governance.ConfigurationListener;
import org.apache.dubbo.governance.DynamicConfiguration;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.RegistryFactory;
import org.apache.dubbo.registry.RegistryService;
import org.apache.dubbo.registry.integration.parser.ConfigParser;
import org.apache.dubbo.registry.support.ProviderConsumerRegTable;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.protocol.InvokerWrapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import static org.apache.dubbo.common.Constants.ACCEPT_FOREIGN_IP;
import static org.apache.dubbo.common.Constants.APPLICATION_KEY;
import static org.apache.dubbo.common.Constants.CONFIGURATORS_SUFFIX;
import static org.apache.dubbo.common.Constants.EXCHANGING_KEYS;
import static org.apache.dubbo.common.Constants.EXPORT_KEY;
import static org.apache.dubbo.common.Constants.INTERFACES;
import static org.apache.dubbo.common.Constants.METHODS_KEY;
import static org.apache.dubbo.common.Constants.QOS_ENABLE;
import static org.apache.dubbo.common.Constants.QOS_PORT;
import static org.apache.dubbo.common.Constants.REFER_KEY;
import static org.apache.dubbo.common.Constants.VALIDATION_KEY;

public class RegistryProtocol implements Protocol {

    private final static Logger logger = LoggerFactory.getLogger(RegistryProtocol.class);

    private static RegistryProtocol INSTANCE;

    private final Map<URL, NotifyListener> overrideListeners = new ConcurrentHashMap<URL, NotifyListener>();

    private final Map<String, ExporterChangeableWrapper<?>> bounds = new ConcurrentHashMap<String, ExporterChangeableWrapper<?>>();

    private Cluster cluster;

    private Protocol protocol;

    private RegistryFactory registryFactory;

    private ProxyFactory proxyFactory;

    private DynamicConfiguration dynamicConfiguration;

    public RegistryProtocol() {
        INSTANCE = this;
        dynamicConfiguration = Environment.getInstance().getDynamicConfiguration();
    }

    public static RegistryProtocol getRegistryProtocol() {
        if (INSTANCE == null) {
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(Constants.REGISTRY_PROTOCOL);
        }
        return INSTANCE;
    }

    private static String[] getFilteredKeys(URL url) {
        Map<String, String> params = url.getParameters();
        if (params != null && !params.isEmpty()) {
            List<String> filteredKeys = new ArrayList<String>();
            for (Map.Entry<String, String> entry : params.entrySet()) {
                if (entry != null && entry.getKey() != null && entry.getKey().startsWith(Constants.HIDE_KEY_PREFIX)) {
                    filteredKeys.add(entry.getKey());
                }
            }
            return filteredKeys.toArray(new String[filteredKeys.size()]);
        } else {
            return new String[] {};
        }
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistryFactory(RegistryFactory registryFactory) {
        this.registryFactory = registryFactory;
    }

    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    @Override
    public int getDefaultPort() {
        return 9090;
    }

    public Map<URL, NotifyListener> getOverrideListeners() {
        return overrideListeners;
    }

    public void register(URL registryUrl, URL registedProviderUrl) {
        Registry registry = registryFactory.getRegistry(registryUrl);
        registry.register(registedProviderUrl);
    }

    @Override
    public <T> Exporter<T> export(final Invoker<T> originInvoker) throws RpcException {
        URL registryUrl = getRegistryUrl(originInvoker);
        URL providerUrl = getProviderUrl(originInvoker);
        providerUrl = overrideUrlWithConfig(providerUrl);
        final ExporterChangeableWrapper<T> exporter = doLocalExport(originInvoker, providerUrl);
        final Registry registry = getRegistry(originInvoker);
        final URL registeredProviderUrl = getRegistedProviderUrl(providerUrl, registryUrl);
        ProviderConsumerRegTable.registerProvider(originInvoker, registryUrl, registeredProviderUrl);
        boolean register = registeredProviderUrl.getParameter("register", true);
        if (register) {
            register(registryUrl, registeredProviderUrl);
            ProviderConsumerRegTable.getProviderWrapper(originInvoker).setReg(true);
        }
        final URL overrideSubscribeUrl = getSubscribedOverrideUrl(registeredProviderUrl);
        final OverrideListener overrideSubscribeListener = new OverrideListener(overrideSubscribeUrl, originInvoker);
        overrideListeners.put(overrideSubscribeUrl, overrideSubscribeListener);
        registry.subscribe(overrideSubscribeUrl, overrideSubscribeListener);
        dynamicConfiguration.addListener(overrideSubscribeUrl.getServiceKey() + Constants.CONFIGURATORS_SUFFIX, overrideSubscribeListener);
        return new DestroyableExporter<T>(exporter, originInvoker, overrideSubscribeUrl, registeredProviderUrl);
    }

    private <T> URL overrideUrlWithConfig(URL providerUrl) {
        List<Configurator> dynamicConfigurators = new LinkedList<>();
        String appRawConfig = dynamicConfiguration.getConfig(providerUrl.getParameter(Constants.APPLICATION_KEY) + Constants.CONFIGURATORS_SUFFIX);
        if (!StringUtils.isEmpty(appRawConfig)) {
            dynamicConfigurators.addAll(RegistryDirectory.configToConfiguratiors(appRawConfig));
        }
        String rawConfig = dynamicConfiguration.getConfig(providerUrl.getServiceKey() + Constants.CONFIGURATORS_SUFFIX);
        if (!StringUtils.isEmpty(rawConfig)) {
            dynamicConfigurators.addAll(RegistryDirectory.configToConfiguratiors(rawConfig));
        }
        providerUrl = getConfigedInvokerUrl(dynamicConfigurators, providerUrl);
        return providerUrl;
    }

    @SuppressWarnings("unchecked")
    private <T> ExporterChangeableWrapper<T> doLocalExport(final Invoker<T> originInvoker, URL providerUrl) {
        String key = getCacheKey(originInvoker);
        ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        if (exporter == null) {
            synchronized (bounds) {
                exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
                if (exporter == null) {
                    final Invoker<?> invokerDelegete = new InvokerDelegete<T>(originInvoker, providerUrl);
                    exporter = new ExporterChangeableWrapper<T>((Exporter<T>) protocol.export(invokerDelegete), originInvoker);
                    bounds.put(key, exporter);
                }
            }
        }
        return exporter;
    }

    @SuppressWarnings("unchecked")
    private <T> void doChangeLocalExport(final Invoker<T> originInvoker, URL newInvokerUrl) {
        String key = getCacheKey(originInvoker);
        final ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        if (exporter == null) {
            logger.warn(new IllegalStateException("error state, exporter should not be null"));
        } else {
            final Invoker<T> invokerDelegete = new InvokerDelegete<T>(originInvoker, newInvokerUrl);
            exporter.setExporter(protocol.export(invokerDelegete));
        }
    }

    private Registry getRegistry(final Invoker<?> originInvoker) {
        URL registryUrl = getRegistryUrl(originInvoker);
        return registryFactory.getRegistry(registryUrl);
    }

    private URL getRegistryUrl(Invoker<?> originInvoker) {
        URL registryUrl = originInvoker.getUrl();
        if (Constants.REGISTRY_PROTOCOL.equals(registryUrl.getProtocol())) {
            String protocol = registryUrl.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_DIRECTORY);
            registryUrl = registryUrl.setProtocol(protocol).removeParameter(Constants.REGISTRY_KEY);
        }
        return registryUrl;
    }

    private URL getRegistedProviderUrl(final URL providerUrl, final URL registryUrl) {
        if (!registryUrl.getParameter(Constants.SIMPLE_PROVIDER_URL_KEY, false)) {
            final URL registedProviderUrl = providerUrl.removeParameters(getFilteredKeys(providerUrl)).removeParameter(Constants.MONITOR_KEY).removeParameter(Constants.BIND_IP_KEY).removeParameter(Constants.BIND_PORT_KEY).removeParameter(QOS_ENABLE).removeParameter(QOS_PORT).removeParameter(ACCEPT_FOREIGN_IP).removeParameter(VALIDATION_KEY).removeParameter(INTERFACES);
            return registedProviderUrl;
        } else {
            return URL.valueOf(providerUrl, getParamsToRegistry(registryUrl.getParameter(Constants.EXTRA_PROVIDER_URL_PARAM_KEYS_KEY, new String[0])), providerUrl.getParameter(METHODS_KEY, (String[]) null));
        }
    }

    private URL getSubscribedOverrideUrl(URL registedProviderUrl) {
        return registedProviderUrl.setProtocol(Constants.PROVIDER_PROTOCOL).addParameters(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY, Constants.CHECK_KEY, String.valueOf(false));
    }

    private URL getProviderUrl(final Invoker<?> origininvoker) {
        String export = origininvoker.getUrl().getParameterAndDecoded(EXPORT_KEY);
        if (export == null || export.length() == 0) {
            throw new IllegalArgumentException("The registry export url is null! registry: " + origininvoker.getUrl());
        }
        return URL.valueOf(export);
    }

    private String getCacheKey(final Invoker<?> originInvoker) {
        URL providerUrl = getProviderUrl(originInvoker);
        String key = providerUrl.removeParameters("dynamic", "enabled").toFullString();
        return key;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        url = url.setProtocol(url.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_REGISTRY)).removeParameter(Constants.REGISTRY_KEY);
        Registry registry = registryFactory.getRegistry(url);
        if (RegistryService.class.equals(type)) {
            return proxyFactory.getInvoker((T) registry, type, url);
        }
        Map<String, String> qs = StringUtils.parseQueryString(url.getParameterAndDecoded(REFER_KEY));
        String group = qs.get(Constants.GROUP_KEY);
        if (group != null && group.length() > 0) {
            if ((Constants.COMMA_SPLIT_PATTERN.split(group)).length > 1 || "*".equals(group)) {
                return doRefer(getMergeableCluster(), registry, type, url);
            }
        }
        return doRefer(cluster, registry, type, url);
    }

    private Cluster getMergeableCluster() {
        return ExtensionLoader.getExtensionLoader(Cluster.class).getExtension("mergeable");
    }

    private <T> Invoker<T> doRefer(Cluster cluster, Registry registry, Class<T> type, URL url) {
        RegistryDirectory<T> directory = new RegistryDirectory<T>(type, url);
        directory.setRegistry(registry);
        directory.setProtocol(protocol);
        directory.setDynamicConfiguration(dynamicConfiguration);
        directory.buildRouterChain(dynamicConfiguration);
        Map<String, String> parameters = new HashMap<String, String>(directory.getUrl().getParameters());
        URL subscribeUrl = new URL(Constants.CONSUMER_PROTOCOL, parameters.remove(Constants.REGISTER_IP_KEY), 0, type.getName(), parameters);
        if (!Constants.ANY_VALUE.equals(url.getServiceInterface()) && url.getParameter(Constants.REGISTER_KEY, true)) {
            registry.register(getRegistedConsumerUrl(subscribeUrl, directory.getUrl()));
        }
        directory.subscribe(subscribeUrl.addParameter(Constants.CATEGORY_KEY, Constants.PROVIDERS_CATEGORY + "," + Constants.CONFIGURATORS_CATEGORY + "," + Constants.ROUTERS_CATEGORY));
        Invoker invoker = cluster.join(directory);
        ProviderConsumerRegTable.registerConsumer(invoker, url, subscribeUrl, directory);
        return invoker;
    }

    private URL getRegistedConsumerUrl(final URL consumerUrl, URL registryUrl) {
        if (!registryUrl.getParameter(Constants.SIMPLE_CONSUMER_URL_KEY, false)) {
            return consumerUrl.addParameters(Constants.CATEGORY_KEY, Constants.CONSUMERS_CATEGORY, Constants.CHECK_KEY, String.valueOf(false));
        } else {
            return URL.valueOf(consumerUrl, getParamsToRegistry(registryUrl.getParameter(Constants.EXTRA_CONSUMER_URL_PARAM_KEYS_KEY, new String[0])), consumerUrl.getParameter(METHODS_KEY, (String[]) null)).addParameters(Constants.CATEGORY_KEY, Constants.CONSUMERS_CATEGORY, Constants.CHECK_KEY, String.valueOf(false));
        }
    }

    public String[] getParamsToRegistry(String[] addionalParameterKeys) {
        int additionalLen = addionalParameterKeys.length;
        String[] registryParams = new String[EXCHANGING_KEYS.length + additionalLen];
        System.arraycopy(EXCHANGING_KEYS, 0, registryParams, 0, EXCHANGING_KEYS.length);
        System.arraycopy(addionalParameterKeys, 0, registryParams, EXCHANGING_KEYS.length, additionalLen);
        return registryParams;
    }

    @Override
    public void destroy() {
        List<Exporter<?>> exporters = new ArrayList<Exporter<?>>(bounds.values());
        for (Exporter<?> exporter : exporters) {
            exporter.unexport();
        }
        bounds.clear();
    }

    private URL getConfigedInvokerUrl(List<Configurator> configurators, URL url) {
        if (configurators != null && configurators.size() > 0) {
            for (Configurator configurator : configurators) {
                url = configurator.configure(url);
            }
        }
        return url;
    }

    public static class InvokerDelegete<T> extends InvokerWrapper<T> {

        private final Invoker<T> invoker;

        public InvokerDelegete(Invoker<T> invoker, URL url) {
            super(invoker, url);
            this.invoker = invoker;
        }

        public Invoker<T> getInvoker() {
            if (invoker instanceof InvokerDelegete) {
                return ((InvokerDelegete<T>) invoker).getInvoker();
            } else {
                return invoker;
            }
        }
    }

    static private class DestroyableExporter<T> implements Exporter<T> {

        public static final ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("Exporter-Unexport", true));

        private Exporter<T> exporter;

        private Invoker<T> originInvoker;

        private URL subscribeUrl;

        private URL registerUrl;

        public DestroyableExporter(Exporter<T> exporter, Invoker<T> originInvoker, URL subscribeUrl, URL registerUrl) {
            this.exporter = exporter;
            this.originInvoker = originInvoker;
            this.subscribeUrl = subscribeUrl;
            this.registerUrl = registerUrl;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        @Override
        public void unexport() {
            Registry registry = RegistryProtocol.INSTANCE.getRegistry(originInvoker);
            try {
                registry.unregister(registerUrl);
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
            try {
                NotifyListener listener = RegistryProtocol.INSTANCE.overrideListeners.remove(subscribeUrl);
                registry.unsubscribe(subscribeUrl, listener);
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
            executor.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        int timeout = ConfigUtils.getServerShutdownTimeout();
                        if (timeout > 0) {
                            logger.info("Waiting " + timeout + "ms for registry to notify all consumers before unexport. Usually, this is called when you use dubbo API");
                            Thread.sleep(timeout);
                        }
                        exporter.unexport();
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }
            });
        }
    }

    private class OverrideListener implements NotifyListener, ConfigurationListener {

        private final URL subscribeUrl;

        private final Invoker originInvoker;

        private List<Configurator> configurators;

        private List<Configurator> dynamicConfigurators;

        private List<Configurator> appDynamicConfigurators;

        public OverrideListener(URL subscribeUrl, Invoker originalInvoker) {
            this.subscribeUrl = subscribeUrl;
            this.originInvoker = originalInvoker;
        }

        @Override
        public synchronized void notify(List<URL> urls) {
            logger.debug("original override urls: " + urls);
            List<URL> matchedUrls = getMatchedUrls(urls, subscribeUrl.addParameter(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY + "," + Constants.DYNAMIC_CONFIGURATORS_CATEGORY + "," + Constants.APP_DYNAMIC_CONFIGURATORS_CATEGORY));
            logger.debug("subscribe url: " + subscribeUrl + ", override urls: " + matchedUrls);
            if (matchedUrls.isEmpty()) {
                return;
            }
            List<URL> configuratorUrls = matchedUrls.stream().filter(u -> u.getParameter(Constants.CATEGORY_KEY).equals(Constants.CONFIGURATORS_CATEGORY)).collect(Collectors.toList());
            List<URL> dynamicConfiguratorUrls = matchedUrls.stream().filter(u -> u.getParameter(Constants.CATEGORY_KEY).equals(Constants.DYNAMIC_CONFIGURATORS_CATEGORY)).collect(Collectors.toList());
            List<URL> appDynamicConfiguratorUrls = matchedUrls.stream().filter(u -> u.getParameter(Constants.CATEGORY_KEY).equals(Constants.APP_DYNAMIC_CONFIGURATORS_CATEGORY)).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(configuratorUrls)) {
                configurators = RegistryDirectory.toConfigurators(configuratorUrls);
            }
            if (CollectionUtils.isNotEmpty(dynamicConfiguratorUrls)) {
                dynamicConfigurators = RegistryDirectory.toConfigurators(dynamicConfiguratorUrls);
            }
            if (CollectionUtils.isNotEmpty(appDynamicConfiguratorUrls)) {
                appDynamicConfigurators = RegistryDirectory.toConfigurators(appDynamicConfiguratorUrls);
            }
            final Invoker<?> invoker;
            if (originInvoker instanceof InvokerDelegete) {
                invoker = ((InvokerDelegete<?>) originInvoker).getInvoker();
            } else {
                invoker = originInvoker;
            }
            URL originUrl = RegistryProtocol.this.getProviderUrl(invoker);
            String key = getCacheKey(originInvoker);
            ExporterChangeableWrapper<?> exporter = bounds.get(key);
            if (exporter == null) {
                logger.warn(new IllegalStateException("error state, exporter should not be null"));
                return;
            }
            URL currentUrl = exporter.getInvoker().getUrl();
            URL newUrl = getConfigedInvokerUrl(configurators, originUrl);
            newUrl = getConfigedInvokerUrl(appDynamicConfigurators, newUrl);
            newUrl = getConfigedInvokerUrl(dynamicConfigurators, newUrl);
            if (!currentUrl.equals(newUrl)) {
                RegistryProtocol.this.doChangeLocalExport(originInvoker, newUrl);
                logger.info("exported provider url changed, origin url: " + originUrl + ", old export url: " + currentUrl + ", new export url: " + newUrl);
            }
        }

        private List<URL> getMatchedUrls(List<URL> configuratorUrls, URL currentSubscribe) {
            List<URL> result = new ArrayList<URL>();
            for (URL url : configuratorUrls) {
                URL overrideUrl = url;
                if (url.getParameter(Constants.CATEGORY_KEY) == null && Constants.OVERRIDE_PROTOCOL.equals(url.getProtocol())) {
                    overrideUrl = url.addParameter(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY);
                }
                if (UrlUtils.isMatch(currentSubscribe, overrideUrl)) {
                    result.add(url);
                }
            }
            return result;
        }

        @Override
        public void process(ConfigChangeEvent event) {
            List<URL> urls;
            if (event.getChangeType().equals(ConfigChangeType.DELETED)) {
                URL originUrl = RegistryProtocol.this.getProviderUrl(originInvoker);
                originUrl = originUrl.clearParameters().setProtocol(Constants.EMPTY_PROTOCOL);
                if (event.getKey().endsWith(originUrl.getParameter(APPLICATION_KEY) + CONFIGURATORS_SUFFIX)) {
                    originUrl = originUrl.addParameter(Constants.CATEGORY_KEY, Constants.APP_DYNAMIC_CONFIGURATORS_CATEGORY);
                } else {
                    originUrl = originUrl.addParameter(Constants.CATEGORY_KEY, Constants.DYNAMIC_CONFIGURATORS_CATEGORY);
                }
                urls = new ArrayList<>();
                urls.add(originUrl);
            } else {
                try {
                    urls = ConfigParser.parseConfigurators(event.getNewValue());
                } catch (Exception e) {
                    logger.error("Failed to parse raw dynamic config and it will not take effect, the raw config is: " + event.getNewValue(), e);
                    return;
                }
            }
            notify(urls);
        }

        @Override
        public URL getUrl() {
            return subscribeUrl;
        }
    }

    private class ExporterChangeableWrapper<T> implements Exporter<T> {

        private final Invoker<T> originInvoker;

        private Exporter<T> exporter;

        public ExporterChangeableWrapper(Exporter<T> exporter, Invoker<T> originInvoker) {
            this.exporter = exporter;
            this.originInvoker = originInvoker;
        }

        public Invoker<T> getOriginInvoker() {
            return originInvoker;
        }

        @Override
        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }

        public void setExporter(Exporter<T> exporter) {
            this.exporter = exporter;
        }

        @Override
        public void unexport() {
            String key = getCacheKey(this.originInvoker);
            bounds.remove(key);
            exporter.unexport();
        }
    }
}