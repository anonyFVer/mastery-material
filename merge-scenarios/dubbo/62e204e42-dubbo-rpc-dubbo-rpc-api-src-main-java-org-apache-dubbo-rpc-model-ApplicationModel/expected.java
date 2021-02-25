package org.apache.dubbo.rpc.model;

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ApplicationModel {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ApplicationModel.class);

    private static final ConcurrentMap<String, ProviderModel> PROVIDED_SERVICES = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, ConsumerModel> CONSUMED_SERVICES = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, ServiceModel> SERVICES = new ConcurrentHashMap<>();

    private static String application;

    private static AtomicBoolean INIT_FLAG = new AtomicBoolean(false);

    public static Collection<ConsumerModel> allConsumerModels() {
        return CONSUMED_SERVICES.values();
    }

    public static Collection<ProviderModel> allProviderModels() {
        return PROVIDED_SERVICES.values();
    }

    public static ProviderModel getProviderModel(String serviceKey) {
        return PROVIDED_SERVICES.get(serviceKey);
    }

    public static ConsumerModel getConsumerModel(String serviceKey) {
        return CONSUMED_SERVICES.get(serviceKey);
    }

    public static void init() {
        if (INIT_FLAG.compareAndSet(false, true)) {
            ExtensionLoader<ApplicationInitListener> extensionLoader = ExtensionLoader.getExtensionLoader(ApplicationInitListener.class);
            Set<String> listenerNames = extensionLoader.getSupportedExtensions();
            for (String listenerName : listenerNames) {
                extensionLoader.getExtension(listenerName).init();
            }
        }
    }

    public static void initConsumerModel(String serviceName, ConsumerModel consumerModel) {
        if (CONSUMED_SERVICES.putIfAbsent(serviceName, consumerModel) != null) {
            LOGGER.warn("Already register the same consumer:" + serviceName);
        }
    }

    public static void initProviderModel(String serviceName, ProviderModel providerModel) {
        if (PROVIDED_SERVICES.putIfAbsent(serviceName, providerModel) != null) {
            LOGGER.warn("Already register the same:" + serviceName);
        }
    }

    public static ServiceModel registerServiceModel(Class<?> interfaceClass) {
        return SERVICES.computeIfAbsent(interfaceClass.getName(), (k) -> new ServiceModel(interfaceClass));
    }

    public static ServiceModel registerServiceModel(String path, Class<?> interfaceClass) {
        ServiceModel serviceModel = registerServiceModel(interfaceClass);
        if (!interfaceClass.getName().equals(path)) {
            SERVICES.putIfAbsent(path, serviceModel);
        }
        return serviceModel;
    }

    public static Optional<ServiceModel> getServiceModel(String interfaceName) {
        return Optional.ofNullable(SERVICES.get(interfaceName));
    }

    public static Optional<ServiceModel> getServiceModel(Class<?> interfaceClass) {
        return Optional.ofNullable(SERVICES.get(interfaceClass.getName()));
    }

    public static String getApplication() {
        return application;
    }

    public static void setApplication(String application) {
        ApplicationModel.application = application;
    }

    public static void reset() {
        PROVIDED_SERVICES.clear();
        CONSUMED_SERVICES.clear();
    }
}