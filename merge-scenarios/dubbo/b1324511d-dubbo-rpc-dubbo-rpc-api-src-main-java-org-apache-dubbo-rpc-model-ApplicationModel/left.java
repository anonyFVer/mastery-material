package org.apache.dubbo.rpc.model;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ApplicationModel {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ApplicationModel.class);

    private static final ConcurrentMap<String, ProviderModel> providedServices = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, ConsumerModel> consumedServices = new ConcurrentHashMap<>();

    public static Collection<ConsumerModel> allConsumerModels() {
        return consumedServices.values();
    }

    public static Collection<ProviderModel> allProviderModels() {
        return providedServices.values();
    }

    public static ProviderModel getProviderModel(String serviceName) {
        return providedServices.get(serviceName);
    }

    public static ConsumerModel getConsumerModel(String serviceName) {
        return consumedServices.get(serviceName);
    }

    public static void initConsumerModel(String serviceName, ConsumerModel consumerModel) {
        if (consumedServices.putIfAbsent(serviceName, consumerModel) != null) {
            LOGGER.warn("Already register the same consumer:" + serviceName);
        }
    }

    public static void initProviderModel(String serviceName, ProviderModel providerModel) {
        if (providedServices.putIfAbsent(serviceName, providerModel) != null) {
            LOGGER.warn("Already register the same:" + serviceName);
        }
    }

    public static void reset() {
        providedServices.clear();
        consumedServices.clear();
    }
}