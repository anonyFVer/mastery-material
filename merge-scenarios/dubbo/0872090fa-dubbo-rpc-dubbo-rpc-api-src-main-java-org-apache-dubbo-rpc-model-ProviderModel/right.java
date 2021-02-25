package org.apache.dubbo.rpc.model;

import java.util.Set;

public class ProviderModel {

    private final String serviceKey;

    private final Object serviceInstance;

    private final ServiceModel serviceModel;

    public ProviderModel(String serviceKey, Object serviceInstance, ServiceModel serviceModel) {
        if (null == serviceInstance) {
            throw new IllegalArgumentException("Service[" + serviceKey + "]Target is NULL.");
        }
        this.serviceKey = serviceKey;
        this.serviceInstance = serviceInstance;
        this.serviceModel = serviceModel;
    }

    public String getServiceKey() {
        return serviceKey;
    }

    public Class<?> getServiceInterfaceClass() {
        return serviceModel.getServiceInterfaceClass();
    }

    public Object getServiceInstance() {
        return serviceInstance;
    }

    public Set<MethodModel> getAllMethods() {
        return serviceModel.getAllMethods();
    }

    public ServiceModel getServiceModel() {
        return serviceModel;
    }
}