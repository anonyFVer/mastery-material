package org.apache.dubbo.rpc.model;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProviderModel {

    private final String serviceKey;

    private final Object serviceInstance;

    private ServiceModel serviceModel;

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

    private ServiceMetadata serviceMetadata;

    private final Map<String, List<ProviderMethodModel>> methods = new HashMap<String, List<ProviderMethodModel>>();

    public ProviderModel(String serviceKey, Object serviceInstance, ServiceModel serviceModel, ServiceMetadata serviceMetadata) {
        this(serviceKey, serviceInstance, serviceModel);
        this.serviceMetadata = serviceMetadata;
    }

    public String getServiceName() {
        return this.serviceMetadata.getServiceKey();
    }

    public List<ProviderMethodModel> getAllMethodModels() {
        List<ProviderMethodModel> result = new ArrayList<ProviderMethodModel>();
        for (List<ProviderMethodModel> models : methods.values()) {
            result.addAll(models);
        }
        return result;
    }

    public ProviderMethodModel getMethodModel(String methodName, String[] argTypes) {
        List<ProviderMethodModel> methodModels = methods.get(methodName);
        if (methodModels != null) {
            for (ProviderMethodModel methodModel : methodModels) {
                if (Arrays.equals(argTypes, methodModel.getMethodArgTypes())) {
                    return methodModel;
                }
            }
        }
        return null;
    }

    public List<ProviderMethodModel> getMethodModelList(String methodName) {
        List<ProviderMethodModel> resultList = methods.get(methodName);
        return resultList == null ? Collections.emptyList() : resultList;
    }

    private void initMethod(Class<?> serviceInterfaceClass) {
        Method[] methodsToExport;
        methodsToExport = serviceInterfaceClass.getMethods();
        for (Method method : methodsToExport) {
            method.setAccessible(true);
            List<ProviderMethodModel> methodModels = methods.get(method.getName());
            if (methodModels == null) {
                methodModels = new ArrayList<ProviderMethodModel>();
                methods.put(method.getName(), methodModels);
            }
            methodModels.add(new ProviderMethodModel(method));
        }
    }

    public ServiceMetadata getServiceMetadata() {
        return serviceMetadata;
    }
}