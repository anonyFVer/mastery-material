package org.apache.dubbo.rpc.model;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProviderModel {

    private final Object serviceInstance;

    private final ServiceMetadata serviceMetadata;

    private final String serivceKey;

    private final Map<String, List<ProviderMethodModel>> methods = new HashMap<String, List<ProviderMethodModel>>();

    public ProviderModel(String serviceName, String group, String version, Object serviceInstance, Class<?> serviceInterfaceClass) {
        if (null == serviceInstance) {
            throw new IllegalArgumentException("Service[" + serviceName + "]Target is NULL.");
        }
        this.serviceInstance = serviceInstance;
        this.serviceMetadata = new ServiceMetadata(serviceName, group, version, serviceInterfaceClass);
        this.serivceKey = serviceMetadata.getServiceKey();
        initMethod(serviceInterfaceClass);
    }

    public ProviderModel(Object serviceInstance, ServiceMetadata serviceMetadata) {
        this.serviceInstance = serviceInstance;
        this.serviceMetadata = serviceMetadata;
        this.serivceKey = serviceMetadata.getServiceKey();
        initMethod(serviceMetadata.getServiceType());
    }

    public String getServiceName() {
        return this.serviceMetadata.getServiceKey();
    }

    public Class<?> getServiceInterfaceClass() {
        return this.serviceMetadata.getServiceType();
    }

    public Object getServiceInstance() {
        return serviceInstance;
    }

    public List<ProviderMethodModel> getAllMethods() {
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