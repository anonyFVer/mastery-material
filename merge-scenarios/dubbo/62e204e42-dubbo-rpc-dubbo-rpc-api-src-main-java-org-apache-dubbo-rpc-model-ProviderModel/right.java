package org.apache.dubbo.rpc.model;

import org.apache.dubbo.rpc.model.invoker.ProviderInvokerWrapper;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProviderModel {

    private final String serviceKey;

    private final Object serviceInstance;

    private final Class<?> serviceInterfaceClass;

    private final Map<String, List<ProviderMethodModel>> methods = new HashMap<String, List<ProviderMethodModel>>();

    private Map<String, ProviderInvokerWrapper> protocolInvokers = new HashMap<>();

    public ProviderModel(String serviceKey, Object serviceInstance, Class<?> serviceInterfaceClass) {
        if (null == serviceInstance) {
            throw new IllegalArgumentException("Service[" + serviceKey + "]Target is NULL.");
        }
        this.serviceKey = serviceKey;
        this.serviceInstance = serviceInstance;
        this.serviceInterfaceClass = serviceInterfaceClass;
        initMethod();
    }

    public String getServiceKey() {
        return serviceKey;
    }

    public Class<?> getServiceInterfaceClass() {
        return serviceInterfaceClass;
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

    private void initMethod() {
        Method[] methodsToExport = null;
        methodsToExport = this.serviceInterfaceClass.getMethods();
        for (Method method : methodsToExport) {
            method.setAccessible(true);
            List<ProviderMethodModel> methodModels = methods.get(method.getName());
            if (methodModels == null) {
                methodModels = new ArrayList<ProviderMethodModel>(1);
                methods.put(method.getName(), methodModels);
            }
            methodModels.add(new ProviderMethodModel(method, serviceKey));
        }
    }

    public void addInvoker(ProviderInvokerWrapper invoker) {
        protocolInvokers.put(invoker.getUrl().getProtocol(), invoker);
    }

    public ProviderInvokerWrapper getInvoker(String protocol) {
        return protocolInvokers.get(protocol);
    }

    public Collection<ProviderInvokerWrapper> getInvokers() {
        return Collections.unmodifiableCollection(protocolInvokers.values());
    }
}