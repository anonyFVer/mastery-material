package org.apache.dubbo.rpc.model;

import org.apache.dubbo.common.utils.Assert;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ConsumerModel {

    private final Object proxyObject;

    private final String serviceName;

    private final Class<?> serviceInterfaceClass;

    private final Map<Method, ConsumerMethodModel> methodModels = new IdentityHashMap<Method, ConsumerMethodModel>();

    public ConsumerModel(String serviceName, Class<?> serviceInterfaceClass, Object proxyObject, Method[] methods, Map<String, Object> attributes) {
        Assert.notEmptyString(serviceName, "Service name can't be null or blank");
        Assert.notNull(serviceInterfaceClass, "Service interface class can't null");
        Assert.notNull(proxyObject, "Proxy object can't be null");
        Assert.notNull(methods, "Methods can't be null");
        this.serviceName = serviceName;
        this.serviceInterfaceClass = serviceInterfaceClass;
        this.proxyObject = proxyObject;
        for (Method method : methods) {
            methodModels.put(method, new ConsumerMethodModel(method, attributes));
        }
    }

    public Object getProxyObject() {
        return proxyObject;
    }

    public ConsumerMethodModel getMethodModel(Method method) {
        return methodModels.get(method);
    }

    public ConsumerMethodModel getMethodModel(String method) {
        Optional<Map.Entry<Method, ConsumerMethodModel>> consumerMethodModelEntry = methodModels.entrySet().stream().filter(entry -> entry.getKey().getName().equals(method)).findFirst();
        return consumerMethodModelEntry.map(Map.Entry::getValue).orElse(null);
    }

    public List<ConsumerMethodModel> getAllMethods() {
        return new ArrayList<ConsumerMethodModel>(methodModels.values());
    }

    public Class<?> getServiceInterfaceClass() {
        return serviceInterfaceClass;
    }

    public String getServiceName() {
        return serviceName;
    }
}