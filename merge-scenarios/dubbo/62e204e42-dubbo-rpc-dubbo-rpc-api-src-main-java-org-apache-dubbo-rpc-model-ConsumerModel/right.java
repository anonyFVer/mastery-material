package org.apache.dubbo.rpc.model;

import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.rpc.Invoker;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ConsumerModel {

    private final String serviceKey;

    private final Class<?> serviceInterfaceClass;

    private final Map<Method, ConsumerMethodModel> methodModels = new IdentityHashMap<Method, ConsumerMethodModel>();

    private Object proxyObject;

    private Invoker<?> invoker;

    public ConsumerModel(String serviceKey, Class<?> serviceInterfaceClass, Object proxyObject, Method[] methods, Map<String, Object> attributes) {
        Assert.notEmptyString(serviceKey, "Service name can't be null or blank");
        Assert.notNull(serviceInterfaceClass, "Service interface class can't null");
        Assert.notNull(methods, "Methods can't be null");
        this.serviceKey = serviceKey;
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

    public String getServiceKey() {
        return serviceKey;
    }

    public void setProxyObject(Object proxyObject) {
        this.proxyObject = proxyObject;
    }

    public Invoker<?> getInvoker() {
        return invoker;
    }

    public void setInvoker(Invoker<?> invoker) {
        this.invoker = invoker;
    }
}