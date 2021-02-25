package org.apache.dubbo.rpc.model;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ConsumerModel {

    private final ServiceMetadata serviceMetadata;

    private final Map<Method, ConsumerMethodModel> methodModels = new IdentityHashMap<Method, ConsumerMethodModel>();

    public ConsumerModel(Map<String, Object> attributes, ServiceMetadata metadata) {
        this.serviceMetadata = metadata;
        for (Method method : metadata.getServiceType().getMethods()) {
            methodModels.put(method, new ConsumerMethodModel(method, attributes));
        }
    }

    public ServiceMetadata getServiceMetadata() {
        return serviceMetadata;
    }

    public ConsumerMethodModel getMethodModel(Method method) {
        return methodModels.get(method);
    }

    public ConsumerMethodModel getMethodModel(String method) {
        Optional<Map.Entry<Method, ConsumerMethodModel>> consumerMethodModelEntry = methodModels.entrySet().stream().filter(entry -> entry.getKey().getName().equals(method)).findFirst();
        return consumerMethodModelEntry.map(Map.Entry::getValue).orElse(null);
    }

    public ConsumerMethodModel getMethodModel(String method, String[] argsType) {
        Optional<ConsumerMethodModel> consumerMethodModel = methodModels.entrySet().stream().filter(entry -> entry.getKey().getName().equals(method)).map(Map.Entry::getValue).filter(methodModel -> Arrays.equals(argsType, methodModel.getParameterTypes())).findFirst();
        return consumerMethodModel.orElse(null);
    }

    public Class<?> getServiceInterfaceClass() {
        return serviceMetadata.getServiceType();
    }

    public List<ConsumerMethodModel> getAllMethods() {
        return new ArrayList<ConsumerMethodModel>(methodModels.values());
    }

    public Object getProxyObject() {
        return this.serviceMetadata.getTarget();
    }

    public String getServiceName() {
        return this.serviceMetadata.getServiceKey();
    }
}