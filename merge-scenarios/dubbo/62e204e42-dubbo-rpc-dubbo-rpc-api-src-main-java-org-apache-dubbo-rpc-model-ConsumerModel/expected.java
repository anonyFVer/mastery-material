package org.apache.dubbo.rpc.model;

import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.CollectionUtils;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ConsumerModel {

    private final String serviceKey;

    private final Object proxyObject;

    private final ServiceModel serviceModel;

    private final Map<String, AsyncMethodInfo> methodConfigs = new HashMap<>();

    public ConsumerModel(String serviceKey, Class<?> serviceInterfaceClass, Object proxyObject, ServiceModel serviceModel, Map<String, Object> attributes) {
        Assert.notEmptyString(serviceKey, "Service name can't be null or blank");
        Assert.notNull(serviceInterfaceClass, "Service interface class can't null");
        Assert.notNull(proxyObject, "Proxy object can't be null");
        this.serviceKey = serviceKey;
        this.proxyObject = proxyObject;
        this.serviceModel = serviceModel;
        if (CollectionUtils.isNotEmptyMap(attributes)) {
            attributes.forEach((method, object) -> {
                methodConfigs.put(method, (AsyncMethodInfo) object);
            });
        }
    }

    public Object getProxyObject() {
        return proxyObject;
    }

    public Set<MethodModel> getAllMethods() {
        return serviceModel.getAllMethods();
    }

    public Class<?> getServiceInterfaceClass() {
        return serviceModel.getServiceInterfaceClass();
    }

    public String getServiceKey() {
        return serviceKey;
    }

    public AsyncMethodInfo getMethodConfig(String methodName) {
        return methodConfigs.get(methodName);
    }

    public ServiceModel getServiceModel() {
        return serviceModel;
    }

    public static class AsyncMethodInfo {

        private Object oninvokeInstance;

        private Method oninvokeMethod;

        private Object onreturnInstance;

        private Method onreturnMethod;

        private Object onthrowInstance;

        private Method onthrowMethod;

        public Object getOninvokeInstance() {
            return oninvokeInstance;
        }

        public void setOninvokeInstance(Object oninvokeInstance) {
            this.oninvokeInstance = oninvokeInstance;
        }

        public Method getOninvokeMethod() {
            return oninvokeMethod;
        }

        public void setOninvokeMethod(Method oninvokeMethod) {
            this.oninvokeMethod = oninvokeMethod;
        }

        public Object getOnreturnInstance() {
            return onreturnInstance;
        }

        public void setOnreturnInstance(Object onreturnInstance) {
            this.onreturnInstance = onreturnInstance;
        }

        public Method getOnreturnMethod() {
            return onreturnMethod;
        }

        public void setOnreturnMethod(Method onreturnMethod) {
            this.onreturnMethod = onreturnMethod;
        }

        public Object getOnthrowInstance() {
            return onthrowInstance;
        }

        public void setOnthrowInstance(Object onthrowInstance) {
            this.onthrowInstance = onthrowInstance;
        }

        public Method getOnthrowMethod() {
            return onthrowMethod;
        }

        public void setOnthrowMethod(Method onthrowMethod) {
            this.onthrowMethod = onthrowMethod;
        }
    }

    private ServiceMetadata serviceMetadata;

    private final Map<Method, ConsumerMethodModel> methodModels = new IdentityHashMap<Method, ConsumerMethodModel>();

    public ConsumerModel(String serviceKey, Class<?> serviceInterfaceClass, Object proxyObject, ServiceModel serviceModel, Map<String, Object> attributes, ServiceMetadata metadata) {
        this(serviceKey, serviceInterfaceClass, proxyObject, serviceModel, attributes);
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

    public List<ConsumerMethodModel> getAllMethodModels() {
        return new ArrayList<>(methodModels.values());
    }

    public String getServiceName() {
        return this.serviceMetadata.getServiceKey();
    }
}