package org.apache.dubbo.registry.etcd;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.event.EventDispatcher;
import org.apache.dubbo.event.EventListener;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.client.DefaultServiceInstance;
import org.apache.dubbo.registry.client.ServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.remoting.etcd.ChildListener;
import org.apache.dubbo.remoting.etcd.EtcdClient;
import org.apache.dubbo.remoting.etcd.EtcdTransporter;
import org.apache.dubbo.remoting.etcd.StateListener;
import org.apache.dubbo.remoting.etcd.option.OptionUtil;
import org.apache.dubbo.rpc.RpcException;
import com.google.gson.Gson;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;

public class EtcdServiceDiscovery implements ServiceDiscovery, EventListener<ServiceInstancesChangedEvent> {

    private final static Logger logger = LoggerFactory.getLogger(EtcdServiceDiscovery.class);

    private final String root = "/services";

    private final Set<String> services = new ConcurrentHashSet<>();

    private final Map<String, ChildListener> childListenerMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<URL, ConcurrentMap<NotifyListener, ChildListener>> etcdListeners = new ConcurrentHashMap<>();

    private EtcdClient etcdClient;

    private EventDispatcher dispatcher;

    private ServiceInstance serviceInstance;

    @Override
    public void onEvent(ServiceInstancesChangedEvent event) {
        registerServiceWatcher(event.getServiceName());
    }

    @Override
    public void initialize(URL registryURL) throws Exception {
        EtcdTransporter etcdTransporter = ExtensionLoader.getExtensionLoader(EtcdTransporter.class).getAdaptiveExtension();
        if (registryURL.isAnyHost()) {
            throw new IllegalStateException("Service discovery address is invalid, actual: '" + registryURL.getHost() + "'");
        }
        etcdClient = etcdTransporter.connect(registryURL);
        etcdClient.addStateListener(state -> {
            if (state == StateListener.CONNECTED) {
                try {
                    recover();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        });
        this.dispatcher = EventDispatcher.getDefaultExtension();
        this.dispatcher.addEventListener(this);
    }

    @Override
    public void destroy() {
    }

    @Override
    public void register(ServiceInstance serviceInstance) throws RuntimeException {
        try {
            this.serviceInstance = serviceInstance;
            String path = toPath(serviceInstance);
            etcdClient.putEphemeral(path, new Gson().toJson(serviceInstance));
            services.add(serviceInstance.getServiceName());
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + serviceInstance + " to etcd " + etcdClient.getUrl() + ", cause: " + (OptionUtil.isProtocolError(e) ? "etcd3 registry may not be supported yet or etcd3 registry is not available." : e.getMessage()), e);
        }
    }

    String toPath(ServiceInstance serviceInstance) {
        return root + File.separator + serviceInstance.getServiceName() + File.separator + serviceInstance.getHost() + ":" + serviceInstance.getPort();
    }

    String toParentPath(String serviceName) {
        return root + File.separator + serviceName;
    }

    @Override
    public void update(ServiceInstance serviceInstance) throws RuntimeException {
        try {
            String path = toPath(serviceInstance);
            etcdClient.put(path, new Gson().toJson(serviceInstance));
            services.add(serviceInstance.getServiceName());
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + serviceInstance + " to etcd " + etcdClient.getUrl() + ", cause: " + (OptionUtil.isProtocolError(e) ? "etcd3 registry may not be supported yet or etcd3 registry is not available." : e.getMessage()), e);
        }
    }

    @Override
    public void unregister(ServiceInstance serviceInstance) throws RuntimeException {
        try {
            String path = toPath(serviceInstance);
            etcdClient.delete(path);
            services.remove(serviceInstance.getServiceName());
            this.serviceInstance = null;
        } catch (Throwable e) {
            throw new RpcException("Failed to unregister " + serviceInstance + " to etcd " + etcdClient.getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public Set<String> getServices() {
        return Collections.unmodifiableSet(services);
    }

    @Override
    public void addServiceInstancesChangedListener(String serviceName, ServiceInstancesChangedListener listener) throws NullPointerException, IllegalArgumentException {
        registerServiceWatcher(serviceName);
        dispatcher.addEventListener(listener);
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceName) {
        List<String> children = etcdClient.getChildren(toParentPath(serviceName));
        if (CollectionUtils.isEmpty(children)) {
            return Collections.EMPTY_LIST;
        }
        List<ServiceInstance> list = new ArrayList<>(children.size());
        for (String child : children) {
            ServiceInstance serviceInstance = new Gson().fromJson(etcdClient.getKVValue(child), DefaultServiceInstance.class);
            list.add(serviceInstance);
        }
        return list;
    }

    protected void registerServiceWatcher(String serviceName) {
        String path = root + File.separator + serviceName;
        ChildListener childListener = Optional.ofNullable(childListenerMap.get(serviceName)).orElseGet(() -> {
            ChildListener watchListener, prev;
            prev = childListenerMap.putIfAbsent(serviceName, watchListener = (parentPath, currentChildren) -> dispatcher.dispatch(new ServiceInstancesChangedEvent(serviceName, getInstances(serviceName))));
            return prev != null ? prev : watchListener;
        });
        etcdClient.create(path);
        etcdClient.addChildListener(path, childListener);
    }

    private void recover() throws Exception {
        if (serviceInstance != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover application register: " + serviceInstance);
            }
            register(serviceInstance);
        }
    }
}