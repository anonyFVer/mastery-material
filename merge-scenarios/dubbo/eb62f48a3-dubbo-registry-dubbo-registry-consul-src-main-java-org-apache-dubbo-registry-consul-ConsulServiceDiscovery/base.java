package org.apache.dubbo.registry.consul;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.event.EventDispatcher;
import org.apache.dubbo.event.EventListener;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.client.ServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.health.model.HealthService;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.registry.consul.AbstractConsulRegistry.CHECK_PASS_INTERVAL;
import static org.apache.dubbo.registry.consul.AbstractConsulRegistry.DEFAULT_CHECK_PASS_INTERVAL;
import static org.apache.dubbo.registry.consul.AbstractConsulRegistry.DEFAULT_DEREGISTER_TIME;
import static org.apache.dubbo.registry.consul.AbstractConsulRegistry.DEFAULT_PORT;
import static org.apache.dubbo.registry.consul.AbstractConsulRegistry.DEFAULT_WATCH_TIMEOUT;
import static org.apache.dubbo.registry.consul.AbstractConsulRegistry.DEREGISTER_AFTER;
import static org.apache.dubbo.registry.consul.AbstractConsulRegistry.SERVICE_TAG;
import static org.apache.dubbo.registry.consul.AbstractConsulRegistry.URL_META_KEY;
import static org.apache.dubbo.registry.consul.AbstractConsulRegistry.WATCH_TIMEOUT;

public class ConsulServiceDiscovery implements ServiceDiscovery, EventListener<ServiceInstancesChangedEvent> {

    private ConsulClient client;

    private long checkPassInterval;

    private ExecutorService notifierExecutor = newCachedThreadPool(new NamedThreadFactory("dubbo-consul-notifier", true));

    private ScheduledExecutorService ttlConsulCheckExecutor;

    public ConsulServiceDiscovery(URL url) {
        String host = url.getHost();
        int port = url.getPort() != 0 ? url.getPort() : DEFAULT_PORT;
        client = new ConsulClient(host, port);
        checkPassInterval = url.getParameter(CHECK_PASS_INTERVAL, DEFAULT_CHECK_PASS_INTERVAL);
        ttlConsulCheckExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void onEvent(ServiceInstancesChangedEvent event) {
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void register(ServiceInstance serviceInstance) throws RuntimeException {
        client.agentServiceRegister(buildService(serviceInstance));
    }

    @Override
    public void update(ServiceInstance serviceInstance) throws RuntimeException {
    }

    @Override
    public void unregister(ServiceInstance serviceInstance) throws RuntimeException {
        client.agentServiceDeregister(buildId(serviceInstance));
    }

    @Override
    public Set<String> getServices() {
        return null;
    }

    @Override
    public void addServiceInstancesChangedListener(String serviceName, ServiceInstancesChangedListener listener) throws NullPointerException, IllegalArgumentException {
    }

    private NewService buildService(ServiceInstance serviceInstance) {
        NewService service = new NewService();
        service.setAddress(serviceInstance.getHost());
        service.setPort(serviceInstance.getPort());
        service.setId(buildId(serviceInstance));
        service.setName(serviceInstance.getServiceName());
        service.setCheck(buildCheck(serviceInstance));
        service.setTags(buildTags(serviceInstance));
        service.setMeta(Collections.singletonMap(URL_META_KEY, serviceInstance.toString()));
        return service;
    }

    private String buildId(ServiceInstance serviceInstance) {
        return Integer.toHexString(serviceInstance.hashCode());
    }

    private List<String> buildTags(ServiceInstance serviceInstance) {
        Map<String, String> params = serviceInstance.getMetadata();
        List<String> tags = params.keySet().stream().map(k -> k + "=" + params.get(k)).collect(Collectors.toList());
        tags.add(SERVICE_TAG);
        return tags;
    }

    private NewService.Check buildCheck(ServiceInstance serviceInstance) {
        NewService.Check check = new NewService.Check();
        check.setTtl((checkPassInterval / 1000) + "s");
        String deregister = serviceInstance.getMetadata().get(DEREGISTER_AFTER);
        check.setDeregisterCriticalServiceAfter(deregister == null ? DEFAULT_DEREGISTER_TIME : deregister);
        return check;
    }

    private int buildWatchTimeout(URL url) {
        return url.getParameter(WATCH_TIMEOUT, DEFAULT_WATCH_TIMEOUT) / 1000;
    }

    private class ConsulNotifier implements Runnable {

        private ServiceInstance serviceInstance;

        private long consulIndex;

        private boolean running;

        ConsulNotifier(ServiceInstance serviceInstance, long consulIndex) {
            this.serviceInstance = serviceInstance;
            this.consulIndex = consulIndex;
            this.running = true;
        }

        @Override
        public void run() {
            while (this.running) {
                processService();
            }
        }

        private void processService() {
        }

        void stop() {
            this.running = false;
        }
    }
}