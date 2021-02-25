package org.apache.dubbo.registry.client.metadata;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.proxy.InvokerInvocationHandler;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import static java.lang.reflect.Proxy.newProxyInstance;
import static org.apache.dubbo.registry.client.metadata.MetadataServiceURLBuilder.INSTANCE;

class MetadataServiceProxy implements MetadataService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final List<URL> urls;

    private final Protocol protocol;

    public MetadataServiceProxy(ServiceInstance serviceInstance, Protocol protocol) {
        this(INSTANCE.build(serviceInstance), protocol);
    }

    public MetadataServiceProxy(List<URL> urls, Protocol protocol) {
        this.urls = urls;
        this.protocol = protocol;
    }

    @Override
    public String serviceName() {
        return doInMetadataService(MetadataService::serviceName);
    }

    @Override
    public List<String> getSubscribedURLs() {
        return doInMetadataService(MetadataService::getSubscribedURLs);
    }

    @Override
    public List<String> getExportedURLs(String serviceInterface, String group, String version, String protocol) {
        return doInMetadataService(metadataService -> metadataService.getExportedURLs(serviceInterface, group, version, protocol));
    }

    @Override
    public String getServiceDefinition(String interfaceName, String version, String group) {
        return null;
    }

    @Override
    public String getServiceDefinition(String serviceKey) {
        return null;
    }

    protected <T> T doInMetadataService(Function<MetadataService, T> callback) {
        T result = null;
        Throwable exception = null;
        Iterator<URL> iterator = urls.iterator();
        while (iterator.hasNext()) {
            URL url = iterator.next();
            Invoker<MetadataService> invoker = null;
            try {
                invoker = this.protocol.refer(MetadataService.class, url);
                MetadataService proxy = (MetadataService) newProxyInstance(getClass().getClassLoader(), new Class[] { MetadataService.class }, new InvokerInvocationHandler(invoker));
                result = callback.apply(proxy);
                exception = null;
            } catch (Throwable e) {
                exception = e;
                if (logger.isErrorEnabled()) {
                    logger.error(e.getMessage(), e);
                }
            } finally {
                if (invoker != null) {
                    invoker.destroy();
                    invoker = null;
                }
            }
        }
        if (exception != null) {
            throw new RuntimeException(exception.getMessage(), exception);
        }
        return result;
    }
}