package org.apache.dubbo.registry.dubbo;

import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.registry.RegistryService;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import java.io.IOException;
import java.net.ServerSocket;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_PROTOCOL;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.rpc.Constants.CALLBACK_INSTANCES_LIMIT_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.CLUSTER_STICKY_KEY;

public class SimpleRegistryExporter {

    private static final Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();

    private static final ProxyFactory PROXY_FACTORY = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    public synchronized static Exporter<RegistryService> exportIfAbsent(int port) {
        try {
            new ServerSocket(port).close();
            return export(port);
        } catch (IOException e) {
            return null;
        }
    }

    public static Exporter<RegistryService> export(int port) {
        return export(port, new SimpleRegistryService());
    }

    public static Exporter<RegistryService> export(int port, RegistryService registryService) {
        return protocol.export(PROXY_FACTORY.getInvoker(registryService, RegistryService.class, new URLBuilder(DUBBO_PROTOCOL, NetUtils.getLocalHost(), port, RegistryService.class.getName()).setPath(RegistryService.class.getName()).addParameter(INTERFACE_KEY, RegistryService.class.getName()).addParameter(CLUSTER_STICKY_KEY, "true").addParameter(CALLBACK_INSTANCES_LIMIT_KEY, "1000").addParameter("ondisconnect", "disconnect").addParameter("subscribe.1.callback", "true").addParameter("unsubscribe.1.callback", "false").build()));
    }
}