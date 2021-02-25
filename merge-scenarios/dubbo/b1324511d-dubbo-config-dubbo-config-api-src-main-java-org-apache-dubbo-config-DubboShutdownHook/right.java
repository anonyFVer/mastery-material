package org.apache.dubbo.config;

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.registry.support.AbstractRegistryFactory;
import org.apache.dubbo.rpc.Protocol;
import java.util.concurrent.atomic.AtomicBoolean;

public class DubboShutdownHook extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(DubboShutdownHook.class);

    private static final DubboShutdownHook dubboShutdownHook = new DubboShutdownHook("DubboShutdownHook");

    private final AtomicBoolean destroyed;

    private DubboShutdownHook(String name) {
        super(name);
        this.destroyed = new AtomicBoolean(false);
    }

    public static DubboShutdownHook getDubboShutdownHook() {
        return dubboShutdownHook;
    }

    @Override
    public void run() {
        if (logger.isInfoEnabled()) {
            logger.info("Run shutdown hook now.");
        }
        destroyAll();
    }

    public void destroyAll() {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }
        AbstractRegistryFactory.destroyAll();
        destroyProtocols();
    }

    private void destroyProtocols() {
        ExtensionLoader<Protocol> loader = ExtensionLoader.getExtensionLoader(Protocol.class);
        for (String protocolName : loader.getLoadedExtensions()) {
            try {
                Protocol protocol = loader.getLoadedExtension(protocolName);
                if (protocol != null) {
                    protocol.destroy();
                }
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
        }
    }
}