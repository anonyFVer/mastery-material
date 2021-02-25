package org.apache.dubbo.config;

import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.lang.ShutdownHookCallbacks;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.config.event.DubboServiceDestroyedEvent;
import org.apache.dubbo.config.event.DubboShutdownHookRegisteredEvent;
import org.apache.dubbo.config.event.DubboShutdownHookUnregisteredEvent;
import org.apache.dubbo.event.Event;
import org.apache.dubbo.event.EventDispatcher;
import org.apache.dubbo.registry.support.AbstractRegistryFactory;
import org.apache.dubbo.rpc.Protocol;
import java.util.concurrent.atomic.AtomicBoolean;

public class DubboShutdownHook extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(DubboShutdownHook.class);

    private static final DubboShutdownHook DUBBO_SHUTDOWN_HOOK = new DubboShutdownHook("DubboShutdownHook");

    private final ShutdownHookCallbacks callbacks = ShutdownHookCallbacks.INSTANCE;

    private final AtomicBoolean registered = new AtomicBoolean(false);

    private static final AtomicBoolean destroyed = new AtomicBoolean(false);

    private final EventDispatcher eventDispatcher = EventDispatcher.getDefaultExtension();

    private DubboShutdownHook(String name) {
        super(name);
    }

    public static DubboShutdownHook getDubboShutdownHook() {
        return DUBBO_SHUTDOWN_HOOK;
    }

    @Override
    public void run() {
        if (logger.isInfoEnabled()) {
            logger.info("Run shutdown hook now.");
        }
        callback();
        doDestroy();
    }

    void clear() {
        callbacks.clear();
    }

    private void callback() {
        callbacks.callback();
    }

    public void register() {
        if (!registered.get() && registered.compareAndSet(false, true)) {
            DubboShutdownHook dubboShutdownHook = getDubboShutdownHook();
            Runtime.getRuntime().addShutdownHook(dubboShutdownHook);
            dispatch(new DubboShutdownHookRegisteredEvent(dubboShutdownHook));
        }
    }

    public void unregister() {
        if (registered.get() && registered.compareAndSet(true, false)) {
            DubboShutdownHook dubboShutdownHook = getDubboShutdownHook();
            Runtime.getRuntime().removeShutdownHook(dubboShutdownHook);
            dispatch(new DubboShutdownHookUnregisteredEvent(dubboShutdownHook));
        }
    }

    public void doDestroy() {
        dispatch(new DubboServiceDestroyedEvent(this));
    }

    public static void destroyAll() {
        if (destroyed.compareAndSet(false, true)) {
            AbstractRegistryFactory.destroyAll();
            destroyProtocols();
        }
    }

    private void dispatch(Event event) {
        eventDispatcher.dispatch(event);
    }

    private static void destroyProtocols() {
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