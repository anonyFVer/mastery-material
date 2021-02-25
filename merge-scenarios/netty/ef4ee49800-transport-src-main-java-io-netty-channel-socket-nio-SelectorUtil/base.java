package io.netty.channel.socket.nio;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Selector;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

public final class SelectorUtil {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(SelectorUtil.class);

    public static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    static {
        String key = "sun.nio.ch.bugLevel";
        try {
            String buglevel = System.getProperty(key);
            if (buglevel == null) {
                System.setProperty(key, "");
            }
        } catch (SecurityException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Unable to get/set System Property '" + key + "'", e);
            }
        }
    }

    static void select(Selector selector) throws IOException {
        try {
            selector.select(10);
        } catch (CancelledKeyException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(CancelledKeyException.class.getSimpleName() + " raised by a Selector - JDK bug?", e);
            }
        }
    }

    private SelectorUtil() {
    }
}