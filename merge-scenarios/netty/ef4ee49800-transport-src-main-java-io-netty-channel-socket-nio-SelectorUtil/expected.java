package io.netty.channel.socket.nio;

import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;
import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Selector;

final class SelectorUtil {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(SelectorUtil.class);

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

    static void cleanupKeys(Selector selector) {
        try {
            selector.selectNow();
        } catch (Throwable t) {
            logger.warn("Failed to update SelectionKeys.", t);
        }
    }

    private SelectorUtil() {
    }
}