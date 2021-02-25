package io.netty.channel.socket.nio;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Selector;
import io.netty.logging.InternalLogger;
import io.netty.logging.InternalLoggerFactory;

final class SelectorUtil {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(SelectorUtil.class);

    static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;

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