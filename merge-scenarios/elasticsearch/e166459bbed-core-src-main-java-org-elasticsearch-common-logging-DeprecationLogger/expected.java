package org.elasticsearch.common.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class DeprecationLogger {

    private final Logger logger;

    public static final String DEPRECATION_HEADER = "Warning";

    private static final CopyOnWriteArraySet<ThreadContext> THREAD_CONTEXT = new CopyOnWriteArraySet<>();

    public static void setThreadContext(ThreadContext threadContext) {
        assert threadContext != null;
        if (THREAD_CONTEXT.add(threadContext) == false) {
            throw new IllegalStateException("Double-setting ThreadContext not allowed!");
        }
    }

    public static void removeThreadContext(ThreadContext threadContext) {
        assert threadContext != null;
        if (THREAD_CONTEXT.remove(threadContext) == false) {
            throw new IllegalStateException("Removing unknown ThreadContext not allowed!");
        }
    }

    public DeprecationLogger(Logger parentLogger) {
        String name = parentLogger.getName();
        if (name.startsWith("org.elasticsearch")) {
            name = name.replace("org.elasticsearch.", "org.elasticsearch.deprecation.");
        } else {
            name = "deprecation." + name;
        }
        this.logger = LogManager.getLogger(name, parentLogger.getMessageFactory());
    }

    public void deprecated(String msg, Object... params) {
        deprecated(THREAD_CONTEXT, msg, params);
    }

    @SuppressLoggerChecks(reason = "safely delegates to logger")
    void deprecated(Set<ThreadContext> threadContexts, String message, Object... params) {
        Iterator<ThreadContext> iterator = threadContexts.iterator();
        if (iterator.hasNext()) {
            final String formattedMessage = LoggerMessageFormat.format(message, params);
            while (iterator.hasNext()) {
                try {
                    iterator.next().addResponseHeader(DEPRECATION_HEADER, formattedMessage);
                } catch (IllegalStateException e) {
                }
            }
            logger.warn(formattedMessage);
        } else {
            logger.warn(message, params);
        }
    }
}