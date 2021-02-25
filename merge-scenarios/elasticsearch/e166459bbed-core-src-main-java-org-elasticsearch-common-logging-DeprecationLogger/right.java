package org.elasticsearch.common.logging;

import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class DeprecationLogger {

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

    private final ESLogger logger;

    public DeprecationLogger(ESLogger parentLogger) {
        String name = parentLogger.getName();
        if (name.startsWith("org.elasticsearch")) {
            name = name.replace("org.elasticsearch.", "org.elasticsearch.deprecation.");
        } else {
            name = "deprecation." + name;
        }
        this.logger = ESLoggerFactory.getLogger(parentLogger.getPrefix(), name);
    }

    public void deprecated(String msg, Object... params) {
        deprecated(THREAD_CONTEXT, msg, params);
    }

    @SuppressLoggerChecks(reason = "safely delegates to logger")
    void deprecated(Set<ThreadContext> threadContexts, String msg, Object... params) {
        Iterator<ThreadContext> iterator = threadContexts.iterator();
        if (iterator.hasNext()) {
            final String formattedMsg = LoggerMessageFormat.format(msg, params);
            while (iterator.hasNext()) {
                try {
                    iterator.next().addResponseHeader(DEPRECATION_HEADER, formattedMsg);
                } catch (IllegalStateException e) {
                }
            }
            logger.warn(formattedMsg);
        } else {
            logger.warn(msg, params);
        }
    }
}