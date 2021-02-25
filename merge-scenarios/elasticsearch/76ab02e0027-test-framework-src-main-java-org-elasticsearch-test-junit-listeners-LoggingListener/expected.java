package org.elasticsearch.test.junit.listeners;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;
import java.util.HashMap;
import java.util.Map;

public class LoggingListener extends RunListener {

    private Map<String, String> previousLoggingMap;

    private Map<String, String> previousClassLoggingMap;

    private Map<String, String> previousPackageLoggingMap;

    @Override
    public void testRunStarted(Description description) throws Exception {
        Package testClassPackage = description.getTestClass().getPackage();
        previousPackageLoggingMap = processTestLogging(testClassPackage != null ? testClassPackage.getAnnotation(TestLogging.class) : null);
        previousClassLoggingMap = processTestLogging(description.getAnnotation(TestLogging.class));
    }

    @Override
    public void testRunFinished(Result result) throws Exception {
        previousClassLoggingMap = reset(previousClassLoggingMap);
        previousPackageLoggingMap = reset(previousPackageLoggingMap);
    }

    @Override
    public void testStarted(Description description) throws Exception {
        final TestLogging testLogging = description.getAnnotation(TestLogging.class);
        previousLoggingMap = processTestLogging(testLogging);
    }

    @Override
    public void testFinished(Description description) throws Exception {
        previousLoggingMap = reset(previousLoggingMap);
    }

    private static Logger resolveLogger(String loggerName) {
        if (loggerName.equalsIgnoreCase("_root")) {
            return ESLoggerFactory.getRootLogger();
        }
        return Loggers.getLogger(loggerName);
    }

    private Map<String, String> processTestLogging(TestLogging testLogging) {
        Map<String, String> map = getLoggersAndLevelsFromAnnotation(testLogging);
        if (map == null) {
            return null;
        }
        Map<String, String> previousValues = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            Logger logger = resolveLogger(entry.getKey());
            previousValues.put(entry.getKey(), logger.getLevel().toString());
            Loggers.setLevel(logger, entry.getValue());
        }
        return previousValues;
    }

    public static Map<String, String> getLoggersAndLevelsFromAnnotation(TestLogging testLogging) {
        if (testLogging == null) {
            return null;
        }
        Map<String, String> map = new HashMap<>();
        final String[] loggersAndLevels = testLogging.value().split(",");
        for (String loggerAndLevel : loggersAndLevels) {
            String[] loggerAndLevelArray = loggerAndLevel.split(":");
            if (loggerAndLevelArray.length >= 2) {
                String loggerName = loggerAndLevelArray[0];
                String level = loggerAndLevelArray[1];
                map.put(loggerName, level);
            }
        }
        return map;
    }

    private Map<String, String> reset(Map<String, String> map) {
        if (map != null) {
            for (Map.Entry<String, String> previousLogger : map.entrySet()) {
                Logger logger = resolveLogger(previousLogger.getKey());
                Loggers.setLevel(logger, previousLogger.getValue());
            }
        }
        return null;
    }
}