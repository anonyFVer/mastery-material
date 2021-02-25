package org.elasticsearch.test.test;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import org.junit.runner.Description;
import org.junit.runner.Result;
import java.lang.reflect.Method;
import static org.hamcrest.CoreMatchers.equalTo;

public class LoggingListenerTests extends ESTestCase {

    public void testCustomLevelPerMethod() throws Exception {
        LoggingListener loggingListener = new LoggingListener();
        Description suiteDescription = Description.createSuiteDescription(TestClass.class);
        Logger xyzLogger = Loggers.getLogger("xyz");
        Logger abcLogger = Loggers.getLogger("abc");
        assertEquals(Level.ERROR, abcLogger.getLevel());
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.ERROR));
        loggingListener.testRunStarted(suiteDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.ERROR));
        Method method = TestClass.class.getMethod("annotatedTestMethod");
        TestLogging annotation = method.getAnnotation(TestLogging.class);
        Description testDescription = Description.createTestDescription(LoggingListenerTests.class, "annotatedTestMethod", annotation);
        loggingListener.testStarted(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.TRACE));
        assertThat(abcLogger.getLevel(), equalTo(Level.ERROR));
        loggingListener.testFinished(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.ERROR));
        loggingListener.testRunFinished(new Result());
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.ERROR));
    }

    public void testCustomLevelPerClass() throws Exception {
        LoggingListener loggingListener = new LoggingListener();
        Description suiteDescription = Description.createSuiteDescription(AnnotatedTestClass.class);
        Logger abcLogger = Loggers.getLogger("abc");
        Logger xyzLogger = Loggers.getLogger("xyz");
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.ERROR));
        loggingListener.testRunStarted(suiteDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));
        Description testDescription = Description.createTestDescription(LoggingListenerTests.class, "test");
        loggingListener.testStarted(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));
        loggingListener.testFinished(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));
        loggingListener.testRunFinished(new Result());
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.ERROR));
    }

    public void testCustomLevelPerClassAndPerMethod() throws Exception {
        LoggingListener loggingListener = new LoggingListener();
        Description suiteDescription = Description.createSuiteDescription(AnnotatedTestClass.class);
        Logger abcLogger = Loggers.getLogger("abc");
        Logger xyzLogger = Loggers.getLogger("xyz");
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.ERROR));
        loggingListener.testRunStarted(suiteDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));
        Method method = TestClass.class.getMethod("annotatedTestMethod");
        TestLogging annotation = method.getAnnotation(TestLogging.class);
        Description testDescription = Description.createTestDescription(LoggingListenerTests.class, "annotatedTestMethod", annotation);
        loggingListener.testStarted(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.TRACE));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));
        loggingListener.testFinished(testDescription);
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));
        Method method2 = TestClass.class.getMethod("annotatedTestMethod2");
        TestLogging annotation2 = method2.getAnnotation(TestLogging.class);
        Description testDescription2 = Description.createTestDescription(LoggingListenerTests.class, "annotatedTestMethod2", annotation2);
        loggingListener.testStarted(testDescription2);
        assertThat(xyzLogger.getLevel(), equalTo(Level.DEBUG));
        assertThat(abcLogger.getLevel(), equalTo(Level.TRACE));
        loggingListener.testFinished(testDescription2);
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.WARN));
        loggingListener.testRunFinished(new Result());
        assertThat(xyzLogger.getLevel(), equalTo(Level.ERROR));
        assertThat(abcLogger.getLevel(), equalTo(Level.ERROR));
    }

    @TestLogging("abc:WARN")
    public static class AnnotatedTestClass {
    }

    public static class TestClass {

        @SuppressWarnings("unused")
        @TestLogging("xyz:TRACE")
        public void annotatedTestMethod() {
        }

        @SuppressWarnings("unused")
        @TestLogging("abc:TRACE,xyz:DEBUG")
        public void annotatedTestMethod2() {
        }
    }
}