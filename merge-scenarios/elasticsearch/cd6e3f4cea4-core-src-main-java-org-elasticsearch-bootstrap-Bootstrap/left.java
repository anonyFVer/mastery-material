package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.PidFile;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.inject.CreationException;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

final class Bootstrap {

    private static volatile Bootstrap INSTANCE;

    private volatile Node node;

    private final CountDownLatch keepAliveLatch = new CountDownLatch(1);

    private final Thread keepAliveThread;

    private final Spawner spawner = new Spawner();

    Bootstrap() {
        keepAliveThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    keepAliveLatch.await();
                } catch (InterruptedException e) {
                }
            }
        }, "elasticsearch[keepAlive/" + Version.CURRENT + "]");
        keepAliveThread.setDaemon(false);
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                keepAliveLatch.countDown();
            }
        });
    }

    public static void initializeNatives(Path tmpFile, boolean mlockAll, boolean systemCallFilter, boolean ctrlHandler) {
        final Logger logger = Loggers.getLogger(Bootstrap.class);
        if (Natives.definitelyRunningAsRoot()) {
            throw new RuntimeException("can not run elasticsearch as root");
        }
        if (systemCallFilter) {
            Natives.tryInstallSystemCallFilter(tmpFile);
        }
        if (mlockAll) {
            if (Constants.WINDOWS) {
                Natives.tryVirtualLock();
            } else {
                Natives.tryMlockall();
            }
        }
        if (ctrlHandler) {
            Natives.addConsoleCtrlHandler(new ConsoleCtrlHandler() {

                @Override
                public boolean handle(int code) {
                    if (CTRL_CLOSE_EVENT == code) {
                        logger.info("running graceful exit on windows");
                        try {
                            Bootstrap.stop();
                        } catch (IOException e) {
                            throw new ElasticsearchException("failed to stop node", e);
                        }
                        return true;
                    }
                    return false;
                }
            });
        }
        try {
            JNAKernel32Library.getInstance();
        } catch (Exception ignored) {
        }
        Natives.trySetMaxNumberOfThreads();
        Natives.trySetMaxSizeVirtualMemory();
        StringHelper.randomId();
    }

    static void initializeProbes() {
        ProcessProbe.getInstance();
        OsProbe.getInstance();
        JvmInfo.jvmInfo();
    }

    private void setup(boolean addShutdownHook, Environment environment) throws BootstrapException {
        Settings settings = environment.settings();
        try {
            spawner.spawnNativePluginControllers(environment);
            Runtime.getRuntime().addShutdownHook(new Thread() {

                @Override
                public void run() {
                    try {
                        spawner.close();
                    } catch (IOException e) {
                        throw new ElasticsearchException("Failed to destroy spawned controllers", e);
                    }
                }
            });
        } catch (IOException e) {
            throw new BootstrapException(e);
        }
        initializeNatives(environment.tmpFile(), BootstrapSettings.MEMORY_LOCK_SETTING.get(settings), BootstrapSettings.SYSTEM_CALL_FILTER_SETTING.get(settings), BootstrapSettings.CTRLHANDLER_SETTING.get(settings));
        initializeProbes();
        if (addShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread() {

                @Override
                public void run() {
                    try {
                        IOUtils.close(node);
                        LoggerContext context = (LoggerContext) LogManager.getContext(false);
                        Configurator.shutdown(context);
                    } catch (IOException ex) {
                        throw new ElasticsearchException("failed to stop node", ex);
                    }
                }
            });
        }
        try {
            JarHell.checkJarHell();
        } catch (IOException | URISyntaxException e) {
            throw new BootstrapException(e);
        }
        try {
            Security.configure(environment, BootstrapSettings.SECURITY_FILTER_BAD_DEFAULTS_SETTING.get(settings));
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new BootstrapException(e);
        }
        node = new Node(environment) {

            @Override
            protected void validateNodeBeforeAcceptingRequests(final Settings settings, final BoundTransportAddress boundTransportAddress, List<BootstrapCheck> checks) throws NodeValidationException {
                BootstrapChecks.check(settings, boundTransportAddress, checks);
            }
        };
    }

    private static KeyStoreWrapper loadKeyStore(Environment initialEnv) throws BootstrapException {
        final KeyStoreWrapper keystore;
        try {
            keystore = KeyStoreWrapper.load(initialEnv.configFile());
        } catch (IOException e) {
            throw new BootstrapException(e);
        }
        if (keystore == null) {
            return null;
        }
        try {
            keystore.decrypt(new char[0]);
        } catch (Exception e) {
            throw new BootstrapException(e);
        }
        return keystore;
    }

    private static Environment createEnvironment(boolean foreground, Path pidFile, KeyStoreWrapper keystore, Settings initialSettings) {
        Terminal terminal = foreground ? Terminal.DEFAULT : null;
        Settings.Builder builder = Settings.builder();
        if (pidFile != null) {
            builder.put(Environment.PIDFILE_SETTING.getKey(), pidFile);
        }
        builder.put(initialSettings);
        if (keystore != null) {
            builder.setKeyStore(keystore);
        }
        return InternalSettingsPreparer.prepareEnvironment(builder.build(), terminal, Collections.emptyMap());
    }

    private void start() throws NodeValidationException {
        node.start();
        keepAliveThread.start();
    }

    static void stop() throws IOException {
        try {
            IOUtils.close(INSTANCE.node);
        } finally {
            INSTANCE.keepAliveLatch.countDown();
        }
    }

    @SuppressForbidden(reason = "sets logger prefix on initialization")
    static void initLoggerPrefix() {
        System.setProperty("es.logger.prefix", "");
    }

    static void init(final boolean foreground, final Path pidFile, final boolean quiet, final Environment initialEnv) throws BootstrapException, NodeValidationException, UserException {
        initLoggerPrefix();
        BootstrapInfo.init();
        INSTANCE = new Bootstrap();
        final KeyStoreWrapper keystore = loadKeyStore(initialEnv);
        Environment environment = createEnvironment(foreground, pidFile, keystore, initialEnv.settings());
        try {
            LogConfigurator.configure(environment);
        } catch (IOException e) {
            throw new BootstrapException(e);
        }
        checkForCustomConfFile();
        if (environment.pidFile() != null) {
            try {
                PidFile.create(environment.pidFile(), true);
            } catch (IOException e) {
                throw new BootstrapException(e);
            }
        }
        final boolean closeStandardStreams = (foreground == false) || quiet;
        try {
            if (closeStandardStreams) {
                final Logger rootLogger = ESLoggerFactory.getRootLogger();
                final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
                if (maybeConsoleAppender != null) {
                    Loggers.removeAppender(rootLogger, maybeConsoleAppender);
                }
                closeSystOut();
            }
            checkLucene();
            Thread.setDefaultUncaughtExceptionHandler(new ElasticsearchUncaughtExceptionHandler(() -> Node.NODE_NAME_SETTING.get(environment.settings())));
            INSTANCE.setup(true, environment);
            try {
                IOUtils.close(keystore);
            } catch (IOException e) {
                throw new BootstrapException(e);
            }
            INSTANCE.start();
            if (closeStandardStreams) {
                closeSysError();
            }
        } catch (NodeValidationException | RuntimeException e) {
            final Logger rootLogger = ESLoggerFactory.getRootLogger();
            final Appender maybeConsoleAppender = Loggers.findAppender(rootLogger, ConsoleAppender.class);
            if (foreground && maybeConsoleAppender != null) {
                Loggers.removeAppender(rootLogger, maybeConsoleAppender);
            }
            Logger logger = Loggers.getLogger(Bootstrap.class);
            if (INSTANCE.node != null) {
                logger = Loggers.getLogger(Bootstrap.class, Node.NODE_NAME_SETTING.get(INSTANCE.node.settings()));
            }
            if (e instanceof CreationException) {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                PrintStream ps = null;
                try {
                    ps = new PrintStream(os, false, "UTF-8");
                } catch (UnsupportedEncodingException uee) {
                    assert false;
                    e.addSuppressed(uee);
                }
                new StartupException(e).printStackTrace(ps);
                ps.flush();
                try {
                    logger.error("Guice Exception: {}", os.toString("UTF-8"));
                } catch (UnsupportedEncodingException uee) {
                    assert false;
                    e.addSuppressed(uee);
                }
            } else if (e instanceof NodeValidationException) {
                logger.error("node validation exception\n{}", e.getMessage());
            } else {
                logger.error("Exception", e);
            }
            if (foreground && maybeConsoleAppender != null) {
                Loggers.addAppender(rootLogger, maybeConsoleAppender);
            }
            throw e;
        }
    }

    @SuppressForbidden(reason = "System#out")
    private static void closeSystOut() {
        System.out.close();
    }

    @SuppressForbidden(reason = "System#err")
    private static void closeSysError() {
        System.err.close();
    }

    private static void checkForCustomConfFile() {
        String confFileSetting = System.getProperty("es.default.config");
        checkUnsetAndMaybeExit(confFileSetting, "es.default.config");
        confFileSetting = System.getProperty("es.config");
        checkUnsetAndMaybeExit(confFileSetting, "es.config");
        confFileSetting = System.getProperty("elasticsearch.config");
        checkUnsetAndMaybeExit(confFileSetting, "elasticsearch.config");
    }

    private static void checkUnsetAndMaybeExit(String confFileSetting, String settingName) {
        if (confFileSetting != null && confFileSetting.isEmpty() == false) {
            Logger logger = Loggers.getLogger(Bootstrap.class);
            logger.info("{} is no longer supported. elasticsearch.yml must be placed in the config directory and cannot be renamed.", settingName);
            exit(1);
        }
    }

    @SuppressForbidden(reason = "Allowed to exit explicitly in bootstrap phase")
    private static void exit(int status) {
        System.exit(status);
    }

    private static void checkLucene() {
        if (Version.CURRENT.luceneVersion.equals(org.apache.lucene.util.Version.LATEST) == false) {
            throw new AssertionError("Lucene version mismatch this version of Elasticsearch requires lucene version [" + Version.CURRENT.luceneVersion + "]  but the current lucene version is [" + org.apache.lucene.util.Version.LATEST + "]");
        }
    }
}