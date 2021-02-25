package org.apache.dubbo.metadata.support;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.metadata.store.MetadataReport;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractMetadataReport implements MetadataReport {

    private static final char URL_SEPARATOR = ' ';

    private static final String URL_SPLIT = "\\s+";

    private final static String TAG = "sd.";

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    final Properties properties = new Properties();

    private final ExecutorService servicestoreCacheExecutor = Executors.newFixedThreadPool(1, new NamedThreadFactory("DubboSaveServicestoreCache", true));

    private final AtomicLong lastCacheChanged = new AtomicLong();

    private final Set<URL> registered = new ConcurrentHashSet<URL>();

    final Set<URL> failedServiceStore = new ConcurrentHashSet<URL>();

    private URL serviceStoreURL;

    File file;

    private AtomicBoolean INIT = new AtomicBoolean(false);

    private final ScheduledExecutorService retryExecutor = Executors.newScheduledThreadPool(0, new NamedThreadFactory("DubboRegistryFailedRetryTimer", true));

    private AtomicInteger retryTimes = new AtomicInteger(0);

    public AbstractMetadataReport(URL servicestoreURL) {
        setUrl(servicestoreURL);
        String filename = servicestoreURL.getParameter(Constants.FILE_KEY, System.getProperty("user.home") + "/.dubbo/dubbo-servicestore-" + servicestoreURL.getParameter(Constants.APPLICATION_KEY) + "-" + servicestoreURL.getAddress() + ".cache");
        File file = null;
        if (ConfigUtils.isNotEmpty(filename)) {
            file = new File(filename);
            if (!file.exists() && file.getParentFile() != null && !file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    throw new IllegalArgumentException("Invalid service store file " + file + ", cause: Failed to create directory " + file.getParentFile() + "!");
                }
            }
            if (!INIT.getAndSet(true) && file.exists()) {
                file.delete();
            }
        }
        this.file = file;
        loadProperties();
        retryExecutor.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                try {
                    retry();
                } catch (Throwable t) {
                    logger.error("Unexpected error occur at failed retry, cause: " + t.getMessage(), t);
                }
            }
        }, 1000, 3000, TimeUnit.MILLISECONDS);
    }

    public URL getUrl() {
        return serviceStoreURL;
    }

    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("servicestore url == null");
        }
        this.serviceStoreURL = url;
    }

    public Set<URL> getRegistered() {
        return registered;
    }

    private void doSaveProperties(long version) {
        if (version < lastCacheChanged.get()) {
            return;
        }
        if (file == null) {
            return;
        }
        try {
            File lockfile = new File(file.getAbsolutePath() + ".lock");
            if (!lockfile.exists()) {
                lockfile.createNewFile();
            }
            RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");
            try {
                FileChannel channel = raf.getChannel();
                try {
                    FileLock lock = channel.tryLock();
                    if (lock == null) {
                        throw new IOException("Can not lock the servicestore cache file " + file.getAbsolutePath() + ", ignore and retry later, maybe multi java process use the file, please config: dubbo.servicestore.file=xxx.properties");
                    }
                    try {
                        if (!file.exists()) {
                            file.createNewFile();
                        }
                        FileOutputStream outputFile = new FileOutputStream(file);
                        try {
                            properties.store(outputFile, "Dubbo Servicestore Cache");
                        } finally {
                            outputFile.close();
                        }
                    } finally {
                        lock.release();
                    }
                } finally {
                    channel.close();
                }
            } finally {
                raf.close();
            }
        } catch (Throwable e) {
            if (version < lastCacheChanged.get()) {
                return;
            } else {
                servicestoreCacheExecutor.execute(new SaveProperties(lastCacheChanged.incrementAndGet()));
            }
            logger.warn("Failed to save service store file, cause: " + e.getMessage(), e);
        }
    }

    void loadProperties() {
        if (file != null && file.exists()) {
            InputStream in = null;
            try {
                in = new FileInputStream(file);
                properties.load(in);
                if (logger.isInfoEnabled()) {
                    logger.info("Load service store file " + file + ", data: " + properties);
                }
            } catch (Throwable e) {
                logger.warn("Failed to load service store file " + file, e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        }
    }

    private void saveProperties(URL url, boolean add) {
        if (file == null) {
            return;
        }
        try {
            if (add) {
                properties.setProperty(url.getServiceKey(), url.toFullString());
            } else {
                properties.remove(url.getServiceKey());
            }
            long version = lastCacheChanged.incrementAndGet();
            servicestoreCacheExecutor.execute(new SaveProperties(version));
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    @Override
    public String toString() {
        return getUrl().toString();
    }

    private class SaveProperties implements Runnable {

        private long version;

        private SaveProperties(long version) {
            this.version = version;
        }

        @Override
        public void run() {
            doSaveProperties(version);
        }
    }

    public void put(URL url) {
        try {
            url = url.removeParameters(Constants.PID_KEY, Constants.TIMESTAMP_KEY);
            if (logger.isInfoEnabled()) {
                logger.info("Servicestore Put: " + url);
            }
            failedServiceStore.remove(url);
            doPut(url);
            saveProperties(url, true);
        } catch (Exception e) {
            failedServiceStore.add(url);
            logger.error("Failed to put servicestore " + url + " in  " + getUrl().toFullString() + ", cause: " + e.getMessage(), e);
        }
    }

    public URL peek(URL url) {
        try {
            if (logger.isInfoEnabled()) {
                logger.info("Servicestore Peek: " + url);
            }
            return doPeek(url);
        } catch (Exception e) {
            logger.error("Failed to peek servicestore " + url + " in  " + getUrl().toFullString() + ", cause: " + e.getMessage(), e);
        }
        return null;
    }

    public String getUrlKey(URL url) {
        String protocol = getProtocol(url);
        String app = url.getParameter(Constants.APPLICATION_KEY);
        String appStr = Constants.PROVIDER_PROTOCOL.equals(protocol) ? "" : (app == null ? "" : (app + "."));
        return TAG + protocol + "." + appStr + url.getServiceKey();
    }

    String getProtocol(URL url) {
        String protocol = url.getParameter(Constants.SIDE_KEY);
        protocol = protocol == null ? url.getProtocol() : protocol;
        return protocol;
    }

    public void retry() {
        if (retryTimes.incrementAndGet() > 120000 && failedServiceStore.isEmpty()) {
            retryExecutor.shutdown();
        }
        if (failedServiceStore.isEmpty()) {
            return;
        }
        for (URL url : new HashSet<URL>(failedServiceStore)) {
            this.put(url);
        }
    }

    protected abstract void doPut(URL url);

    protected abstract URL doPeek(URL url);
}