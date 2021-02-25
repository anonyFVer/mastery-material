package org.apache.dubbo.configcenter.support.zookeeper;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.configcenter.ConfigurationListener;
import org.apache.dubbo.configcenter.DynamicConfiguration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.apache.dubbo.common.Constants.CONFIG_NAMESPACE_KEY;

public class ZookeeperDynamicConfiguration implements DynamicConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperDynamicConfiguration.class);

    private Executor executor;

    private CuratorFramework client;

    private String rootPath;

    private TreeCache treeCache;

    private CountDownLatch initializedLatch;

    private CacheListener cacheListener;

    private URL url;

    ZookeeperDynamicConfiguration(URL url) {
        this.url = url;
        rootPath = "/" + url.getParameter(CONFIG_NAMESPACE_KEY, DEFAULT_GROUP) + "/config";
        RetryPolicy policy = new ExponentialBackoffRetry(1000, 3);
        int sessionTimeout = url.getParameter("config.session.timeout", 60 * 1000);
        int connectTimeout = url.getParameter("config.connect.timeout", 10 * 1000);
        String connectString = url.getBackupAddress();
        client = newClient(connectString, sessionTimeout, connectTimeout, policy);
        client.start();
        try {
            boolean connected = client.blockUntilConnected(3 * connectTimeout, TimeUnit.MILLISECONDS);
            if (!connected) {
                if (url.getParameter(Constants.CONFIG_CHECK_KEY, true)) {
                    throw new IllegalStateException("Failed to connect to config center (zookeeper): " + connectString + " in " + 3 * connectTimeout + "ms.");
                } else {
                    logger.warn("The config center (zookeeper) is not fully initialized in " + 3 * connectTimeout + "ms, address is: " + connectString);
                }
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("The thread was interrupted unexpectedly when trying connecting to zookeeper " + connectString + " config center, ", e);
        }
        initializedLatch = new CountDownLatch(1);
        this.cacheListener = new CacheListener(rootPath, initializedLatch);
        this.executor = Executors.newFixedThreadPool(1, new NamedThreadFactory(this.getClass().getSimpleName(), true));
        try {
            this.buildCache();
        } catch (Exception e) {
            logger.warn("Failed to build local cache for config center (zookeeper), address is ." + connectString);
        }
    }

    @Override
    public Object getInternalProperty(String key) {
        ChildData childData = treeCache.getCurrentData(key);
        if (childData != null) {
            return new String(childData.getData(), StandardCharsets.UTF_8);
        }
        return null;
    }

    @Override
    public void addListener(String key, String group, ConfigurationListener listener) {
        cacheListener.addListener(key, listener);
    }

    @Override
    public void removeListener(String key, String group, ConfigurationListener listener) {
        cacheListener.removeListener(key, listener);
    }

    @Override
    public String getConfig(String key, String group, long timeout) throws IllegalStateException {
        if (StringUtils.isNotEmpty(group)) {
            key = group + "/" + key;
        } else {
            int i = key.lastIndexOf(".");
            key = key.substring(0, i) + "/" + key.substring(i + 1);
        }
        return (String) getInternalProperty(rootPath + "/" + key);
    }

    private void buildCache() throws Exception {
        this.treeCache = new TreeCache(client, rootPath);
        treeCache.getListenable().addListener(cacheListener, executor);
        treeCache.start();
        initializedLatch.await();
    }
}