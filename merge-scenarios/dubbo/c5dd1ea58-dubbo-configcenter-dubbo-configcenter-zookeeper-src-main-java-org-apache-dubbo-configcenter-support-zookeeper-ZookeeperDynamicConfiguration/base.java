package org.apache.dubbo.configcenter.support.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.configcenter.ConfigurationListener;
import org.apache.dubbo.configcenter.DynamicConfiguration;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import static org.apache.dubbo.configcenter.Constants.CONFIG_NAMESPACE_KEY;

public class ZookeeperDynamicConfiguration implements DynamicConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperDynamicConfiguration.class);

    private Executor executor;

    private String rootPath;

    private final ZookeeperClient zkClient;

    private CountDownLatch initializedLatch;

    private CacheListener cacheListener;

    private URL url;

    ZookeeperDynamicConfiguration(URL url, ZookeeperTransporter zookeeperTransporter) {
        this.url = url;
        rootPath = "/" + url.getParameter(CONFIG_NAMESPACE_KEY, DEFAULT_GROUP) + "/config";
        initializedLatch = new CountDownLatch(1);
        this.cacheListener = new CacheListener(rootPath, initializedLatch);
        this.executor = Executors.newFixedThreadPool(1, new NamedThreadFactory(this.getClass().getSimpleName(), true));
        zkClient = zookeeperTransporter.connect(url);
        zkClient.addDataListener(rootPath, cacheListener, executor);
        try {
            this.initializedLatch.await();
        } catch (InterruptedException e) {
            logger.warn("Failed to build local cache for config center (zookeeper)." + url);
        }
    }

    @Override
    public Object getInternalProperty(String key) {
        return zkClient.getContent(key);
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

    @Override
    public String getConfigs(String key, String group, long timeout) throws IllegalStateException {
        return (String) getConfig(key, group, timeout);
    }
}