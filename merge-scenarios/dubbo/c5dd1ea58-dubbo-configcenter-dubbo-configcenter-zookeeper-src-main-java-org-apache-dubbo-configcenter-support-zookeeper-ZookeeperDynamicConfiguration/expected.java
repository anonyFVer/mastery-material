package org.apache.dubbo.configcenter.support.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_SEPARATOR;
import static org.apache.dubbo.configcenter.Constants.CONFIG_NAMESPACE_KEY;

public class ZookeeperDynamicConfiguration implements DynamicConfiguration {

    private static final String EMPTY_STRING = "";

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperDynamicConfiguration.class);

    private Executor executor;

    private String rootPath;

    private final ZookeeperClient zkClient;

    private CountDownLatch initializedLatch;

    private CacheListener cacheListener;

    private URL url;

    ZookeeperDynamicConfiguration(URL url, ZookeeperTransporter zookeeperTransporter) {
        this.url = url;
        rootPath = PATH_SEPARATOR + url.getParameter(CONFIG_NAMESPACE_KEY, DEFAULT_GROUP) + "/config";
        initializedLatch = new CountDownLatch(1);
        this.cacheListener = new CacheListener(rootPath, initializedLatch);
        this.executor = Executors.newFixedThreadPool(1, new NamedThreadFactory(this.getClass().getSimpleName(), true));
        zkClient = zookeeperTransporter.connect(url);
        zkClient.addDataListener(rootPath, cacheListener, executor);
        try {
            long timeout = url.getParameter("init.timeout", 5000);
            boolean isCountDown = this.initializedLatch.await(timeout, TimeUnit.MILLISECONDS);
            if (!isCountDown) {
                throw new IllegalStateException("Failed to receive INITIALIZED event from zookeeper, pls. check if url " + url + " is correct");
            }
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
        cacheListener.addListener(getPathKey(group, key), listener);
    }

    @Override
    public void removeListener(String key, String group, ConfigurationListener listener) {
        cacheListener.removeListener(getPathKey(group, key), listener);
    }

    @Override
    public String getRule(String key, String group, long timeout) throws IllegalStateException {
        return (String) getInternalProperty(getPathKey(group, key));
    }

    @Override
    public String getProperties(String key, String group, long timeout) throws IllegalStateException {
        if (StringUtils.isEmpty(group)) {
            group = DEFAULT_GROUP;
        }
        return (String) getInternalProperty(getPathKey(group, key));
    }

    @Override
    public boolean publishConfig(String key, String group, String content) {
        String path = buildPath(key, group);
        zkClient.create(path, content, true);
        return true;
    }

    @Override
    public SortedSet<String> getConfigKeys(String group) {
        String path = buildPath(group);
        List<String> nodes = zkClient.getChildren(path);
        return isEmpty(nodes) ? emptySortedSet() : unmodifiableSortedSet(new TreeSet<>(nodes));
    }

    private String getPathKey(String group, String key) {
        return rootPath + PATH_SEPARATOR + group + PATH_SEPARATOR + key;
    }

    protected String buildPath(String key, String group) {
        String path = null;
        if (StringUtils.isNotEmpty(group)) {
            path = group + "/" + key;
        } else {
            int i = key.lastIndexOf(".");
            path = key.substring(0, i) + "/" + key.substring(i + 1);
        }
        return buildPath(path);
    }

    protected String buildPath(String relativePath) {
        String path = rootPath + "/" + relativePath;
        return path;
    }
}