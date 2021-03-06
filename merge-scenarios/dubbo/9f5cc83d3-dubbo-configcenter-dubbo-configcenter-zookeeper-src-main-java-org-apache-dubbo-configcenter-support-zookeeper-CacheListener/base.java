package org.apache.dubbo.configcenter.support.zookeeper;

import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.configcenter.ConfigChangeEvent;
import org.apache.dubbo.configcenter.ConfigChangeType;
import org.apache.dubbo.configcenter.ConfigurationListener;
import org.apache.dubbo.remoting.zookeeper.DataListener;
import org.apache.dubbo.remoting.zookeeper.EventType;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;

public class CacheListener implements DataListener {

    private Map<String, Set<ConfigurationListener>> keyListeners = new ConcurrentHashMap<>();

    private CountDownLatch initializedLatch;

    private String rootPath;

    public CacheListener(String rootPath, CountDownLatch initializedLatch) {
        this.rootPath = rootPath;
        this.initializedLatch = initializedLatch;
    }

    public void addListener(String key, ConfigurationListener configurationListener) {
        Set<ConfigurationListener> listeners = this.keyListeners.computeIfAbsent(key, k -> new CopyOnWriteArraySet<>());
        listeners.add(configurationListener);
    }

    public void removeListener(String key, ConfigurationListener configurationListener) {
        Set<ConfigurationListener> listeners = this.keyListeners.get(key);
        if (listeners != null) {
            listeners.remove(configurationListener);
        }
    }

    private String pathToKey(String path) {
        if (StringUtils.isEmpty(path)) {
            return path;
        }
        return path.replace(rootPath + "/", "").replaceAll("/", ".");
    }

    @Override
    public void dataChanged(String path, Object value, EventType eventType) {
        if (eventType == null) {
            return;
        }
        if (eventType == EventType.INITIALIZED) {
            initializedLatch.countDown();
            return;
        }
        if (path == null || (value == null && eventType != EventType.NodeDeleted)) {
            return;
        }
        if (path.split("/").length >= 5) {
            String key = pathToKey(path);
            ConfigChangeType changeType;
            switch(eventType) {
                case NodeCreated:
                    changeType = ConfigChangeType.ADDED;
                    break;
                case NodeDeleted:
                    changeType = ConfigChangeType.DELETED;
                    break;
                case NodeDataChanged:
                    changeType = ConfigChangeType.MODIFIED;
                    break;
                default:
                    return;
            }
            ConfigChangeEvent configChangeEvent = new ConfigChangeEvent(key, (String) value, changeType);
            Set<ConfigurationListener> listeners = keyListeners.get(key);
            if (CollectionUtils.isNotEmpty(listeners)) {
                listeners.forEach(listener -> listener.process(configChangeEvent));
            }
        }
    }
}