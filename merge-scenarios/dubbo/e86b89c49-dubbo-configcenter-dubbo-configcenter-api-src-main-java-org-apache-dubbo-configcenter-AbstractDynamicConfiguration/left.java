package org.apache.dubbo.configcenter;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.AbstractConfiguration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class AbstractDynamicConfiguration<TargetListener> extends AbstractConfiguration implements DynamicConfiguration {

    protected static final String DEFAULT_GROUP = "dubbo";

    protected URL url;

    protected ConcurrentMap<String, TargetListener> targetListeners = new ConcurrentHashMap<>();

    public AbstractDynamicConfiguration() {
    }

    public AbstractDynamicConfiguration(URL url) {
        this.url = url;
        initWith(url);
    }

    @Override
    public void addListener(String key, ConfigurationListener listener) {
        addListener(key, Constants.DUBBO, listener);
    }

    @Override
    public void addListener(String key, String group, ConfigurationListener listener) {
        TargetListener targetListener = targetListeners.computeIfAbsent(group + key, ignoreK -> this.createTargetListener(key, group));
        addConfigurationListener(key, group, targetListener, listener);
    }

    @Override
    public String getConfig(String key) {
        return getConfig(key, null, 0L);
    }

    @Override
    public String getConfig(String key, String group) {
        return getConfig(key, group, 0L);
    }

    @Override
    public String getConfig(String key, String group, long timeout) {
        try {
            return getTargetConfig(key, group, timeout);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void removeListener(String key, ConfigurationListener listener) {
        removeListener(key, Constants.DUBBO, listener);
    }

    @Override
    public void removeListener(String key, String group, ConfigurationListener listener) {
        TargetListener targetListener = targetListeners.get(group + key);
        if (targetListener != null) {
            removeConfigurationListener(key, group, targetListener, listener);
        }
    }

    protected abstract void initWith(URL url);

    protected abstract String getTargetConfig(String key, String group, long timeout);

    protected abstract void addConfigurationListener(String key, String group, TargetListener targetListener, ConfigurationListener configurationListener);

    protected abstract void removeConfigurationListener(String key, String group, TargetListener targetListener, ConfigurationListener configurationListener);

    protected abstract TargetListener createTargetListener(String key, String group);
}