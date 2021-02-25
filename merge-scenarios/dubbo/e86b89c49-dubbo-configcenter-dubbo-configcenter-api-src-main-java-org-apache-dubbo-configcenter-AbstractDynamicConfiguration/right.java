package org.apache.dubbo.configcenter;

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
        TargetListener targetListener = targetListeners.computeIfAbsent(key, this::createTargetListener);
        addConfigurationListener(key, targetListener, listener);
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
    }

    protected abstract void initWith(URL url);

    protected abstract void recover();

    protected abstract String getTargetConfig(String key, String group, long timeout);

    protected abstract void addConfigurationListener(String key, TargetListener targetListener, ConfigurationListener configurationListener);

    protected abstract TargetListener createTargetListener(String key);
}