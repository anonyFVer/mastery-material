package org.apache.dubbo.common.config.configcenter.wrapper;

import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.function.Consumer;
import java.util.function.Function;

public class CompositeDynamicConfiguration implements DynamicConfiguration {

    public static final String NAME = "COMPOSITE";

    private Set<DynamicConfiguration> configurations = new HashSet<>();

    public void addConfiguration(DynamicConfiguration configuration) {
        this.configurations.add(configuration);
    }

    @Override
    public void addListener(String key, String group, ConfigurationListener listener) {
        iterateListenerOperation(configuration -> configuration.addListener(key, group, listener));
    }

    @Override
    public void removeListener(String key, String group, ConfigurationListener listener) {
        iterateListenerOperation(configuration -> configuration.removeListener(key, group, listener));
    }

    @Override
    public String getRule(String key, String group, long timeout) throws IllegalStateException {
        return (String) iterateConfigOperation(configuration -> configuration.getRule(key, group, timeout));
    }

    @Override
    public String getProperties(String key, String group, long timeout) throws IllegalStateException {
        return (String) iterateConfigOperation(configuration -> configuration.getProperties(key, group, timeout));
    }

    @Override
    public Object getInternalProperty(String key) {
        return iterateConfigOperation(configuration -> configuration.getInternalProperty(key));
    }

    @Override
    public boolean publishConfig(String key, String group, String content) throws UnsupportedOperationException {
        boolean publishedAll = true;
        for (DynamicConfiguration configuration : configurations) {
            if (!configuration.publishConfig(key, group, content)) {
                publishedAll = false;
            }
        }
        return publishedAll;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SortedSet<String> getConfigKeys(String group) throws UnsupportedOperationException {
        return (SortedSet<String>) iterateConfigOperation(configuration -> configuration.getConfigKeys(group));
    }

    @Override
    @SuppressWarnings("unchecked")
    public SortedMap<String, String> getConfigs(String group) throws UnsupportedOperationException {
        return (SortedMap<String, String>) iterateConfigOperation(configuration -> configuration.getConfigs(group));
    }

    private void iterateListenerOperation(Consumer<DynamicConfiguration> consumer) {
        for (DynamicConfiguration configuration : configurations) {
            consumer.accept(configuration);
        }
    }

    private Object iterateConfigOperation(Function<DynamicConfiguration, Object> func) {
        Object value = null;
        for (DynamicConfiguration configuration : configurations) {
            value = func.apply(configuration);
        }
        return value;
    }
}