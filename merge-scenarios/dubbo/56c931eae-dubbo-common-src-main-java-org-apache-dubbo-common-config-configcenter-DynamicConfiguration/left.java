package org.apache.dubbo.common.config.configcenter;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.Configuration;
import org.apache.dubbo.common.config.Environment;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import static org.apache.dubbo.common.extension.ExtensionLoader.getExtensionLoader;

public interface DynamicConfiguration extends Configuration {

    String DEFAULT_GROUP = "dubbo";

    default void addListener(String key, ConfigurationListener listener) {
        addListener(key, DEFAULT_GROUP, listener);
    }

    default void removeListener(String key, ConfigurationListener listener) {
        removeListener(key, DEFAULT_GROUP, listener);
    }

    void addListener(String key, String group, ConfigurationListener listener);

    void removeListener(String key, String group, ConfigurationListener listener);

    default String getRule(String key, String group) {
        return getRule(key, group, -1L);
    }

    String getRule(String key, String group, long timeout) throws IllegalStateException;

    default String getProperties(String key, String group) throws IllegalStateException {
        return getProperties(key, group, -1L);
    }

    String getProperties(String key, String group, long timeout) throws IllegalStateException;

    default boolean publishConfig(String key, String group, String content) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("No support");
    }

    default SortedSet<String> getConfigKeys(String group) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("No support");
    }

    default SortedMap<String, String> getConfigs(String group) throws UnsupportedOperationException {
        return getConfigs(group, -1);
    }

    default SortedMap<String, String> getConfigs(String group, long timeout) throws UnsupportedOperationException, IllegalStateException {
        SortedMap<String, String> configs = new TreeMap<>();
        SortedSet<String> configKeys = getConfigKeys(group);
        configKeys.forEach(key -> configs.put(key, getString(key)));
        return Collections.unmodifiableSortedMap(configs);
    }

    static DynamicConfiguration getDynamicConfiguration() {
        Optional<DynamicConfiguration> optional = Environment.getInstance().getDynamicConfiguration();
        return optional.orElseGet(() -> getExtensionLoader(DynamicConfigurationFactory.class).getDefaultExtension().getDynamicConfiguration(null));
    }

    static String getRuleKey(URL url) {
        return url.getColonSeparatedKey();
    }
}