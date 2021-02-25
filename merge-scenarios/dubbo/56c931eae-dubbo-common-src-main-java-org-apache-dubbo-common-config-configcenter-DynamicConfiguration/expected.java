package org.apache.dubbo.common.config.configcenter;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.Configuration;
import org.apache.dubbo.common.config.Environment;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import static org.apache.dubbo.common.config.configcenter.DynamicConfigurationFactory.getDynamicConfigurationFactory;
import static org.apache.dubbo.common.extension.ExtensionLoader.getExtensionLoader;

public interface DynamicConfiguration extends Configuration, AutoCloseable {

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

    default boolean publishConfig(String key, String content) throws UnsupportedOperationException {
        return publishConfig(key, DEFAULT_GROUP, content);
    }

    default boolean publishConfig(String key, String group, String content) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("No support");
    }

    default String removeConfig(String key) throws UnsupportedOperationException {
        return removeConfig(key, DEFAULT_GROUP);
    }

    default String removeConfig(String key, String group) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("No support");
    }

    default Set<String> getConfigGroups() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("No support");
    }

    default Set<String> getConfigKeys(String group) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("No support");
    }

    default Map<String, String> getConfigs(String group) throws UnsupportedOperationException {
        return getConfigs(group, -1);
    }

    default Map<String, String> getConfigs(String group, long timeout) throws UnsupportedOperationException, IllegalStateException {
        Map<String, String> configs = new LinkedHashMap<>();
        Set<String> configKeys = getConfigKeys(group);
        configKeys.forEach(key -> configs.put(key, getString(key)));
        return Collections.unmodifiableMap(configs);
    }

    @Override
    default void close() throws Exception {
        throw new UnsupportedOperationException();
    }

    static DynamicConfiguration getDynamicConfiguration() {
        Optional<DynamicConfiguration> optional = Environment.getInstance().getDynamicConfiguration();
        return optional.orElseGet(() -> getExtensionLoader(DynamicConfigurationFactory.class).getDefaultExtension().getDynamicConfiguration(null));
    }

    static DynamicConfiguration getDynamicConfiguration(URL connectionURL) {
        String protocol = connectionURL.getProtocol();
        DynamicConfigurationFactory factory = getDynamicConfigurationFactory(protocol);
        return factory.getDynamicConfiguration(connectionURL);
    }

    static String getRuleKey(URL url) {
        return url.getColonSeparatedKey();
    }
}