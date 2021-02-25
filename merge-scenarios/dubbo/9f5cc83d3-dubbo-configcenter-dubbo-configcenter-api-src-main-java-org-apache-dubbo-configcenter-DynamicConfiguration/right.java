package org.apache.dubbo.configcenter;

import org.apache.dubbo.common.config.Configuration;
import org.apache.dubbo.common.config.Environment;
import java.util.Optional;
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

    default String getConfig(String key) {
        return getConfig(key, null, 0L);
    }

    default String getConfig(String key, String group) {
        return getConfig(key, group, 0L);
    }

    String getConfig(String key, String group, long timeout) throws IllegalStateException;

    static DynamicConfiguration getDynamicConfiguration() {
        Optional<Configuration> optional = Environment.getInstance().getDynamicConfiguration();
        return (DynamicConfiguration) optional.orElseGet(() -> getExtensionLoader(DynamicConfigurationFactory.class).getDefaultExtension().getDynamicConfiguration(null));
    }
}