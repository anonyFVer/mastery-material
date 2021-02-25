package org.apache.dubbo.common.config.configcenter.nop;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import java.util.SortedSet;
import static java.util.Collections.emptySortedSet;

public class NopDynamicConfiguration implements DynamicConfiguration {

    public NopDynamicConfiguration(URL url) {
    }

    @Override
    public Object getInternalProperty(String key) {
        return null;
    }

    @Override
    public void addListener(String key, String group, ConfigurationListener listener) {
    }

    @Override
    public void removeListener(String key, String group, ConfigurationListener listener) {
    }

    @Override
    public String getConfig(String key, String group, long timeout) throws IllegalStateException {
        return null;
    }

    @Override
    public String getConfigs(String key, String group, long timeout) throws IllegalStateException {
        return null;
    }

    @Override
    public boolean publishConfig(String key, String group, String content) {
        return true;
    }

    @Override
    public SortedSet<String> getConfigKeys(String group) {
        return emptySortedSet();
    }
}