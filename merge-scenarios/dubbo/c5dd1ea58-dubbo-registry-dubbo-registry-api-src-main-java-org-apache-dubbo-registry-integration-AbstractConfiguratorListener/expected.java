package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.configurator.parser.ConfigParser;
import java.util.Collections;
import java.util.List;

public abstract class AbstractConfiguratorListener implements ConfigurationListener {

    private static final Logger logger = LoggerFactory.getLogger(AbstractConfiguratorListener.class);

    protected List<Configurator> configurators = Collections.emptyList();

    protected final void initWith(String key) {
        DynamicConfiguration dynamicConfiguration = DynamicConfiguration.getDynamicConfiguration();
        dynamicConfiguration.addListener(key, this);
        String rawConfig = dynamicConfiguration.getRule(key, DynamicConfiguration.DEFAULT_GROUP);
        if (!StringUtils.isEmpty(rawConfig)) {
            genConfiguratorsFromRawRule(rawConfig);
        }
    }

    @Override
    public void process(ConfigChangeEvent event) {
        if (logger.isInfoEnabled()) {
            logger.info("Notification of overriding rule, change type is: " + event.getChangeType() + ", raw config content is:\n " + event.getValue());
        }
        if (event.getChangeType().equals(ConfigChangeType.DELETED)) {
            configurators.clear();
        } else {
            if (!genConfiguratorsFromRawRule(event.getValue())) {
                return;
            }
        }
        notifyOverrides();
    }

    private boolean genConfiguratorsFromRawRule(String rawConfig) {
        boolean parseSuccess = true;
        try {
            configurators = Configurator.toConfigurators(ConfigParser.parseConfigurators(rawConfig)).orElse(configurators);
        } catch (Exception e) {
            logger.error("Failed to parse raw dynamic config and it will not take effect, the raw config is: " + rawConfig, e);
            parseSuccess = false;
        }
        return parseSuccess;
    }

    protected abstract void notifyOverrides();

    public List<Configurator> getConfigurators() {
        return configurators;
    }

    public void setConfigurators(List<Configurator> configurators) {
        this.configurators = configurators;
    }
}