package org.apache.dubbo.config;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.config.support.Parameter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.apache.dubbo.common.config.configcenter.Constants.CONFIG_CHECK_KEY;
import static org.apache.dubbo.common.config.configcenter.Constants.CONFIG_CLUSTER_KEY;
import static org.apache.dubbo.common.config.configcenter.Constants.CONFIG_GROUP_KEY;
import static org.apache.dubbo.common.config.configcenter.Constants.CONFIG_NAMESPACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.config.Constants.CONFIG_CONFIGFILE_KEY;
import static org.apache.dubbo.config.Constants.CONFIG_ENABLE_KEY;
import static org.apache.dubbo.config.Constants.CONFIG_TIMEOUT_KEY;
import static org.apache.dubbo.config.Constants.ZOOKEEPER_PROTOCOL;

public class ConfigCenterConfig extends AbstractConfig {

    private AtomicBoolean inited = new AtomicBoolean(false);

    private String protocol;

    private String address;

    private String cluster;

    private String namespace = CommonConstants.DUBBO;

    private String group = CommonConstants.DUBBO;

    private String username;

    private String password;

    private Long timeout = 3000L;

    private Boolean highestPriority = true;

    private Boolean check = true;

    private String configFile = CommonConstants.DEFAULT_DUBBO_PROPERTIES;

    private String appConfigFile;

    private Map<String, String> parameters;

    public ConfigCenterConfig() {
    }

    public URL toUrl() {
        Map<String, String> map = this.getMetaData();
        if (StringUtils.isEmpty(address)) {
            address = ANYHOST_VALUE;
        }
        map.put(PATH_KEY, ConfigCenterConfig.class.getSimpleName());
        if (StringUtils.isEmpty(map.get(PROTOCOL_KEY))) {
            map.put(PROTOCOL_KEY, ZOOKEEPER_PROTOCOL);
        }
        return UrlUtils.parseURL(address, map);
    }

    public boolean checkOrUpdateInited() {
        return inited.compareAndSet(false, true);
    }

    public void setExternalConfig(Map<String, String> externalConfiguration) {
        Environment.getInstance().setExternalConfigMap(externalConfiguration);
    }

    public void setAppExternalConfig(Map<String, String> appExternalConfiguration) {
        Environment.getInstance().setAppExternalConfigMap(appExternalConfiguration);
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    @Parameter(excluded = true)
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Parameter(key = CONFIG_CLUSTER_KEY, useKeyAsProperty = false)
    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    @Parameter(key = CONFIG_NAMESPACE_KEY, useKeyAsProperty = false)
    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    @Parameter(key = CONFIG_GROUP_KEY, useKeyAsProperty = false)
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @Parameter(key = CONFIG_CHECK_KEY, useKeyAsProperty = false)
    public Boolean isCheck() {
        return check;
    }

    public void setCheck(Boolean check) {
        this.check = check;
    }

    @Parameter(key = CONFIG_ENABLE_KEY, useKeyAsProperty = false)
    public Boolean isHighestPriority() {
        return highestPriority;
    }

    public void setHighestPriority(Boolean highestPriority) {
        this.highestPriority = highestPriority;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Parameter(key = CONFIG_TIMEOUT_KEY, useKeyAsProperty = false)
    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    @Parameter(key = CONFIG_CONFIGFILE_KEY, useKeyAsProperty = false)
    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    @Parameter(excluded = true)
    public String getAppConfigFile() {
        return appConfigFile;
    }

    public void setAppConfigFile(String appConfigFile) {
        this.appConfigFile = appConfigFile;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        checkParameterName(parameters);
        this.parameters = parameters;
    }

    @Override
    @Parameter(excluded = true)
    public boolean isValid() {
        if (StringUtils.isEmpty(address)) {
            return false;
        }
        return address.contains("://") || StringUtils.isNotEmpty(protocol);
    }
}