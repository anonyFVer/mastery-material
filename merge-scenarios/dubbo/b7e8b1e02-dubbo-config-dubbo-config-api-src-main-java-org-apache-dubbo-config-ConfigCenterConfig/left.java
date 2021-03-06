package org.apache.dubbo.config;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.context.Environment;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.governance.DynamicConfiguration;
import org.apache.dubbo.governance.DynamicConfigurationFactory;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigCenterConfig extends AbstractConfig {

    private String type;

    private String address;

    private String env;

    private String cluster;

    private String namespace = "dubbo";

    private String group = "dubbo";

    private String username;

    private String password;

    private Long timeout = 3000L;

    private Boolean priority = true;

    private Boolean check = true;

    private String appname;

    private String configfile = "dubbo.properties";

    private String localconfigfile;

    private ApplicationConfig application;

    private Map<String, String> parameters;

    public ConfigCenterConfig() {
    }

    private URL toConfigUrl() {
        String host = address;
        int port = 0;
        try {
            if (StringUtils.isNotEmpty(address)) {
                String[] addrs = address.split(":");
                if (addrs.length == 2) {
                    host = addrs[0];
                    port = Integer.parseInt(addrs[1]);
                }
            }
        } catch (Exception e) {
            throw e;
        }
        Map<String, String> map = this.getMetaData();
        return new URL(Constants.CONFIG_PROTOCOL, username, password, host, port, ConfigCenterConfig.class.getSimpleName(), map);
    }

    public void init() {
        refresh();
        URL url = toConfigUrl();
        DynamicConfiguration dynamicConfiguration = ExtensionLoader.getExtensionLoader(DynamicConfigurationFactory.class).getAdaptiveExtension().getDynamicConfiguration(url);
        Environment.getInstance().setDynamicConfiguration(dynamicConfiguration);
        String configContent = dynamicConfiguration.getConfig(configfile, group);
        String appConfigContent = dynamicConfiguration.getConfig(StringUtils.isNotEmpty(localconfigfile) ? localconfigfile : configfile, getApplicationName());
        try {
            Environment.getInstance().setConfigCenterFirst(priority);
            Environment.getInstance().updateExternalConfigurationMap(parseProperties(configContent));
            Environment.getInstance().updateAppExternalConfigurationMap(parseProperties(appConfigContent));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse configurations from Config Center.", e);
        }
    }

    private String getApplicationName() {
        if (application != null) {
            if (!application.isValid()) {
                throw new IllegalStateException("No application config found or it's not a valid config! Please add <dubbo:application name=\"...\" /> to your spring config.");
            }
            return application.getName();
        }
        return appname;
    }

    private Map<String, String> parseProperties(String content) throws IOException {
        Map<String, String> map = new HashMap<>();
        if (content == null) {
            logger.warn("You specified the config centre, but there's not even one single config item in it.");
        } else {
            Properties properties = new Properties();
            properties.load(new StringReader(content));
            properties.stringPropertyNames().forEach(k -> map.put(k, properties.getProperty(k)));
        }
        return map;
    }

    @Parameter(key = Constants.CONFIG_TYPE_KEY)
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Parameter(key = Constants.CONFIG_ADDRESS_KEY)
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Parameter(key = Constants.CONFIG_ENV_KEY)
    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    @Parameter(key = Constants.CONFIG_CLUSTER_KEY)
    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    @Parameter(key = Constants.CONFIG_NAMESPACE_KEY)
    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    @Parameter(key = Constants.CONFIG_GROUP_KEY)
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @Parameter(key = Constants.CONFIG_CHECK_KEY)
    public Boolean isCheck() {
        return check;
    }

    public void setCheck(Boolean check) {
        this.check = check;
    }

    @Parameter(key = Constants.CONFIG_PRIORITY_KEY)
    public Boolean isPriority() {
        return priority;
    }

    public void setPriority(Boolean priority) {
        this.priority = priority;
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

    @Parameter(key = Constants.CONFIG_TIMEOUT_KEY)
    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    @Parameter(key = Constants.CONFIG_CONFIGFILE_KEY)
    public String getConfigfile() {
        return configfile;
    }

    public void setConfigfile(String configfile) {
        this.configfile = configfile;
    }

    @Parameter(excluded = true)
    public String getLocalconfigfile() {
        return localconfigfile;
    }

    public void setLocalconfigfile(String localconfigfile) {
        this.localconfigfile = localconfigfile;
    }

    @Parameter(key = Constants.CONFIG_APPNAME_KEY)
    public String getAppname() {
        return appname;
    }

    public void setAppname(String appname) {
        this.appname = appname;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        checkParameterName(parameters);
        this.parameters = parameters;
    }

    public ApplicationConfig getApplication() {
        return application;
    }

    public void setApplication(ApplicationConfig application) {
        this.application = application;
    }
}