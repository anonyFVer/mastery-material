package org.apache.dubbo.config;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.remoting.Constants;
import java.util.Map;
import static org.apache.dubbo.common.constants.CommonConstants.FILE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PASSWORD_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SHUTDOWN_WAIT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.USERNAME_KEY;
import static org.apache.dubbo.config.Constants.REGISTRIES_SUFFIX;
import static org.apache.dubbo.config.Constants.ZOOKEEPER_PROTOCOL;
import static org.apache.dubbo.registry.Constants.EXTRA_KEYS_KEY;

public class RegistryConfig extends AbstractConfig {

    public static final String NO_AVAILABLE = "N/A";

    private static final long serialVersionUID = 5508512956753757169L;

    private String address;

    private String username;

    private String password;

    private Integer port;

    private String protocol;

    private String transporter;

    private String server;

    private String client;

    private String cluster;

    private String group;

    private String version;

    private Integer timeout;

    private Integer session;

    private String file;

    private Integer wait;

    private Boolean check;

    private Boolean dynamic;

    private Boolean register;

    private Boolean subscribe;

    private Map<String, String> parameters;

    private Boolean isDefault;

    private Boolean simplified;

    private String extraKeys;

    public RegistryConfig() {
    }

    public RegistryConfig(String address) {
        setAddress(address);
    }

    public RegistryConfig(String address, String protocol) {
        setAddress(address);
        setProtocol(protocol);
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        checkName(PROTOCOL_KEY, protocol);
        this.protocol = protocol;
        this.updateIdIfAbsent(protocol);
    }

    @Parameter(excluded = true)
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
        if (address != null) {
            try {
                URL url = URL.valueOf(address);
                this.updateIdIfAbsent(url.getProtocol());
                this.updateProtocolIfAbsent(url.getProtocol());
                this.updatePortIfAbsent(url.getPort());
                setUsername(url.getUsername());
                setPassword(url.getPassword());
                setParameters(url.getParameters());
            } catch (Exception ignored) {
            }
        }
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        checkName(USERNAME_KEY, username);
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        checkLength(PASSWORD_KEY, password);
        this.password = password;
    }

    @Deprecated
    public Integer getWait() {
        return wait;
    }

    @Deprecated
    public void setWait(Integer wait) {
        this.wait = wait;
        if (wait != null && wait > 0) {
            System.setProperty(SHUTDOWN_WAIT_KEY, String.valueOf(wait));
        }
    }

    public Boolean isCheck() {
        return check;
    }

    public void setCheck(Boolean check) {
        this.check = check;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        checkPathLength(FILE_KEY, file);
        this.file = file;
    }

    @Deprecated
    @Parameter(excluded = true)
    public String getTransport() {
        return getTransporter();
    }

    @Deprecated
    public void setTransport(String transport) {
        setTransporter(transport);
    }

    public String getTransporter() {
        return transporter;
    }

    public void setTransporter(String transporter) {
        checkName(Constants.TRANSPORTER_KEY, transporter);
        this.transporter = transporter;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        checkName(Constants.SERVER_KEY, server);
        this.server = server;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        checkName(Constants.CLIENT_KEY, client);
        this.client = client;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public Integer getSession() {
        return session;
    }

    public void setSession(Integer session) {
        this.session = session;
    }

    public Boolean isDynamic() {
        return dynamic;
    }

    public void setDynamic(Boolean dynamic) {
        this.dynamic = dynamic;
    }

    public Boolean isRegister() {
        return register;
    }

    public void setRegister(Boolean register) {
        this.register = register;
    }

    public Boolean isSubscribe() {
        return subscribe;
    }

    public void setSubscribe(Boolean subscribe) {
        this.subscribe = subscribe;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        checkParameterName(parameters);
        this.parameters = parameters;
    }

    public Boolean isDefault() {
        return isDefault;
    }

    public void setDefault(Boolean isDefault) {
        this.isDefault = isDefault;
    }

    public Boolean getSimplified() {
        return simplified;
    }

    public void setSimplified(Boolean simplified) {
        this.simplified = simplified;
    }

    @Parameter(key = EXTRA_KEYS_KEY)
    public String getExtraKeys() {
        return extraKeys;
    }

    public void setExtraKeys(String extraKeys) {
        this.extraKeys = extraKeys;
    }

    @Parameter(excluded = true)
    public boolean isZookeeperProtocol() {
        if (!isValid()) {
            return false;
        }
        return ZOOKEEPER_PROTOCOL.equals(getProtocol()) || getAddress().startsWith(ZOOKEEPER_PROTOCOL);
    }

    @Override
    public void refresh() {
        super.refresh();
        if (StringUtils.isNotEmpty(this.getId())) {
            this.setPrefix(REGISTRIES_SUFFIX);
            super.refresh();
        }
    }

    @Override
    @Parameter(excluded = true)
    public boolean isValid() {
        return !StringUtils.isEmpty(address);
    }

    protected void updatePortIfAbsent(Integer value) {
        if (value != null && value > 0 && port == null) {
            this.port = value;
        }
    }

    protected void updateProtocolIfAbsent(String value) {
        if (StringUtils.isNotEmpty(value) && StringUtils.isEmpty(protocol)) {
            this.protocol = value;
        }
    }
}