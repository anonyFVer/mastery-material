package org.apache.dubbo.config;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.support.Parameter;
import java.util.Map;

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

    private Boolean simple;

    private String addParamKeys;

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
        checkName(Constants.PROTOCOL_KEY, protocol);
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
            int i = address.indexOf("://");
            if (i > 0) {
                this.updateIdIfAbsent(address.substring(0, i));
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
        checkName("username", username);
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        checkLength("password", password);
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
            System.setProperty(Constants.SHUTDOWN_WAIT_KEY, String.valueOf(wait));
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
        checkPathLength("file", file);
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
        checkName("client", client);
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

    @Parameter(excluded = true)
    public boolean isZookeeperProtocol() {
        if (!isValid()) {
            return false;
        }
        return Constants.ZOOKEEPER_PROTOCOL.equals(getProtocol()) || getAddress().startsWith(Constants.ZOOKEEPER_PROTOCOL);
    }

    @Override
    @Parameter(excluded = true)
    public boolean isValid() {
        return !StringUtils.isEmpty(address);
    }
}