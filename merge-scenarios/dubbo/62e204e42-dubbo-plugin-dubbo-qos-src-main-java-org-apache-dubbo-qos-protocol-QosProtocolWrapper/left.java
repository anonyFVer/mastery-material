package org.apache.dubbo.qos.protocol;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.qos.common.QosConstants;
import org.apache.dubbo.qos.server.Server;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.RpcException;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.apache.dubbo.common.constants.QosConstants.ACCEPT_FOREIGN_IP;
import static org.apache.dubbo.common.constants.QosConstants.QOS_ENABLE;
import static org.apache.dubbo.common.constants.QosConstants.QOS_HOST;
import static org.apache.dubbo.common.constants.QosConstants.QOS_PORT;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_PROTOCOL;

public class QosProtocolWrapper implements Protocol {

    private final Logger logger = LoggerFactory.getLogger(QosProtocolWrapper.class);

    private static AtomicBoolean hasStarted = new AtomicBoolean(false);

    private Protocol protocol;

    public QosProtocolWrapper(Protocol protocol) {
        if (protocol == null) {
            throw new IllegalArgumentException("protocol == null");
        }
        this.protocol = protocol;
    }

    @Override
    public int getDefaultPort() {
        return protocol.getDefaultPort();
    }

    @Override
    public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
        if (REGISTRY_PROTOCOL.equals(invoker.getUrl().getProtocol())) {
            startQosServer(invoker.getUrl());
            return protocol.export(invoker);
        }
        return protocol.export(invoker);
    }

    @Override
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        if (REGISTRY_PROTOCOL.equals(url.getProtocol())) {
            startQosServer(url);
            return protocol.refer(type, url);
        }
        return protocol.refer(type, url);
    }

    @Override
    public void destroy() {
        protocol.destroy();
        stopServer();
    }

    private void startQosServer(URL url) {
        try {
            if (!hasStarted.compareAndSet(false, true)) {
                return;
            }
            boolean qosEnable = url.getParameter(QOS_ENABLE, true);
            if (!qosEnable) {
                logger.info("qos won't be started because it is disabled. " + "Please check dubbo.application.qos.enable is configured either in system property, " + "dubbo.properties or XML/spring-boot configuration.");
                return;
            }
            String host = url.getParameter(QOS_HOST);
            int port = url.getParameter(QOS_PORT, QosConstants.DEFAULT_PORT);
            boolean acceptForeignIp = Boolean.parseBoolean(url.getParameter(ACCEPT_FOREIGN_IP, "false"));
            Server server = Server.getInstance();
            server.setHost(host);
            server.setPort(port);
            server.setAcceptForeignIp(acceptForeignIp);
            server.start();
        } catch (Throwable throwable) {
            logger.warn("Fail to start qos server: ", throwable);
        }
    }

    void stopServer() {
        if (hasStarted.compareAndSet(true, false)) {
            Server server = Server.getInstance();
            server.stop();
        }
    }
}