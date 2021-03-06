package org.apache.dubbo.monitor.dubbo;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.monitor.Monitor;
import org.apache.dubbo.monitor.MonitorService;
import org.apache.dubbo.monitor.support.AbstractMonitorFactory;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.model.ApplicationModel;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_PROTOCOL;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.remoting.Constants.CHECK_KEY;
import static org.apache.dubbo.rpc.Constants.REFERENCE_FILTER_KEY;

public class DubboMonitorFactory extends AbstractMonitorFactory {

    private Protocol protocol;

    private ProxyFactory proxyFactory;

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    @Override
    protected Monitor createMonitor(URL url) {
        URLBuilder urlBuilder = URLBuilder.from(url);
        urlBuilder.setProtocol(url.getParameter(PROTOCOL_KEY, DUBBO_PROTOCOL));
        if (StringUtils.isEmpty(url.getPath())) {
            urlBuilder.setPath(MonitorService.class.getName());
        }
        String filter = url.getParameter(REFERENCE_FILTER_KEY);
        if (StringUtils.isEmpty(filter)) {
            filter = "";
        } else {
            filter = filter + ",";
        }
        urlBuilder.addParameters(CHECK_KEY, String.valueOf(false), REFERENCE_FILTER_KEY, filter + "-monitor");
        ApplicationModel.registerServiceModel(MonitorService.class);
        Invoker<MonitorService> monitorInvoker = protocol.refer(MonitorService.class, urlBuilder.build());
        MonitorService monitorService = proxyFactory.getProxy(monitorInvoker);
        return new DubboMonitor(monitorInvoker, monitorService);
    }
}