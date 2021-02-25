package org.apache.dubbo.monitor.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.monitor.Monitor;
import org.apache.dubbo.monitor.MonitorFactory;
import org.apache.dubbo.monitor.MonitorService;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.support.RpcUtils;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.MONITOR_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.monitor.Constants.COUNT_PROTOCOL;
import static org.apache.dubbo.rpc.Constants.INPUT_KEY;
import static org.apache.dubbo.rpc.Constants.OUTPUT_KEY;

@Activate(group = { PROVIDER, CONSUMER })
public class MonitorFilter implements Filter, Filter.Listener {

    private static final Logger logger = LoggerFactory.getLogger(MonitorFilter.class);

    private static final String MONITOR_FILTER_START_TIME = "monitor_filter_start_time";

    private final ConcurrentMap<String, AtomicInteger> concurrents = new ConcurrentHashMap<String, AtomicInteger>();

    private MonitorFactory monitorFactory;

    public void setMonitorFactory(MonitorFactory monitorFactory) {
        this.monitorFactory = monitorFactory;
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        if (invoker.getUrl().hasParameter(MONITOR_KEY)) {
            invocation.setAttachment(MONITOR_FILTER_START_TIME, String.valueOf(System.currentTimeMillis()));
            getConcurrent(invoker, invocation).incrementAndGet();
        }
        return invoker.invoke(invocation);
    }

    private AtomicInteger getConcurrent(Invoker<?> invoker, Invocation invocation) {
        String key = invoker.getInterface().getName() + "." + invocation.getMethodName();
        AtomicInteger concurrent = concurrents.get(key);
        if (concurrent == null) {
            concurrents.putIfAbsent(key, new AtomicInteger());
            concurrent = concurrents.get(key);
        }
        return concurrent;
    }

    @Override
    public void onMessage(Result result, Invoker<?> invoker, Invocation invocation) {
        if (invoker.getUrl().hasParameter(MONITOR_KEY)) {
            collect(invoker, invocation, result, RpcContext.getContext().getRemoteHost(), Long.valueOf((String) invocation.getAttachment(MONITOR_FILTER_START_TIME)), false);
            getConcurrent(invoker, invocation).decrementAndGet();
        }
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        if (invoker.getUrl().hasParameter(MONITOR_KEY)) {
            collect(invoker, invocation, null, RpcContext.getContext().getRemoteHost(), Long.valueOf((String) invocation.getAttachment(MONITOR_FILTER_START_TIME)), true);
            getConcurrent(invoker, invocation).decrementAndGet();
        }
    }

    private void collect(Invoker<?> invoker, Invocation invocation, Result result, String remoteHost, long start, boolean error) {
        try {
            URL monitorUrl = invoker.getUrl().getUrlParameter(MONITOR_KEY);
            Monitor monitor = monitorFactory.getMonitor(monitorUrl);
            if (monitor == null) {
                return;
            }
            URL statisticsURL = createStatisticsUrl(invoker, invocation, result, remoteHost, start, error);
            monitor.collect(statisticsURL);
        } catch (Throwable t) {
            logger.warn("Failed to monitor count service " + invoker.getUrl() + ", cause: " + t.getMessage(), t);
        }
    }

    private URL createStatisticsUrl(Invoker<?> invoker, Invocation invocation, Result result, String remoteHost, long start, boolean error) {
        long elapsed = System.currentTimeMillis() - start;
        int concurrent = getConcurrent(invoker, invocation).get();
        String application = invoker.getUrl().getParameter(APPLICATION_KEY);
        String service = invoker.getInterface().getName();
        String method = RpcUtils.getMethodName(invocation);
        String group = invoker.getUrl().getParameter(GROUP_KEY);
        String version = invoker.getUrl().getParameter(VERSION_KEY);
        int localPort;
        String remoteKey, remoteValue;
        if (CONSUMER_SIDE.equals(invoker.getUrl().getParameter(SIDE_KEY))) {
            localPort = 0;
            remoteKey = MonitorService.PROVIDER;
            remoteValue = invoker.getUrl().getAddress();
        } else {
            localPort = invoker.getUrl().getPort();
            remoteKey = MonitorService.CONSUMER;
            remoteValue = remoteHost;
        }
        String input = "", output = "";
        if (invocation.getAttachment(INPUT_KEY) != null) {
            input = (String) invocation.getAttachment(INPUT_KEY);
        }
        if (result != null && result.getAttachment(OUTPUT_KEY) != null) {
            output = (String) result.getAttachment(OUTPUT_KEY);
        }
        return new URL(COUNT_PROTOCOL, NetUtils.getLocalHost(), localPort, service + PATH_SEPARATOR + method, MonitorService.APPLICATION, application, MonitorService.INTERFACE, service, MonitorService.METHOD, method, remoteKey, remoteValue, error ? MonitorService.FAILURE : MonitorService.SUCCESS, "1", MonitorService.ELAPSED, String.valueOf(elapsed), MonitorService.CONCURRENT, String.valueOf(concurrent), INPUT_KEY, input, OUTPUT_KEY, output, GROUP_KEY, group, VERSION_KEY, version);
    }
}