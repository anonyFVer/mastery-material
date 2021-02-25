package org.apache.dubbo.rpc.protocol.dubbo.status;

import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.status.Status;
import org.apache.dubbo.common.status.StatusChecker;
import org.apache.dubbo.common.store.DataStore;
import org.apache.dubbo.remoting.Constants;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

@Activate
public class ThreadPoolStatusChecker implements StatusChecker {

    @Override
    public Status check() {
        DataStore dataStore = ExtensionLoader.getExtensionLoader(DataStore.class).getDefaultExtension();
        Map<String, Object> executors = dataStore.get(Constants.EXECUTOR_SERVICE_COMPONENT_KEY);
        StringBuilder msg = new StringBuilder();
        Status.Level level = Status.Level.OK;
        for (Map.Entry<String, Object> entry : executors.entrySet()) {
            String port = entry.getKey();
            ExecutorService executor = (ExecutorService) entry.getValue();
            if (executor instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor tp = (ThreadPoolExecutor) executor;
                boolean ok = tp.getActiveCount() < tp.getMaximumPoolSize() - 1;
                Status.Level lvl = Status.Level.OK;
                if (!ok) {
                    level = Status.Level.WARN;
                    lvl = Status.Level.WARN;
                }
                if (msg.length() > 0) {
                    msg.append(";");
                }
                msg.append("Pool status:").append(lvl).append(", max:").append(tp.getMaximumPoolSize()).append(", core:").append(tp.getCorePoolSize()).append(", largest:").append(tp.getLargestPoolSize()).append(", active:").append(tp.getActiveCount()).append(", task:").append(tp.getTaskCount()).append(", service port: ").append(port);
            }
        }
        return msg.length() == 0 ? new Status(Status.Level.UNKNOWN) : new Status(level, msg.toString());
    }
}