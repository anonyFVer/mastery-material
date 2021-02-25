package org.apache.dubbo.metadata.support;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.metadata.store.MetadataReport;
import org.apache.dubbo.metadata.store.MetadataReportFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractMetadataReportFactory implements MetadataReportFactory {

    private static final ReentrantLock LOCK = new ReentrantLock();

    private static final Map<String, MetadataReport> SERVICE_STORE_MAP = new ConcurrentHashMap<String, MetadataReport>();

    @Override
    public MetadataReport getMetadataReport(URL url) {
        url = url.setPath(MetadataReport.class.getName()).addParameter(Constants.INTERFACE_KEY, MetadataReport.class.getName()).removeParameters(Constants.EXPORT_KEY, Constants.REFER_KEY);
        String key = url.toServiceString();
        LOCK.lock();
        try {
            MetadataReport metadataReport = SERVICE_STORE_MAP.get(key);
            if (metadataReport != null) {
                return metadataReport;
            }
            metadataReport = createMetadataReport(url);
            if (metadataReport == null) {
                throw new IllegalStateException("Can not create metadata Report " + url);
            }
            SERVICE_STORE_MAP.put(key, metadataReport);
            return metadataReport;
        } finally {
            LOCK.unlock();
        }
    }

    protected abstract MetadataReport createMetadataReport(URL url);
}