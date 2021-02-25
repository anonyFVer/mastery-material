package org.apache.dubbo.registry.client.metadata;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.metadata.WritableMetadataService;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.ServiceInstanceMetadataCustomizer;
import java.util.List;
import static org.apache.dubbo.metadata.MetadataService.toURLs;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.METADATA_SERVICE_URL_PARAMS_KEY;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.getMetadataServiceParameter;

public class MetadataServiceURLParamsMetadataCustomizer extends ServiceInstanceMetadataCustomizer {

    @Override
    public String buildMetadataKey(ServiceInstance serviceInstance) {
        return METADATA_SERVICE_URL_PARAMS_KEY;
    }

    @Override
    public String buildMetadataValue(ServiceInstance serviceInstance) {
        WritableMetadataService writableMetadataService = WritableMetadataService.getDefaultExtension();
        String serviceInterface = MetadataService.class.getName();
        String group = serviceInstance.getServiceName();
        String version = MetadataService.VERSION;
        List<String> urls = writableMetadataService.getExportedURLs(serviceInterface, group, version);
        return getMetadataServiceParameter(toURLs(urls));
    }
}