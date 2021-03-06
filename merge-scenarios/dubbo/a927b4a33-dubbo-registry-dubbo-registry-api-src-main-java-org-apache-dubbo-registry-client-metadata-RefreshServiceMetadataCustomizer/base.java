package org.apache.dubbo.registry.client.metadata;

import org.apache.dubbo.metadata.WritableMetadataService;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.ServiceInstanceCustomizer;
import org.apache.dubbo.registry.client.ServiceInstanceMetadataCustomizer;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_KEY;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.EXPORTED_SERVICES_REVISION_KEY;

public class RefreshServiceMetadataCustomizer implements ServiceInstanceCustomizer {

    @Override
    public void customize(ServiceInstance serviceInstance) {
        WritableMetadataService remoteWritableMetadataService = WritableMetadataService.getExtension(serviceInstance.getMetadata().getOrDefault("", DEFAULT_KEY));
        remoteWritableMetadataService.refreshMetadata(serviceInstance.getMetadata().get(EXPORTED_SERVICES_REVISION_KEY));
    }
}