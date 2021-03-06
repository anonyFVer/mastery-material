package org.apache.dubbo.registry.client.metadata;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.compiler.support.ClassUtils;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.metadata.WritableMetadataService;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.ServiceInstanceMetadataCustomizer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import static java.lang.String.valueOf;
import static java.util.Objects.hash;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.EXPORTED_SERVICES_REVISION_KEY;

public class ExportedServicesRevisionMetadataCustomizer extends ServiceInstanceMetadataCustomizer {

    @Override
    protected String buildMetadataKey(ServiceInstance serviceInstance) {
        return EXPORTED_SERVICES_REVISION_KEY;
    }

    @Override
    protected String buildMetadataValue(ServiceInstance serviceInstance) {
        WritableMetadataService writableMetadataService = WritableMetadataService.getDefaultExtension();
        List<String> exportedURLs = writableMetadataService.getExportedURLs();
        Object[] data = exportedURLs.stream().map(URL::valueOf).map(URL::getServiceInterface).filter(this::isNotMetadataService).map(ClassUtils::forName).map(Class::getMethods).map(Arrays::asList).flatMap(Collection::stream).map(Object::toString).sorted().toArray();
        return valueOf(hash(data));
    }

    private boolean isNotMetadataService(String serviceInterface) {
        return !MetadataService.class.getName().equals(serviceInterface);
    }
}