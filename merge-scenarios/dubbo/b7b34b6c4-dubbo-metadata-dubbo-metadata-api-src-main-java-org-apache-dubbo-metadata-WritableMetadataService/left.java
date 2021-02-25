package org.apache.dubbo.metadata;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.extension.SPI;
import org.apache.dubbo.metadata.store.InMemoryWritableMetadataService;
import org.apache.dubbo.rpc.model.ApplicationModel;
import static org.apache.dubbo.common.extension.ExtensionLoader.getExtensionLoader;

@SPI("local")
public interface WritableMetadataService extends MetadataService {

    @Override
    default String serviceName() {
        return ApplicationModel.getApplication();
    }

    boolean exportURL(URL url);

    boolean unexportURL(URL url);

    boolean subscribeURL(URL url);

    boolean unsubscribeURL(URL url);

    void publishServiceDefinition(URL providerUrl);

    static WritableMetadataService getDefaultExtension() {
        return getExtensionLoader(WritableMetadataService.class).getDefaultExtension();
    }

    static WritableMetadataService getExtension(String name) {
        return getExtensionLoader(WritableMetadataService.class).getExtension(name);
    }
}