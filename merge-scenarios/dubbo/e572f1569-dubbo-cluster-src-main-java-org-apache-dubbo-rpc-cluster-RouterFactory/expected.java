package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.SPI;
import org.apache.dubbo.config.dynamic.DynamicConfiguration;
import org.apache.dubbo.rpc.Invocation;

@SPI
public interface RouterFactory {

    @Adaptive("protocol")
    Router getRouter(URL url);

    default Router getRouter(DynamicConfiguration dynamicConfiguration) {
        return null;
    }
}