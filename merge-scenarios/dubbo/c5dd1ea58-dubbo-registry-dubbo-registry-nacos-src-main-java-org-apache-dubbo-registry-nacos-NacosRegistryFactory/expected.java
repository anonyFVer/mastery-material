package org.apache.dubbo.registry.nacos;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.RegistryFactory;
import org.apache.dubbo.registry.support.AbstractRegistryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.dubbo.registry.nacos.util.NacosNamingServiceUtils.createNamingService;

public class NacosRegistryFactory extends AbstractRegistryFactory {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected Registry createRegistry(URL url) {
        return new NacosRegistry(url, createNamingService(url));
    }
}