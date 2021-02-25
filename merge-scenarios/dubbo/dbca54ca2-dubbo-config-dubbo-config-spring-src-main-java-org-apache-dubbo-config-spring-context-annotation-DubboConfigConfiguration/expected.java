package org.apache.dubbo.config.spring.context.annotation;

import org.apache.dubbo.config.AbstractConfig;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ConsumerConfig;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.config.ModuleConfig;
import org.apache.dubbo.config.MonitorConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.RegistryDataConfig;
import org.apache.dubbo.config.spring.ConfigCenterBean;
import org.springframework.context.annotation.Configuration;

public class DubboConfigConfiguration {

    @EnableDubboConfigBindings({ @EnableDubboConfigBinding(prefix = "dubbo.application", type = ApplicationConfig.class), @EnableDubboConfigBinding(prefix = "dubbo.module", type = ModuleConfig.class), @EnableDubboConfigBinding(prefix = "dubbo.registry", type = RegistryConfig.class), @EnableDubboConfigBinding(prefix = "dubbo.protocol", type = ProtocolConfig.class), @EnableDubboConfigBinding(prefix = "dubbo.monitor", type = MonitorConfig.class), @EnableDubboConfigBinding(prefix = "dubbo.provider", type = ProviderConfig.class), @EnableDubboConfigBinding(prefix = "dubbo.consumer", type = ConsumerConfig.class), @EnableDubboConfigBinding(prefix = "dubbo.configcenter", type = ConfigCenterBean.class), @EnableDubboConfigBinding(prefix = "dubbo.registrydata", type = RegistryDataConfig.class), @EnableDubboConfigBinding(prefix = "dubbo.metadatareport", type = MetadataReportConfig.class) })
    public static class Single {
    }

    @EnableDubboConfigBindings({ @EnableDubboConfigBinding(prefix = "dubbo.applications", type = ApplicationConfig.class, multiple = true), @EnableDubboConfigBinding(prefix = "dubbo.modules", type = ModuleConfig.class, multiple = true), @EnableDubboConfigBinding(prefix = "dubbo.registries", type = RegistryConfig.class, multiple = true), @EnableDubboConfigBinding(prefix = "dubbo.protocols", type = ProtocolConfig.class, multiple = true), @EnableDubboConfigBinding(prefix = "dubbo.monitors", type = MonitorConfig.class, multiple = true), @EnableDubboConfigBinding(prefix = "dubbo.providers", type = ProviderConfig.class, multiple = true), @EnableDubboConfigBinding(prefix = "dubbo.consumers", type = ConsumerConfig.class, multiple = true) })
    public static class Multiple {
    }
}