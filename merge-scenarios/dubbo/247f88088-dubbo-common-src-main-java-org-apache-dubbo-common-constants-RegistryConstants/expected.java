package org.apache.dubbo.common.constants;

public interface RegistryConstants {

    String REGISTRY_KEY = "registry";

    String REGISTRY_PROTOCOL = "registry";

    String DYNAMIC_KEY = "dynamic";

    String CATEGORY_KEY = "category";

    String PROVIDERS_CATEGORY = "providers";

    String CONSUMERS_CATEGORY = "consumers";

    String ROUTERS_CATEGORY = "routers";

    String DYNAMIC_ROUTERS_CATEGORY = "dynamicrouters";

    String DEFAULT_CATEGORY = PROVIDERS_CATEGORY;

    String CONFIGURATORS_CATEGORY = "configurators";

    String DYNAMIC_CONFIGURATORS_CATEGORY = "dynamicconfigurators";

    String APP_DYNAMIC_CONFIGURATORS_CATEGORY = "appdynamicconfigurators";

    String ROUTERS_SUFFIX = ".routers";

    String EMPTY_PROTOCOL = "empty";

    String ROUTE_PROTOCOL = "route";

    String OVERRIDE_PROTOCOL = "override";

    String COMPATIBLE_CONFIG_KEY = "compatible_config";

    String REGISTRY_TYPE_KEY = "registry-type";

    String SERVICE_REGISTRY_TYPE = "service";

    String SERVICE_REGISTRY_PROTOCOL = "service-discovery-registry";

    String SUBSCRIBED_SERVICE_NAMES_KEY = "subscribed-services";

    String INSTANCES_REQUEST_SIZE_KEY = "instances-request-size";

    int DEFAULT_INSTANCES_REQUEST_SIZE = 100;

    String ACCEPTS_KEY = "accepts";

    String REGISTRY_ZONE = "registry_zone";

    String REGISTRY_ZONE_FORCE = "registry_zone_force";

    String ZONE_KEY = "zone";
}