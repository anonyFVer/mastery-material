package org.apache.dubbo.registry.client.metadata;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.metadata.WritableMetadataService;
import org.apache.dubbo.registry.client.ServiceInstance;
import com.alibaba.fastjson.JSON;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static java.lang.String.valueOf;
import static java.util.Collections.emptyMap;
import static org.apache.dubbo.common.constants.CommonConstants.METADATA_DEFAULT;
import static org.apache.dubbo.common.utils.StringUtils.isBlank;
import static org.apache.dubbo.registry.integration.RegistryProtocol.DEFAULT_REGISTER_PROVIDER_KEYS;

public class ServiceInstanceMetadataUtils {

    public static final String METADATA_SERVICE_PREFIX = "dubbo.metadata-service.";

    public static String METADATA_SERVICE_URL_PARAMS_PROPERTY_NAME = METADATA_SERVICE_PREFIX + "url-params";

    public static final String METADATA_SERVICE_URLS_PROPERTY_NAME = METADATA_SERVICE_PREFIX + "urls";

    public static String EXPORTED_SERVICES_REVISION_PROPERTY_NAME = "dubbo.exported-services.revision";

    public static String SUBSCRIBER_SERVICES_REVISION_PROPERTY_NAME = "dubbo.subscribed-services.revision";

    public static String METADATA_STORAGE_TYPE_PROPERTY_NAME = "dubbo.metadata.storage-type";

    public static final String PROVIDER_HOST_PROPERTY_NAME = "provider.host";

    public static final String PROVIDER_PORT_PROPERTY_NAME = "provider.port";

    public static Map<String, Map<String, Object>> getMetadataServiceURLsParams(ServiceInstance serviceInstance) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        String param = metadata.get(METADATA_SERVICE_URL_PARAMS_PROPERTY_NAME);
        return isBlank(param) ? emptyMap() : (Map) JSON.parse(param);
    }

    public static Map<String, Object> getMetadataServiceURLParams(ServiceInstance serviceInstance, String protocol) {
        Map<String, Map<String, Object>> params = getMetadataServiceURLsParams(serviceInstance);
        return params.getOrDefault(protocol, emptyMap());
    }

    public static Integer getProviderPort(ServiceInstance serviceInstance, String protocol) {
        Map<String, Object> params = getMetadataServiceURLParams(serviceInstance, protocol);
        return getProviderPort(params);
    }

    public static String getProviderHost(ServiceInstance serviceInstance, String protocol) {
        Map<String, Object> params = getMetadataServiceURLParams(serviceInstance, protocol);
        return getProviderHost(params);
    }

    public static String getMetadataServiceParameter(List<URL> urls) {
        Map<String, Map<String, String>> params = new HashMap<>();
        urls.forEach(url -> {
            String protocol = url.getProtocol();
            params.put(protocol, getParams(url));
        });
        if (params.isEmpty()) {
            return null;
        }
        return JSON.toJSONString(params);
    }

    private static Map<String, String> getParams(URL providerURL) {
        Map<String, String> params = new LinkedHashMap<>();
        setDefaultParams(params, providerURL);
        setProviderHostParam(params, providerURL);
        setProviderPortParam(params, providerURL);
        return params;
    }

    public static String getProviderHost(Map<String, Object> params) {
        return valueOf(params.get(PROVIDER_HOST_PROPERTY_NAME));
    }

    public static Integer getProviderPort(Map<String, Object> params) {
        return Integer.valueOf(valueOf(params.get(PROVIDER_PORT_PROPERTY_NAME)));
    }

    public static String getExportedServicesRevision(ServiceInstance serviceInstance) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        return metadata.get(EXPORTED_SERVICES_REVISION_PROPERTY_NAME);
    }

    public static String getSubscribedServicesRevision(ServiceInstance serviceInstance) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        return metadata.get(SUBSCRIBER_SERVICES_REVISION_PROPERTY_NAME);
    }

    public static String getMetadataStorageType(URL registryURL) {
        return registryURL.getParameter(METADATA_STORAGE_TYPE_PROPERTY_NAME, METADATA_DEFAULT);
    }

    public static String getMetadataStorageType(ServiceInstance serviceInstance) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        return metadata.getOrDefault(METADATA_STORAGE_TYPE_PROPERTY_NAME, METADATA_DEFAULT);
    }

    public static void setMetadataStorageType(ServiceInstance serviceInstance, String metadataType) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        metadata.put(METADATA_STORAGE_TYPE_PROPERTY_NAME, metadataType);
    }

    public static boolean isDubboServiceInstance(ServiceInstance serviceInstance) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        return metadata.containsKey(METADATA_SERVICE_URL_PARAMS_PROPERTY_NAME) || metadata.containsKey(METADATA_SERVICE_URLS_PROPERTY_NAME);
    }

    private static void setProviderHostParam(Map<String, String> params, URL providerURL) {
        params.put(PROVIDER_HOST_PROPERTY_NAME, providerURL.getHost());
    }

    private static void setProviderPortParam(Map<String, String> params, URL providerURL) {
        params.put(PROVIDER_PORT_PROPERTY_NAME, valueOf(providerURL.getPort()));
    }

    private static void setDefaultParams(Map<String, String> params, URL providerURL) {
        for (String parameterName : DEFAULT_REGISTER_PROVIDER_KEYS) {
            String parameterValue = providerURL.getParameter(parameterName);
            if (!isBlank(parameterValue)) {
                params.put(parameterName, parameterValue);
            }
        }
    }
}