package org.apache.dubbo.registry.client.metadata;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.metadata.WritableMetadataService;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.rpc.Protocol;
import com.alibaba.fastjson.JSON;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static java.util.Collections.emptyMap;
import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.utils.StringUtils.isBlank;
import static org.apache.dubbo.metadata.WritableMetadataService.DEFAULT_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.registry.integration.RegistryProtocol.DEFAULT_REGISTER_PROVIDER_KEYS;
import static org.apache.dubbo.rpc.Constants.DEPRECATED_KEY;

public class ServiceInstanceMetadataUtils {

    public static final String METADATA_SERVICE_PREFIX = "dubbo.metadata-service.";

    public static final String DUBBO_PROTOCOLS_PREFIX = "dubbo.protocols.";

    public static final String DUBBO_PORT_SUFFIX = ".port";

    public static String METADATA_SERVICE_URL_PARAMS_PROPERTY_NAME = METADATA_SERVICE_PREFIX + "url-params";

    public static final String METADATA_SERVICE_URLS_PROPERTY_NAME = METADATA_SERVICE_PREFIX + "urls";

    public static String EXPORTED_SERVICES_REVISION_PROPERTY_NAME = "dubbo.exported-services.revision";

    public static String SUBSCRIBER_SERVICES_REVISION_PROPERTY_NAME = "dubbo.subscribed-services.revision";

    public static String METADATA_STORAGE_TYPE_PROPERTY_NAME = "dubbo.metadata.storage-type";

    public static Map<String, Map<String, Object>> getMetadataServiceURLsParams(ServiceInstance serviceInstance) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        String param = metadata.get(METADATA_SERVICE_URL_PARAMS_PROPERTY_NAME);
        return isBlank(param) ? emptyMap() : (Map) JSON.parse(param);
    }

    public static Map<String, Object> getMetadataServiceURLParams(ServiceInstance serviceInstance, String protocol) {
        Map<String, Map<String, Object>> params = getMetadataServiceURLsParams(serviceInstance);
        return params.getOrDefault(protocol, emptyMap());
    }

    public static String getMetadataServiceParameter(List<URL> urls) {
        Map<String, Map<String, String>> params = new HashMap<>();
        urls.stream().map(url -> url.removeParameter(APPLICATION_KEY)).map(url -> url.removeParameter(GROUP_KEY)).map(url -> url.removeParameter(DEPRECATED_KEY)).map(url -> url.removeParameter(TIMESTAMP_KEY)).forEach(url -> {
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
        return params;
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
        return registryURL.getParameter(METADATA_STORAGE_TYPE_PROPERTY_NAME, DEFAULT_METADATA_STORAGE_TYPE);
    }

    public static String getMetadataStorageType(ServiceInstance serviceInstance) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        return metadata.getOrDefault(METADATA_STORAGE_TYPE_PROPERTY_NAME, DEFAULT_METADATA_STORAGE_TYPE);
    }

    public static void setMetadataStorageType(ServiceInstance serviceInstance, String metadataType) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        metadata.put(METADATA_STORAGE_TYPE_PROPERTY_NAME, metadataType);
    }

    public static boolean isDubboServiceInstance(ServiceInstance serviceInstance) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        return metadata.containsKey(METADATA_SERVICE_URL_PARAMS_PROPERTY_NAME) || metadata.containsKey(METADATA_SERVICE_URLS_PROPERTY_NAME);
    }

    public static String createProtocolPortPropertyName(String protocol) {
        return DUBBO_PROTOCOLS_PREFIX + protocol + DUBBO_PORT_SUFFIX;
    }

    public static void setProtocolPort(ServiceInstance serviceInstance, String protocol, int port) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        String propertyName = createProtocolPortPropertyName(protocol);
        metadata.put(propertyName, Integer.toString(port));
    }

    public static Integer getProtocolPort(ServiceInstance serviceInstance, String protocol) {
        Map<String, String> metadata = serviceInstance.getMetadata();
        String propertyName = createProtocolPortPropertyName(protocol);
        String propertyValue = metadata.get(propertyName);
        return propertyValue == null ? null : Integer.valueOf(propertyValue);
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