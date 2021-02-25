package org.apache.dubbo.metadata.store.test;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.metadata.identifier.ConsumerMetadataIdentifier;
import org.apache.dubbo.metadata.identifier.ProviderMetadataIdentifier;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.apache.dubbo.metadata.support.AbstractMetadataReport;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JTestMetadataReport4Test extends AbstractMetadataReport {

    private final static Logger logger = LoggerFactory.getLogger(JTestMetadataReport4Test.class);

    public JTestMetadataReport4Test(URL url, ZookeeperTransporter zookeeperTransporter) {
        super(url);
    }

    public Map<String, String> store = new ConcurrentHashMap<>();

    private static String getProtocol(URL url) {
        String protocol = url.getParameter(Constants.SIDE_KEY);
        protocol = protocol == null ? url.getProtocol() : protocol;
        return protocol;
    }

    @Override
    protected void doStoreProviderMetadata(ProviderMetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
        store.put(providerMetadataIdentifier.getIdentifierKey(), serviceDefinitions);
    }

    @Override
    protected void doStoreConsumerMetadata(ConsumerMetadataIdentifier consumerMetadataIdentifier, String serviceParameterString) {
        store.put(consumerMetadataIdentifier.getIdentifierKey(), serviceParameterString);
    }

    public static String getKey(URL url) {
        return getProtocol(url) + url.getServiceKey();
    }
}