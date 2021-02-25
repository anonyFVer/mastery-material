package org.apache.dubbo.metadata.store.zookeeper;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.metadata.identifier.ConsumerMetadataIdentifier;
import org.apache.dubbo.metadata.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.identifier.ProviderMetadataIdentifier;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.metadata.support.AbstractMetadataReport;
import java.util.ArrayList;
import java.util.List;

public class ZookeeperMetadataReport extends AbstractMetadataReport {

    private final static Logger logger = LoggerFactory.getLogger(ZookeeperMetadataReport.class);

    private final static String DEFAULT_ROOT = "dubbo";

    private final String root;

    final ZookeeperClient zkClient;

    public ZookeeperMetadataReport(URL url, ZookeeperTransporter zookeeperTransporter) {
        super(url);
        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        String group = url.getParameter(Constants.GROUP_KEY, DEFAULT_ROOT);
        if (!group.startsWith(Constants.PATH_SEPARATOR)) {
            group = Constants.PATH_SEPARATOR + group;
        }
        this.root = group;
        zkClient = zookeeperTransporter.connect(url);
    }

    void deletePath(String category) {
        List<String> urlStrs = zkClient.getChildren(category);
        if (CollectionUtils.isEmpty(urlStrs)) {
            return;
        }
        for (String urlStr : urlStrs) {
            zkClient.delete(category + Constants.PATH_SEPARATOR + urlStr);
        }
    }

    String toRootDir() {
        if (root.equals(Constants.PATH_SEPARATOR)) {
            return root;
        }
        return root + Constants.PATH_SEPARATOR;
    }

    @Override
    protected void doStoreProviderMetadata(ProviderMetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
        storeMetadata(providerMetadataIdentifier, serviceDefinitions);
    }

    @Override
    protected void doStoreConsumerMetadata(ConsumerMetadataIdentifier consumerMetadataIdentifier, String value) {
        storeMetadata(consumerMetadataIdentifier, value);
    }

    private void storeMetadata(MetadataIdentifier metadataIdentifier, String v) {
        String category = getCategory(metadataIdentifier);
        String filePath = category + Constants.PATH_SEPARATOR + URL.encode(v);
        deletePath(category);
        zkClient.create(filePath, false);
    }

    String getCategory(MetadataIdentifier metadataIdentifier) {
        return toRootDir() + metadataIdentifier.getFilePathKey();
    }
}