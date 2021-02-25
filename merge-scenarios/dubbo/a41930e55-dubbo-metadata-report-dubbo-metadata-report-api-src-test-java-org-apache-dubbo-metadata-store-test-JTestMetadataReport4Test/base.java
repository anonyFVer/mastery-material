package org.apache.dubbo.metadata.store.test;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
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

    @Override
    protected void doPut(URL url) {
        store.put(getKey(url), url.toParameterString());
    }

    @Override
    protected URL doPeek(URL url) {
        String queryV = store.get(getKey(url));
        return url.clearParameters().addParameterString(queryV);
    }

    private static String getProtocol(URL url) {
        String protocol = url.getParameter(Constants.SIDE_KEY);
        protocol = protocol == null ? url.getProtocol() : protocol;
        return protocol;
    }

    public static String getKey(URL url) {
        return getProtocol(url) + url.getServiceKey();
    }
}