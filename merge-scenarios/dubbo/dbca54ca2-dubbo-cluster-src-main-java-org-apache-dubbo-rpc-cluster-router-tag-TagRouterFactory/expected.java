package org.apache.dubbo.rpc.cluster.router.tag;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.governance.DynamicConfiguration;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.RouterFactory;

@Activate(order = 100)
public class TagRouterFactory implements RouterFactory {

    public static final String NAME = "tag";

    @Override
    public Router getRouter(URL url) {
        return new TagRouter(url);
    }

    @Override
    public Router getRouter(DynamicConfiguration dynamicConfiguration, URL url) {
        TagRouter router = new TagRouter(dynamicConfiguration, url);
        return router;
    }
}