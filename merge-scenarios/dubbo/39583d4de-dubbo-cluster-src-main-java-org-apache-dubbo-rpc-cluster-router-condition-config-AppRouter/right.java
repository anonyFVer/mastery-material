package org.apache.dubbo.rpc.cluster.router.condition.config;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.configcenter.DynamicConfiguration;

public class AppRouter extends ListenableRouter {

    public static final String NAME = "APP_ROUTER";

    public AppRouter(DynamicConfiguration configuration, URL url) {
        super(configuration, url, url.getParameter(Constants.APPLICATION_KEY));
    }
}