package org.apache.dubbo.rpc.cluster.router;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.configcenter.DynamicConfiguration;
import org.apache.dubbo.rpc.cluster.Router;

public abstract class AbstractRouter implements Router {

    protected int priority;

    protected boolean force = false;

    protected URL url;

    protected DynamicConfiguration configuration;

    public AbstractRouter(DynamicConfiguration configuration, URL url) {
        this.configuration = configuration;
        this.url = url;
    }

    public AbstractRouter() {
    }

    @Override
    public URL getUrl() {
        return url;
    }

    public void setUrl(URL url) {
        this.url = url;
    }

    public void setConfiguration(DynamicConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public boolean isRuntime() {
        return true;
    }

    @Override
    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    @Override
    public int compareTo(Router o) {
        return (this.getPriority() >= o.getPriority()) ? 1 : -1;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }
}