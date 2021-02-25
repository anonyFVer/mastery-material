package org.apache.dubbo.rpc.cluster.router.tag;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.configcenter.ConfigChangeEvent;
import org.apache.dubbo.configcenter.ConfigChangeType;
import org.apache.dubbo.configcenter.ConfigurationListener;
import org.apache.dubbo.configcenter.DynamicConfiguration;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.RouterChain;
import org.apache.dubbo.rpc.cluster.router.AbstractRouter;
import org.apache.dubbo.rpc.cluster.router.tag.model.TagRouterRule;
import org.apache.dubbo.rpc.cluster.router.tag.model.TagRuleParser;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TagRouter extends AbstractRouter implements Comparable<Router>, ConfigurationListener {

    public static final String NAME = "TAG_ROUTER";

    private static final int DEFAULT_PRIORITY = 100;

    private static final Logger logger = LoggerFactory.getLogger(TagRouter.class);

    private static final String TAGROUTERRULES_DATAID = ".tagrouters";

    private TagRouterRule tagRouterRule;

    private String application;

    private boolean inited = false;

    public TagRouter(DynamicConfiguration configuration, URL url) {
        super(configuration, url);
    }

    protected TagRouter() {
    }

    @Override
    public synchronized void process(ConfigChangeEvent event) {
        if (logger.isDebugEnabled()) {
            logger.debug("Notification of tag rule, change type is: " + event.getChangeType() + ", raw rule is:\n " + event.getValue());
        }
        try {
            if (event.getChangeType().equals(ConfigChangeType.DELETED)) {
                this.tagRouterRule = null;
            } else {
                this.tagRouterRule = TagRuleParser.parse(event.getValue());
            }
            routerChains.forEach(RouterChain::notifyRuleChanged);
        } catch (Exception e) {
            logger.error("Failed to parse the raw tag router rule and it will not take effect, please check if the rule matches with the template, the raw rule is:\n ", e);
        }
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        if (CollectionUtils.isEmpty(invokers)) {
            return invokers;
        }
        if (tagRouterRule == null || !tagRouterRule.isValid() || !tagRouterRule.isEnabled()) {
            return invokers;
        }
        List<Invoker<T>> result = invokers;
        String tag = StringUtils.isEmpty(invocation.getAttachment(Constants.TAG_KEY)) ? url.getParameter(Constants.TAG_KEY) : invocation.getAttachment(Constants.TAG_KEY);
        if (StringUtils.isNotEmpty(tag)) {
            List<String> addresses = tagRouterRule.getTagnameToAddresses().get(tag);
            if (CollectionUtils.isNotEmpty(addresses)) {
                result = filterInvoker(invokers, invoker -> addressMatches(invoker.getUrl(), addresses));
                if (CollectionUtils.isNotEmpty(result) || tagRouterRule.isForce()) {
                    return result;
                }
            } else {
                result = filterInvoker(invokers, invoker -> tag.equals(invoker.getUrl().getParameter(Constants.TAG_KEY)));
            }
            if (CollectionUtils.isNotEmpty(result) || Boolean.valueOf(invocation.getAttachment(Constants.FORCE_USE_TAG, url.getParameter(Constants.FORCE_USE_TAG, "false")))) {
                return result;
            } else {
                List<Invoker<T>> tmp = filterInvoker(invokers, invoker -> addressNotMatches(invoker.getUrl(), tagRouterRule.getAddresses()));
                return filterInvoker(tmp, invoker -> StringUtils.isEmpty(invoker.getUrl().getParameter(Constants.TAG_KEY)));
            }
        } else {
            List<String> addresses = tagRouterRule.getAddresses();
            if (CollectionUtils.isNotEmpty(addresses)) {
                result = filterInvoker(invokers, invoker -> addressNotMatches(invoker.getUrl(), addresses));
                if (CollectionUtils.isEmpty(result)) {
                    return result;
                }
            }
            return filterInvoker(result, invoker -> {
                String localTag = invoker.getUrl().getParameter(Constants.TAG_KEY);
                if (StringUtils.isEmpty(localTag) || !tagRouterRule.getTagNames().contains(localTag)) {
                    return true;
                }
                return false;
            });
        }
    }

    @Override
    public <T> Map<String, List<Invoker<T>>> preRoute(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        if (CollectionUtils.isNotEmpty(invokers)) {
            checkAndInit(invokers.get(0).getUrl());
        }
        return super.preRoute(invokers, url, invocation);
    }

    @Override
    public int getPriority() {
        return DEFAULT_PRIORITY;
    }

    @Override
    public boolean isRuntime() {
        return tagRouterRule != null && tagRouterRule.isRuntime();
    }

    @Override
    public boolean isForce() {
        return tagRouterRule != null && tagRouterRule.isForce();
    }

    private <T> List<Invoker<T>> filterInvoker(List<Invoker<T>> invokers, Predicate<Invoker<T>> predicate) {
        return invokers.stream().filter(predicate).collect(Collectors.toList());
    }

    private boolean addressMatches(URL url, List<String> addresses) {
        return addresses != null && addresses.contains(url.getAddress());
    }

    private boolean addressNotMatches(URL url, List<String> addresses) {
        return addresses == null || !addresses.contains(url.getAddress());
    }

    public void setApplication(String app) {
        this.application = app;
    }

    private synchronized void checkAndInit(URL url) {
        String providerApplication = url.getParameter(Constants.REMOTE_APPLICATION_KEY);
        if (StringUtils.isEmpty(application) || !providerApplication.equals(application)) {
            setApplication(providerApplication);
            inited = false;
        }
        if (StringUtils.isEmpty(application)) {
            logger.error("TagRouter must getConfig from or subscribe to a specific application, but the application in this TagRouter is not specified.");
            return;
        }
        if (!inited) {
            inited = true;
            String key = application + TAGROUTERRULES_DATAID;
            configuration.addListener(key, this);
            String rawRule = configuration.getConfig(key);
            if (rawRule != null) {
                this.process(new ConfigChangeEvent(key, rawRule));
            }
        }
    }
}