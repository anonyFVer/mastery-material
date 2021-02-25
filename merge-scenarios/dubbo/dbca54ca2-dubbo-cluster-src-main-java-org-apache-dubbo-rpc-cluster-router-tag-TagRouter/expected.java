package org.apache.dubbo.rpc.cluster.router.tag;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.governance.ConfigChangeEvent;
import org.apache.dubbo.governance.ConfigChangeType;
import org.apache.dubbo.governance.ConfigurationListener;
import org.apache.dubbo.governance.DynamicConfiguration;
import org.apache.dubbo.governance.DynamicConfigurationFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.router.AbstractRouter;
import org.apache.dubbo.rpc.cluster.router.TreeNode;
import org.apache.dubbo.rpc.cluster.router.tag.model.TagRouterRule;
import org.apache.dubbo.rpc.cluster.router.tag.model.TagRuleParser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TagRouter extends AbstractRouter implements Comparable<Router>, ConfigurationListener {

    public static final String NAME = "TAG_ROUTER";

    private static final int DEFAULT_PRIORITY = 100;

    private static final Logger logger = LoggerFactory.getLogger(TagRouter.class);

    private static final String TAGROUTERRULES_DATAID = ".tagrouters";

    private DynamicConfiguration configuration;

    private TagRouterRule tagRouterRule;

    private String application;

    private AtomicBoolean isInited = new AtomicBoolean(false);

    public TagRouter(URL url) {
        this(ExtensionLoader.getExtensionLoader(DynamicConfigurationFactory.class).getAdaptiveExtension().getDynamicConfiguration(url), url);
    }

    public TagRouter(DynamicConfiguration configuration, URL url) {
        setConfiguration(configuration);
        this.url = url;
    }

    protected TagRouter() {
    }

    public void setConfiguration(DynamicConfiguration configuration) {
        this.configuration = configuration;
    }

    private void init() {
        if (!isInited.compareAndSet(false, true)) {
            return;
        }
        if (StringUtils.isEmpty(application)) {
            logger.error("TagRouter must getConfig from or subscribe to a specific application, but the application in this TagRouter is not specified.");
        }
        try {
            String rawRule = this.configuration.getConfig(application + TAGROUTERRULES_DATAID, this);
            if (StringUtils.isNotEmpty(rawRule)) {
                this.tagRouterRule = TagRuleParser.parse(rawRule);
            }
        } catch (Exception e) {
            logger.error("Failed to parse the raw tag router rule and it will not take effect, please check if the rule matches with the template, the raw rule is:\n ", e);
        }
    }

    @Override
    public void process(ConfigChangeEvent event) {
        try {
            if (event.getChangeType().equals(ConfigChangeType.DELETED)) {
                this.tagRouterRule = null;
            } else {
                this.tagRouterRule = TagRuleParser.parse(event.getNewValue());
            }
            routerChain.notifyRuleChanged();
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
        checkAndInit(invokers.get(0).getUrl());
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
            }
            result = filterInvoker(invokers, invoker -> tag.equals(invoker.getUrl().getParameter(Constants.TAG_KEY)));
            if (CollectionUtils.isNotEmpty(result) || Boolean.valueOf(invocation.getAttachment(Constants.FORCE_USE_TAG, url.getParameter(Constants.FORCE_USE_TAG, "false")))) {
                return result;
            } else {
                return filterInvoker(invokers, invoker -> StringUtils.isEmpty(invoker.getUrl().getParameter(Constants.TAG_KEY)));
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
        Map<String, List<Invoker<T>>> map = new HashMap<>();
        if (CollectionUtils.isEmpty(invokers)) {
            return map;
        }
        checkAndInit(invokers.get(0).getUrl());
        if (tagRouterRule == null || !tagRouterRule.isValid() || !tagRouterRule.isEnabled()) {
            invokers.forEach(invoker -> {
                String tag = invoker.getUrl().getParameter(Constants.TAG_KEY);
                if (StringUtils.isEmpty(tag)) {
                    tag = TreeNode.FAILOVER_KEY;
                }
                List<Invoker<T>> subInvokers = map.computeIfAbsent(tag, t -> new ArrayList<>());
                subInvokers.add(invoker);
            });
            return map;
        }
        invokers.forEach(invoker -> {
            String address = invoker.getUrl().getAddress();
            List<String> tags = tagRouterRule.getAddressToTagnames().get(address);
            if (CollectionUtils.isEmpty(tags)) {
                String tag = invoker.getUrl().getParameter(Constants.TAG_KEY);
                if (tagRouterRule.getTagNames().contains(tag) || StringUtils.isEmpty(tag)) {
                    tag = TreeNode.FAILOVER_KEY;
                }
                tags = new ArrayList<>();
                tags.add(tag);
            }
            tags.forEach(tag -> {
                List<Invoker<T>> subInvokers = map.computeIfAbsent(tag, k -> new ArrayList<>());
                subInvokers.add(invoker);
            });
        });
        return map;
    }

    public void checkAndInit(URL providerUrl) {
        if (StringUtils.isEmpty(application)) {
            setApplication(providerUrl.getParameter(Constants.REMOTE_APPLICATION_KEY));
        }
        this.init();
    }

    @Override
    public String getName() {
        return NAME;
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
    public String getKey() {
        return Constants.TAG_KEY;
    }

    @Override
    public boolean isForce() {
        return tagRouterRule != null && tagRouterRule.isForce();
    }

    private <T> List<Invoker<T>> filterInvoker(List<Invoker<T>> invokers, Predicate<Invoker<T>> predicate) {
        return invokers.stream().filter(predicate).collect(Collectors.toList());
    }

    private boolean addressMatches(URL url, List<String> addresses) {
        return addresses.contains(url.getAddress());
    }

    private boolean addressNotMatches(URL url, List<String> addresses) {
        return !addresses.contains(url.getAddress());
    }

    public void setApplication(String app) {
        this.application = app;
    }
}