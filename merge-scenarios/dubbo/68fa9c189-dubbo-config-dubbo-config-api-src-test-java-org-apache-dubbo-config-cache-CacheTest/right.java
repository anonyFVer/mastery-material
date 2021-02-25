package org.apache.dubbo.config.cache;

import org.apache.dubbo.cache.Cache;
import org.apache.dubbo.cache.CacheFactory;
import org.apache.dubbo.cache.support.threadlocal.ThreadLocalCache;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.MethodConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.RpcInvocation;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CacheTest extends TestCase {

    @Before
    public void setUp() {
        ConfigManager.getInstance().clear();
    }

    @After
    public void tearDown() {
        ConfigManager.getInstance().clear();
    }

    private void testCache(String type) throws Exception {
        ApplicationConfig applicationConfig = new ApplicationConfig("cache-test");
        RegistryConfig registryConfig = new RegistryConfig("N/A");
        ProtocolConfig protocolConfig = new ProtocolConfig("injvm");
        ServiceConfig<CacheService> service = new ServiceConfig<CacheService>();
        service.setApplication(applicationConfig);
        service.setRegistry(registryConfig);
        service.setProtocol(protocolConfig);
        service.setInterface(CacheService.class.getName());
        service.setRef(new CacheServiceImpl());
        service.export();
        try {
            ReferenceConfig<CacheService> reference = new ReferenceConfig<CacheService>();
            reference.setApplication(applicationConfig);
            reference.setInterface(CacheService.class);
            reference.setUrl("injvm://127.0.0.1?scope=remote&cache=true");
            MethodConfig method = new MethodConfig();
            method.setName("findCache");
            method.setCache(type);
            reference.setMethods(Arrays.asList(method));
            CacheService cacheService = reference.get();
            try {
                String fix = null;
                for (int i = 0; i < 3; i++) {
                    String result = cacheService.findCache("0");
                    assertTrue(fix == null || fix.equals(result));
                    fix = result;
                    Thread.sleep(100);
                }
                if ("lru".equals(type)) {
                    for (int n = 0; n < 1001; n++) {
                        String pre = null;
                        for (int i = 0; i < 10; i++) {
                            String result = cacheService.findCache(String.valueOf(n));
                            assertTrue(pre == null || pre.equals(result));
                            pre = result;
                        }
                    }
                    String result = cacheService.findCache("0");
                    assertFalse(fix == null || fix.equals(result));
                }
            } finally {
                reference.destroy();
            }
        } finally {
            service.unexport();
        }
    }

    @Test
    public void testCacheLru() throws Exception {
        testCache("lru");
    }

    @Test
    public void testCacheThreadlocal() throws Exception {
        testCache("threadlocal");
    }

    @Test
    public void testCacheProvider() throws Exception {
        CacheFactory cacheFactory = ExtensionLoader.getExtensionLoader(CacheFactory.class).getAdaptiveExtension();
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("findCache.cache", "threadlocal");
        URL url = new URL("dubbo", "127.0.0.1", 29582, "org.apache.dubbo.config.cache.CacheService", parameters);
        Invocation invocation = new RpcInvocation("findCache", new Class[] { String.class }, new String[] { "0" }, null, null);
        Cache cache = cacheFactory.getCache(url, invocation);
        assertTrue(cache instanceof ThreadLocalCache);
    }
}